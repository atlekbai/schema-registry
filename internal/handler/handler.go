package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/atlekbai/schema_registry/internal/query"
	"github.com/atlekbai/schema_registry/internal/schema"
)

type Handler struct {
	pool  *pgxpool.Pool
	cache *schema.Cache
}

func New(pool *pgxpool.Pool, cache *schema.Cache) *Handler {
	return &Handler{pool: pool, cache: cache}
}

// List handles GET /api/{object}
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	objectName := mux.Vars(r)["object"]
	obj := h.cache.Get(objectName)
	if obj == nil {
		writeError(w, http.StatusNotFound, "OBJECT_NOT_FOUND",
			"Object not found",
			"No object registered with api_name '"+objectName+"'")
		return
	}

	params, err := query.ParseQueryParams(r, obj)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAM", err.Error(), "")
		return
	}

	// Resolve expand plans for lateral joins
	query.ResolveExpands(params, obj, h.cache)

	builder := query.NewBuilder(obj)

	// Run count and list queries concurrently.
	g, ctx := errgroup.WithContext(r.Context())

	var totalCount int64
	g.Go(func() error {
		countSQL, countArgs, err := builder.BuildCount(obj, params)
		if err != nil {
			return err
		}
		return h.pool.QueryRow(ctx, countSQL, countArgs...).Scan(&totalCount)
	})

	var results []map[string]any
	g.Go(func() error {
		sqlStr, args, err := builder.BuildList(obj, params)
		if err != nil {
			return err
		}
		rows, err := h.pool.Query(ctx, sqlStr, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		results, err = scanRows(rows, obj)
		return err
	})

	if err := g.Wait(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Query failed", err.Error())
		return
	}

	// Pagination: if we got limit+1 rows, there's a next page
	var nextCursor *string
	if len(results) > params.Limit {
		results = results[:params.Limit]
		last := results[params.Limit-1]
		if id, ok := last["id"].(string); ok {
			var orderVal string
			if params.Order != nil {
				if v := last[params.Order.FieldAPIName]; v != nil {
					orderVal = fmt.Sprint(v)
				}
			}
			encoded := query.EncodeCursor(id, orderVal)
			nextCursor = &encoded
		}
	}

	// Flatten "data" keys for custom-target expanded fields
	flattenCustomExpands(results, params.ExpandPlans)

	writeJSON(w, http.StatusOK, ListResponse{
		TotalCount: totalCount,
		NextCursor: nextCursor,
		Results:    results,
	})
}

// GetByID handles GET /api/{object}/{id}
func (h *Handler) GetByID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	objectName := vars["object"]
	obj := h.cache.Get(objectName)
	if obj == nil {
		writeError(w, http.StatusNotFound, "OBJECT_NOT_FOUND",
			"Object not found",
			"No object registered with api_name '"+objectName+"'")
		return
	}

	id, err := uuid.Parse(vars["id"])
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAM", "Invalid ID format", err.Error())
		return
	}

	params, err := query.ParseQueryParams(r, obj)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_PARAM", err.Error(), "")
		return
	}

	query.ResolveExpands(params, obj, h.cache)

	builder := query.NewBuilder(obj)
	sqlStr, args, err := builder.BuildGetByID(obj, id, params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to build query", err.Error())
		return
	}

	rows, err := h.pool.Query(r.Context(), sqlStr, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Query failed", err.Error())
		return
	}
	defer rows.Close()

	results, err := scanRows(rows, obj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to scan results", err.Error())
		return
	}

	if len(results) == 0 {
		writeError(w, http.StatusNotFound, "RECORD_NOT_FOUND", "Record not found", "")
		return
	}

	flattenCustomExpands(results, params.ExpandPlans)

	writeJSON(w, http.StatusOK, results[0])
}

// scanRows dynamically scans query results into maps.
func scanRows(rows pgx.Rows, obj *schema.ObjectDef) ([]map[string]any, error) {
	descs := rows.FieldDescriptions()
	var results []map[string]any

	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}

		row := make(map[string]any, len(vals))

		if !obj.IsStandard {
			// Custom object: id, timestamps, data, then expanded columns
			for i, desc := range descs {
				name := string(desc.Name)
				if name == "data" {
					if data, ok := vals[i].(map[string]any); ok {
						for k, v := range data {
							row[k] = v
						}
					}
				} else {
					row[name] = formatValue(vals[i])
				}
			}
		} else {
			// Standard object: columns are aliased to api_names
			for i, desc := range descs {
				row[string(desc.Name)] = formatValue(vals[i])
			}
		}

		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// formatValue converts pgx types to JSON-friendly values.
func formatValue(v any) any {
	switch val := v.(type) {
	case time.Time:
		return val.UTC().Format(time.RFC3339)
	case pgtype.UUID:
		if val.Valid {
			id, _ := uuid.FromBytes(val.Bytes[:])
			return id.String()
		}
		return nil
	case [16]byte:
		id, _ := uuid.FromBytes(val[:])
		return id.String()
	case []byte:
		var m any
		if json.Unmarshal(val, &m) == nil {
			return m
		}
		return string(val)
	default:
		return v
	}
}

// flattenCustomExpands merges the "data" key from custom-target expanded objects
// into the parent map, so the API returns flat objects instead of nested {data: ...}.
func flattenCustomExpands(results []map[string]any, expands []query.ExpandPlan) {
	for _, ep := range expands {
		for _, row := range results {
			obj, ok := row[ep.FieldName].(map[string]any)
			if !ok || obj == nil {
				continue
			}
			if !ep.Target.IsStandard {
				if data, ok := obj["data"].(map[string]any); ok {
					delete(obj, "data")
					for k, v := range data {
						obj[k] = v
					}
				}
			}
			// Handle nested level-2 expansions
			if len(ep.Children) > 0 {
				flattenCustomExpands([]map[string]any{obj}, ep.Children)
			}
		}
	}
}
