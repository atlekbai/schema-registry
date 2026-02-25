package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/atlekbai/schema_registry/internal/query"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// exactCountThreshold is the planner estimate below which we run an exact count.
// Above this, the EXPLAIN estimate is returned directly.
const exactCountThreshold = 50_000

type Handler struct {
	pool  *pgxpool.Pool
	cache *schema.Cache
}

func New(pool *pgxpool.Pool, cache *schema.Cache) *Handler {
	return &Handler{pool: pool, cache: cache}
}

// jsonRow holds a single result row as raw JSON plus cursor extraction columns.
type jsonRow struct {
	Data      json.RawMessage
	CursorID  string
	CursorVal string
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

	query.ResolveExpands(params, obj, h.cache)

	builder := query.NewBuilder(obj)

	g, ctx := errgroup.WithContext(r.Context())

	var totalCount int64
	g.Go(func() error {
		var err error
		totalCount, err = h.resolveCount(ctx, builder, obj, params)
		return err
	})

	var results []jsonRow
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
		results, err = scanJSONRows(rows, params.Order != nil)
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
		encoded := query.EncodeCursor(last.CursorID, last.CursorVal)
		nextCursor = &encoded
	}

	writeJSONList(w, totalCount, nextCursor, results)
}

// Count handles GET /api/{object}/count — always returns exact count.
func (h *Handler) Count(w http.ResponseWriter, r *http.Request) {
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

	builder := query.NewBuilder(obj)
	countSQL, countArgs, err := builder.BuildCount(obj, params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to build query", err.Error())
		return
	}

	var count int64
	err = h.pool.QueryRow(r.Context(), countSQL, countArgs...).Scan(&count)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Query failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]int64{"count": count})
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

	var data json.RawMessage
	err = h.pool.QueryRow(r.Context(), sqlStr, args...).Scan(&data)
	if err == pgx.ErrNoRows {
		writeError(w, http.StatusNotFound, "RECORD_NOT_FOUND", "Record not found", "")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Query failed", err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
	w.Write([]byte{'\n'})
}

// resolveCount uses the EXPLAIN trick for cheap estimation on large tables,
// falling back to exact count only when the planner estimate is small.
func (h *Handler) resolveCount(ctx context.Context, builder query.Builder, obj *schema.ObjectDef, params *query.QueryParams) (int64, error) {
	// Step 1: Get planner estimate (always fast, ~1ms — no data touched)
	estSQL, estArgs, err := builder.BuildEstimate(obj, params)
	if err != nil {
		return 0, err
	}

	var planJSON string
	err = h.pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+estSQL, estArgs...).Scan(&planJSON)
	if err != nil {
		return 0, fmt.Errorf("explain estimate: %w", err)
	}

	estimated := parsePlanRows(planJSON)

	// Step 2: If the estimate is small, run exact count
	if estimated <= exactCountThreshold {
		countSQL, countArgs, err := builder.BuildCount(obj, params)
		if err != nil {
			return estimated, nil
		}

		var count int64
		if err := h.pool.QueryRow(ctx, countSQL, countArgs...).Scan(&count); err != nil {
			return estimated, nil
		}

		return count, nil
	}

	return estimated, nil
}

// parsePlanRows extracts "Plan Rows" from EXPLAIN (FORMAT JSON) output.
func parsePlanRows(planJSON string) int64 {
	var plan []struct {
		Plan struct {
			PlanRows float64 `json:"Plan Rows"`
		} `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil || len(plan) == 0 {
		return 0
	}
	return int64(plan[0].Plan.PlanRows)
}

// scanJSONRows scans rows where the first column is a JSON object (_row),
// the second is the cursor ID, and optionally the third is the cursor order value.
func scanJSONRows(rows pgx.Rows, hasOrderVal bool) ([]jsonRow, error) {
	var results []jsonRow
	for rows.Next() {
		var r jsonRow
		var err error
		if hasOrderVal {
			err = rows.Scan(&r.Data, &r.CursorID, &r.CursorVal)
		} else {
			err = rows.Scan(&r.Data, &r.CursorID)
		}
		if err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// writeJSONList writes the list response, streaming raw JSON rows without re-marshaling.
func writeJSONList(w http.ResponseWriter, totalCount int64, nextCursor *string, rows []jsonRow) {
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(`{"total_count":%d`, totalCount))
	if nextCursor != nil {
		buf.WriteString(`,"next_cursor":`)
		enc, _ := json.Marshal(*nextCursor)
		buf.Write(enc)
	}
	buf.WriteString(`,"results":[`)
	for i, r := range rows {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(r.Data)
	}
	buf.WriteString("]}\n")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(buf.Bytes())
}
