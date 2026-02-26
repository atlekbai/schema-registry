package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/structpb"

	registryv1 "github.com/atlekbai/schema_registry/gen/registry/v1"
	registryv1connect "github.com/atlekbai/schema_registry/gen/registry/v1/registryv1connect"
	"github.com/atlekbai/schema_registry/internal/query"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// exactCountThreshold is the planner estimate below which we run an exact count.
const exactCountThreshold = 50_000

type RegistryService struct {
	pool  *pgxpool.Pool
	cache *schema.Cache
}

func NewRegistryService(pool *pgxpool.Pool, cache *schema.Cache) *RegistryService {
	return &RegistryService{pool: pool, cache: cache}
}

func (s *RegistryService) RegisterHandler(interceptors ...connect.Interceptor) (string, http.Handler) {
	return registryv1connect.NewRegistryServiceHandler(s, connect.WithInterceptors(interceptors...))
}

func (s *RegistryService) List(ctx context.Context, req *connect.Request[registryv1.ListRequest]) (*connect.Response[registryv1.ListResponse], error) {
	msg := req.Msg
	obj := s.cache.Get(msg.ObjectName)
	if obj == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("no object registered with api_name %q", msg.ObjectName))
	}

	params, err := query.ParseParams(obj, query.ParamsInput{
		Select:  msg.Select,
		Expand:  msg.Expand,
		Order:   msg.Order,
		Limit:   msg.Limit,
		Cursor:  msg.Cursor,
		Filters: msg.Filters,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	params.ExpandPlans = query.ResolveExpands(params.Expand, obj, s.cache)
	builder := query.NewBuilder(obj)

	g, gctx := errgroup.WithContext(ctx)

	var totalCount int64
	g.Go(func() error {
		var err error
		totalCount, err = s.resolveCount(gctx, builder, obj, params)
		return err
	})

	var rows []jsonRow
	g.Go(func() error {
		sqlStr, args, err := builder.BuildList(params)
		if err != nil {
			return err
		}
		dbRows, err := s.pool.Query(gctx, sqlStr, args...)
		if err != nil {
			return err
		}
		defer dbRows.Close()
		rows, err = scanJSONRows(dbRows, params.Order != nil)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("query failed: %w", err))
	}

	resp := &registryv1.ListResponse{
		TotalCount: totalCount,
	}

	// Pagination: if we got limit+1 rows, there's a next page.
	if len(rows) > params.Limit {
		rows = rows[:params.Limit]
		last := rows[params.Limit-1]
		encoded := query.EncodeCursor(last.CursorID, last.CursorVal)
		resp.NextCursor = &encoded
	}

	resp.Results = make([]*structpb.Struct, len(rows))
	for i, r := range rows {
		st, err := rawJSONToStruct(r.Data)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("marshal result: %w", err))
		}
		resp.Results[i] = st
	}

	return connect.NewResponse(resp), nil
}

func (s *RegistryService) Get(ctx context.Context, req *connect.Request[registryv1.GetRequest]) (*connect.Response[registryv1.GetResponse], error) {
	msg := req.Msg
	obj := s.cache.Get(msg.ObjectName)
	if obj == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("no object registered with api_name %q", msg.ObjectName))
	}

	id, err := uuid.Parse(msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid ID format: %w", err))
	}

	params, err := query.ParseParams(obj, query.ParamsInput{
		Select: msg.Select,
		Expand: msg.Expand,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	params.ExpandPlans = query.ResolveExpands(params.Expand, obj, s.cache)
	builder := query.NewBuilder(obj)

	sqlStr, args, err := builder.BuildGetByID(id, params)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("build query: %w", err))
	}

	var data json.RawMessage
	err = s.pool.QueryRow(ctx, sqlStr, args...).Scan(&data)
	if err == pgx.ErrNoRows {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("record not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("query failed: %w", err))
	}

	record, err := rawJSONToStruct(data)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("marshal result: %w", err))
	}

	return connect.NewResponse(&registryv1.GetResponse{Record: record}), nil
}

// resolveCount uses the EXPLAIN trick for cheap estimation on large tables,
// falling back to exact count only when the planner estimate is small.
func (s *RegistryService) resolveCount(ctx context.Context, builder query.Builder, obj *schema.ObjectDef, params *query.QueryParams) (int64, error) {
	estSQL, estArgs, err := builder.BuildEstimate(params)
	if err != nil {
		return 0, err
	}

	var planJSON string
	err = s.pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+estSQL, estArgs...).Scan(&planJSON)
	if err != nil {
		return 0, fmt.Errorf("explain estimate: %w", err)
	}

	estimated := parsePlanRows(planJSON)

	if estimated <= exactCountThreshold {
		countSQL, countArgs, err := builder.BuildCount(params)
		if err != nil {
			return estimated, nil
		}
		var count int64
		if err := s.pool.QueryRow(ctx, countSQL, countArgs...).Scan(&count); err != nil {
			return estimated, nil
		}
		return count, nil
	}

	return estimated, nil
}

// jsonRow holds a single result row as raw JSON plus cursor extraction columns.
type jsonRow struct {
	Data      json.RawMessage
	CursorID  string
	CursorVal string
}

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

func rawJSONToStruct(data json.RawMessage) (*structpb.Struct, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return structpb.NewStruct(m)
}
