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
	"google.golang.org/protobuf/types/known/structpb"

	registryv1 "github.com/atlekbai/schema_registry/gen/registry/v1"
	registryv1connect "github.com/atlekbai/schema_registry/gen/registry/v1/registryv1connect"
	"github.com/atlekbai/schema_registry/internal/hrql"
	hrqlpg "github.com/atlekbai/schema_registry/internal/hrql/pg"
	"github.com/atlekbai/schema_registry/internal/schema"
)

type RegistryService struct {
	pool   *pgxpool.Pool
	cache  *schema.Cache
	engine *hrql.Engine
	q      hrql.Queryable
}

func NewRegistryService(pool *pgxpool.Pool, cache *schema.Cache, engine *hrql.Engine, q hrql.Queryable) *RegistryService {
	return &RegistryService{pool: pool, cache: cache, engine: engine, q: q}
}

func (s *RegistryService) RegisterHandler(interceptors ...connect.Interceptor) (string, http.Handler) {
	return registryv1connect.NewRegistryServiceHandler(s, connect.WithInterceptors(interceptors...))
}

func (s *RegistryService) List(ctx context.Context, req *connect.Request[registryv1.ListRequest]) (*connect.Response[registryv1.ListResponse], error) {
	msg := req.Msg
	if obj := s.cache.Get(msg.ObjectName); obj == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("no object registered with api_name %q", msg.ObjectName))
	}

	val, err := s.engine.List(ctx, s.q, msg.ObjectName, hrql.QueryOpts{
		Select:  msg.Select,
		Expand:  msg.Expand,
		Order:   msg.Order,
		Limit:   msg.Limit,
		Cursor:  msg.Cursor,
		Filters: msg.Filters,
	})
	if err != nil {
		code := connect.CodeInternal
		if hrql.IsInputError(err) {
			code = connect.CodeInvalidArgument
		}
		return nil, connect.NewError(code, err)
	}

	list, ok := val.(hrql.List)
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unexpected result type %T", val))
	}
	resp := &registryv1.ListResponse{
		TotalCount: list.TotalCount,
		NextCursor: list.NextCursor,
	}

	resp.Results, err = rowsToStructs(list.Rows)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
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

	params, err := hrqlpg.ParseParams(obj, hrqlpg.ParamsInput{
		Select: msg.Select,
		Expand: msg.Expand,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	params.ExpandPlans = hrqlpg.ResolveExpands(params.Expand, obj, s.cache)
	builder := hrqlpg.NewBuilder(obj)

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

	records, err := rowsToStructs([]json.RawMessage{data})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&registryv1.GetResponse{Record: records[0]}), nil
}

func rowsToStructs(rows []json.RawMessage) ([]*structpb.Struct, error) {
	out := make([]*structpb.Struct, len(rows))
	for i, row := range rows {
		var m map[string]any
		if err := json.Unmarshal(row, &m); err != nil {
			return nil, fmt.Errorf("marshal result: %w", err)
		}
		st, err := structpb.NewStruct(m)
		if err != nil {
			return nil, fmt.Errorf("marshal result: %w", err)
		}
		out[i] = st
	}
	return out, nil
}
