package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	registryv1 "github.com/atlekbai/schema_registry/gen/registry/v1"
	registryv1connect "github.com/atlekbai/schema_registry/gen/registry/v1/registryv1connect"
	"github.com/atlekbai/schema_registry/internal/hrest"
	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// ListQuerier executes list queries against the database.
type ListQuerier interface {
	ListRecords(ctx context.Context, objectName string, params *hrest.Params) (hrest.ListResult, error)
}

type RegistryService struct {
	cache *schema.Cache
	lq    ListQuerier
}

func NewRegistryService(cache *schema.Cache, lq ListQuerier) *RegistryService {
	return &RegistryService{cache: cache, lq: lq}
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

	params, err := hrest.ParseParams(obj, hrest.QueryOpts{
		Sel:     msg.Select,
		Expand:  msg.Expand,
		Order:   msg.Order,
		Limit:   msg.Limit,
		Filters: msg.Filters,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	list, err := s.lq.ListRecords(ctx, msg.ObjectName, params)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &registryv1.ListResponse{
		TotalCount: list.TotalCount,
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

	params, err := hrest.ParseParams(obj, hrest.QueryOpts{
		Sel:    msg.Select,
		Expand: msg.Expand,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	params.SkipCount = true
	params.Limit = 1
	params.Conditions = append(params.Conditions, hrql.IdentityFilter{ID: msg.Id})

	list, err := s.lq.ListRecords(ctx, msg.ObjectName, params)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if len(list.Rows) == 0 {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("record not found"))
	}

	records, err := rowsToStructs(list.Rows[:1])
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
