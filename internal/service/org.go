package service

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"

	registryv1 "github.com/atlekbai/schema_registry/gen/registry/v1"
	"github.com/atlekbai/schema_registry/gen/registry/v1/registryv1connect"
	"github.com/atlekbai/schema_registry/internal/hrql"
)

type OrgService struct {
	engine *hrql.Engine
	q      hrql.Queryable
}

func NewOrgService(engine *hrql.Engine, q hrql.Queryable) *OrgService {
	return &OrgService{engine: engine, q: q}
}

func (s *OrgService) RegisterHandler(interceptors ...connect.Interceptor) (string, http.Handler) {
	return registryv1connect.NewOrgServiceHandler(s, connect.WithInterceptors(interceptors...))
}

func (s *OrgService) Query(ctx context.Context, req *connect.Request[registryv1.QueryRequest]) (*connect.Response[registryv1.QueryResponse], error) {
	msg := req.Msg

	val, err := s.engine.Query(ctx, s.q, hrql.QueryRequest{
		Query:      msg.Query,
		SelfID:     msg.SelfId,
		SelfObject: msg.SelfObject,
	})
	if err != nil {
		code := connect.CodeInternal
		if hrql.IsInputError(err) {
			code = connect.CodeInvalidArgument
		}
		return nil, connect.NewError(code, err)
	}

	resp := &registryv1.QueryResponse{}
	switch v := val.(type) {
	case hrql.List:
		resp.ResultObject = v.ObjectAPIName
		resp.TotalCount = v.TotalCount
		resp.Results, err = rowsToStructs(v.Rows)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	case hrql.Scalar:
		resp.ResultObject = v.ObjectAPIName
		resp.Scalar = v.Value
		if v.Value != nil && (*v.Value == "true" || *v.Value == "false") {
			b := *v.Value == "true"
			resp.ReportsTo = &b
		}
	default:
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unexpected result type %T", v))
	}

	return connect.NewResponse(resp), nil
}
