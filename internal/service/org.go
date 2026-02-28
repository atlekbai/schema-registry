package service

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/structpb"

	registryv1 "github.com/atlekbai/schema_registry/gen/registry/v1"
	"github.com/atlekbai/schema_registry/gen/registry/v1/registryv1connect"
	"github.com/atlekbai/schema_registry/internal/hrql"
	hrqlpg "github.com/atlekbai/schema_registry/internal/hrql/pg"
	"github.com/atlekbai/schema_registry/internal/hrql/parser"
	"github.com/atlekbai/schema_registry/internal/schema"
)

type OrgService struct {
	pool  *pgxpool.Pool
	cache *schema.Cache
}

func NewOrgService(pool *pgxpool.Pool, cache *schema.Cache) *OrgService {
	return &OrgService{pool: pool, cache: cache}
}

func (s *OrgService) RegisterHandler(interceptors ...connect.Interceptor) (string, http.Handler) {
	return registryv1connect.NewOrgServiceHandler(s, connect.WithInterceptors(interceptors...))
}

func (s *OrgService) Query(ctx context.Context, req *connect.Request[registryv1.QueryRequest]) (*connect.Response[registryv1.QueryResponse], error) {
	msg := req.Msg

	// Parse HRQL expression.
	ast, err := parser.Parse(msg.Query)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Compile AST to a storage-agnostic Plan.
	resolver := hrqlpg.NewResolver(s.pool, s.cache)
	compiler := hrql.NewCompiler(s.cache, resolver, msg.SelfId)
	plan, err := compiler.Compile(ctx, ast)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	switch plan.Kind {
	case hrql.PlanList:
		return s.runHRQLList(ctx, plan, msg)
	case hrql.PlanScalar:
		return s.runScalar(ctx, plan)
	case hrql.PlanBoolean:
		return connect.NewResponse(&registryv1.QueryResponse{ReportsTo: plan.BoolResult}), nil
	default:
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unknown plan kind %v", plan.Kind))
	}
}

// runHRQLList executes a list-producing HRQL plan.
func (s *OrgService) runHRQLList(ctx context.Context, plan *hrql.Plan, msg *registryv1.QueryRequest) (*connect.Response[registryv1.QueryResponse], error) {
	obj, err := s.employeesObj()
	if err != nil {
		return nil, err
	}

	// Translate plan to SQL.
	sqlResult, err := hrqlpg.Translate(plan, obj, s.cache)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("translate plan: %w", err))
	}

	input := listInputFromMsg(msg)

	// Apply plan-determined ordering/limit overrides.
	if sqlResult.OrderBy != nil {
		input.Order = sqlResult.OrderBy.FieldAPIName
		if sqlResult.OrderBy.Desc {
			input.Order += ".desc"
		}
	}
	if sqlResult.Limit > 0 && input.Limit == 0 {
		input.Limit = int32(sqlResult.Limit)
	}

	params, err := hrqlpg.ParseParams(obj, input)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Merge HRQL plan conditions with REST conditions.
	params.Conditions = append(params.Conditions, plan.Conditions...)
	params.SQLConditions, err = hrqlpg.TranslateConditions(params.Conditions, obj, s.cache)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	params.ExpandPlans = hrqlpg.ResolveExpands(params.Expand, obj, s.cache)

	builder := hrqlpg.NewBuilder(obj)
	g, gctx := errgroup.WithContext(ctx)

	var totalCount int64
	g.Go(func() error {
		var err error
		totalCount, err = s.resolveCount(gctx, builder, params)
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

	resp := &registryv1.QueryResponse{TotalCount: totalCount}

	if len(rows) > params.Limit {
		rows = rows[:params.Limit]
		last := rows[params.Limit-1]
		encoded := hrqlpg.EncodeCursor(last.CursorID, last.CursorVal)
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

// runScalar executes a scalar-producing HRQL plan (aggregation).
func (s *OrgService) runScalar(ctx context.Context, plan *hrql.Plan) (*connect.Response[registryv1.QueryResponse], error) {
	obj, err := s.employeesObj()
	if err != nil {
		return nil, err
	}

	sqlResult, err := hrqlpg.Translate(plan, obj, s.cache)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("translate plan: %w", err))
	}

	var rawResult *string
	if err := s.pool.QueryRow(ctx, sqlResult.AggSQL, sqlResult.AggArgs...).Scan(&rawResult); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("aggregate query: %w", err))
	}

	var scalar float64
	if rawResult != nil {
		scalar, err = strconv.ParseFloat(*rawResult, 64)
		if err != nil {
			n, err2 := strconv.ParseInt(*rawResult, 10, 64)
			if err2 != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("parse aggregate result %q: %w", *rawResult, err))
			}
			scalar = float64(n)
		}
	}

	return connect.NewResponse(&registryv1.QueryResponse{Scalar: &scalar}), nil
}

// -- helpers --

func listInputFromMsg(msg *registryv1.QueryRequest) hrqlpg.ParamsInput {
	return hrqlpg.ParamsInput{
		Select: msg.Select,
		Expand: msg.Expand,
		Order:  msg.Order,
		Limit:  msg.Limit,
		Cursor: msg.Cursor,
	}
}

func (s *OrgService) employeesObj() (*schema.ObjectDef, error) {
	obj := s.cache.Get("employees")
	if obj == nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("employees object not in cache"))
	}
	return obj, nil
}

func (s *OrgService) resolveCount(ctx context.Context, builder hrqlpg.Builder, params *hrqlpg.QueryParams) (int64, error) {
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
