package service

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/structpb"

	registryv1 "github.com/atlekbai/schema_registry/gen/registry/v1"
	"github.com/atlekbai/schema_registry/gen/registry/v1/registryv1connect"
	"github.com/atlekbai/schema_registry/internal/query"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// peersDimensions maps dimension API names to storage columns.
var peersDimensions = map[string]string{
	"manager":    "manager_id",
	"department": "department_id",
}

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
	cmd, err := parseDSL(msg.Query)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	switch cmd.Op {
	case "chain":
		return s.execChain(ctx, cmd, msg)
	case "peers":
		return s.execPeers(ctx, cmd, msg)
	case "reports":
		return s.execReports(ctx, cmd, msg)
	case "reportsto":
		return s.execReportsTo(ctx, cmd)
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown op %q", cmd.Op))
	}
}

func (s *OrgService) execChain(ctx context.Context, cmd *dslCommand, msg *registryv1.QueryRequest) (*connect.Response[registryv1.QueryResponse], error) {
	path, err := s.lookupPath(ctx, cmd.EmployeeID)
	if err != nil {
		return nil, err
	}

	var conds []sq.Sqlizer
	if cmd.Steps > 0 {
		depth := nlevelFromPath(path)
		if cmd.Steps >= depth {
			return connect.NewResponse(&registryv1.QueryResponse{}), nil
		}
		conds = append(conds, query.ChainUp(path, cmd.Steps))
	} else {
		conds = append(conds, query.ChainDown(path, -cmd.Steps))
	}

	return s.runList(ctx, conds, listInputFromMsg(msg))
}

func (s *OrgService) execPeers(ctx context.Context, cmd *dslCommand, msg *registryv1.QueryRequest) (*connect.Response[registryv1.QueryResponse], error) {
	column, ok := peersDimensions[cmd.Dimension]
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("unknown dimension %q, valid: %s", cmd.Dimension, validDimensions()))
	}

	value, err := s.lookupField(ctx, cmd.EmployeeID, column)
	if err != nil {
		return nil, err
	}
	if value == "" {
		return connect.NewResponse(&registryv1.QueryResponse{}), nil
	}

	conds := []sq.Sqlizer{query.SameField(column, value, cmd.EmployeeID)}
	return s.runList(ctx, conds, listInputFromMsg(msg))
}

func (s *OrgService) execReports(ctx context.Context, cmd *dslCommand, msg *registryv1.QueryRequest) (*connect.Response[registryv1.QueryResponse], error) {
	path, err := s.lookupPath(ctx, cmd.EmployeeID)
	if err != nil {
		return nil, err
	}

	var conds []sq.Sqlizer
	if cmd.Direct {
		conds = append(conds, query.ChainDown(path, 1))
	} else {
		conds = append(conds, query.Subtree(path))
	}

	return s.runList(ctx, conds, listInputFromMsg(msg))
}

func (s *OrgService) execReportsTo(ctx context.Context, cmd *dslCommand) (*connect.Response[registryv1.QueryResponse], error) {
	empPath, err := s.lookupPath(ctx, cmd.EmployeeID)
	if err != nil {
		return nil, err
	}
	tgtPath, err := s.lookupPath(ctx, cmd.TargetID)
	if err != nil {
		return nil, err
	}

	var result bool
	err = s.pool.QueryRow(ctx,
		`SELECT $1::ltree <@ $2::ltree AND $1::ltree != $2::ltree`,
		empPath, tgtPath,
	).Scan(&result)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&registryv1.QueryResponse{ReportsTo: &result}), nil
}

// ── helpers ──────────────────────────────────────────────────────────

func listInputFromMsg(msg *registryv1.QueryRequest) query.ParamsInput {
	return query.ParamsInput{
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

func (s *OrgService) lookupPath(ctx context.Context, id string) (string, error) {
	var path string
	err := s.pool.QueryRow(ctx,
		`SELECT "manager_path"::text FROM "core"."employees" WHERE "id" = $1`, id,
	).Scan(&path)
	if err == pgx.ErrNoRows {
		return "", connect.NewError(connect.CodeNotFound, fmt.Errorf("employee %s not found", id))
	}
	if err != nil {
		return "", connect.NewError(connect.CodeInternal, err)
	}
	return path, nil
}

func (s *OrgService) lookupField(ctx context.Context, id, column string) (string, error) {
	var value *string
	q := fmt.Sprintf(`SELECT %s::text FROM "core"."employees" WHERE "id" = $1`, schema.QuoteIdent(column))
	err := s.pool.QueryRow(ctx, q, id).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", connect.NewError(connect.CodeNotFound, fmt.Errorf("employee %s not found", id))
	}
	if err != nil {
		return "", connect.NewError(connect.CodeInternal, err)
	}
	if value == nil {
		return "", nil
	}
	return *value, nil
}

func (s *OrgService) runList(ctx context.Context, extraConds []sq.Sqlizer, input query.ParamsInput) (*connect.Response[registryv1.QueryResponse], error) {
	obj, err := s.employeesObj()
	if err != nil {
		return nil, err
	}

	params, err := query.ParseParams(obj, input)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	params.ExtraConditions = extraConds
	params.ExpandPlans = query.ResolveExpands(params.Expand, obj, s.cache)

	builder := query.NewBuilder(obj)
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

func (s *OrgService) resolveCount(ctx context.Context, builder query.Builder, params *query.QueryParams) (int64, error) {
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

func nlevelFromPath(path string) int {
	if path == "" {
		return 0
	}
	return strings.Count(path, ".") + 1
}

func validDimensions() string {
	keys := make([]string, 0, len(peersDimensions))
	for k := range peersDimensions {
		keys = append(keys, k)
	}
	return strings.Join(keys, ", ")
}
