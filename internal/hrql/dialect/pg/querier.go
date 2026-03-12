package pg

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// PGQueryable implements hrql.Queryable for PostgreSQL.
type PGQueryable struct {
	pool  *pgxpool.Pool
	cache *schema.Cache
}

// NewPGQueryable creates a new PGQueryable.
func NewPGQueryable(pool *pgxpool.Pool, cache *schema.Cache) *PGQueryable {
	return &PGQueryable{pool: pool, cache: cache}
}

// Querier creates a PGQuerier.
func (pq *PGQueryable) Querier(ctx context.Context) (hrql.Querier, error) {
	return &PGQuerier{pool: pq.pool, cache: pq.cache}, nil
}

// PGQuerier implements hrql.Querier using pgxpool.
type PGQuerier struct {
	pool  *pgxpool.Pool
	cache *schema.Cache
}

// Execute dispatches to the appropriate executor based on plan kind.
func (q *PGQuerier) Execute(ctx context.Context, plan *hrql.Plan) (hrql.Value, error) {
	obj := q.cache.Get(plan.ObjectAPIName)
	if obj == nil {
		return nil, fmt.Errorf("object %q not in cache", plan.ObjectAPIName)
	}

	switch plan.Kind {
	case hrql.PlanList:
		return q.execList(ctx, plan, obj)
	case hrql.PlanScalar:
		return q.execScalar(ctx, plan, obj)
	default:
		return nil, fmt.Errorf("unknown plan kind %v", plan.Kind)
	}
}

// Close is a no-op (pgxpool is shared, not owned by the querier).
func (q *PGQuerier) Close() error { return nil }

// execList executes a simple HRQL list query — no expand, no select filtering, no count.
func (q *PGQuerier) execList(ctx context.Context, plan *hrql.Plan, obj *schema.ObjectDef) (hrql.Value, error) {
	sqlConds, err := TranslateConditions(plan.Conditions, obj, q.cache)
	if err != nil {
		return nil, fmt.Errorf("translate conditions: %w", err)
	}

	from, baseWhere := TableSource(obj, Alias())
	qb := PG.Select().Column(sq.Alias(jsonbBuildObject(obj), "_row")).From(from)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}
	for _, cond := range sqlConds {
		qb = qb.Where(cond)
	}

	qb = AddOrderBy(qb, obj, plan.OrderBy)

	qb = qb.Limit(uint64(plan.Limit))

	sqlStr, args, err := qb.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build list query: %w", err)
	}

	dbRows, err := q.pool.Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer dbRows.Close()

	rows, err := ScanJSONRows(dbRows)
	if err != nil {
		return nil, fmt.Errorf("scan rows: %w", err)
	}

	return hrql.List{
		ObjectAPIName: plan.ObjectAPIName,
		TotalCount:    int64(len(rows)),
		Rows:          rows,
	}, nil
}

// jsonbBuildObject builds a jsonb_build_object(...) Sqlizer for all fields (no select/expand filtering).
func jsonbBuildObject(obj *schema.ObjectDef) sq.Sqlizer {
	var (
		alias = Alias()
		pairs []string
	)

	pairs = append(pairs,
		fmt.Sprintf(`'id', %s."id"`, QI(alias)),
		fmt.Sprintf(`'created_at', %s."created_at"`, QI(alias)),
		fmt.Sprintf(`'updated_at', %s."updated_at"`, QI(alias)),
	)

	for i := range obj.Fields {
		f := &obj.Fields[i]
		if IsSystemField(f.APIName) {
			continue
		}
		pairs = append(pairs, fmt.Sprintf(`%s, %s`, QuoteLit(JSONKey(f)), SelectExpr(alias, f)))
	}

	return sq.Expr("jsonb_build_object(" + strings.Join(pairs, ", ") + ")")
}

func (q *PGQuerier) execScalar(ctx context.Context, plan *hrql.Plan, obj *schema.ObjectDef) (hrql.Value, error) {
	sqlResult, err := Translate(plan, obj, q.cache)
	if err != nil {
		return nil, fmt.Errorf("translate plan: %w", err)
	}

	sqlStr, args, err := PG.Select().Column(sqlResult.Scalar).ToSql()
	if err != nil {
		return nil, fmt.Errorf("build scalar query: %w", err)
	}

	var rawResult *string
	if err := q.pool.QueryRow(ctx, sqlStr, args...).Scan(&rawResult); err != nil {
		return nil, fmt.Errorf("aggregate query: %w", err)
	}

	return hrql.Scalar{ObjectAPIName: plan.ObjectAPIName, Value: rawResult}, nil
}

// Ensure PGQueryable implements hrql.Queryable at compile time.
var _ hrql.Queryable = (*PGQueryable)(nil)
