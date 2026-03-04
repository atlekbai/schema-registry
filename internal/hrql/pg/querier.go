package pg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// ExactCountThreshold is the planner estimate below which we run an exact count.
const ExactCountThreshold = 50_000

// JSONRow holds a single result row as raw JSON plus cursor extraction columns.
type JSONRow struct {
	Data      json.RawMessage
	CursorID  string
	CursorVal string
}

// ScanJSONRows scans pgx rows into JSONRow slices.
func ScanJSONRows(rows pgx.Rows, hasOrderVal bool) ([]JSONRow, error) {
	var results []JSONRow
	for rows.Next() {
		var r JSONRow
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

// ParsePlanRows extracts the estimated row count from EXPLAIN JSON output.
func ParsePlanRows(planJSON string) int64 {
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

// ResolveCount uses the EXPLAIN trick for cheap estimation on large tables,
// falling back to exact count only when the planner estimate is small.
func ResolveCount(ctx context.Context, pool *pgxpool.Pool, builder Builder, params *QueryParams) (int64, error) {
	estSQL, estArgs, err := builder.BuildEstimate(params)
	if err != nil {
		return 0, err
	}

	var planJSON string
	err = pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+estSQL, estArgs...).Scan(&planJSON)
	if err != nil {
		return 0, fmt.Errorf("explain estimate: %w", err)
	}

	estimated := ParsePlanRows(planJSON)

	if estimated <= ExactCountThreshold {
		countSQL, countArgs, err := builder.BuildCount(params)
		if err != nil {
			return estimated, nil
		}
		var count int64
		if err := pool.QueryRow(ctx, countSQL, countArgs...).Scan(&count); err != nil {
			return estimated, nil
		}
		return count, nil
	}

	return estimated, nil
}

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
func (q *PGQuerier) Execute(ctx context.Context, plan *hrql.Plan, opts hrql.QueryOpts) (hrql.Value, error) {
	obj := q.cache.Get(plan.ObjectAPIName)
	if obj == nil {
		return nil, fmt.Errorf("object %q not in cache", plan.ObjectAPIName)
	}

	switch plan.Kind {
	case hrql.PlanList:
		return q.execList(ctx, plan, obj, opts)
	case hrql.PlanScalar:
		return q.execScalar(ctx, plan, obj)
	case hrql.PlanBoolean:
		return q.execBoolean(ctx, plan, obj)
	default:
		return nil, fmt.Errorf("unknown plan kind %v", plan.Kind)
	}
}

// Close is a no-op (pgxpool is shared, not owned by the querier).
func (q *PGQuerier) Close() error { return nil }

func (q *PGQuerier) execList(ctx context.Context, plan *hrql.Plan, obj *schema.ObjectDef, opts hrql.QueryOpts) (hrql.Value, error) {
	sqlResult, err := Translate(plan, obj, q.cache)
	if err != nil {
		return nil, fmt.Errorf("translate plan: %w", err)
	}

	input := ParamsInput{
		Select:  opts.Select,
		Expand:  opts.Expand,
		Order:   opts.Order,
		Limit:   opts.Limit,
		Cursor:  opts.Cursor,
		Filters: opts.Filters,
	}

	if sqlResult.OrderBy != nil {
		input.Order = sqlResult.OrderBy.FieldAPIName
		if sqlResult.OrderBy.Desc {
			input.Order += ".desc"
		}
	}
	if sqlResult.Limit > 0 && input.Limit == 0 {
		input.Limit = int32(sqlResult.Limit)
	}

	params, err := ParseParams(obj, input)
	if err != nil {
		return nil, err
	}

	// Merge plan conditions (from HRQL) with filter conditions (from REST params).
	params.SQLConditions = sqlResult.Conditions
	if len(params.Conditions) > 0 {
		filterSQL, err := TranslateConditions(params.Conditions, obj, q.cache)
		if err != nil {
			return nil, fmt.Errorf("translate filter conditions: %w", err)
		}
		params.SQLConditions = append(params.SQLConditions, filterSQL...)
	}

	params.ExpandPlans = ResolveExpands(params.Expand, obj, q.cache)

	builder := NewBuilder(obj)
	g, gctx := errgroup.WithContext(ctx)

	var totalCount int64
	g.Go(func() error {
		var err error
		totalCount, err = ResolveCount(gctx, q.pool, builder, params)
		return err
	})

	var rows []JSONRow
	g.Go(func() error {
		sqlStr, args, err := builder.BuildList(params)
		if err != nil {
			return err
		}
		dbRows, err := q.pool.Query(gctx, sqlStr, args...)
		if err != nil {
			return err
		}
		defer dbRows.Close()
		rows, err = ScanJSONRows(dbRows, params.Order != nil)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	result := hrql.List{
		ObjectAPIName: plan.ObjectAPIName,
		TotalCount:    totalCount,
	}

	if len(rows) > params.Limit {
		rows = rows[:params.Limit]
		last := rows[params.Limit-1]
		encoded := EncodeCursor(last.CursorID, last.CursorVal)
		result.NextCursor = &encoded
	}

	result.Rows = make([]json.RawMessage, len(rows))
	for i, r := range rows {
		result.Rows[i] = r.Data
	}

	return result, nil
}

func (q *PGQuerier) execScalar(ctx context.Context, plan *hrql.Plan, obj *schema.ObjectDef) (hrql.Value, error) {
	sqlResult, err := Translate(plan, obj, q.cache)
	if err != nil {
		return nil, fmt.Errorf("translate plan: %w", err)
	}

	var rawResult *string
	if err := q.pool.QueryRow(ctx, sqlResult.AggSQL, sqlResult.AggArgs...).Scan(&rawResult); err != nil {
		return nil, fmt.Errorf("aggregate query: %w", err)
	}

	return hrql.Scalar{ObjectAPIName: plan.ObjectAPIName, Value: rawResult}, nil
}

func (q *PGQuerier) execBoolean(ctx context.Context, plan *hrql.Plan, obj *schema.ObjectDef) (hrql.Value, error) {
	sql, args, err := TranslateBooleanPlan(plan, obj)
	if err != nil {
		return nil, fmt.Errorf("translate boolean plan: %w", err)
	}

	var result *bool
	if err := q.pool.QueryRow(ctx, sql, args...).Scan(&result); err != nil {
		return nil, fmt.Errorf("boolean query: %w", err)
	}

	return hrql.Boolean{ObjectAPIName: plan.ObjectAPIName, Value: result}, nil
}

// Ensure PGQueryable implements hrql.Queryable at compile time.
var _ hrql.Queryable = (*PGQueryable)(nil)
