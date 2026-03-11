package pg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/atlekbai/schema_registry/internal/hrest"
	hrqlpg "github.com/atlekbai/schema_registry/internal/hrql/dialect/pg"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// exactCountThreshold is the planner estimate below which we run an exact count.
const exactCountThreshold = 50_000

// parsePlanRows extracts the estimated row count from EXPLAIN JSON output.
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

	estimated := parsePlanRows(planJSON)

	if estimated <= exactCountThreshold {
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

// PGListQuerier executes REST-style list queries against PostgreSQL.
type PGListQuerier struct {
	pool  *pgxpool.Pool
	cache *schema.Cache
}

// NewPGListQuerier creates a new PGListQuerier.
func NewPGListQuerier(pool *pgxpool.Pool, cache *schema.Cache) *PGListQuerier {
	return &PGListQuerier{pool: pool, cache: cache}
}

// ListRecords executes a REST-style list query for the given object.
func (q *PGListQuerier) ListRecords(ctx context.Context, objectName string, params *hrest.Params) (hrest.ListResult, error) {
	obj := q.cache.Get(objectName)
	if obj == nil {
		return hrest.ListResult{}, fmt.Errorf("object %q not in cache", objectName)
	}

	qp := &QueryParams{Params: *params}

	if len(qp.Conditions) > 0 {
		sqlConds, err := hrqlpg.TranslateConditions(qp.Conditions, obj, q.cache)
		if err != nil {
			return hrest.ListResult{}, fmt.Errorf("translate conditions: %w", err)
		}
		qp.SQLConditions = sqlConds
	}

	qp.ExpandPlans = ResolveExpands(qp.Expand, obj, q.cache)

	builder := NewBuilder(obj)
	g, gctx := errgroup.WithContext(ctx)

	var totalCount int64
	if !params.SkipCount {
		g.Go(func() error {
			var err error
			totalCount, err = ResolveCount(gctx, q.pool, builder, qp)
			return err
		})
	}

	var rows []json.RawMessage
	g.Go(func() error {
		sqlStr, args, err := builder.BuildList(qp)
		if err != nil {
			return err
		}
		dbRows, err := q.pool.Query(gctx, sqlStr, args...)
		if err != nil {
			return err
		}
		defer dbRows.Close()
		rows, err = hrqlpg.ScanJSONRows(dbRows)
		return err
	})

	if err := g.Wait(); err != nil {
		return hrest.ListResult{}, fmt.Errorf("query failed: %w", err)
	}

	if len(rows) > qp.Limit {
		rows = rows[:qp.Limit]
	}

	return hrest.ListResult{
		ObjectAPIName: objectName,
		TotalCount:    totalCount,
		Rows:          rows,
	}, nil
}
