package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/atlekbai/schema_registry/internal/schema"
)

// Resolver implements hrql.Resolver using a PostgreSQL connection pool.
// It translates field API names to storage columns internally.
type Resolver struct {
	pool   *pgxpool.Pool
	cache  *schema.Cache
	empObj *schema.ObjectDef
}

// NewResolver creates a Resolver backed by PostgreSQL.
func NewResolver(pool *pgxpool.Pool, cache *schema.Cache) *Resolver {
	return &Resolver{pool: pool, cache: cache, empObj: cache.Get("employees")}
}

func (r *Resolver) LookupPath(ctx context.Context, id string) (string, error) {
	var path string
	err := r.pool.QueryRow(ctx,
		`SELECT "manager_path"::text FROM "core"."employees" WHERE "id" = $1`, id,
	).Scan(&path)
	if err == pgx.ErrNoRows {
		return "", fmt.Errorf("employee %s not found", id)
	}
	if err != nil {
		return "", fmt.Errorf("lookup path: %w", err)
	}
	return path, nil
}

func (r *Resolver) LookupFieldValue(ctx context.Context, id, fieldAPIName string) (string, error) {
	column := r.resolveColumn(fieldAPIName)

	var value *string
	q := fmt.Sprintf(`SELECT %s::text FROM "core"."employees" WHERE "id" = $1`, schema.QuoteIdent(column))
	err := r.pool.QueryRow(ctx, q, id).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", fmt.Errorf("employee %s not found", id)
	}
	if err != nil {
		return "", fmt.Errorf("lookup field %s: %w", fieldAPIName, err)
	}
	if value == nil {
		return "", nil
	}
	return *value, nil
}

// resolveColumn maps a field API name to its storage column.
func (r *Resolver) resolveColumn(apiName string) string {
	if r.empObj != nil {
		if fd, ok := r.empObj.FieldsByAPIName[apiName]; ok && fd.StorageColumn != nil {
			return *fd.StorageColumn
		}
	}
	// Fallback: API name == column name (for standard fields like id).
	return apiName
}
