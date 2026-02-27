package hrql

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/atlekbai/schema_registry/internal/schema"
)

// Resolver abstracts DB lookups needed during HRQL compilation.
// This separates "translate AST to SQL" from "fetch runtime data",
// and enables unit testing the compiler without a database.
type Resolver interface {
	LookupPath(ctx context.Context, id string) (string, error)
	LookupField(ctx context.Context, id, column string) (string, error)
}

// PgResolver implements Resolver using a pgx connection pool.
type PgResolver struct {
	pool *pgxpool.Pool
}

// NewPgResolver creates a Resolver backed by a PostgreSQL connection pool.
func NewPgResolver(pool *pgxpool.Pool) *PgResolver {
	return &PgResolver{pool: pool}
}

func (r *PgResolver) LookupPath(ctx context.Context, id string) (string, error) {
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

func (r *PgResolver) LookupField(ctx context.Context, id, column string) (string, error) {
	var value *string
	q := fmt.Sprintf(`SELECT %s::text FROM "core"."employees" WHERE "id" = $1`, schema.QuoteIdent(column))
	err := r.pool.QueryRow(ctx, q, id).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", fmt.Errorf("employee %s not found", id)
	}
	if err != nil {
		return "", fmt.Errorf("lookup field: %w", err)
	}
	if value == nil {
		return "", nil
	}
	return *value, nil
}

// --- Argument resolution helpers ---

// resolveEmployeeArg resolves a function argument to an employee UUID string.
func (c *Compiler) resolveEmployeeArg(ctx context.Context, arg Node) (string, error) {
	switch a := arg.(type) {
	case *SelfExpr:
		if c.selfID == "" {
			return "", fmt.Errorf("`self` requires self_id in the request")
		}
		return c.selfID, nil
	case *DotExpr:
		// `.` in function args means the current pipe item — only valid in correlated contexts.
		return "", fmt.Errorf("'.' cannot be resolved to an employee ID outside of where subqueries")
	case *PipeExpr:
		// self.manager → need to resolve.
		if len(a.Steps) == 2 {
			if _, ok := a.Steps[0].(*SelfExpr); ok {
				if fa, ok := a.Steps[1].(*FieldAccess); ok {
					return c.resolveSelfLookup(ctx, fa)
				}
			}
		}
		return "", fmt.Errorf("cannot resolve complex pipe expression to employee ID")
	case *IdentExpr:
		// Could be a UUID passed directly (frontend-resolved).
		return a.Name, nil
	case *Literal:
		if a.Kind == TokString {
			return a.Value, nil
		}
		return "", fmt.Errorf("expected employee reference, got %s", a.Kind)
	default:
		return "", fmt.Errorf("cannot resolve %T to employee ID", arg)
	}
}

// resolveSelfLookup resolves self.field to a value (for LOOKUP fields, returns the FK UUID).
func (c *Compiler) resolveSelfLookup(ctx context.Context, fa *FieldAccess) (string, error) {
	if len(fa.Chain) == 0 {
		return "", fmt.Errorf("empty field access")
	}
	fieldName := fa.Chain[0]
	fd, ok := c.empObj.FieldsByAPIName[fieldName]
	if !ok {
		return "", fmt.Errorf("unknown field %q", fieldName)
	}

	var column string
	if fd.StorageColumn != nil {
		column = *fd.StorageColumn
	} else {
		return "", fmt.Errorf("field %q has no storage column", fieldName)
	}

	value, err := c.resolver.LookupField(ctx, c.selfID, column)
	if err != nil {
		return "", err
	}

	// If there are more chain segments (self.manager.manager), resolve recursively.
	if len(fa.Chain) > 1 && value != "" {
		// The value is a FK UUID — look up the next field on that record.
		return c.resolveChainedLookup(ctx, value, fa.Chain[1:])
	}

	return value, nil
}

// resolveChainedLookup resolves a chain of LOOKUP fields from a starting ID.
func (c *Compiler) resolveChainedLookup(ctx context.Context, currentID string, fields []string) (string, error) {
	for _, fieldName := range fields {
		fd, ok := c.empObj.FieldsByAPIName[fieldName]
		if !ok {
			return "", fmt.Errorf("unknown field %q", fieldName)
		}
		var column string
		if fd.StorageColumn != nil {
			column = *fd.StorageColumn
		} else {
			return "", fmt.Errorf("field %q has no storage column", fieldName)
		}

		value, err := c.resolver.LookupField(ctx, currentID, column)
		if err != nil {
			return "", err
		}
		if value == "" {
			return "", nil
		}
		currentID = value
	}
	return currentID, nil
}

func (c *Compiler) resolveIntArg(arg Node) (int, error) {
	switch a := arg.(type) {
	case *Literal:
		if a.Kind != TokNumber {
			return 0, fmt.Errorf("expected number, got %s", a.Kind)
		}
		n, err := strconv.Atoi(a.Value)
		if err != nil {
			return 0, fmt.Errorf("invalid integer %q: %w", a.Value, err)
		}
		return n, nil
	case *UnaryMinus:
		inner, err := c.resolveIntArg(a.Expr)
		if err != nil {
			return 0, err
		}
		return -inner, nil
	default:
		return 0, fmt.Errorf("expected integer literal, got %T", arg)
	}
}
