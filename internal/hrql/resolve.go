package hrql

import (
	"context"
	"fmt"
	"strconv"
)

// Resolver abstracts DB lookups needed during HRQL compilation.
// Implementations resolve domain-level identifiers (API names),
// not storage-level details (column names, SQL).
type Resolver interface {
	LookupPath(ctx context.Context, id string) (string, error)
	LookupFieldValue(ctx context.Context, id, fieldAPIName string) (string, error)
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
	if _, ok := c.empObj.FieldsByAPIName[fieldName]; !ok {
		return "", fmt.Errorf("unknown field %q", fieldName)
	}

	value, err := c.resolver.LookupFieldValue(ctx, c.selfID, fieldName)
	if err != nil {
		return "", err
	}

	// If there are more chain segments (self.manager.manager), resolve recursively.
	if len(fa.Chain) > 1 && value != "" {
		return c.resolveChainedLookup(ctx, value, fa.Chain[1:])
	}

	return value, nil
}

// resolveChainedLookup resolves a chain of LOOKUP fields from a starting ID.
func (c *Compiler) resolveChainedLookup(ctx context.Context, currentID string, fields []string) (string, error) {
	for _, fieldName := range fields {
		if _, ok := c.empObj.FieldsByAPIName[fieldName]; !ok {
			return "", fmt.Errorf("unknown field %q", fieldName)
		}

		value, err := c.resolver.LookupFieldValue(ctx, currentID, fieldName)
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
