package hrql

import (
	"fmt"
	"strconv"

	"github.com/atlekbai/schema_registry/internal/hrql/parser"
)

// --- Argument resolution helpers ---

// resolveEmployeeArg resolves a function argument to an unresolved EmployeeRef.
// No I/O — the pg backend resolves to SQL at translation time.
func (c *Compiler) resolveEmployeeArg(arg parser.Node) (EmployeeRef, error) {
	switch a := arg.(type) {
	case *parser.SelfExpr:
		if c.selfID == "" {
			return EmployeeRef{}, fmt.Errorf("`self` requires self_id in the request")
		}
		return EmployeeRef{ID: c.selfID}, nil
	case *parser.DotExpr:
		return EmployeeRef{}, fmt.Errorf("'.' cannot be resolved to an employee ID outside of where subqueries")
	case *parser.PipeExpr:
		// self.manager → EmployeeRef with chain.
		if len(a.Steps) == 2 {
			if _, ok := a.Steps[0].(*parser.SelfExpr); ok {
				if fa, ok := a.Steps[1].(*parser.FieldAccess); ok {
					if c.selfID == "" {
						return EmployeeRef{}, fmt.Errorf("`self` requires self_id in the request")
					}
					if len(fa.Chain) == 0 {
						return EmployeeRef{}, fmt.Errorf("empty field access")
					}
					// Validate all fields in the chain exist.
					for _, fieldName := range fa.Chain {
						if _, ok := c.empObj.FieldsByAPIName[fieldName]; !ok {
							return EmployeeRef{}, fmt.Errorf("unknown field %q", fieldName)
						}
					}
					return EmployeeRef{ID: c.selfID, Chain: fa.Chain}, nil
				}
			}
		}
		return EmployeeRef{}, fmt.Errorf("cannot resolve complex pipe expression to employee ID")
	case *parser.IdentExpr:
		return EmployeeRef{ID: a.Name}, nil
	case *parser.Literal:
		if a.Kind == parser.TokString {
			return EmployeeRef{ID: a.Value}, nil
		}
		return EmployeeRef{}, fmt.Errorf("expected employee reference, got %s", a.Kind)
	default:
		return EmployeeRef{}, fmt.Errorf("cannot resolve %T to employee ID", arg)
	}
}

func (c *Compiler) resolveIntArg(arg parser.Node) (int, error) {
	switch a := arg.(type) {
	case *parser.Literal:
		if a.Kind != parser.TokNumber {
			return 0, fmt.Errorf("expected number, got %s", a.Kind)
		}
		n, err := strconv.Atoi(a.Value)
		if err != nil {
			return 0, fmt.Errorf("invalid integer %q: %w", a.Value, err)
		}
		return n, nil
	case *parser.UnaryMinus:
		inner, err := c.resolveIntArg(a.Expr)
		if err != nil {
			return 0, err
		}
		return -inner, nil
	default:
		return 0, fmt.Errorf("expected integer literal, got %T", arg)
	}
}
