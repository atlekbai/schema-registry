package hrql

import (
	"fmt"

	"github.com/atlekbai/schema_registry/internal/hrql/parser"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// Compiler compiles an HRQL AST into a Plan.
type Compiler struct {
	cache  *schema.Cache
	selfID string
	empObj *schema.ObjectDef
}

// NewCompiler creates a compiler for HRQL expressions.
func NewCompiler(cache *schema.Cache, selfID string) *Compiler {
	return &Compiler{
		cache:  cache,
		selfID: selfID,
		empObj: cache.Get("employees"),
	}
}

// Compile compiles an AST node into a storage-agnostic Plan.
func (c *Compiler) Compile(node parser.Node) (*Plan, error) {
	if c.empObj == nil {
		return nil, fmt.Errorf("employees object not found in schema cache")
	}
	return c.compileNode(node)
}

func (c *Compiler) compileNode(node parser.Node) (*Plan, error) {
	switch n := node.(type) {
	case *parser.PipeExpr:
		return c.compilePipe(n)
	case *parser.SelfExpr:
		return c.compileSelf()
	case *parser.IdentExpr:
		return c.compileIdent(n)
	case *parser.FuncCall:
		return c.compileFuncCall(n)
	case *parser.BinaryOp, *parser.Literal, *parser.UnaryMinus:
		expr, err := c.compileScalarExpr(node)
		if err != nil {
			return nil, err
		}
		return &Plan{Kind: PlanScalar, ScalarExpr: expr}, nil
	case *parser.FieldAccess:
		return nil, fmt.Errorf("field access requires a source (use self.field or pipe)")
	default:
		return nil, fmt.Errorf("unexpected node type %T at top level", node)
	}
}

// compilePipe walks pipe steps left-to-right, accumulating state.
func (c *Compiler) compilePipe(pipe *parser.PipeExpr) (*Plan, error) {
	if len(pipe.Steps) == 0 {
		return nil, fmt.Errorf("empty pipe expression")
	}

	plan, err := c.compileNode(pipe.Steps[0])
	if err != nil {
		return nil, err
	}

	for _, step := range pipe.Steps[1:] {
		plan, err = c.applyStep(plan, step)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}

// applyStep applies a single pipe step to the current plan.
func (c *Compiler) applyStep(plan *Plan, step parser.Node) (*Plan, error) {
	switch s := step.(type) {
	case *parser.FieldAccess:
		return c.applyFieldAccess(plan, s)
	case *parser.WhereExpr:
		return c.applyWhere(plan, s)
	case *parser.SortExpr:
		return c.applySort(plan, s)
	case *parser.PickExpr:
		return c.applyPick(plan, s)
	case *parser.AggExpr:
		return c.applyAgg(plan, s)
	case *parser.FuncCall:
		return c.applyFuncInPipe(plan, s)
	default:
		return nil, fmt.Errorf("unexpected pipe step type %T", step)
	}
}

// compileSelf: the `self` employee — filter by ID.
func (c *Compiler) compileSelf() (*Plan, error) {
	if c.selfID == "" {
		return nil, fmt.Errorf("`self` requires self_id in the request")
	}
	return &Plan{
		Kind:       PlanList,
		Conditions: []Condition{IdentityFilter{ID: c.selfID}},
		Limit:      1,
	}, nil
}

// compileIdent: `employees` → full scan.
func (c *Compiler) compileIdent(n *parser.IdentExpr) (*Plan, error) {
	switch n.Name {
	case "employees":
		return &Plan{Kind: PlanList}, nil
	default:
		return nil, fmt.Errorf("unknown identifier %q", n.Name)
	}
}

// --- Step application ---

func (c *Compiler) applyFieldAccess(plan *Plan, fa *parser.FieldAccess) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("field access requires a list, got %v", plan.Kind)
	}
	if len(fa.Chain) == 0 {
		return nil, fmt.Errorf("empty field access")
	}

	fd, ok := c.empObj.FieldsByAPIName[fa.Chain[0]]
	if !ok {
		return nil, fmt.Errorf("unknown field %q on employees", fa.Chain[0])
	}

	// For LOOKUP fields with deeper chains, tracked for service layer.
	if fd.Type == schema.FieldLookup && len(fa.Chain) > 1 {
	}

	plan.AggField = fd.APIName
	return plan, nil
}

func (c *Compiler) applyWhere(plan *Plan, w *parser.WhereExpr) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("where requires a list source")
	}

	cond, err := c.compileWhereCond(w.Cond)
	if err != nil {
		return nil, fmt.Errorf("where: %w", err)
	}

	plan.Conditions = append(plan.Conditions, cond)
	return plan, nil
}

func (c *Compiler) applySort(plan *Plan, s *parser.SortExpr) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("sort_by requires a list source")
	}
	if len(s.Field.Chain) == 0 {
		return nil, fmt.Errorf("sort_by: empty field")
	}

	fieldName := s.Field.Chain[0]
	if _, ok := c.empObj.FieldsByAPIName[fieldName]; !ok {
		return nil, fmt.Errorf("sort_by: unknown field %q", fieldName)
	}

	plan.OrderBy = &OrderBy{Field: fieldName, Desc: s.Desc}
	return plan, nil
}

func (c *Compiler) applyPick(plan *Plan, p *parser.PickExpr) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("%s requires a list source", p.Op)
	}

	plan.PickOp = p.Op
	plan.PickN = p.N

	switch p.Op {
	case "first":
		plan.Limit = 1
	case "last":
		plan.Limit = 1
		if plan.OrderBy != nil {
			plan.OrderBy.Desc = !plan.OrderBy.Desc
		} else {
			plan.OrderBy = &OrderBy{Field: "id", Desc: true}
		}
	case "nth":
		plan.Limit = 1
	}

	return plan, nil
}

func (c *Compiler) applyAgg(plan *Plan, a *parser.AggExpr) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("%s requires a list source", a.Op)
	}

	plan.Kind = PlanScalar
	plan.AggFunc = a.Op
	return plan, nil
}

// --- Arithmetic expression compilation ---

func isArithOp(op string) bool {
	return op == "+" || op == "-" || op == "*" || op == "/"
}

// compileScalarExpr compiles a node into a ScalarExpr for arithmetic contexts.
// Handles literals, unary minus, arithmetic BinaryOp, and falls back to compileNode
// for pipe expressions / function calls that produce PlanScalar.
func (c *Compiler) compileScalarExpr(node parser.Node) (ScalarExpr, error) {
	switch n := node.(type) {
	case *parser.Literal:
		if n.Kind == parser.TokNumber {
			return ScalarLiteral{Value: n.Value}, nil
		}
		return nil, fmt.Errorf("expected number in arithmetic, got %s", n.Kind)
	case *parser.UnaryMinus:
		inner, err := c.compileScalarExpr(n.Expr)
		if err != nil {
			return nil, err
		}
		if lit, ok := inner.(ScalarLiteral); ok {
			return ScalarLiteral{Value: "-" + lit.Value}, nil
		}
		return ScalarArith{Op: "-", Left: ScalarLiteral{Value: "0"}, Right: inner}, nil
	case *parser.BinaryOp:
		if isArithOp(n.Op) {
			left, err := c.compileScalarExpr(n.Left)
			if err != nil {
				return nil, err
			}
			right, err := c.compileScalarExpr(n.Right)
			if err != nil {
				return nil, err
			}
			return ScalarArith{Op: n.Op, Left: left, Right: right}, nil
		}
		return nil, fmt.Errorf("unsupported operator %q in arithmetic expression", n.Op)
	default:
		plan, err := c.compileNode(node)
		if err != nil {
			return nil, err
		}
		if plan.Kind != PlanScalar {
			return nil, fmt.Errorf("expected scalar expression, got %v", plan.Kind)
		}
		return ScalarSubquery{Plan: plan}, nil
	}
}
