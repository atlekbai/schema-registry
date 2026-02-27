package hrql

import (
	"context"
	"fmt"

	"github.com/atlekbai/schema_registry/internal/schema"
)

// Compiler compiles an HRQL AST into a Plan.
type Compiler struct {
	cache    *schema.Cache
	resolver Resolver
	selfID   string
	empObj   *schema.ObjectDef
}

// NewCompiler creates a compiler for HRQL expressions.
func NewCompiler(cache *schema.Cache, resolver Resolver, selfID string) *Compiler {
	return &Compiler{
		cache:    cache,
		resolver: resolver,
		selfID:   selfID,
		empObj:   cache.Get("employees"),
	}
}

// Compile compiles an AST node into a storage-agnostic Plan.
func (c *Compiler) Compile(ctx context.Context, node Node) (*Plan, error) {
	if c.empObj == nil {
		return nil, fmt.Errorf("employees object not found in schema cache")
	}
	return c.compileNode(ctx, node)
}

func (c *Compiler) compileNode(ctx context.Context, node Node) (*Plan, error) {
	switch n := node.(type) {
	case *PipeExpr:
		return c.compilePipe(ctx, n)
	case *SelfExpr:
		return c.compileSelf()
	case *IdentExpr:
		return c.compileIdent(n)
	case *FuncCall:
		return c.compileFuncCall(ctx, n)
	case *FieldAccess:
		return nil, fmt.Errorf("field access requires a source (use self.field or pipe)")
	default:
		return nil, fmt.Errorf("unexpected node type %T at top level", node)
	}
}

// compilePipe walks pipe steps left-to-right, accumulating state.
func (c *Compiler) compilePipe(ctx context.Context, pipe *PipeExpr) (*Plan, error) {
	if len(pipe.Steps) == 0 {
		return nil, fmt.Errorf("empty pipe expression")
	}

	plan, err := c.compileNode(ctx, pipe.Steps[0])
	if err != nil {
		return nil, err
	}

	for _, step := range pipe.Steps[1:] {
		plan, err = c.applyStep(ctx, plan, step)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}

// applyStep applies a single pipe step to the current plan.
func (c *Compiler) applyStep(ctx context.Context, plan *Plan, step Node) (*Plan, error) {
	switch s := step.(type) {
	case *FieldAccess:
		return c.applyFieldAccess(plan, s)
	case *WhereExpr:
		return c.applyWhere(ctx, plan, s)
	case *SortExpr:
		return c.applySort(plan, s)
	case *PickExpr:
		return c.applyPick(plan, s)
	case *AggExpr:
		return c.applyAgg(plan, s)
	case *FuncCall:
		return c.applyFuncInPipe(ctx, plan, s)
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
func (c *Compiler) compileIdent(n *IdentExpr) (*Plan, error) {
	switch n.Name {
	case "employees":
		return &Plan{Kind: PlanList}, nil
	default:
		return nil, fmt.Errorf("unknown identifier %q", n.Name)
	}
}

// --- Step application ---

func (c *Compiler) applyFieldAccess(plan *Plan, fa *FieldAccess) (*Plan, error) {
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

func (c *Compiler) applyWhere(ctx context.Context, plan *Plan, w *WhereExpr) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("where requires a list source")
	}

	cond, err := c.compileWhereCond(ctx, w.Cond)
	if err != nil {
		return nil, fmt.Errorf("where: %w", err)
	}

	plan.Conditions = append(plan.Conditions, cond)
	return plan, nil
}

func (c *Compiler) applySort(plan *Plan, s *SortExpr) (*Plan, error) {
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

func (c *Compiler) applyPick(plan *Plan, p *PickExpr) (*Plan, error) {
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

func (c *Compiler) applyAgg(plan *Plan, a *AggExpr) (*Plan, error) {
	if plan.Kind != PlanList {
		return nil, fmt.Errorf("%s requires a list source", a.Op)
	}

	plan.Kind = PlanScalar
	plan.AggFunc = a.Op
	return plan, nil
}

func (c *Compiler) applyFuncInPipe(_ context.Context, plan *Plan, fn *FuncCall) (*Plan, error) {
	switch fn.Name {
	case "contains", "starts_with", "ends_with":
		return nil, fmt.Errorf("%s() is only supported inside where() conditions", fn.Name)
	case "unique", "upper", "lower", "length":
		if fn.Name == "length" {
			plan.Kind = PlanScalar
			plan.AggFunc = "count"
			return plan, nil
		}
		return plan, nil
	default:
		return nil, fmt.Errorf("function %q is not supported in pipe position", fn.Name)
	}
}
