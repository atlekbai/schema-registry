package hrql

import (
	"context"
	"fmt"

	"github.com/atlekbai/schema_registry/internal/hrql/parser"
)

// SourceCall compiles a function at source position into a Plan.
type SourceCall func(c *Compiler, ctx context.Context, fn *parser.FuncCall) (*Plan, error)

// PipeCall applies a function in pipe position to an existing Plan.
type PipeCall func(c *Compiler, ctx context.Context, plan *Plan, fn *parser.FuncCall) (*Plan, error)

// SourceCalls maps function names to their source-position compilers.
var SourceCalls = map[string]SourceCall{
	"chain":      (*Compiler).compileChain,
	"reports":    (*Compiler).compileReports,
	"peers":      (*Compiler).compilePeers,
	"colleagues": (*Compiler).compileColleagues,
	"reports_to": (*Compiler).compileReportsTo,
}

// PipeCalls maps function names to their pipe-position handlers.
var PipeCalls = map[string]PipeCall{
	"contains":    pipeStringOpError,
	"starts_with": pipeStringOpError,
	"ends_with":   pipeStringOpError,
	"unique":      pipePassthrough,
	"upper":       pipePassthrough,
	"lower":       pipePassthrough,
	"length":      pipeLength,
}

// --- Dispatchers ---

// compileFuncCall handles functions at source position via SourceCalls map.
func (c *Compiler) compileFuncCall(ctx context.Context, fn *parser.FuncCall) (*Plan, error) {
	call, ok := SourceCalls[fn.Name]
	if !ok {
		return nil, fmt.Errorf("unknown function %q", fn.Name)
	}
	return call(c, ctx, fn)
}

func (c *Compiler) applyFuncInPipe(ctx context.Context, plan *Plan, fn *parser.FuncCall) (*Plan, error) {
	call, ok := PipeCalls[fn.Name]
	if !ok {
		return nil, fmt.Errorf("function %q is not supported in pipe position", fn.Name)
	}
	return call(c, ctx, plan, fn)
}

// --- Source function implementations ---

func (c *Compiler) compileChain(ctx context.Context, fn *parser.FuncCall) (*Plan, error) {
	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("chain arg 1: %w", err)
	}

	depth := 0
	if len(fn.Args) == 2 {
		depth, err = c.resolveIntArg(fn.Args[1])
		if err != nil {
			return nil, fmt.Errorf("chain arg 2: %w", err)
		}
	}

	path, err := c.resolver.LookupPath(ctx, empID)
	if err != nil {
		return nil, err
	}

	var cond Condition
	if depth == 0 {
		cond = OrgChainAll{Path: path}
	} else {
		nlevel := nlevelFromPath(path)
		if depth >= nlevel {
			cond = NullFilter{}
		} else {
			cond = OrgChainUp{Path: path, Steps: depth}
		}
	}

	return &Plan{Kind: PlanList, Conditions: []Condition{cond}}, nil
}

func (c *Compiler) compileReports(ctx context.Context, fn *parser.FuncCall) (*Plan, error) {
	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("reports arg 1: %w", err)
	}

	depth := 0
	if len(fn.Args) == 2 {
		depth, err = c.resolveIntArg(fn.Args[1])
		if err != nil {
			return nil, fmt.Errorf("reports arg 2: %w", err)
		}
	}

	path, err := c.resolver.LookupPath(ctx, empID)
	if err != nil {
		return nil, err
	}

	var cond Condition
	if depth == 0 {
		cond = OrgSubtree{Path: path}
	} else {
		cond = OrgChainDown{Path: path, Depth: depth}
	}

	return &Plan{Kind: PlanList, Conditions: []Condition{cond}}, nil
}

func (c *Compiler) compilePeers(ctx context.Context, fn *parser.FuncCall) (*Plan, error) {
	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("peers arg 1: %w", err)
	}

	managerID, err := c.resolver.LookupFieldValue(ctx, empID, "manager")
	if err != nil {
		return nil, err
	}
	if managerID == "" {
		return &Plan{Kind: PlanList, Conditions: []Condition{NullFilter{}}}, nil
	}

	return &Plan{
		Kind:       PlanList,
		Conditions: []Condition{SameFieldCond{Field: "manager", Value: managerID, ExcludeID: empID}},
	}, nil
}

func (c *Compiler) compileColleagues(ctx context.Context, fn *parser.FuncCall) (*Plan, error) {
	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("colleagues arg 1: %w", err)
	}

	fa, ok := fn.Args[1].(*parser.FieldAccess)
	if !ok {
		return nil, fmt.Errorf("colleagues arg 2: expected field reference (.field), got %T", fn.Args[1])
	}
	if len(fa.Chain) != 1 {
		return nil, fmt.Errorf("colleagues arg 2: expected single field (.field), got .%s", joinChain(fa.Chain))
	}

	fieldName := fa.Chain[0]
	if _, ok := c.empObj.FieldsByAPIName[fieldName]; !ok {
		return nil, fmt.Errorf("colleagues arg 2: unknown field %q", fieldName)
	}

	value, err := c.resolver.LookupFieldValue(ctx, empID, fieldName)
	if err != nil {
		return nil, err
	}
	if value == "" {
		return &Plan{Kind: PlanList, Conditions: []Condition{NullFilter{}}}, nil
	}

	return &Plan{
		Kind:       PlanList,
		Conditions: []Condition{SameFieldCond{Field: fieldName, Value: value, ExcludeID: empID}},
	}, nil
}

func (c *Compiler) compileReportsTo(ctx context.Context, fn *parser.FuncCall) (*Plan, error) {
	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("reports_to arg 1: %w", err)
	}

	targetID, err := c.resolveEmployeeArg(ctx, fn.Args[1])
	if err != nil {
		return nil, fmt.Errorf("reports_to arg 2: %w", err)
	}

	empPath, err := c.resolver.LookupPath(ctx, empID)
	if err != nil {
		return nil, err
	}
	tgtPath, err := c.resolver.LookupPath(ctx, targetID)
	if err != nil {
		return nil, err
	}

	result := isDescendant(empPath, tgtPath)
	return &Plan{Kind: PlanBoolean, BoolResult: &result}, nil
}

// --- Pipe function implementations ---

func pipeStringOpError(_ *Compiler, _ context.Context, _ *Plan, fn *parser.FuncCall) (*Plan, error) {
	return nil, fmt.Errorf("%s() is only supported inside where() conditions", fn.Name)
}

func pipePassthrough(_ *Compiler, _ context.Context, plan *Plan, _ *parser.FuncCall) (*Plan, error) {
	return plan, nil
}

func pipeLength(_ *Compiler, _ context.Context, plan *Plan, _ *parser.FuncCall) (*Plan, error) {
	plan.Kind = PlanScalar
	plan.AggFunc = "count"
	return plan, nil
}
