package hrql

import (
	"context"
	"fmt"
)

// compileFuncCall handles org functions at source position.
func (c *Compiler) compileFuncCall(ctx context.Context, fn *FuncCall) (*Plan, error) {
	switch fn.Name {
	case "chain":
		return c.compileChain(ctx, fn)
	case "reports":
		return c.compileReports(ctx, fn)
	case "peers":
		return c.compilePeers(ctx, fn)
	case "colleagues":
		return c.compileColleagues(ctx, fn)
	case "reports_to":
		return c.compileReportsTo(ctx, fn)
	default:
		return nil, fmt.Errorf("unknown function %q", fn.Name)
	}
}

func (c *Compiler) compileChain(ctx context.Context, fn *FuncCall) (*Plan, error) {
	if len(fn.Args) < 1 || len(fn.Args) > 2 {
		return nil, fmt.Errorf("chain() requires 1-2 arguments: chain(employee [, depth])")
	}

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

func (c *Compiler) compileReports(ctx context.Context, fn *FuncCall) (*Plan, error) {
	if len(fn.Args) < 1 || len(fn.Args) > 2 {
		return nil, fmt.Errorf("reports() requires 1-2 arguments: reports(employee [, depth])")
	}

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

func (c *Compiler) compilePeers(ctx context.Context, fn *FuncCall) (*Plan, error) {
	if len(fn.Args) != 1 {
		return nil, fmt.Errorf("peers() requires 1 argument: peers(employee)")
	}

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

func (c *Compiler) compileColleagues(ctx context.Context, fn *FuncCall) (*Plan, error) {
	if len(fn.Args) != 2 {
		return nil, fmt.Errorf("colleagues() requires 2 arguments: colleagues(employee, .field)")
	}

	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("colleagues arg 1: %w", err)
	}

	fa, ok := fn.Args[1].(*FieldAccess)
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

func (c *Compiler) compileReportsTo(ctx context.Context, fn *FuncCall) (*Plan, error) {
	if len(fn.Args) != 2 {
		return nil, fmt.Errorf("reports_to() requires 2 arguments: reports_to(employee, target)")
	}

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
