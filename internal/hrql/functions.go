package hrql

import (
	"fmt"

	"github.com/atlekbai/schema_registry/internal/hrql/parser"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// sourceCall compiles a function at source position into a Plan.
type sourceCall func(c *Compiler, fn *parser.FuncCall) (*Plan, error)

// pipeCall applies a function in pipe position to an existing Plan.
type pipeCall func(c *Compiler, plan *Plan, fn *parser.FuncCall) (*Plan, error)

// sourceCalls maps function names to their source-position compilers.
var sourceCalls = map[string]sourceCall{
	"chain":      (*Compiler).compileChain,
	"reports":    (*Compiler).compileReports,
	"peers":      (*Compiler).compilePeers,
	"colleagues": (*Compiler).compileColleagues,
	"reports_to": (*Compiler).compileReportsTo,
}

// pipeCalls maps function names to their pipe-position handlers.
var pipeCalls = map[string]pipeCall{
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
func (c *Compiler) compileFuncCall(fn *parser.FuncCall) (*Plan, error) {
	call, ok := sourceCalls[fn.Name]
	if !ok {
		return nil, fmt.Errorf("unknown function %q", fn.Name)
	}
	return call(c, fn)
}

func (c *Compiler) applyFuncInPipe(plan *Plan, fn *parser.FuncCall) (*Plan, error) {
	call, ok := pipeCalls[fn.Name]
	if !ok {
		return nil, fmt.Errorf("function %q is not supported in pipe position", fn.Name)
	}
	return call(c, plan, fn)
}

// --- Source function implementations ---

// requireEmployeesObj ensures the employees object is in the cache
// and sets currentObj to it. All org functions produce employee results.
func (c *Compiler) requireEmployeesObj(funcName string) (*schema.ObjectDef, error) {
	obj := c.cache.Get("employees")
	if obj == nil {
		return nil, fmt.Errorf("%s() requires employees object in schema cache", funcName)
	}
	c.currentObj = obj
	return obj, nil
}

func (c *Compiler) compileChain(fn *parser.FuncCall) (*Plan, error) {
	if _, err := c.requireEmployeesObj("chain"); err != nil {
		return nil, err
	}

	ref, err := c.resolveEmployeeArg(fn.Args[0])
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

	var cond Condition
	if depth == 0 {
		cond = OrgChainAll{Emp: ref}
	} else {
		cond = OrgChainUp{Emp: ref, Steps: depth}
	}

	return &Plan{Kind: PlanList, ObjectAPIName: "employees", Conditions: []Condition{cond}}, nil
}

func (c *Compiler) compileReports(fn *parser.FuncCall) (*Plan, error) {
	if _, err := c.requireEmployeesObj("reports"); err != nil {
		return nil, err
	}

	ref, err := c.resolveEmployeeArg(fn.Args[0])
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

	var cond Condition
	if depth == 0 {
		cond = OrgSubtree{Emp: ref}
	} else {
		cond = OrgChainDown{Emp: ref, Depth: depth}
	}

	return &Plan{Kind: PlanList, ObjectAPIName: "employees", Conditions: []Condition{cond}}, nil
}

func (c *Compiler) compilePeers(fn *parser.FuncCall) (*Plan, error) {
	if _, err := c.requireEmployeesObj("peers"); err != nil {
		return nil, err
	}

	ref, err := c.resolveEmployeeArg(fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("peers arg 1: %w", err)
	}

	return &Plan{
		Kind:          PlanList,
		ObjectAPIName: "employees",
		Conditions:    []Condition{SameFieldCond{Field: "manager", Emp: ref}},
	}, nil
}

func (c *Compiler) compileColleagues(fn *parser.FuncCall) (*Plan, error) {
	empObj, err := c.requireEmployeesObj("colleagues")
	if err != nil {
		return nil, err
	}

	ref, err := c.resolveEmployeeArg(fn.Args[0])
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
	if _, ok := empObj.FieldsByAPIName[fieldName]; !ok {
		return nil, fmt.Errorf("colleagues arg 2: unknown field %q on employees", fieldName)
	}

	return &Plan{
		Kind:          PlanList,
		ObjectAPIName: "employees",
		Conditions:    []Condition{SameFieldCond{Field: fieldName, Emp: ref}},
	}, nil
}

func (c *Compiler) compileReportsTo(fn *parser.FuncCall) (*Plan, error) {
	if _, err := c.requireEmployeesObj("reports_to"); err != nil {
		return nil, err
	}

	empRef, err := c.resolveEmployeeArg(fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("reports_to arg 1: %w", err)
	}

	tgtRef, err := c.resolveEmployeeArg(fn.Args[1])
	if err != nil {
		return nil, fmt.Errorf("reports_to arg 2: %w", err)
	}

	return &Plan{
		Kind:          PlanScalar,
		ObjectAPIName: "employees",
		ScalarExpr:    ScalarBool{Cond: ReportsToCheck{Emp: empRef, Target: tgtRef}},
	}, nil
}

// --- Pipe function implementations ---

func pipeStringOpError(_ *Compiler, _ *Plan, fn *parser.FuncCall) (*Plan, error) {
	return nil, fmt.Errorf("%s() is only supported inside where() conditions", fn.Name)
}

func pipePassthrough(_ *Compiler, plan *Plan, _ *parser.FuncCall) (*Plan, error) {
	return plan, nil
}

func pipeLength(_ *Compiler, plan *Plan, _ *parser.FuncCall) (*Plan, error) {
	plan.Kind = PlanScalar
	plan.AggFunc = "count"
	return plan, nil
}
