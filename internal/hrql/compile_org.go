package hrql

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/atlekbai/schema_registry/internal/query"
)

// compileFuncCall handles org functions at source position.
func (c *Compiler) compileFuncCall(ctx context.Context, fn *FuncCall) (*Result, error) {
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

func (c *Compiler) compileChain(ctx context.Context, fn *FuncCall) (*Result, error) {
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

	var conds []sq.Sqlizer
	if depth == 0 {
		conds = append(conds, ChainAll(path))
	} else {
		nlevel := nlevelFromPath(path)
		if depth >= nlevel {
			// No ancestors that far up — return empty.
			col := fmt.Sprintf(`%s."id"`, query.QI(query.Alias()))
			conds = append(conds, sq.Eq{col: nil})
		} else {
			conds = append(conds, ChainUp(path, depth))
		}
	}

	return &Result{Kind: KindList, Conditions: conds}, nil
}

func (c *Compiler) compileReports(ctx context.Context, fn *FuncCall) (*Result, error) {
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

	var conds []sq.Sqlizer
	if depth == 0 {
		conds = append(conds, Subtree(path))
	} else {
		conds = append(conds, ChainDown(path, depth))
	}

	return &Result{Kind: KindList, Conditions: conds}, nil
}

func (c *Compiler) compilePeers(ctx context.Context, fn *FuncCall) (*Result, error) {
	if len(fn.Args) != 1 {
		return nil, fmt.Errorf("peers() requires 1 argument: peers(employee)")
	}

	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("peers arg 1: %w", err)
	}

	managerID, err := c.resolver.LookupField(ctx, empID, "manager_id")
	if err != nil {
		return nil, err
	}
	if managerID == "" {
		// Root node — no peers.
		col := fmt.Sprintf(`%s."id"`, query.QI(query.Alias()))
		return &Result{Kind: KindList, Conditions: []sq.Sqlizer{sq.Eq{col: nil}}}, nil
	}

	return &Result{
		Kind:       KindList,
		Conditions: []sq.Sqlizer{SameField("manager_id", managerID, empID)},
	}, nil
}

func (c *Compiler) compileColleagues(ctx context.Context, fn *FuncCall) (*Result, error) {
	if len(fn.Args) != 2 {
		return nil, fmt.Errorf("colleagues() requires 2 arguments: colleagues(employee, .field)")
	}

	empID, err := c.resolveEmployeeArg(ctx, fn.Args[0])
	if err != nil {
		return nil, fmt.Errorf("colleagues arg 1: %w", err)
	}

	// Second arg must be a field access like .department
	fa, ok := fn.Args[1].(*FieldAccess)
	if !ok {
		return nil, fmt.Errorf("colleagues arg 2: expected field reference (.field), got %T", fn.Args[1])
	}
	if len(fa.Chain) != 1 {
		return nil, fmt.Errorf("colleagues arg 2: expected single field (.field), got .%s", joinChain(fa.Chain))
	}

	fieldName := fa.Chain[0]
	fd, ok := c.empObj.FieldsByAPIName[fieldName]
	if !ok {
		return nil, fmt.Errorf("colleagues arg 2: unknown field %q", fieldName)
	}

	// Resolve the storage column for the field.
	var column string
	if fd.StorageColumn != nil {
		column = *fd.StorageColumn
	} else {
		return nil, fmt.Errorf("colleagues arg 2: field %q has no storage column", fieldName)
	}

	value, err := c.resolver.LookupField(ctx, empID, column)
	if err != nil {
		return nil, err
	}
	if value == "" {
		col := fmt.Sprintf(`%s."id"`, query.QI(query.Alias()))
		return &Result{Kind: KindList, Conditions: []sq.Sqlizer{sq.Eq{col: nil}}}, nil
	}

	return &Result{
		Kind:       KindList,
		Conditions: []sq.Sqlizer{SameField(column, value, empID)},
	}, nil
}

func (c *Compiler) compileReportsTo(ctx context.Context, fn *FuncCall) (*Result, error) {
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
	return &Result{Kind: KindBoolean, BoolResult: &result}, nil
}
