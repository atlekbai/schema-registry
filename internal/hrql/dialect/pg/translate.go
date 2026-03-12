package pg

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// SQLResult is the output of translating a Plan into SQL-ready components.
type SQLResult struct {
	Conditions []sq.Sqlizer

	// For PlanScalar: the aggregate expression as a composable Sqlizer.
	// Caller serializes via sq.Select().Column(Scalar).PlaceholderFormat(sq.Dollar).ToSql().
	Scalar sq.Sqlizer
}

// Translate converts a storage-agnostic Plan into SQL-ready components.
func Translate(plan *hrql.Plan, obj *schema.ObjectDef, cache *schema.Cache) (*SQLResult, error) {
	result := &SQLResult{}

	// Translate conditions.
	for _, c := range plan.Conditions {
		sqlCond, err := conditionToSQL(c, obj, cache)
		if err != nil {
			return nil, err
		}
		result.Conditions = append(result.Conditions, sqlCond)
	}

	// For scalar plans, build the aggregate expression as a composable Sqlizer.
	if plan.Kind == hrql.PlanScalar {
		if plan.ScalarExpr != nil {
			s, err := scalarExprToSQL(plan.ScalarExpr, obj, cache)
			if err != nil {
				return nil, fmt.Errorf("build scalar: %w", err)
			}
			result.Scalar = s
		} else {
			result.Scalar = sq.Expr("(?)", buildAggregateBuilder(obj, plan.AggFunc, plan.AggField, result.Conditions))
		}
	}

	return result, nil
}

// TranslateConditions converts a slice of storage-agnostic Conditions to SQL expressions.
func TranslateConditions(conds []hrql.Condition, obj *schema.ObjectDef, cache *schema.Cache) ([]sq.Sqlizer, error) {
	var result []sq.Sqlizer
	for _, c := range conds {
		sql, err := conditionToSQL(c, obj, cache)
		if err != nil {
			return nil, err
		}
		result = append(result, sql)
	}
	return result, nil
}

// conditionToSQL translates a single Condition to a Squirrel SQL expression.
func conditionToSQL(c hrql.Condition, obj *schema.ObjectDef, cache *schema.Cache) (sq.Sqlizer, error) {
	switch c := c.(type) {
	case hrql.IdentityFilter:
		col := idCol()
		return sq.Eq{col: c.ID}, nil

	case hrql.NullFilter:
		return nullCondition(), nil

	case hrql.FieldCmp:
		return fieldCmpToSQL(c, obj, cache)

	case hrql.FieldCmpRef:
		return fieldCmprefToSQL(c, obj, cache)

	case hrql.StringMatch:
		return stringMatchToSQL(c, obj)

	case hrql.AndCond:
		left, err := conditionToSQL(c.Left, obj, cache)
		if err != nil {
			return nil, err
		}
		right, err := conditionToSQL(c.Right, obj, cache)
		if err != nil {
			return nil, err
		}
		return sq.And{left, right}, nil

	case hrql.OrCond:
		left, err := conditionToSQL(c.Left, obj, cache)
		if err != nil {
			return nil, err
		}
		right, err := conditionToSQL(c.Right, obj, cache)
		if err != nil {
			return nil, err
		}
		return sq.Or{left, right}, nil

	case hrql.OrgChainUp:
		return chainUp(c.Emp, c.Steps, obj, cache), nil

	case hrql.OrgChainDown:
		return chainDown(c.Emp, c.Depth, obj, cache), nil

	case hrql.OrgChainAll:
		return chainAll(c.Emp, obj, cache), nil

	case hrql.OrgSubtree:
		return subtree(c.Emp, obj, cache), nil

	case hrql.SameFieldCond:
		return sameField(c.Field, c.Emp, obj, cache), nil

	case hrql.ReportsTo:
		return reportsToWhere(c.Target, obj, cache), nil

	case hrql.ReportsToCheck:
		return reportsToCheck(c.Emp, c.Target, obj, cache), nil

	case hrql.SubqueryAgg:
		return subqueryAggToSQL(c, obj)

	case hrql.InFilter:
		col, err := resolveFilterCol(c.Field[0], obj)
		if err != nil {
			return nil, err
		}
		return sq.Eq{col: c.Values}, nil

	case hrql.IsNullFilter:
		col, err := resolveFilterCol(c.Field[0], obj)
		if err != nil {
			return nil, err
		}
		if c.IsNull {
			return sq.Eq{col: nil}, nil
		}
		return sq.NotEq{col: nil}, nil

	case hrql.LikeFilter:
		col, err := resolveFilterCol(c.Field[0], obj)
		if err != nil {
			return nil, err
		}
		if c.CaseInsensitive {
			return sq.ILike{col: c.Pattern}, nil
		}
		return sq.Like{col: c.Pattern}, nil

	default:
		return nil, fmt.Errorf("unknown condition type %T", c)
	}
}

// fieldCmpToSQL translates a FieldCmp to SQL.
func fieldCmpToSQL(c hrql.FieldCmp, obj *schema.ObjectDef, cache *schema.Cache) (sq.Sqlizer, error) {
	if len(c.Field) == 1 {
		col, err := resolveFilterCol(c.Field[0], obj)
		if err != nil {
			return nil, err
		}
		return comparisonExpr(col, c.Op, c.Value), nil
	}

	// Lookup chain: .department.title == "Eng"
	return lookupChainToSQL(c, obj, cache)
}

// fieldCmpRefToSQL translates a FieldCmpRef (field vs EmployeeRef subquery) to SQL.
func fieldCmprefToSQL(c hrql.FieldCmpRef, obj *schema.ObjectDef, cache *schema.Cache) (sq.Sqlizer, error) {
	if len(c.Field) == 0 {
		return nil, fmt.Errorf("empty field in FieldCmpRef")
	}

	col, err := resolveFilterCol(c.Field[0], obj)
	if err != nil {
		return nil, err
	}

	// RefToSQL already walks the chain to produce the correct subquery.
	// For {ID: selfID, Chain: ["department"]} → (SELECT "department_id" FROM ... WHERE "id" = $1)
	if len(c.Ref.Chain) == 0 {
		return comparisonExpr(col, c.Op, c.Ref.ID), nil
	}

	ref := refToSQL(c.Ref, obj, cache)
	return sq.Expr(fmt.Sprintf(`%s %s (?)`, col, cmpOp(c.Op)), ref), nil
}

// lookupChainToSQL builds a subquery for lookup-chain field comparisons.
func lookupChainToSQL(c hrql.FieldCmp, obj *schema.ObjectDef, cache *schema.Cache) (sq.Sqlizer, error) {
	alias := Alias()

	fd := obj.FieldsByAPIName[c.Field[0]]
	if fd == nil || fd.Type != schema.FieldLookup || fd.LookupObjectID == nil {
		return nil, fmt.Errorf("field %q is not a LOOKUP field", c.Field[0])
	}

	targetObj := cache.GetByID(*fd.LookupObjectID)
	if targetObj == nil {
		return nil, fmt.Errorf("lookup target for field %q not found", c.Field[0])
	}

	// For 2-level chains: (SELECT col FROM target WHERE id = fk_ref) op value
	if len(c.Field) == 2 {
		fkCol := FKRef(alias, fd)
		nextFd := targetObj.FieldsByAPIName[c.Field[1]]
		if nextFd == nil {
			return nil, fmt.Errorf("unknown field %q on %s", c.Field[1], targetObj.APIName)
		}
		sub := sq.Select(FilterExpr("_sub", nextFd)).
			From(tableName(targetObj) + ` "_sub"`).
			Where(sq.Expr(fmt.Sprintf(`"_sub"."id" = %s`, fkCol)))
		return sq.Expr(fmt.Sprintf(`(?) %s ?`, cmpOp(c.Op)), sub, c.Value), nil
	}

	return nil, fmt.Errorf("LOOKUP chain too deep (max 2 levels)")
}

// stringMatchToSQL translates a StringMatch to an ILIKE expression.
func stringMatchToSQL(c hrql.StringMatch, obj *schema.ObjectDef) (sq.Sqlizer, error) {
	if len(c.Field) == 0 {
		return nil, fmt.Errorf("empty field in string match")
	}
	col, err := resolveFilterCol(c.Field[0], obj)
	if err != nil {
		return nil, err
	}

	switch c.Op {
	case "contains":
		return sq.ILike{col: "%" + c.Pattern + "%"}, nil
	case "starts_with":
		return sq.ILike{col: c.Pattern + "%"}, nil
	case "ends_with":
		return sq.ILike{col: "%" + c.Pattern}, nil
	default:
		return nil, fmt.Errorf("unknown string op %q", c.Op)
	}
}

// subqueryAggToSQL translates a SubqueryAgg to a correlated subquery expression.
func subqueryAggToSQL(c hrql.SubqueryAgg, obj *schema.ObjectDef) (sq.Sqlizer, error) {
	switch c.OrgFunc {
	case "reports":
		sub := reportsSubquery(c, obj)
		if c.Op != "" && c.Value != "" {
			return sq.Expr(fmt.Sprintf(`(?) %s ?`, cmpOp(c.Op)), sub, c.Value), nil
		}
		return sub, nil
	default:
		return nil, fmt.Errorf("correlated subquery not supported for %s()", c.OrgFunc)
	}
}

// buildAggregateBuilder builds a Squirrel select builder for a terminal aggregation.
func buildAggregateBuilder(
	obj *schema.ObjectDef,
	aggFunc string,
	aggField string,
	conditions []sq.Sqlizer,
) sq.SelectBuilder {
	alias := Alias()
	from, baseWhere := TableSource(obj, alias)

	qb := sq.Select(aggExpr(aggFunc, aggField, obj)).From(from)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}
	for _, cond := range conditions {
		qb = qb.Where(cond)
	}
	return qb
}

// aggExpr builds the SELECT expression for an aggregate function + field.
func aggExpr(fn, field string, obj *schema.ObjectDef) string {
	if field == "" {
		return fmt.Sprintf(`%s(*)`, fn)
	}
	fd := obj.FieldsByAPIName[field]
	if fd == nil {
		return fmt.Sprintf(`%s(*)`, fn)
	}
	col := FilterExpr(Alias(), fd)

	// PostgreSQL doesn't support avg() on date/timestamp columns.
	if fn == "avg" && (fd.Type == schema.FieldDate || fd.Type == schema.FieldDatetime) {
		return fmt.Sprintf(`to_timestamp(avg(extract(epoch from %s)))::text`, col)
	}

	return fmt.Sprintf(`%s(%s)`, fn, col)
}

// scalarExprToSQL translates a ScalarExpr tree into a Sqlizer with ? placeholders.
func scalarExprToSQL(expr hrql.ScalarExpr, obj *schema.ObjectDef, cache *schema.Cache) (sq.Sqlizer, error) {
	switch e := expr.(type) {
	case hrql.ScalarLiteral:
		return sq.Expr("?::numeric", e.Value), nil

	case hrql.ScalarSubquery:
		conds, err := TranslateConditions(e.Plan.Conditions, obj, cache)
		if err != nil {
			return nil, err
		}
		return sq.Expr("(?)", buildAggregateBuilder(obj, e.Plan.AggFunc, e.Plan.AggField, conds)), nil

	case hrql.ScalarBool:
		cond, err := conditionToSQL(e.Cond, obj, cache)
		if err != nil {
			return nil, err
		}
		return cond, nil

	case hrql.ScalarArith:
		switch e.Op {
		case "+", "-", "*", "/":
		default:
			return nil, fmt.Errorf("unsupported arithmetic operator %q", e.Op)
		}
		left, err := scalarExprToSQL(e.Left, obj, cache)
		if err != nil {
			return nil, err
		}
		right, err := scalarExprToSQL(e.Right, obj, cache)
		if err != nil {
			return nil, err
		}
		return sq.Expr(fmt.Sprintf("(? %s ?)", e.Op), left, right), nil

	default:
		return nil, fmt.Errorf("unknown scalar expr type %T", expr)
	}
}

// --- SQL helpers ---

// resolveFilterCol looks up a field by API name and returns the SQL filter expression.
func resolveFilterCol(fieldName string, obj *schema.ObjectDef) (string, error) {
	fd := obj.FieldsByAPIName[fieldName]
	if fd == nil {
		return "", fmt.Errorf("unknown field %q", fieldName)
	}
	return FilterExpr(Alias(), fd), nil
}

func comparisonExpr(col, op, val string) sq.Sqlizer {
	switch op {
	case "==":
		return sq.Eq{col: val}
	case "!=":
		return sq.NotEq{col: val}
	case ">":
		return sq.Gt{col: val}
	case ">=":
		return sq.GtOrEq{col: val}
	case "<":
		return sq.Lt{col: val}
	case "<=":
		return sq.LtOrEq{col: val}
	default:
		return sq.Expr(fmt.Sprintf(`%s %s ?`, col, op), val)
	}
}

func cmpOp(op string) string {
	if op == "==" {
		return "="
	}
	return op
}
