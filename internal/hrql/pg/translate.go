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
	OrderBy    *OrderClause
	Limit      int
	PickOp     string
	PickN      int

	// For PlanScalar: pre-built aggregate query.
	AggSQL  string
	AggArgs []any
}

// Translate converts a storage-agnostic Plan into SQL-ready components.
func Translate(plan *hrql.Plan, obj *schema.ObjectDef, cache *schema.Cache) (*SQLResult, error) {
	result := &SQLResult{
		Limit:  plan.Limit,
		PickOp: plan.PickOp,
		PickN:  plan.PickN,
	}

	// Translate ordering.
	if plan.OrderBy != nil {
		result.OrderBy = &OrderClause{
			FieldAPIName: plan.OrderBy.Field,
			Desc:         plan.OrderBy.Desc,
		}
	}

	// Translate conditions.
	for _, c := range plan.Conditions {
		sqlCond, err := ConditionToSQL(c, obj, cache)
		if err != nil {
			return nil, err
		}
		result.Conditions = append(result.Conditions, sqlCond)
	}

	// For scalar plans, build the aggregate query.
	if plan.Kind == hrql.PlanScalar {
		sql, args, err := buildAggregate(obj, plan.AggFunc, plan.AggField, result.Conditions)
		if err != nil {
			return nil, fmt.Errorf("build aggregate: %w", err)
		}
		result.AggSQL = sql
		result.AggArgs = args
	}

	return result, nil
}

// TranslateConditions converts a slice of storage-agnostic Conditions to SQL expressions.
func TranslateConditions(conds []hrql.Condition, obj *schema.ObjectDef, cache *schema.Cache) ([]sq.Sqlizer, error) {
	var result []sq.Sqlizer
	for _, c := range conds {
		sql, err := ConditionToSQL(c, obj, cache)
		if err != nil {
			return nil, err
		}
		result = append(result, sql)
	}
	return result, nil
}

// ConditionToSQL translates a single Condition to a Squirrel SQL expression.
func ConditionToSQL(c hrql.Condition, obj *schema.ObjectDef, cache *schema.Cache) (sq.Sqlizer, error) {
	switch c := c.(type) {
	case hrql.IdentityFilter:
		col := fmt.Sprintf(`%s."id"`, QI(Alias()))
		return sq.Eq{col: c.ID}, nil

	case hrql.NullFilter:
		return NullCondition(), nil

	case hrql.FieldCmp:
		return fieldCmpToSQL(c, obj, cache)

	case hrql.StringMatch:
		return stringMatchToSQL(c, obj)

	case hrql.AndCond:
		left, err := ConditionToSQL(c.Left, obj, cache)
		if err != nil {
			return nil, err
		}
		right, err := ConditionToSQL(c.Right, obj, cache)
		if err != nil {
			return nil, err
		}
		return sq.And{left, right}, nil

	case hrql.OrCond:
		left, err := ConditionToSQL(c.Left, obj, cache)
		if err != nil {
			return nil, err
		}
		right, err := ConditionToSQL(c.Right, obj, cache)
		if err != nil {
			return nil, err
		}
		return sq.Or{left, right}, nil

	case hrql.OrgChainUp:
		return ChainUp(c.Path, c.Steps), nil

	case hrql.OrgChainDown:
		return ChainDown(c.Path, c.Depth), nil

	case hrql.OrgChainAll:
		return ChainAll(c.Path), nil

	case hrql.OrgSubtree:
		return Subtree(c.Path), nil

	case hrql.SameFieldCond:
		column := ResolveColumn(obj, c.Field)
		return SameField(column, c.Value, c.ExcludeID), nil

	case hrql.ReportsTo:
		return ReportsToWhere(c.TargetPath), nil

	case hrql.SubqueryAgg:
		return subqueryAggToSQL(c, obj)

	case hrql.InFilter:
		fd := obj.FieldsByAPIName[c.Field[0]]
		if fd == nil {
			return nil, fmt.Errorf("unknown field %q", c.Field[0])
		}
		col := FilterExpr(Alias(), fd)
		return sq.Expr(fmt.Sprintf(`%s = ANY(?)`, col), c.Values), nil

	case hrql.IsNullFilter:
		fd := obj.FieldsByAPIName[c.Field[0]]
		if fd == nil {
			return nil, fmt.Errorf("unknown field %q", c.Field[0])
		}
		col := FilterExpr(Alias(), fd)
		if c.IsNull {
			return sq.Eq{col: nil}, nil
		}
		return sq.NotEq{col: nil}, nil

	case hrql.LikeFilter:
		fd := obj.FieldsByAPIName[c.Field[0]]
		if fd == nil {
			return nil, fmt.Errorf("unknown field %q", c.Field[0])
		}
		col := FilterExpr(Alias(), fd)
		if c.CaseInsensitive {
			return sq.Expr(fmt.Sprintf(`%s ILIKE ?`, col), c.Pattern), nil
		}
		return sq.Expr(fmt.Sprintf(`%s LIKE ?`, col), c.Pattern), nil

	default:
		return nil, fmt.Errorf("unknown condition type %T", c)
	}
}

// fieldCmpToSQL translates a FieldCmp to SQL.
func fieldCmpToSQL(c hrql.FieldCmp, obj *schema.ObjectDef, cache *schema.Cache) (sq.Sqlizer, error) {
	alias := Alias()

	if len(c.Field) == 1 {
		fd := obj.FieldsByAPIName[c.Field[0]]
		if fd == nil {
			return nil, fmt.Errorf("unknown field %q", c.Field[0])
		}
		col := FilterExpr(alias, fd)
		return comparisonExpr(col, c.Op, c.Value), nil
	}

	// Lookup chain: .department.title == "Eng"
	return lookupChainToSQL(c, obj, cache)
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

	// For 2-level chains: (SELECT col FROM target WHERE id = fk_ref)
	if len(c.Field) == 2 {
		fkCol := FKRef(alias, fd)
		nextFd := targetObj.FieldsByAPIName[c.Field[1]]
		if nextFd == nil {
			return nil, fmt.Errorf("unknown field %q on %s", c.Field[1], targetObj.APIName)
		}
		targetCol := FilterExpr("_sub", nextFd)
		targetFrom := targetObj.TableName()
		subSQL := fmt.Sprintf(`(SELECT %s FROM %s "_sub" WHERE "_sub"."id" = %s)`, targetCol, targetFrom, fkCol)
		return comparisonExpr(subSQL, c.Op, c.Value), nil
	}

	return nil, fmt.Errorf("LOOKUP chain too deep (max 2 levels)")
}

// stringMatchToSQL translates a StringMatch to an ILIKE expression.
func stringMatchToSQL(c hrql.StringMatch, obj *schema.ObjectDef) (sq.Sqlizer, error) {
	if len(c.Field) == 0 {
		return nil, fmt.Errorf("empty field in string match")
	}
	fd := obj.FieldsByAPIName[c.Field[0]]
	if fd == nil {
		return nil, fmt.Errorf("unknown field %q", c.Field[0])
	}
	col := FilterExpr(Alias(), fd)

	switch c.Op {
	case "contains":
		return sq.Expr(fmt.Sprintf(`%s ILIKE '%%' || ? || '%%'`, col), c.Pattern), nil
	case "starts_with":
		return sq.Expr(fmt.Sprintf(`%s ILIKE ? || '%%'`, col), c.Pattern), nil
	case "ends_with":
		return sq.Expr(fmt.Sprintf(`%s ILIKE '%%' || ?`, col), c.Pattern), nil
	default:
		return nil, fmt.Errorf("unknown string op %q", c.Op)
	}
}

// subqueryAggToSQL translates a SubqueryAgg to a correlated subquery expression.
func subqueryAggToSQL(c hrql.SubqueryAgg, obj *schema.ObjectDef) (sq.Sqlizer, error) {
	from := obj.TableName() + ` "_sub_e"`
	subCol := `"_sub_e"."manager_path"`

	switch c.OrgFunc {
	case "reports":
		outerPath := fmt.Sprintf(`%s."manager_path"`, QI(Alias()))

		var whereCond string
		if c.Depth == 0 {
			whereCond = fmt.Sprintf(`%s <@ %s AND %s != %s`, subCol, outerPath, subCol, outerPath)
		} else {
			whereCond = fmt.Sprintf(`%s <@ %s AND nlevel(%s) = nlevel(%s) + %d`,
				subCol, outerPath, subCol, outerPath, c.Depth)
		}

		subSQL := fmt.Sprintf(`(SELECT %s(*) FROM %s WHERE %s)`, c.AggFunc, from, whereCond)

		if c.Op != "" && c.Value != "" {
			return sq.Expr(fmt.Sprintf(`%s %s ?`, subSQL, sqlOp(c.Op)), c.Value), nil
		}
		return sq.Expr(subSQL), nil

	default:
		return nil, fmt.Errorf("correlated subquery not supported for %s()", c.OrgFunc)
	}
}

// buildAggregate builds a SQL query for a terminal aggregation.
func buildAggregate(
	obj *schema.ObjectDef,
	aggFunc string,
	aggField string,
	conditions []sq.Sqlizer,
) (string, []any, error) {
	alias := Alias()
	from, baseWhere := TableSource(obj, alias)

	var col string
	switch {
	case aggFunc == "count" && aggField == "":
		col = "*"
	case aggField != "":
		fd := obj.FieldsByAPIName[aggField]
		if fd != nil {
			col = FilterExpr(alias, fd)
		} else {
			col = "*"
		}
	default:
		col = "*"
	}

	selectExpr := fmt.Sprintf(`%s(%s)`, aggFunc, col)
	qb := sq.Select(selectExpr).From(from).PlaceholderFormat(sq.Dollar)

	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}
	for _, cond := range conditions {
		qb = qb.Where(cond)
	}

	return qb.ToSql()
}

// --- SQL helpers ---

func comparisonExpr(col, op, val string) sq.Sqlizer {
	switch op {
	case "==":
		return sq.Eq{col: val}
	case "!=":
		return sq.NotEq{col: val}
	default:
		return sq.Expr(fmt.Sprintf(`%s %s ?`, col, sqlOp(op)), val)
	}
}

func sqlOp(op string) string {
	switch op {
	case "==":
		return "="
	case "!=":
		return "!="
	default:
		return op
	}
}

// ResolveColumn maps a field API name to its storage column via the object definition.
func ResolveColumn(obj *schema.ObjectDef, apiName string) string {
	if fd, ok := obj.FieldsByAPIName[apiName]; ok && fd.StorageColumn != nil {
		return *fd.StorageColumn
	}
	return apiName
}
