package pg

import (
	sq "github.com/Masterminds/squirrel"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// refToSQL resolves an EmployeeRef to a SQL expression that yields an employee UUID.
//   - {ID: "abc", Chain: nil}          → ? (bind "abc")
//   - {ID: "abc", Chain: ["manager"]}  → (SELECT "manager_id" FROM "core"."employees" WHERE "id" = ?)
//   - {SubPlan: plan}                  → (SELECT "id" FROM ... WHERE ... ORDER BY ... LIMIT ...)
func refToSQL(ref hrql.EmployeeRef, obj *schema.ObjectDef, cache *schema.Cache) sq.Sqlizer {
	if ref.SubPlan != nil {
		return subPlanToIDExpr(ref.SubPlan, obj, cache)
	}
	var inner sq.Sqlizer = sq.Expr("?", ref.ID)
	for _, fieldName := range ref.Chain {
		col := resolveColumn(obj, fieldName)
		inner = sq.Select(QI(col)).
			From(tableName(obj)).
			Where(sq.Expr(`"id" = (?)`, inner))
	}
	return inner
}

// subPlanToIDExpr translates a SubPlan into a SELECT "id" subquery.
func subPlanToIDExpr(plan *hrql.Plan, obj *schema.ObjectDef, cache *schema.Cache) sq.Sqlizer {
	planObj := obj
	if plan.ObjectAPIName != "" && cache != nil {
		if o := cache.Get(plan.ObjectAPIName); o != nil {
			planObj = o
		}
	}

	alias := Alias()
	from, baseWhere := TableSource(planObj, alias)
	qb := sq.Select(idCol()).From(from)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}
	for _, c := range plan.Conditions {
		sqlCond, err := conditionToSQL(c, planObj, cache)
		if err != nil {
			continue
		}
		qb = qb.Where(sqlCond)
	}
	if plan.OrderBy != nil {
		qb = AddOrderBy(qb, planObj, plan.OrderBy)
	}
	if plan.Limit > 0 {
		qb = qb.Limit(uint64(plan.Limit))
	}
	return qb
}

// pathSubquery wraps an EmployeeRef in a subquery that yields the manager_path.
func pathSubquery(ref hrql.EmployeeRef, obj *schema.ObjectDef, cache *schema.Cache) sq.Sqlizer {
	return sq.Select(`"manager_path"`).
		From(tableName(obj)).
		Where(sq.Expr(`"id" = (?)`, refToSQL(ref, obj, cache)))
}

// fieldSubquery wraps an EmployeeRef in a subquery that yields a specific field value.
func fieldSubquery(ref hrql.EmployeeRef, fieldAPIName string, obj *schema.ObjectDef, cache *schema.Cache) sq.Sqlizer {
	col := resolveColumn(obj, fieldAPIName)
	return sq.Select(QI(col)).
		From(tableName(obj)).
		Where(sq.Expr(`"id" = (?)`, refToSQL(ref, obj, cache)))
}

// resolveColumn maps a field API name to its storage column via the object definition.
func resolveColumn(obj *schema.ObjectDef, apiName string) string {
	if fd, ok := obj.FieldsByAPIName[apiName]; ok && fd.StorageColumn != nil {
		return *fd.StorageColumn
	}
	return apiName
}
