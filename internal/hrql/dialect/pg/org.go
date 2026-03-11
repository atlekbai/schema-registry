package pg

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// chainUp returns a condition matching the ancestor at exactly `steps` levels above target.
func chainUp(ref hrql.EmployeeRef, steps int, obj *schema.ObjectDef) sq.Sqlizer {
	return atAncestor(sq.Expr(mpCol()), pathSubquery(ref, obj), steps)
}

// chainDown returns a condition matching descendants at exactly `depth` levels below target.
func chainDown(ref hrql.EmployeeRef, depth int, obj *schema.ObjectDef) sq.Sqlizer {
	path := pathSubquery(ref, obj)
	mp := sq.Expr(mpCol())
	return sq.And{
		descendantOf(mp, path),
		atDepth(sq.Expr(mpCol()), path, depth),
	}
}

// subtree returns a condition matching all descendants (any depth), excluding the target itself.
func subtree(ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	path := pathSubquery(ref, obj)
	mp := sq.Expr(mpCol())
	return sq.And{
		descendantOf(mp, path),
		sqlNeq(mp, path),
	}
}

// sameField returns: column = (SELECT field FROM emp WHERE id = ref.ID) AND id != ref.ID.
func sameField(fieldAPIName string, ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	col := fmt.Sprintf(`%s.%s`, QI(Alias()), QI(resolveColumn(obj, fieldAPIName)))
	field := fieldSubquery(ref, fieldAPIName, obj)
	return sq.And{
		sq.Expr(col+` = (?)`, field),
		sq.Expr(`(?) IS NOT NULL`, field),
		sqlNeq(sq.Expr(idCol()), refToSQL(ref, obj)),
	}
}

// chainAll returns a condition matching ALL ancestors of the target.
func chainAll(ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	return sq.And{
		ancestorOf(sq.Expr(mpCol()), pathSubquery(ref, obj)),
		sqlNeq(sq.Expr(idCol()), refToSQL(ref, obj)),
	}
}

// reportsToWhere generates a WHERE condition for reports_to(., target) inside where.
func reportsToWhere(ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	return subtree(ref, obj)
}

// reportsToCheck builds a boolean expression for reports_to(emp, target).
func reportsToCheck(emp, target hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	empPath := pathSubquery(emp, obj)
	tgtPath := pathSubquery(target, obj)
	return sq.And{
		descendantOf(empPath, tgtPath),
		sqlNeq(empPath, tgtPath),
	}
}

// reportsSubquery builds a correlated subquery: SELECT agg(*) FROM obj WHERE manager_path <@ outer.
func reportsSubquery(c hrql.SubqueryAgg, obj *schema.ObjectDef) sq.SelectBuilder {
	from := tableName(obj) + ` "_sub_e"`
	sub := sq.Expr(`"_sub_e"."manager_path"`)
	outer := sq.Expr(mpCol())

	qb := sq.Select(fmt.Sprintf(`%s(*)`, c.AggFunc)).From(from).
		Where(descendantOf(sub, outer))

	if c.Depth == 0 {
		qb = qb.Where(sqlNeq(sub, outer))
	} else {
		qb = qb.Where(atDepth(sub, outer, c.Depth))
	}
	return qb
}

// nullCondition returns an always-false condition.
func nullCondition() sq.Sqlizer {
	return sq.Eq{idCol(): nil}
}
