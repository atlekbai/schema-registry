package pg

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// ChainUp returns a condition matching the ancestor at exactly `steps` levels above target.
// SQL: t.manager_path = subpath(PathSubquery(ref), 0, nlevel(PathSubquery(ref)) - steps)
func ChainUp(ref hrql.EmployeeRef, steps int, obj *schema.ObjectDef) sq.Sqlizer {
	col := fmt.Sprintf(`%s."manager_path"`, QI(Alias()))
	pathSQL, pathArgs, _ := PathSubquery(ref, obj).ToSql()
	sql := fmt.Sprintf(
		`%s = subpath(%s, 0, GREATEST(nlevel(%s) - ?, 0))`,
		col, pathSQL, pathSQL,
	)
	args := concatArgs(pathArgs, pathArgs, []any{steps})
	return sq.Expr(sql, args...)
}

// ChainDown returns a condition matching descendants at exactly `depth` levels below target.
// SQL: t.manager_path <@ PathSubquery(ref) AND nlevel(t.mp) = nlevel(PathSubquery(ref)) + depth
func ChainDown(ref hrql.EmployeeRef, depth int, obj *schema.ObjectDef) sq.Sqlizer {
	col := fmt.Sprintf(`%s."manager_path"`, QI(Alias()))
	pathSQL, pathArgs, _ := PathSubquery(ref, obj).ToSql()
	sql := fmt.Sprintf(
		`%s <@ %s AND nlevel(%s) = nlevel(%s) + ?`,
		col, pathSQL, col, pathSQL,
	)
	args := concatArgs(pathArgs, pathArgs, []any{depth})
	return sq.Expr(sql, args...)
}

// Subtree returns a condition matching all descendants (any depth), excluding the target itself.
// SQL: t.manager_path <@ PathSubquery(ref) AND t.manager_path != PathSubquery(ref)
func Subtree(ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	col := fmt.Sprintf(`%s."manager_path"`, QI(Alias()))
	pathSQL, pathArgs, _ := PathSubquery(ref, obj).ToSql()
	sql := fmt.Sprintf(
		`%s <@ %s AND %s != %s`,
		col, pathSQL, col, pathSQL,
	)
	args := concatArgs(pathArgs, pathArgs)
	return sq.Expr(sql, args...)
}

// SameField returns: column = (SELECT field FROM emp WHERE id = ref.ID) AND id != ref.ID.
// Includes IS NOT NULL guard for the subquery to handle null field values.
func SameField(fieldAPIName string, ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	col := ResolveColumn(obj, fieldAPIName)
	fieldSub, fieldArgs, _ := FieldSubquery(ref, fieldAPIName, obj).ToSql()
	refSQL, refArgs, _ := RefToSQL(ref, obj).ToSql()

	sql := fmt.Sprintf(
		`%s.%s = %s AND %s IS NOT NULL AND %s."id" != %s`,
		QI(Alias()), QI(col),
		fieldSub, fieldSub,
		QI(Alias()), refSQL,
	)
	args := concatArgs(fieldArgs, fieldArgs, refArgs)
	return sq.Expr(sql, args...)
}

// ChainAll returns a condition matching ALL ancestors of the target.
// SQL: t.manager_path @> PathSubquery(ref) AND t.id != RefToSQL(ref)
// Uses the SP-GiST index on manager_path.
func ChainAll(ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	col := fmt.Sprintf(`%s."manager_path"`, QI(Alias()))
	pathSQL, pathArgs, _ := PathSubquery(ref, obj).ToSql()
	refSQL, refArgs, _ := RefToSQL(ref, obj).ToSql()

	sql := fmt.Sprintf(
		`%s @> %s AND %s."id" != %s`,
		col, pathSQL, QI(Alias()), refSQL,
	)
	args := concatArgs(pathArgs, refArgs)
	return sq.Expr(sql, args...)
}

// ReportsToWhere generates a WHERE condition for reports_to(., target) inside where.
// Semantically identical to Subtree — checks if current row is a descendant of target.
func ReportsToWhere(ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	return Subtree(ref, obj)
}

// ReportsToCheckSQL builds a SQL query that returns a boolean for a top-level reports_to(emp, target).
// SELECT (emp_path <@ target_path AND emp_path != target_path)
func ReportsToCheckSQL(emp, target hrql.EmployeeRef, obj *schema.ObjectDef) (string, []any, error) {
	empPathSQL, empPathArgs, _ := PathSubquery(emp, obj).ToSql()
	tgtPathSQL, tgtPathArgs, _ := PathSubquery(target, obj).ToSql()

	sql := fmt.Sprintf(
		`SELECT (%s <@ %s AND %s != %s)`,
		empPathSQL, tgtPathSQL, empPathSQL, tgtPathSQL,
	)
	args := concatArgs(empPathArgs, tgtPathArgs, empPathArgs, tgtPathArgs)
	return sql, args, nil
}

// NullCondition returns an always-false condition.
func NullCondition() sq.Sqlizer {
	return sq.Eq{fmt.Sprintf(`%s."id"`, QI(Alias())): nil}
}

// concatArgs safely concatenates multiple arg slices without append aliasing.
func concatArgs(slices ...[]any) []any {
	n := 0
	for _, s := range slices {
		n += len(s)
	}
	result := make([]any, 0, n)
	for _, s := range slices {
		result = append(result, s...)
	}
	return result
}
