package pg

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// RefToSQL resolves an EmployeeRef to a SQL expression that yields an employee UUID.
//   - {ID: "abc", Chain: nil}          → $1 (bind "abc")
//   - {ID: "abc", Chain: ["manager"]}  → (SELECT "manager_id" FROM "core"."employees" WHERE "id" = $1)
func RefToSQL(ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	if len(ref.Chain) == 0 {
		return sq.Expr("?", ref.ID)
	}

	// Walk the chain: each step dereferences a LOOKUP field.
	// Start from the base ID, wrap in nested subqueries.
	sql := "?"
	args := []any{ref.ID}

	for _, fieldName := range ref.Chain {
		col := ResolveColumn(obj, fieldName)
		sql = fmt.Sprintf(
			`(SELECT %s FROM %s WHERE "id" = %s)`,
			QI(col), obj.TableName(), sql,
		)
	}

	return sq.Expr(sql, args...)
}

// PathSubquery wraps an EmployeeRef in a subquery that yields the manager_path.
// Result: (SELECT "manager_path" FROM "core"."employees" WHERE "id" = <RefToSQL>)
func PathSubquery(ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	refSQL, refArgs, _ := RefToSQL(ref, obj).ToSql()
	sql := fmt.Sprintf(
		`(SELECT "manager_path" FROM %s WHERE "id" = %s)`,
		obj.TableName(), refSQL,
	)
	return sq.Expr(sql, refArgs...)
}

// FieldSubquery wraps an EmployeeRef in a subquery that yields a specific field value.
// Result: (SELECT "col" FROM "core"."employees" WHERE "id" = <RefToSQL>)
func FieldSubquery(ref hrql.EmployeeRef, fieldAPIName string, obj *schema.ObjectDef) sq.Sqlizer {
	col := ResolveColumn(obj, fieldAPIName)
	refSQL, refArgs, _ := RefToSQL(ref, obj).ToSql()
	sql := fmt.Sprintf(
		`(SELECT %s FROM %s WHERE "id" = %s)`,
		QI(col), obj.TableName(), refSQL,
	)
	return sq.Expr(sql, refArgs...)
}
