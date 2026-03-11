package pg

import (
	sq "github.com/Masterminds/squirrel"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// refToSQL resolves an EmployeeRef to a SQL expression that yields an employee UUID.
//   - {ID: "abc", Chain: nil}          → ? (bind "abc")
//   - {ID: "abc", Chain: ["manager"]}  → (SELECT "manager_id" FROM "core"."employees" WHERE "id" = ?)
func refToSQL(ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	var inner sq.Sqlizer = sq.Expr("?", ref.ID)
	for _, fieldName := range ref.Chain {
		col := resolveColumn(obj, fieldName)
		inner = sq.Select(QI(col)).
			From(tableName(obj)).
			Where(sq.Expr(`"id" = (?)`, inner))
	}
	return inner
}

// pathSubquery wraps an EmployeeRef in a subquery that yields the manager_path.
func pathSubquery(ref hrql.EmployeeRef, obj *schema.ObjectDef) sq.Sqlizer {
	return sq.Select(`"manager_path"`).
		From(tableName(obj)).
		Where(sq.Expr(`"id" = (?)`, refToSQL(ref, obj)))
}

// fieldSubquery wraps an EmployeeRef in a subquery that yields a specific field value.
func fieldSubquery(ref hrql.EmployeeRef, fieldAPIName string, obj *schema.ObjectDef) sq.Sqlizer {
	col := resolveColumn(obj, fieldAPIName)
	return sq.Select(QI(col)).
		From(tableName(obj)).
		Where(sq.Expr(`"id" = (?)`, refToSQL(ref, obj)))
}

// resolveColumn maps a field API name to its storage column via the object definition.
func resolveColumn(obj *schema.ObjectDef, apiName string) string {
	if fd, ok := obj.FieldsByAPIName[apiName]; ok && fd.StorageColumn != nil {
		return *fd.StorageColumn
	}
	return apiName
}
