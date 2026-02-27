package query

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// Alias returns the standard query alias used in all generated SQL.
func Alias() string {
	return qAlias
}

// TableSource returns the FROM clause and optional base WHERE for an object.
func TableSource(obj *schema.ObjectDef, alias string) (string, sq.Sqlizer) {
	return tableSource(obj, alias)
}

// FilterExpr returns the SQL expression for a field in WHERE/ORDER context.
func FilterExpr(alias string, fd *schema.FieldDef) string {
	return filterExpr(alias, fd)
}

// SelectFieldExpr returns the SQL expression for a field in SELECT context.
func SelectFieldExpr(alias string, fd *schema.FieldDef) string {
	return selectFieldExpr(alias, fd)
}

// QI quotes a SQL identifier.
func QI(name string) string {
	return qi(name)
}
