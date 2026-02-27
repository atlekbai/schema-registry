package query

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// QI is shorthand for schema.QuoteIdent.
func QI(name string) string { return schema.QuoteIdent(name) }

// QuoteLit wraps s in single quotes for use as a SQL string literal.
func QuoteLit(s string) string { return "'" + s + "'" }

// Alias returns the standard query alias used in all generated SQL.
func Alias() string { return qAlias }

// SelectFieldExpr returns the SQL for a field in SELECT context (preserves JSONB types via ->).
func SelectFieldExpr(alias string, fd *schema.FieldDef) string {
	if fd.StorageColumn != nil {
		return fmt.Sprintf(`%s.%s`, QI(alias), QI(*fd.StorageColumn))
	}
	return fmt.Sprintf(`%s."data"->%s`, QI(alias), QuoteLit(fd.APIName))
}

// FilterExpr returns the SQL for a field in WHERE/ORDER context (text extraction via ->> with casts).
func FilterExpr(alias string, fd *schema.FieldDef) string {
	if fd.StorageColumn != nil {
		return fmt.Sprintf(`%s.%s`, QI(alias), QI(*fd.StorageColumn))
	}
	if fd.IsNumeric() {
		return fmt.Sprintf(`(%s."data"->>%s)::numeric`, QI(alias), QuoteLit(fd.APIName))
	}
	if fd.Type == schema.FieldDate || fd.Type == schema.FieldDatetime {
		return fmt.Sprintf(`(%s."data"->>%s)::timestamptz`, QI(alias), QuoteLit(fd.APIName))
	}
	return fmt.Sprintf(`%s."data"->>%s`, QI(alias), QuoteLit(fd.APIName))
}

// jsonKey returns the JSON output key for a field.
// Lookup fields use the storage column name (e.g. "organization_id"), others use the API name.
func jsonKey(f *schema.FieldDef) string {
	if f.Type == schema.FieldLookup && f.StorageColumn != nil {
		return *f.StorageColumn
	}
	return f.APIName
}

// expandExpr returns a CASE WHEN expression for a laterally-joined expanded field.
func expandExpr(alias string) string {
	return fmt.Sprintf(`CASE WHEN %s."id" IS NOT NULL THEN to_jsonb(%s.*) ELSE NULL END`,
		QI(alias), QI(alias))
}

// fkRef returns the SQL for a FK reference in lateral joins.
func fkRef(alias string, fd *schema.FieldDef) string {
	if fd.StorageColumn != nil {
		return fmt.Sprintf(`%s.%s`, QI(alias), QI(*fd.StorageColumn))
	}
	return fmt.Sprintf(`(%s."data"->>%s)::uuid`, QI(alias), QuoteLit(fd.APIName))
}

// TableSource returns the FROM clause and optional base WHERE for an object.
func TableSource(obj *schema.ObjectDef, alias string) (string, sq.Sqlizer) {
	if obj.IsStandard {
		return obj.TableName() + " " + QI(alias), nil
	}
	return `"metadata"."records" ` + QI(alias), sq.Eq{QI(alias) + `."object_id"`: obj.ID}
}
