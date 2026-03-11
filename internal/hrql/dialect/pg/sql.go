package pg

import (
	"encoding/json"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/atlekbai/schema_registry/internal/schema"
	"github.com/jackc/pgx/v5"
)

const qAlias = "_e"

// QI quotes a SQL identifier, escaping embedded double quotes.
func QI(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// QuoteLit wraps s in single quotes for use as a SQL string literal.
func QuoteLit(s string) string { return "'" + s + "'" }

// Alias returns the standard query alias used in all generated SQL.
func Alias() string { return qAlias }

// idCol returns the qualified "id" column for the standard alias.
func idCol() string { return fmt.Sprintf(`%s."id"`, QI(Alias())) }

// mpCol returns the qualified "manager_path" column for the standard alias.
func mpCol() string { return fmt.Sprintf(`%s."manager_path"`, QI(Alias())) }

// SelectExpr returns the SQL for a field in SELECT context (preserves JSONB types via ->).
func SelectExpr(alias string, fd *schema.FieldDef) string {
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

// JSONKey returns the JSON output key for a field.
// Lookup fields use the storage column name (e.g. "organization_id"), others use the API name.
func JSONKey(f *schema.FieldDef) string {
	if f.Type == schema.FieldLookup && f.StorageColumn != nil {
		return *f.StorageColumn
	}
	return f.APIName
}

// tableName returns the fully qualified, quoted table name for a standard object.
func tableName(obj *schema.ObjectDef) string {
	if obj.StorageSchema != nil && obj.StorageTable != nil {
		return QI(*obj.StorageSchema) + "." + QI(*obj.StorageTable)
	}
	return ""
}

// FKRef returns the SQL for a FK reference in lateral joins and subqueries.
func FKRef(alias string, fd *schema.FieldDef) string {
	if fd.StorageColumn != nil {
		return fmt.Sprintf(`%s.%s`, QI(alias), QI(*fd.StorageColumn))
	}
	return fmt.Sprintf(`(%s."data"->>%s)::uuid`, QI(alias), QuoteLit(fd.APIName))
}

// TableSource returns the FROM clause and optional base WHERE for an object.
func TableSource(obj *schema.ObjectDef, alias string) (string, sq.Sqlizer) {
	if obj.IsStandard {
		return tableName(obj) + " " + QI(alias), nil
	}
	return `"metadata"."records" ` + QI(alias), sq.Eq{QI(alias) + `."object_id"`: obj.ID}
}

// --- Binary Sqlizer helpers ---

// descendantOf returns `(left) <@ (right)` — ltree descendant check.
func descendantOf(left, right sq.Sqlizer) sq.Sqlizer {
	return sq.Expr(`(?) <@ (?)`, left, right)
}

// ancestorOf returns `(left) @> (right)` — ltree ancestor check.
func ancestorOf(left, right sq.Sqlizer) sq.Sqlizer {
	return sq.Expr(`(?) @> (?)`, left, right)
}

// sqlNeq returns `(left) != (right)` for two Sqlizer operands.
func sqlNeq(left, right sq.Sqlizer) sq.Sqlizer {
	return sq.Expr(`(?) != (?)`, left, right)
}

// atDepth returns `nlevel(left) = nlevel(right) + depth` — exact ltree depth check.
func atDepth(left, right sq.Sqlizer, depth int) sq.Sqlizer {
	return sq.Expr(`nlevel(?) = nlevel(?) + ?`, left, right, depth)
}

// atAncestor returns `left = subpath(right, 0, GREATEST(nlevel(right) - steps, 0))` — ancestor at exact level.
func atAncestor(left, right sq.Sqlizer, steps int) sq.Sqlizer {
	return sq.Expr(`(?) = subpath(?, 0, GREATEST(nlevel(?) - ?, 0))`, left, right, right, steps)
}

// IsSystemField returns true for system fields (id, created_at, updated_at)
// that are always emitted explicitly and should be skipped in field loops.
func IsSystemField(apiName string) bool {
	return apiName == "id" || apiName == "created_at" || apiName == "updated_at"
}

// ScanJSONRows scans pgx rows into raw JSON slices.
func ScanJSONRows(rows pgx.Rows) ([]json.RawMessage, error) {
	results := make([]json.RawMessage, 0, 64)
	for rows.Next() {
		var data json.RawMessage
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		results = append(results, data)
	}
	return results, rows.Err()
}
