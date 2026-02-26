package query

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// selectFieldExpr returns the SQL for a field in SELECT context (preserves JSONB types via ->).
func selectFieldExpr(alias string, fd *schema.FieldDef) string {
	if fd.StorageColumn != nil {
		return fmt.Sprintf(`%s.%s`, qi(alias), qi(*fd.StorageColumn))
	}
	return fmt.Sprintf(`%s."data"->%s`, qi(alias), quoteLit(fd.APIName))
}

// filterExpr returns the SQL for a field in WHERE/ORDER context (text extraction via ->> with casts).
func filterExpr(alias string, fd *schema.FieldDef) string {
	if fd.StorageColumn != nil {
		return fmt.Sprintf(`%s.%s`, qi(alias), qi(*fd.StorageColumn))
	}
	if fd.IsNumeric() {
		return fmt.Sprintf(`(%s."data"->>%s)::numeric`, qi(alias), quoteLit(fd.APIName))
	}
	if fd.Type == schema.FieldDate || fd.Type == schema.FieldDatetime {
		return fmt.Sprintf(`(%s."data"->>%s)::timestamptz`, qi(alias), quoteLit(fd.APIName))
	}
	return fmt.Sprintf(`%s."data"->>%s`, qi(alias), quoteLit(fd.APIName))
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
		qi(alias), qi(alias))
}

// fkRef returns the SQL for a FK reference in lateral joins.
func fkRef(alias string, fd *schema.FieldDef) string {
	if fd.StorageColumn != nil {
		return fmt.Sprintf(`%s.%s`, qi(alias), qi(*fd.StorageColumn))
	}
	return fmt.Sprintf(`(%s."data"->>%s)::uuid`, qi(alias), quoteLit(fd.APIName))
}

// tableSource returns the FROM clause and optional base WHERE for an object.
func tableSource(obj *schema.ObjectDef, alias string) (string, sq.Sqlizer) {
	if obj.IsStandard {
		return obj.TableName() + " " + qi(alias), nil
	}
	return `"metadata"."records" ` + qi(alias), sq.Eq{qi(alias) + `."object_id"`: obj.ID}
}
