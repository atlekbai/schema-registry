package query

import (
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/atlekbai/schema_registry/internal/schema"
	"github.com/google/uuid"
)

// StandardBuilder builds SQL for standard objects backed by real tables.
type StandardBuilder struct{}

const stdAlias = "_e"

func (b *StandardBuilder) BuildList(obj *schema.ObjectDef, params *QueryParams) (string, []any, error) {
	expandSet := makeExpandSet(params.ExpandPlans)
	jsonExpr := b.jsonObject(obj, params, expandSet)

	columns := []string{jsonExpr + " AS _row"}
	columns = append(columns, fmt.Sprintf(`%s."id"::text AS _cursor_id`, qi(stdAlias)))
	if params.Order != nil {
		fd := obj.FieldsByAPIName[params.Order.FieldAPIName]
		if fd != nil && fd.StorageColumn != nil {
			columns = append(columns, fmt.Sprintf(`%s.%s::text AS _cursor_val`,
				qi(stdAlias), qi(*fd.StorageColumn)))
		}
	}

	qb := sq.Select(columns...).
		From(obj.TableName() + " " + qi(stdAlias)).
		PlaceholderFormat(sq.Dollar)

	qb = b.addLateralJoins(qb, params)
	qb = b.applyFilters(qb, obj, params)
	qb = b.applyOrder(qb, obj, params)
	qb = b.applyCursor(qb, obj, params)
	qb = qb.Suffix("LIMIT ?", params.Limit+1)

	return qb.ToSql()
}

func (b *StandardBuilder) BuildGetByID(obj *schema.ObjectDef, id uuid.UUID, params *QueryParams) (string, []any, error) {
	expandSet := makeExpandSet(params.ExpandPlans)
	jsonExpr := b.jsonObject(obj, params, expandSet)

	columns := []string{jsonExpr + " AS _row"}

	qb := sq.Select(columns...).
		From(obj.TableName() + " " + qi(stdAlias)).
		Where(sq.Eq{qi(stdAlias) + `."id"`: id}).
		PlaceholderFormat(sq.Dollar).
		Limit(1)

	qb = b.addLateralJoins(qb, params)

	return qb.ToSql()
}

func (b *StandardBuilder) BuildCount(obj *schema.ObjectDef, params *QueryParams) (string, []any, error) {
	qb := sq.Select("count(*)").
		From(obj.TableName() + " " + qi(stdAlias)).
		PlaceholderFormat(sq.Dollar)

	qb = b.applyFilters(qb, obj, params)

	return qb.ToSql()
}

func (b *StandardBuilder) BuildEstimate(obj *schema.ObjectDef, params *QueryParams) (string, []any, error) {
	qb := sq.Select("1").
		From(obj.TableName() + " " + qi(stdAlias)).
		PlaceholderFormat(sq.Dollar)

	qb = b.applyFilters(qb, obj, params)

	return qb.ToSql()
}

// jsonObject builds a json_build_object(...) expression for the SELECT clause.
func (b *StandardBuilder) jsonObject(obj *schema.ObjectDef, params *QueryParams, expandSet map[string]*ExpandPlan) string {
	var pairs []string
	pairs = append(pairs,
		fmt.Sprintf(`'id', %s."id"`, qi(stdAlias)),
		fmt.Sprintf(`'created_at', %s."created_at"`, qi(stdAlias)),
		fmt.Sprintf(`'updated_at', %s."updated_at"`, qi(stdAlias)),
	)

	fields := b.resolveFields(obj, params, expandSet)
	for _, f := range fields {
		if isSystemField(f.APIName) {
			continue
		}
		if ep, ok := expandSet[f.APIName]; ok {
			alias := expandAlias(ep.FieldName)
			pairs = append(pairs, fmt.Sprintf(`%s, CASE WHEN %s."id" IS NOT NULL THEN row_to_json(%s.*)::jsonb ELSE NULL END`,
				quoteLit(f.APIName), qi(alias), qi(alias)))
		} else if f.StorageColumn != nil {
			key := f.APIName
			if f.Type == schema.FieldLookup {
				key = *f.StorageColumn
			}
			pairs = append(pairs, fmt.Sprintf(`%s, %s.%s`,
				quoteLit(key), qi(stdAlias), qi(*f.StorageColumn)))
		}
	}

	return fmt.Sprintf("json_build_object(%s)", strings.Join(pairs, ", "))
}

// resolveFields returns which fields to include. Expanded fields are always included.
func (b *StandardBuilder) resolveFields(obj *schema.ObjectDef, params *QueryParams, expandSet map[string]*ExpandPlan) []*schema.FieldDef {
	if len(params.Select) > 0 {
		seen := make(map[string]bool)
		var fields []*schema.FieldDef
		for _, name := range params.Select {
			if f, ok := obj.FieldsByAPIName[name]; ok {
				fields = append(fields, f)
				seen[name] = true
			}
		}
		// Ensure expanded fields are always included
		for name := range expandSet {
			if !seen[name] {
				if f, ok := obj.FieldsByAPIName[name]; ok {
					fields = append(fields, f)
				}
			}
		}
		return fields
	}

	fields := make([]*schema.FieldDef, 0, len(obj.Fields))
	for i := range obj.Fields {
		fields = append(fields, &obj.Fields[i])
	}
	return fields
}

func (b *StandardBuilder) addLateralJoins(qb sq.SelectBuilder, params *QueryParams) sq.SelectBuilder {
	for i := range params.ExpandPlans {
		joinSQL, joinArgs := buildStandardLateral(&params.ExpandPlans[i], stdAlias)
		qb = qb.LeftJoin(joinSQL, joinArgs...)
	}
	return qb
}

func (b *StandardBuilder) applyFilters(qb sq.SelectBuilder, obj *schema.ObjectDef, params *QueryParams) sq.SelectBuilder {
	for _, f := range params.Filters {
		fd := obj.FieldsByAPIName[f.FieldAPIName]
		if fd == nil || fd.StorageColumn == nil {
			continue
		}
		col := fmt.Sprintf(`%s.%s`, qi(stdAlias), qi(*fd.StorageColumn))
		qb = applyFilter(qb, col, f)
	}
	return qb
}

func (b *StandardBuilder) applyOrder(qb sq.SelectBuilder, obj *schema.ObjectDef, params *QueryParams) sq.SelectBuilder {
	if params.Order != nil {
		fd := obj.FieldsByAPIName[params.Order.FieldAPIName]
		if fd != nil && fd.StorageColumn != nil {
			dir := "ASC"
			if params.Order.Desc {
				dir = "DESC"
			}
			qb = qb.OrderBy(fmt.Sprintf(`%s.%s %s, %s."id" %s`,
				qi(stdAlias), qi(*fd.StorageColumn), dir, qi(stdAlias), dir))
		}
	} else {
		qb = qb.OrderBy(fmt.Sprintf(`%s."id" ASC`, qi(stdAlias)))
	}
	return qb
}

func (b *StandardBuilder) applyCursor(qb sq.SelectBuilder, obj *schema.ObjectDef, params *QueryParams) sq.SelectBuilder {
	if params.Cursor == nil {
		return qb
	}
	idCol := fmt.Sprintf(`%s."id"`, qi(stdAlias))

	if params.Order != nil && params.Cursor.OrderVal != "" {
		fd := obj.FieldsByAPIName[params.Order.FieldAPIName]
		if fd != nil && fd.StorageColumn != nil {
			sortCol := fmt.Sprintf(`%s.%s`, qi(stdAlias), qi(*fd.StorageColumn))
			cmp := ">"
			if params.Order.Desc {
				cmp = "<"
			}
			qb = qb.Where(fmt.Sprintf(`(%s, %s) %s (?, ?)`, sortCol, idCol, cmp),
				params.Cursor.OrderVal, params.Cursor.ID)
			return qb
		}
	}

	qb = qb.Where(sq.Gt{idCol: params.Cursor.ID})
	return qb
}
