package query

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/atlekbai/schema_registry/internal/schema"
	"github.com/google/uuid"
)

// CustomBuilder builds SQL for custom objects stored in metadata.records JSONB.
type CustomBuilder struct{}

const cstAlias = "_e"

func (b *CustomBuilder) BuildList(obj *schema.ObjectDef, params *QueryParams) (string, []any, error) {
	columns := b.selectColumns(params)

	qb := sq.Select(columns...).
		From(`"metadata"."records" ` + qi(cstAlias)).
		Where(sq.Eq{qi(cstAlias) + `."object_id"`: obj.ID}).
		PlaceholderFormat(sq.Dollar)

	qb = b.addLateralJoins(qb, params)
	qb = b.applyFilters(qb, obj, params)
	qb = b.applyOrder(qb, obj, params)

	qb = b.applyCursor(qb, obj, params)

	qb = qb.Limit(uint64(params.Limit + 1))

	return qb.ToSql()
}

func (b *CustomBuilder) BuildGetByID(obj *schema.ObjectDef, id uuid.UUID, params *QueryParams) (string, []any, error) {
	columns := b.selectColumns(params)

	qb := sq.Select(columns...).
		From(`"metadata"."records" ` + qi(cstAlias)).
		Where(sq.Eq{qi(cstAlias) + `."object_id"`: obj.ID, qi(cstAlias) + `."id"`: id}).
		PlaceholderFormat(sq.Dollar).
		Limit(1)

	qb = b.addLateralJoins(qb, params)

	return qb.ToSql()
}

func (b *CustomBuilder) selectColumns(params *QueryParams) []string {
	columns := []string{
		fmt.Sprintf(`%s."id"`, qi(cstAlias)),
		fmt.Sprintf(`%s."created_at"`, qi(cstAlias)),
		fmt.Sprintf(`%s."updated_at"`, qi(cstAlias)),
		fmt.Sprintf(`%s."data"`, qi(cstAlias)),
	}

	// Add expanded fields as JSONB columns from lateral joins
	for _, ep := range params.ExpandPlans {
		alias := expandAlias(ep.FieldName)
		columns = append(columns, fmt.Sprintf(
			`CASE WHEN %s."id" IS NOT NULL THEN row_to_json(%s.*)::jsonb ELSE NULL END AS %s`,
			qi(alias), qi(alias), qi(ep.FieldName)))
	}

	return columns
}

func (b *CustomBuilder) BuildCount(obj *schema.ObjectDef, params *QueryParams) (string, []any, error) {
	qb := sq.Select("count(*)").
		From(`"metadata"."records" ` + qi(cstAlias)).
		Where(sq.Eq{qi(cstAlias) + `."object_id"`: obj.ID}).
		PlaceholderFormat(sq.Dollar)

	qb = b.applyFilters(qb, obj, params)

	return qb.ToSql()
}

func (b *CustomBuilder) addLateralJoins(qb sq.SelectBuilder, params *QueryParams) sq.SelectBuilder {
	for i := range params.ExpandPlans {
		joinSQL, joinArgs := buildCustomLateral(&params.ExpandPlans[i], cstAlias)
		qb = qb.LeftJoin(joinSQL, joinArgs...)
	}
	return qb
}

func (b *CustomBuilder) applyFilters(qb sq.SelectBuilder, obj *schema.ObjectDef, params *QueryParams) sq.SelectBuilder {
	for _, f := range params.Filters {
		fd := obj.FieldsByAPIName[f.FieldAPIName]
		if fd == nil {
			continue
		}
		col := jsonbAccessor(cstAlias, f.FieldAPIName, fd)
		qb = applyFilter(qb, col, f)
	}
	return qb
}

func (b *CustomBuilder) applyOrder(qb sq.SelectBuilder, obj *schema.ObjectDef, params *QueryParams) sq.SelectBuilder {
	if params.Order != nil {
		fd := obj.FieldsByAPIName[params.Order.FieldAPIName]
		if fd != nil {
			col := jsonbAccessor(cstAlias, params.Order.FieldAPIName, fd)
			dir := "ASC"
			if params.Order.Desc {
				dir = "DESC"
			}
			qb = qb.OrderBy(fmt.Sprintf(`%s %s, %s."id" %s`, col, dir, qi(cstAlias), dir))
		}
	} else {
		qb = qb.OrderBy(fmt.Sprintf(`%s."id" ASC`, qi(cstAlias)))
	}
	return qb
}

func (b *CustomBuilder) applyCursor(qb sq.SelectBuilder, obj *schema.ObjectDef, params *QueryParams) sq.SelectBuilder {
	if params.Cursor == nil {
		return qb
	}
	idCol := fmt.Sprintf(`%s."id"`, qi(cstAlias))

	if params.Order != nil && params.Cursor.OrderVal != "" {
		fd := obj.FieldsByAPIName[params.Order.FieldAPIName]
		if fd != nil {
			sortCol := jsonbAccessor(cstAlias, params.Order.FieldAPIName, fd)
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

// jsonbAccessor returns the JSONB extraction expression with appropriate type casting.
func jsonbAccessor(alias, fieldName string, fd *schema.FieldDef) string {
	base := fmt.Sprintf(`%s."data"->>%s`, qi(alias), quoteLit(fieldName))
	if fd.IsNumeric() {
		return fmt.Sprintf(`(%s."data"->>%s)::numeric`, qi(alias), quoteLit(fieldName))
	}
	if fd.Type == schema.FieldDate || fd.Type == schema.FieldDatetime {
		return fmt.Sprintf(`(%s."data"->>%s)::timestamptz`, qi(alias), quoteLit(fieldName))
	}
	return base
}
