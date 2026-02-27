package query

import (
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/atlekbai/schema_registry/internal/schema"
	"github.com/google/uuid"
)

const qAlias = "_e"

// Builder generates SQL queries for a given object definition.
type Builder interface {
	BuildList(params *QueryParams) (string, []any, error)
	BuildGetByID(id uuid.UUID, params *QueryParams) (string, []any, error)
	BuildCount(params *QueryParams) (string, []any, error)
	// BuildEstimate returns SELECT 1 FROM ... WHERE ... for use with EXPLAIN (FORMAT JSON).
	BuildEstimate(params *QueryParams) (string, []any, error)
}

// isSystemField returns true for system fields (id, created_at, updated_at)
// that are always emitted by jsonObject and should be skipped in the field loop.
func isSystemField(apiName string) bool {
	return apiName == "id" || apiName == "created_at" || apiName == "updated_at"
}

// QueryBuilder builds SQL for both standard and custom objects.
type QueryBuilder struct {
	obj *schema.ObjectDef
}

// NewBuilder returns a query builder for the given object.
func NewBuilder(obj *schema.ObjectDef) Builder {
	return &QueryBuilder{
		obj: obj,
	}
}

func (b *QueryBuilder) BuildList(params *QueryParams) (string, []any, error) {
	expandSet := makeExpandSet(params.ExpandPlans)
	jsonExpr := buildJsonObject(b.obj, params, expandSet)

	columns := []string{jsonExpr + " AS _row"}
	columns = append(columns, fmt.Sprintf(`%s."id"::text AS _cursor_id`, QI(qAlias)))
	if params.Order != nil {
		fd := b.obj.FieldsByAPIName[params.Order.FieldAPIName]
		if fd != nil {
			col := FilterExpr(qAlias, fd)
			columns = append(columns, fmt.Sprintf(`%s::text AS _cursor_val`, col))
		}
	}

	from, baseWhere := TableSource(b.obj, qAlias)
	qb := sq.Select(columns...).From(from).PlaceholderFormat(sq.Dollar)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}

	qb = addLateralJoins(qb, params)
	for _, cond := range buildFilters(b.obj, params) {
		qb = qb.Where(cond)
	}
	for _, cond := range params.ExtraConditions {
		qb = qb.Where(cond)
	}
	for _, clause := range buildOrderBy(b.obj, params) {
		qb = qb.OrderBy(clause)
	}
	qb = applyCursor(qb, b.obj, params)
	qb = qb.Suffix("LIMIT ?", params.Limit+1)

	return qb.ToSql()
}

func (b *QueryBuilder) BuildGetByID(id uuid.UUID, params *QueryParams) (string, []any, error) {
	expandSet := makeExpandSet(params.ExpandPlans)
	jsonExpr := buildJsonObject(b.obj, params, expandSet)

	columns := []string{jsonExpr + " AS _row"}

	from, baseWhere := TableSource(b.obj, qAlias)
	qb := sq.Select(columns...).
		From(from).
		Where(sq.Eq{QI(qAlias) + `."id"`: id}).
		PlaceholderFormat(sq.Dollar).
		Limit(1)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}

	qb = addLateralJoins(qb, params)

	return qb.ToSql()
}

func (b *QueryBuilder) BuildCount(params *QueryParams) (string, []any, error) {
	from, baseWhere := TableSource(b.obj, qAlias)
	qb := sq.Select("count(*)").From(from).PlaceholderFormat(sq.Dollar)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}
	for _, cond := range buildFilters(b.obj, params) {
		qb = qb.Where(cond)
	}
	for _, cond := range params.ExtraConditions {
		qb = qb.Where(cond)
	}
	return qb.ToSql()
}

func (b *QueryBuilder) BuildEstimate(params *QueryParams) (string, []any, error) {
	from, baseWhere := TableSource(b.obj, qAlias)
	qb := sq.Select("1").From(from).PlaceholderFormat(sq.Dollar)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}
	for _, cond := range buildFilters(b.obj, params) {
		qb = qb.Where(cond)
	}
	for _, cond := range params.ExtraConditions {
		qb = qb.Where(cond)
	}
	return qb.ToSql()
}

// buildJsonObject builds a json_build_object(...) expression for the SELECT clause.
func buildJsonObject(obj *schema.ObjectDef, params *QueryParams, expandSet map[string]*ExpandPlan) string {
	var pairs []string
	pairs = append(pairs,
		fmt.Sprintf(`'id', %s."id"`, QI(qAlias)),
		fmt.Sprintf(`'created_at', %s."created_at"`, QI(qAlias)),
		fmt.Sprintf(`'updated_at', %s."updated_at"`, QI(qAlias)),
	)

	for _, f := range resolveFields(obj, params, expandSet) {
		if isSystemField(f.APIName) {
			continue
		}
		if ep, ok := expandSet[f.APIName]; ok {
			alias := expandAlias(ep.FieldName)
			pairs = append(pairs, fmt.Sprintf(`%s, %s`, QuoteLit(f.APIName), expandExpr(alias)))
		} else {
			pairs = append(pairs, fmt.Sprintf(`%s, %s`, QuoteLit(jsonKey(f)), SelectFieldExpr(qAlias, f)))
		}
	}

	return fmt.Sprintf("json_build_object(%s)", strings.Join(pairs, ", "))
}

// resolveFields returns which fields to include. Expanded fields are always included.
func resolveFields(obj *schema.ObjectDef, params *QueryParams, expandSet map[string]*ExpandPlan) []*schema.FieldDef {
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

func addLateralJoins(qb sq.SelectBuilder, params *QueryParams) sq.SelectBuilder {
	for i := range params.ExpandPlans {
		ep := &params.ExpandPlans[i]
		outerRef := fkRef(qAlias, ep.Field)
		joinSQL, joinArgs := buildLateral(ep, outerRef, "", 0)
		qb = qb.LeftJoin(joinSQL, joinArgs...)
	}
	return qb
}

func buildFilters(obj *schema.ObjectDef, params *QueryParams) []sq.Sqlizer {
	var conds []sq.Sqlizer
	for _, f := range params.Filters {
		if fd := obj.FieldsByAPIName[f.FieldAPIName]; fd != nil {
			conds = append(conds, filterCondition(FilterExpr(qAlias, fd), f))
		}
	}
	return conds
}

func buildOrderBy(obj *schema.ObjectDef, params *QueryParams) []string {
	var (
		clauses []string
		dir     = orderDir(params)
	)

	if params.Order != nil {
		if fd := obj.FieldsByAPIName[params.Order.FieldAPIName]; fd != nil {
			clauses = append(clauses, fmt.Sprintf(`%s %s`, FilterExpr(qAlias, fd), dir))
		}
	}

	clauses = append(clauses, fmt.Sprintf(`%s."id" %s`, QI(qAlias), dir))
	return clauses
}

func orderDir(params *QueryParams) string {
	if params.Order != nil && params.Order.Desc {
		return "DESC"
	}
	return "ASC"
}

func applyCursor(qb sq.SelectBuilder, obj *schema.ObjectDef, params *QueryParams) sq.SelectBuilder {
	if params.Cursor == nil {
		return qb
	}
	idCol := fmt.Sprintf(`%s."id"`, QI(qAlias))

	if params.Order != nil && params.Cursor.OrderVal != "" {
		fd := obj.FieldsByAPIName[params.Order.FieldAPIName]
		if fd != nil {
			sortCol := FilterExpr(qAlias, fd)
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
