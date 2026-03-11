package pg

import (
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	hrqlpg "github.com/atlekbai/schema_registry/internal/hrql/dialect/pg"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// Builder generates SQL queries for a given object definition.
type Builder interface {
	BuildList(params *QueryParams) (string, []any, error)
	BuildCount(params *QueryParams) (string, []any, error)
	// BuildEstimate returns SELECT 1 FROM ... WHERE ... for use with EXPLAIN (FORMAT JSON).
	BuildEstimate(params *QueryParams) (string, []any, error)
}

// QueryBuilder builds SQL for both standard and custom objects.
type QueryBuilder struct {
	obj *schema.ObjectDef
}

// NewBuilder returns a query builder for the given object.
func NewBuilder(obj *schema.ObjectDef) Builder {
	return &QueryBuilder{obj: obj}
}

func (b *QueryBuilder) BuildList(params *QueryParams) (string, []any, error) {
	expandSet := makeExpandSet(params.ExpandPlans)
	jsonExpr := buildJsonObject(b.obj, params, expandSet)

	columns := []string{jsonExpr + " AS _row"}

	from, baseWhere := hrqlpg.TableSource(b.obj, hrqlpg.Alias())
	qb := sq.Select(columns...).From(from).PlaceholderFormat(sq.Dollar)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}

	qb = addLateralJoins(qb, params)
	for _, cond := range params.SQLConditions {
		qb = qb.Where(cond)
	}
	for _, clause := range buildOrderBy(b.obj, params) {
		qb = qb.OrderBy(clause)
	}
	qb = qb.Suffix("LIMIT ?", params.Limit+1)

	return qb.ToSql()
}

func (b *QueryBuilder) BuildCount(params *QueryParams) (string, []any, error) {
	from, baseWhere := hrqlpg.TableSource(b.obj, hrqlpg.Alias())
	qb := sq.Select("count(*)").From(from).PlaceholderFormat(sq.Dollar)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}
	for _, cond := range params.SQLConditions {
		qb = qb.Where(cond)
	}
	return qb.ToSql()
}

func (b *QueryBuilder) BuildEstimate(params *QueryParams) (string, []any, error) {
	from, baseWhere := hrqlpg.TableSource(b.obj, hrqlpg.Alias())
	qb := sq.Select("1").From(from).PlaceholderFormat(sq.Dollar)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}
	for _, cond := range params.SQLConditions {
		qb = qb.Where(cond)
	}
	return qb.ToSql()
}

// buildJsonObject builds a json_build_object(...) expression for the SELECT clause.
func buildJsonObject(obj *schema.ObjectDef, params *QueryParams, expandSet map[string]*ExpandPlan) string {
	alias := hrqlpg.Alias()
	var pairs []string
	pairs = append(pairs,
		fmt.Sprintf(`'id', %s."id"`, hrqlpg.QI(alias)),
		fmt.Sprintf(`'created_at', %s."created_at"`, hrqlpg.QI(alias)),
		fmt.Sprintf(`'updated_at', %s."updated_at"`, hrqlpg.QI(alias)),
	)

	for _, f := range resolveFields(obj, params, expandSet) {
		if hrqlpg.IsSystemField(f.APIName) {
			continue
		}
		if ep, ok := expandSet[f.APIName]; ok {
			alias := expandAlias(ep.FieldName)
			pairs = append(pairs, fmt.Sprintf(`%s, %s`, hrqlpg.QuoteLit(f.APIName), expandExpr(alias)))
		} else {
			pairs = append(pairs, fmt.Sprintf(`%s, %s`, hrqlpg.QuoteLit(hrqlpg.JSONKey(f)), hrqlpg.SelectExpr(alias, f)))
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
		outerRef := hrqlpg.FKRef(hrqlpg.Alias(), ep.Field)
		joinSQL, joinArgs := buildLateral(ep, outerRef, "", 0)
		qb = qb.LeftJoin(joinSQL, joinArgs...)
	}
	return qb
}

func buildOrderBy(obj *schema.ObjectDef, params *QueryParams) []string {
	alias := hrqlpg.Alias()
	var (
		clauses []string
		dir     = orderDir(params)
	)

	if params.Order != nil {
		if fd := obj.FieldsByAPIName[params.Order.Field]; fd != nil {
			clauses = append(clauses, fmt.Sprintf(`%s %s`, hrqlpg.FilterExpr(alias, fd), dir))
		}
	}

	clauses = append(clauses, fmt.Sprintf(`%s."id" %s`, hrqlpg.QI(alias), dir))
	return clauses
}

func orderDir(params *QueryParams) string {
	if params.Order != nil && params.Order.Desc {
		return "DESC"
	}
	return "ASC"
}
