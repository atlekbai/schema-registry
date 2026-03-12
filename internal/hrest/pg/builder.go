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
	BuildList(params *QueryParams) sq.Sqlizer
	BuildCount(params *QueryParams) sq.Sqlizer
}

// QueryBuilder builds SQL for both standard and custom objects.
type QueryBuilder struct {
	obj *schema.ObjectDef
}

// NewBuilder returns a query builder for the given object.
func NewBuilder(obj *schema.ObjectDef) Builder {
	return &QueryBuilder{obj: obj}
}

func (b *QueryBuilder) BuildList(params *QueryParams) sq.Sqlizer {
	expandSet := makeExpandSet(params.ExpandPlans)

	from, baseWhere := hrqlpg.TableSource(b.obj, hrqlpg.Alias())
	qb := hrqlpg.PG.Select().
		Column(sq.Alias(jsonbBuildObject(b.obj, params, expandSet), "_row")).
		From(from)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}

	qb = addLateralJoins(qb, params.ExpandPlans)
	for _, cond := range params.SQLConditions {
		qb = qb.Where(cond)
	}
	qb = hrqlpg.AddOrderBy(qb, b.obj, params.Order)
	qb = qb.Limit(uint64(params.Limit + 1))

	return qb
}

func (b *QueryBuilder) BuildCount(params *QueryParams) sq.Sqlizer {
	from, baseWhere := hrqlpg.TableSource(b.obj, hrqlpg.Alias())
	qb := hrqlpg.PG.Select("count(*)").From(from)
	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}
	for _, cond := range params.SQLConditions {
		qb = qb.Where(cond)
	}
	return qb
}

// jsonbBuildObject builds a jsonb_build_object(...) expression as a Sqlizer for the SELECT clause.
func jsonbBuildObject(obj *schema.ObjectDef, params *QueryParams, expandSet map[string]*ExpandPlan) sq.Sqlizer {
	alias := hrqlpg.Alias()
	qi := hrqlpg.QI(alias)
	var pairs []string
	var args []any

	pairs = append(pairs,
		fmt.Sprintf(`'id', %s."id"`, qi),
		fmt.Sprintf(`'created_at', %s."created_at"`, qi),
		fmt.Sprintf(`'updated_at', %s."updated_at"`, qi),
	)

	for _, f := range resolveFields(obj, params, expandSet) {
		if hrqlpg.IsSystemField(f.APIName) {
			continue
		}
		if ep, ok := expandSet[f.APIName]; ok {
			caseSQL, caseArgs, _ := expandCase(expandAlias(ep.FieldName)).ToSql()
			pairs = append(pairs, hrqlpg.QuoteLit(f.APIName)+", "+caseSQL)
			args = append(args, caseArgs...)
		} else {
			pairs = append(pairs, hrqlpg.QuoteLit(hrqlpg.JSONKey(f))+", "+hrqlpg.SelectExpr(alias, f))
		}
	}

	return sq.Expr("jsonb_build_object("+strings.Join(pairs, ", ")+")", args...)
}
