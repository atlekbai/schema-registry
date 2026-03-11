package pg

import (
	"fmt"
	"strings"

	hrqlpg "github.com/atlekbai/schema_registry/internal/hrql/dialect/pg"
)

// expandExpr returns a CASE WHEN expression for a laterally-joined expanded field.
func expandExpr(alias string) string {
	return fmt.Sprintf(`CASE WHEN %s."id" IS NOT NULL THEN to_jsonb(%s.*) ELSE NULL END`,
		hrqlpg.QI(alias), hrqlpg.QI(alias))
}

// expandAlias returns the join alias for an expand field, e.g. "_xp_organization".
func expandAlias(fieldName string) string { return "_xp_" + fieldName }

// expandInner returns the inner table alias inside a lateral, e.g. "_xp_organization_t".
func expandInner(fieldName string) string { return "_xp_" + fieldName + "_t" }

// makeExpandSet indexes expand plans by field name.
func makeExpandSet(plans []ExpandPlan) map[string]*ExpandPlan {
	m := make(map[string]*ExpandPlan, len(plans))
	for i := range plans {
		m[plans[i].FieldName] = &plans[i]
	}
	return m
}

const maxExpandDepth = 2

// buildLateral builds a LATERAL join clause for an expand plan.
// outerRef is the SQL expression referencing the FK from the outer query.
// prefix namespaces nested aliases to avoid collisions.
// depth controls recursion: 0 = top level (caller adds LEFT JOIN via Squirrel), 1+ = nested.
func buildLateral(ep *ExpandPlan, outerRef, prefix string, depth int) (sql string, args []any) {
	target := ep.Target
	name := prefix + ep.FieldName
	inner := expandInner(name)
	alias := expandAlias(name)

	childSet := makeExpandSet(ep.Children)

	var cols []string
	var nestedJoins []string

	// System fields — always included
	cols = append(cols,
		fmt.Sprintf(`%s."id"`, hrqlpg.QI(inner)),
		fmt.Sprintf(`%s."created_at"`, hrqlpg.QI(inner)),
		fmt.Sprintf(`%s."updated_at"`, hrqlpg.QI(inner)),
	)

	for _, f := range target.Fields {
		if hrqlpg.IsSystemField(f.APIName) {
			continue
		}
		if child, ok := childSet[f.APIName]; ok && depth < maxExpandDepth-1 {
			childName := name + "__" + child.FieldName
			childAlias := expandAlias(childName)
			cols = append(cols, fmt.Sprintf(`%s AS %s`, expandExpr(childAlias), hrqlpg.QI(f.APIName)))

			childRef := hrqlpg.FKRef(inner, child.Field)
			nj, na := buildLateral(child, childRef, name+"__", depth+1)
			nestedJoins = append(nestedJoins, nj)
			args = append(args, na...)
		} else {
			cols = append(cols, fmt.Sprintf(`%s AS %s`, hrqlpg.SelectExpr(inner, &f), hrqlpg.QI(f.APIName)))
		}
	}

	from, baseWhere := hrqlpg.TableSource(target, inner)
	joinCond := fmt.Sprintf(`%s."id" = %s`, hrqlpg.QI(inner), outerRef)
	if baseWhere != nil {
		baseSql, baseArgs, _ := baseWhere.ToSql()
		joinCond = baseSql + " AND " + joinCond
		args = append(args, baseArgs...)
	}

	joinPrefix := ""
	if depth > 0 {
		joinPrefix = "LEFT JOIN "
	}

	sql = fmt.Sprintf(`%sLATERAL (SELECT %s FROM %s %s WHERE %s) %s ON TRUE`,
		joinPrefix,
		strings.Join(cols, ", "),
		from,
		strings.Join(nestedJoins, " "),
		joinCond,
		hrqlpg.QI(alias))

	return sql, args
}
