package query

import (
	"fmt"
	"strings"

	"github.com/atlekbai/schema_registry/internal/schema"
)

// qi is shorthand for schema.QuoteIdent.
func qi(name string) string { return schema.QuoteIdent(name) }

// quoteLit returns a single-quoted SQL string literal with escaping.
func quoteLit(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
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
		fmt.Sprintf(`%s."id"`, qi(inner)),
		fmt.Sprintf(`%s."created_at"`, qi(inner)),
		fmt.Sprintf(`%s."updated_at"`, qi(inner)),
	)

	for _, f := range target.Fields {
		if isSystemField(f.APIName) {
			continue
		}
		if child, ok := childSet[f.APIName]; ok && depth < maxExpandDepth-1 {
			childName := name + "__" + child.FieldName
			childAlias := expandAlias(childName)
			cols = append(cols, fmt.Sprintf(
				`CASE WHEN %s."id" IS NOT NULL THEN row_to_json(%s.*)::jsonb ELSE NULL END AS %s`,
				qi(childAlias), qi(childAlias), qi(f.APIName)))

			childRef := fkRef(inner, child.Field)
			nj, na := buildLateral(child, childRef, name+"__", depth+1)
			nestedJoins = append(nestedJoins, nj)
			args = append(args, na...)
		} else if f.StorageColumn != nil || !target.IsStandard {
			cols = append(cols, fmt.Sprintf(`%s AS %s`,
				selectExpr(inner, &f), qi(f.APIName)))
		}
	}

	// FROM + WHERE
	var from, whereClause string
	if target.IsStandard {
		from = target.TableName() + " " + qi(inner)
		whereClause = fmt.Sprintf(`%s."id" = %s`, qi(inner), outerRef)
	} else {
		from = fmt.Sprintf(`"metadata"."records" %s`, qi(inner))
		whereClause = fmt.Sprintf(`%s."object_id" = ? AND %s."id" = %s`, qi(inner), qi(inner), outerRef)
		args = append(args, target.ID)
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
		whereClause,
		qi(alias))

	return sql, args
}
