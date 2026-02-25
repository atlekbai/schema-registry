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

// expandAlias returns the join alias for an expand field, e.g. "_xp_Organization".
func expandAlias(fieldName string) string { return "_xp_" + fieldName }

// expandInner returns the inner table alias inside a lateral, e.g. "_xp_Organization_t".
func expandInner(fieldName string) string { return "_xp_" + fieldName + "_t" }

// makeExpandSet indexes expand plans by field name.
func makeExpandSet(plans []ExpandPlan) map[string]*ExpandPlan {
	m := make(map[string]*ExpandPlan, len(plans))
	for i := range plans {
		m[plans[i].FieldName] = &plans[i]
	}
	return m
}

// buildStandardLateral builds a LEFT JOIN LATERAL clause for a standard source object.
// outerAlias is the alias of the outer table (e.g. "_e").
func buildStandardLateral(ep *ExpandPlan, outerAlias string) (sql string, args []any) {
	fkCol := *ep.Field.StorageColumn
	outerRef := fmt.Sprintf(`%s.%s`, qi(outerAlias), qi(fkCol))
	return buildLateral(ep, outerRef, "")
}

// buildCustomLateral builds a LEFT JOIN LATERAL clause for a custom source object.
func buildCustomLateral(ep *ExpandPlan, outerAlias string) (sql string, args []any) {
	outerRef := fmt.Sprintf(`(%s."data"->>%s)::uuid`, qi(outerAlias), quoteLit(ep.FieldName))
	return buildLateral(ep, outerRef, "")
}

// buildLateral builds the LATERAL join SQL for an expand plan.
// outerRef is the SQL expression referencing the FK from the outer query.
// prefix namespaces nested aliases to avoid collisions.
func buildLateral(ep *ExpandPlan, outerRef, prefix string) (sql string, args []any) {
	target := ep.Target
	name := prefix + ep.FieldName
	inner := expandInner(name)
	alias := expandAlias(name)

	childSet := makeExpandSet(ep.Children)

	var cols []string
	var nestedJoins []string

	if target.IsStandard {
		cols = append(cols,
			fmt.Sprintf(`%s."id"`, qi(inner)),
			fmt.Sprintf(`%s."created_at"`, qi(inner)),
			fmt.Sprintf(`%s."updated_at"`, qi(inner)),
		)
		for _, f := range target.Fields {
			if f.StorageColumn == nil {
				continue
			}
			if child, ok := childSet[f.APIName]; ok {
				childName := name + "__" + child.FieldName
				childAlias := expandAlias(childName)
				cols = append(cols, fmt.Sprintf(
					`CASE WHEN %s."id" IS NOT NULL THEN row_to_json(%s.*)::jsonb ELSE NULL END AS %s`,
					qi(childAlias), qi(childAlias), qi(f.APIName)))

				childRef := fmt.Sprintf(`%s.%s`, qi(inner), qi(*child.Field.StorageColumn))
				nj, na := buildNestedLateral(child, childRef, name+"__")
				nestedJoins = append(nestedJoins, nj)
				args = append(args, na...)
			} else {
				cols = append(cols, fmt.Sprintf(`%s.%s AS %s`,
					qi(inner), qi(*f.StorageColumn), qi(f.APIName)))
			}
		}
		sql = fmt.Sprintf(`LATERAL (SELECT %s FROM %s %s %s WHERE %s."id" = %s) %s ON TRUE`,
			strings.Join(cols, ", "),
			target.TableName(), qi(inner),
			strings.Join(nestedJoins, " "),
			qi(inner), outerRef, qi(alias))
	} else {
		// Custom target: select id, timestamps, data + any nested expansions
		cols = append(cols,
			fmt.Sprintf(`%s."id"`, qi(inner)),
			fmt.Sprintf(`%s."created_at"`, qi(inner)),
			fmt.Sprintf(`%s."updated_at"`, qi(inner)),
			fmt.Sprintf(`%s."data"`, qi(inner)),
		)
		for _, child := range ep.Children {
			childName := name + "__" + child.FieldName
			childAlias := expandAlias(childName)
			cols = append(cols, fmt.Sprintf(
				`CASE WHEN %s."id" IS NOT NULL THEN row_to_json(%s.*)::jsonb ELSE NULL END AS %s`,
				qi(childAlias), qi(childAlias), qi(child.FieldName)))

			childRef := fmt.Sprintf(`(%s."data"->>%s)::uuid`, qi(inner), quoteLit(child.FieldName))
			nj, na := buildNestedLateral(&child, childRef, name+"__")
			nestedJoins = append(nestedJoins, nj)
			args = append(args, na...)
		}
		sql = fmt.Sprintf(
			`LATERAL (SELECT %s FROM "metadata"."records" %s %s WHERE %s."object_id" = ? AND %s."id" = %s) %s ON TRUE`,
			strings.Join(cols, ", "),
			qi(inner),
			strings.Join(nestedJoins, " "),
			qi(inner), qi(inner), outerRef, qi(alias))
		args = append(args, target.ID)
	}

	return sql, args
}

// buildNestedLateral builds a level-2 lateral join (no further nesting).
func buildNestedLateral(child *ExpandPlan, outerRef, prefix string) (sql string, args []any) {
	target := child.Target
	name := prefix + child.FieldName
	inner := expandInner(name)
	alias := expandAlias(name)

	var cols []string

	if target.IsStandard {
		cols = append(cols,
			fmt.Sprintf(`%s."id"`, qi(inner)),
			fmt.Sprintf(`%s."created_at"`, qi(inner)),
			fmt.Sprintf(`%s."updated_at"`, qi(inner)),
		)
		for _, f := range target.Fields {
			if f.StorageColumn != nil {
				cols = append(cols, fmt.Sprintf(`%s.%s AS %s`,
					qi(inner), qi(*f.StorageColumn), qi(f.APIName)))
			}
		}
		sql = fmt.Sprintf(`LEFT JOIN LATERAL (SELECT %s FROM %s %s WHERE %s."id" = %s) %s ON TRUE`,
			strings.Join(cols, ", "),
			target.TableName(), qi(inner),
			qi(inner), outerRef, qi(alias))
	} else {
		cols = append(cols,
			fmt.Sprintf(`%s."id"`, qi(inner)),
			fmt.Sprintf(`%s."created_at"`, qi(inner)),
			fmt.Sprintf(`%s."updated_at"`, qi(inner)),
			fmt.Sprintf(`%s."data"`, qi(inner)),
		)
		sql = fmt.Sprintf(
			`LEFT JOIN LATERAL (SELECT %s FROM "metadata"."records" %s WHERE %s."object_id" = ? AND %s."id" = %s) %s ON TRUE`,
			strings.Join(cols, ", "),
			qi(inner),
			qi(inner), qi(inner), outerRef, qi(alias))
		args = append(args, target.ID)
	}

	return sql, args
}
