package pg

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	hrqlpg "github.com/atlekbai/schema_registry/internal/hrql/dialect/pg"
)

// expandCase builds: CASE WHEN alias."id" IS NOT NULL THEN to_jsonb(alias.*) ELSE NULL END
//
// Used in both buildJsonObject (for the jsonb_build_object pair value) and
// lateralJoin (as an aliased column in the inner SELECT). The NULL branch
// handles the LEFT JOIN case where no matching row exists.
func expandCase(alias string) sq.CaseBuilder {
	qi := hrqlpg.QI(alias)
	return sq.Case().
		When(sq.NotEq{qi + `."id"`: nil}, sq.Expr(fmt.Sprintf("to_jsonb(%s.*)", qi))).
		Else(sq.Expr("NULL"))
}

// expandAlias returns the join alias for an expand field, e.g. "_xp_organization".
func expandAlias(fieldName string) string { return "_xp_" + fieldName }

// expandInner returns the inner table alias inside a lateral, e.g. "_xp_organization_t".
func expandInner(fieldName string) string { return "_xp_" + fieldName + "_t" }

// makeExpandSet indexes expand plans by field name for O(1) lookup.
func makeExpandSet(plans []ExpandPlan) map[string]*ExpandPlan {
	m := make(map[string]*ExpandPlan, len(plans))
	for i := range plans {
		m[plans[i].FieldName] = &plans[i]
	}
	return m
}

// maxExpandDepth limits nested lateral join recursion (e.g. expand=manager.department).
const maxExpandDepth = 2

// lateralJoin builds a LEFT JOIN LATERAL clause as a Sqlizer for use with JoinClause.
//
// Given an expand on "department", the generated SQL looks like:
//
//	LEFT JOIN LATERAL (
//	    SELECT "_xp_department_t"."id", ..., "_xp_department_t"."title" AS "title"
//	    FROM "core"."departments" "_xp_department_t"
//	    WHERE "_xp_department_t"."id" = "_e"."department_id"
//	) "_xp_department" ON TRUE
//
// For nested expands (e.g. expand=department.parent), the inner SELECT itself
// contains a LEFT JOIN LATERAL for the child, with aliases namespaced via
// prefix ("department__parent") to avoid collisions. Recursion is capped at
// maxExpandDepth.
//
// Parameters:
//   - ep: the expand plan describing the field, target object, and children
//   - outerRef: SQL expression for the FK in the outer query (e.g. "_e"."department_id")
//   - prefix: namespace prefix for nested aliases (empty at top level)
//   - depth: current nesting depth (0 = top level)
func lateralJoin(ep *ExpandPlan, outerRef, prefix string, depth int) sq.Sqlizer {
	target := ep.Target
	name := prefix + ep.FieldName
	inner := expandInner(name) // table alias inside the lateral subquery
	qi := hrqlpg.QI(inner)

	// Inner SELECT: system columns + FROM + WHERE id = outer FK
	from, baseWhere := hrqlpg.TableSource(target, inner)
	qb := sq.Select(qi+`."id"`, qi+`."created_at"`, qi+`."updated_at"`).From(from)
	if baseWhere != nil {
		qb = qb.Where(baseWhere) // custom objects: WHERE object_id = ?
	}
	qb = qb.Where(sq.Expr(qi + `."id" = ` + outerRef))

	// Add field columns. Expanded children become CASE WHEN expressions
	// with their own nested lateral joins; regular fields use SelectExpr.
	childSet := makeExpandSet(ep.Children)
	childPrefix := name + "__"
	for i := range target.Fields {
		f := &target.Fields[i]
		if hrqlpg.IsSystemField(f.APIName) {
			continue
		}
		col := hrqlpg.QI(f.APIName)
		if child, ok := childSet[f.APIName]; ok && depth < maxExpandDepth-1 {
			// Expanded child: CASE WHEN _xp_child."id" IS NOT NULL THEN to_jsonb(...) END
			qb = qb.Column(sq.Alias(expandCase(expandAlias(childPrefix+child.FieldName)), col))
			// Recurse: add nested LEFT JOIN LATERAL for the child
			qb = qb.JoinClause(lateralJoin(child, hrqlpg.FKRef(inner, child.Field), childPrefix, depth+1))
		} else {
			// Regular field: column AS "api_name"
			qb = qb.Column(sq.Alias(sq.Expr(hrqlpg.SelectExpr(inner, f)), col))
		}
	}

	// Wrap inner SELECT in LEFT JOIN LATERAL (...) "alias" ON TRUE
	return sq.Expr(fmt.Sprintf(`LEFT JOIN LATERAL (?) %s ON TRUE`, hrqlpg.QI(expandAlias(name))), qb)
}

// addLateralJoins appends LEFT JOIN LATERAL clauses for each expand plan.
func addLateralJoins(qb sq.SelectBuilder, plans []ExpandPlan) sq.SelectBuilder {
	for i := range plans {
		ep := &plans[i]
		outerRef := hrqlpg.FKRef(hrqlpg.Alias(), ep.Field)
		qb = qb.JoinClause(lateralJoin(ep, outerRef, "", 0))
	}
	return qb
}
