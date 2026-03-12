package pg

import (
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/atlekbai/schema_registry/internal/hrest"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// ExpandPlan describes a lookup field expansion with optional nested children.
type ExpandPlan struct {
	FieldName string
	Field     *schema.FieldDef
	Target    *schema.ObjectDef
	Children  []ExpandPlan
}

// QueryParams extends hrest.Params with SQL-specific fields.
type QueryParams struct {
	hrest.Params
	ExpandPlans   []ExpandPlan
	SQLConditions []sq.Sqlizer
}

// ResolveExpands resolves dot-separated expand strings (e.g. "manager.department.parent")
// into a tree of ExpandPlans up to maxDepth levels deep. Segments beyond maxDepth are ignored.
func ResolveExpands(expands []string, obj *schema.ObjectDef, cache *schema.Cache, maxDepth int) []ExpandPlan {
	if len(expands) == 0 || maxDepth <= 0 {
		return nil
	}

	// Group expand paths by their first segment.
	type entry struct {
		plan *ExpandPlan
		rest []string // remaining dot-separated tails
	}
	seen := make(map[string]*entry)
	var ordered []string

	for _, path := range expands {
		head, tail, _ := strings.Cut(path, ".")

		e, ok := seen[head]
		if !ok {
			fd := obj.FieldsByAPIName[head]
			if fd == nil || fd.Type != schema.FieldLookup || fd.LookupObjectID == nil {
				continue
			}
			target := cache.GetByID(*fd.LookupObjectID)
			if target == nil {
				continue
			}
			e = &entry{plan: &ExpandPlan{FieldName: head, Field: fd, Target: target}}
			seen[head] = e
			ordered = append(ordered, head)
		}
		if tail != "" {
			e.rest = append(e.rest, tail)
		}
	}

	plans := make([]ExpandPlan, 0, len(ordered))
	for _, name := range ordered {
		e := seen[name]
		e.plan.Children = ResolveExpands(e.rest, e.plan.Target, cache, maxDepth-1)
		plans = append(plans, *e.plan)
	}
	return plans
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
