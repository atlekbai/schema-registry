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

// ResolveExpands resolves expand strings into ExpandPlans using the schema cache.
func ResolveExpands(expands []string, obj *schema.ObjectDef, cache *schema.Cache) []ExpandPlan {
	if len(expands) == 0 {
		return nil
	}

	type nested struct{ parent, child string }
	var level1 []string
	var level2 []nested

	for _, f := range expands {
		if before, after, ok := strings.Cut(f, "."); ok {
			level1 = append(level1, before)
			level2 = append(level2, nested{before, after})
		} else {
			level1 = append(level1, f)
		}
	}

	seen := make(map[string]bool)
	planMap := make(map[string]*ExpandPlan)
	var ordered []string

	for _, fn := range level1 {
		if seen[fn] {
			continue
		}
		seen[fn] = true

		fd := obj.FieldsByAPIName[fn]
		if fd == nil || fd.Type != schema.FieldLookup || fd.LookupObjectID == nil {
			continue
		}
		target := cache.GetByID(*fd.LookupObjectID)
		if target == nil {
			continue
		}
		planMap[fn] = &ExpandPlan{FieldName: fn, Field: fd, Target: target}
		ordered = append(ordered, fn)
	}

	for _, n := range level2 {
		ep := planMap[n.parent]
		if ep == nil {
			continue
		}
		childFd := ep.Target.FieldsByAPIName[n.child]
		if childFd == nil || childFd.Type != schema.FieldLookup || childFd.LookupObjectID == nil {
			continue
		}
		childTarget := cache.GetByID(*childFd.LookupObjectID)
		if childTarget == nil {
			continue
		}
		ep.Children = append(ep.Children, ExpandPlan{
			FieldName: n.child, Field: childFd, Target: childTarget,
		})
	}

	var plans []ExpandPlan
	for _, fn := range ordered {
		plans = append(plans, *planMap[fn])
	}
	return plans
}
