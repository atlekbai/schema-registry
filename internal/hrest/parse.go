package hrest

import (
	"fmt"
	"strings"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/schema"
)

const (
	defaultLimit = 50
	maxLimit     = 200
)

// ParseParams validates and parses proto request fields into Params.
// If obj is nil, field-existence validation is skipped.
func ParseParams(obj *schema.ObjectDef, opts QueryOpts) (*Params, error) {
	p := &Params{
		Limit: defaultLimit,
	}

	// select
	if opts.Sel != "" {
		for f := range strings.SplitSeq(opts.Sel, ",") {
			f = strings.TrimSpace(f)
			if f == "" {
				continue
			}
			if obj != nil {
				if _, ok := obj.FieldsByAPIName[f]; !ok {
					return nil, fmt.Errorf("unknown field %q in select", f)
				}
			}
			p.Select = append(p.Select, f)
		}
	}

	// expand
	if opts.Expand != "" {
		for f := range strings.SplitSeq(opts.Expand, ",") {
			f = strings.TrimSpace(f)
			if f == "" {
				continue
			}
			topLevel := f
			if before, _, ok := strings.Cut(f, "."); ok {
				topLevel = before
			}
			if obj != nil {
				fd, ok := obj.FieldsByAPIName[topLevel]
				if !ok {
					return nil, fmt.Errorf("unknown field %q in expand", topLevel)
				}
				if fd.Type != schema.FieldLookup {
					return nil, fmt.Errorf("field %q is not a LOOKUP field, cannot expand", topLevel)
				}
			}
			p.Expand = append(p.Expand, f)
		}
	}

	// order
	if opts.Order != "" {
		parts := strings.SplitN(opts.Order, ".", 2)
		fieldName := parts[0]
		if obj != nil {
			if _, ok := obj.FieldsByAPIName[fieldName]; !ok {
				return nil, fmt.Errorf("unknown field %q in order", fieldName)
			}
		}
		order := &hrql.OrderBy{Field: fieldName}
		if len(parts) == 2 && strings.EqualFold(parts[1], "desc") {
			order.Desc = true
		}
		p.Order = order
	}

	// limit
	if opts.Limit > 0 {
		n := min(int(opts.Limit), maxLimit)
		p.Limit = n
	}

	// filters
	for key, value := range opts.Filters {
		if obj != nil {
			if _, ok := obj.FieldsByAPIName[key]; !ok {
				return nil, fmt.Errorf("unknown filter field %q", key)
			}
		}
		cond, err := parseFilterCondition(key, value)
		if err != nil {
			return nil, fmt.Errorf("filter %q: %w", key, err)
		}
		p.Conditions = append(p.Conditions, cond)
	}

	return p, nil
}
