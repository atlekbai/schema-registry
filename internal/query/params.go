package query

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/atlekbai/schema_registry/internal/schema"
	"github.com/google/uuid"
)

// ParamsInput is a transport-agnostic representation of query parameters.
type ParamsInput struct {
	Select  string            // comma-separated field names
	Expand  string            // comma-separated expand paths
	Order   string            // "FieldName" or "FieldName.desc"
	Limit   int32             // 0 means use default
	Cursor  string            // opaque cursor token
	Filters map[string]string // field API name -> "op.value"
}

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

type OrderClause struct {
	FieldAPIName string
	Desc         bool
}

type ExpandPlan struct {
	FieldName string
	Field     *schema.FieldDef
	Target    *schema.ObjectDef
	Children  []ExpandPlan
}

// Cursor holds keyset pagination state: the last row's ID and optional sort column value.
type Cursor struct {
	ID       string `json:"id"`
	OrderVal string `json:"v,omitempty"`
}

// EncodeCursor returns an opaque base64 token for the cursor.
func EncodeCursor(id string, orderVal string) string {
	c := Cursor{ID: id, OrderVal: orderVal}
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses a cursor token. Accepts both base64 tokens and plain UUIDs.
func DecodeCursor(raw string) (*Cursor, error) {
	// Plain UUID (backward compat / default id-only ordering)
	if _, err := uuid.Parse(raw); err == nil {
		return &Cursor{ID: raw}, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding")
	}
	var c Cursor
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("invalid cursor format")
	}
	if _, err := uuid.Parse(c.ID); err != nil {
		return nil, fmt.Errorf("invalid cursor id")
	}
	return &c, nil
}

type QueryParams struct {
	Select          []string
	Expand          []string
	ExpandPlans     []ExpandPlan
	Filters         []Filter
	Order           *OrderClause
	Limit           int
	Cursor          *Cursor
	ExtraConditions []sq.Sqlizer // additional WHERE clauses (e.g. ltree)
}

// ParseParams builds QueryParams from a transport-agnostic ParamsInput.
func ParseParams(obj *schema.ObjectDef, input ParamsInput) (*QueryParams, error) {
	p := &QueryParams{
		Limit: DefaultLimit,
	}

	// select
	if input.Select != "" {
		for f := range strings.SplitSeq(input.Select, ",") {
			f = strings.TrimSpace(f)
			if f == "" {
				continue
			}
			if _, ok := obj.FieldsByAPIName[f]; !ok {
				return nil, fmt.Errorf("unknown field %q in select", f)
			}
			p.Select = append(p.Select, f)
		}
	}

	// expand
	if input.Expand != "" {
		for f := range strings.SplitSeq(input.Expand, ",") {
			f = strings.TrimSpace(f)
			if f == "" {
				continue
			}
			topLevel := f
			if before, _, ok := strings.Cut(f, "."); ok {
				topLevel = before
			}
			fd, ok := obj.FieldsByAPIName[topLevel]
			if !ok {
				return nil, fmt.Errorf("unknown field %q in expand", topLevel)
			}
			if fd.Type != schema.FieldLookup {
				return nil, fmt.Errorf("field %q is not a LOOKUP field, cannot expand", topLevel)
			}
			p.Expand = append(p.Expand, f)
		}
	}

	// order
	if input.Order != "" {
		parts := strings.SplitN(input.Order, ".", 2)
		fieldName := parts[0]
		if _, ok := obj.FieldsByAPIName[fieldName]; !ok {
			return nil, fmt.Errorf("unknown field %q in order", fieldName)
		}
		clause := &OrderClause{FieldAPIName: fieldName}
		if len(parts) == 2 && strings.EqualFold(parts[1], "desc") {
			clause.Desc = true
		}
		p.Order = clause
	}

	// limit
	if input.Limit > 0 {
		n := int(input.Limit)
		if n > MaxLimit {
			n = MaxLimit
		}
		p.Limit = n
	}

	// cursor
	if input.Cursor != "" {
		c, err := DecodeCursor(input.Cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor %q: %w", input.Cursor, err)
		}
		p.Cursor = c
	}

	// filters
	for key, value := range input.Filters {
		fd, ok := obj.FieldsByAPIName[key]
		if !ok {
			return nil, fmt.Errorf("unknown filter field %q", key)
		}
		_ = fd

		op, val, err := ParseFilter(value)
		if err != nil {
			return nil, fmt.Errorf("filter %q: %w", key, err)
		}

		p.Filters = append(p.Filters, Filter{
			FieldAPIName: key,
			Op:           op,
			Value:        val,
		})
	}

	return p, nil
}

// ResolveExpands resolves expand strings into ExpandPlans using the schema cache.
func ResolveExpands(expands []string, obj *schema.ObjectDef, cache *schema.Cache) []ExpandPlan {
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
