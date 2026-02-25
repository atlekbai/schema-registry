package query

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/atlekbai/schema_registry/internal/schema"
	"github.com/google/uuid"
)

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

var reservedParams = map[string]bool{
	"select": true,
	"expand": true,
	"order":  true,
	"limit":  true,
	"cursor": true,
}

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
	Select      []string
	Expand      []string
	ExpandPlans []ExpandPlan
	Filters     []Filter
	Order       *OrderClause
	Limit       int
	Cursor      *Cursor
}

func ParseQueryParams(r *http.Request, obj *schema.ObjectDef) (*QueryParams, error) {
	p := &QueryParams{
		Limit: DefaultLimit,
	}

	q := r.URL.Query()

	// ?select=Field1,Field2
	if sel := q.Get("select"); sel != "" {
		fields := strings.Split(sel, ",")
		for _, f := range fields {
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

	// ?expand=Field1,Field2.NestedField
	if exp := q.Get("expand"); exp != "" {
		fields := strings.Split(exp, ",")
		for _, f := range fields {
			f = strings.TrimSpace(f)
			if f == "" {
				continue
			}
			// Validate top-level field
			topLevel := f
			if idx := strings.IndexByte(f, '.'); idx >= 0 {
				topLevel = f[:idx]
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

	// ?order=Field.desc
	if ord := q.Get("order"); ord != "" {
		parts := strings.SplitN(ord, ".", 2)
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

	// ?limit=20
	if lim := q.Get("limit"); lim != "" {
		n, err := strconv.Atoi(lim)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("invalid limit %q", lim)
		}
		if n > MaxLimit {
			n = MaxLimit
		}
		p.Limit = n
	}

	// ?cursor=token (base64 keyset cursor or plain UUID)
	if cur := q.Get("cursor"); cur != "" {
		c, err := DecodeCursor(cur)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor %q: %w", cur, err)
		}
		p.Cursor = c
	}

	// Remaining params are filters: ?FieldName=op.value
	for key, values := range q {
		if reservedParams[key] {
			continue
		}
		if len(values) == 0 {
			continue
		}

		fd, ok := obj.FieldsByAPIName[key]
		if !ok {
			return nil, fmt.Errorf("unknown filter field %q", key)
		}
		_ = fd

		op, val, err := ParseFilter(values[0])
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
func ResolveExpands(params *QueryParams, obj *schema.ObjectDef, cache *schema.Cache) {
	type nested struct{ parent, child string }
	var level1 []string
	var level2 []nested

	for _, f := range params.Expand {
		if idx := strings.IndexByte(f, '.'); idx >= 0 {
			level1 = append(level1, f[:idx])
			level2 = append(level2, nested{f[:idx], f[idx+1:]})
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

	for _, fn := range ordered {
		params.ExpandPlans = append(params.ExpandPlans, *planMap[fn])
	}
}
