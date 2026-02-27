package hrql

import "strings"

// PlanKind classifies the output of a compiled HRQL expression.
type PlanKind int

const (
	PlanList    PlanKind = iota // produces a list of records
	PlanScalar                  // produces a single value (aggregation)
	PlanBoolean                 // produces a boolean (reports_to)
)

// Plan is the storage-agnostic output of compiling an HRQL expression.
// It captures what the query means, not how to execute it in SQL.
type Plan struct {
	Kind PlanKind

	// PlanList fields
	Conditions []Condition // top-level conditions, AND'd together
	OrderBy    *OrderBy
	Limit      int    // 0 = no override
	PickOp     string // "first", "last", "nth"
	PickN      int    // for nth (1-indexed)

	// PlanScalar fields
	AggFunc  string // "count", "sum", "avg", "min", "max"
	AggField string // field API name, "" for count(*)

	// PlanBoolean fields
	BoolResult *bool
}

// OrderBy specifies sort order for a list result.
type OrderBy struct {
	Field string
	Desc  bool
}

// --- Condition types ---

// Condition is a storage-agnostic filter element.
type Condition interface {
	condition()
}

// FieldCmp: .field == "value" (single or lookup-chain field)
type FieldCmp struct {
	Field []string // API name chain, e.g. ["department", "title"]
	Op    string   // "==", "!=", ">", ">=", "<", "<="
	Value string
}

func (FieldCmp) condition() {}

// StringMatch: .field | contains("str")
type StringMatch struct {
	Field   []string // API name chain
	Op      string   // "contains", "starts_with", "ends_with"
	Pattern string
}

func (StringMatch) condition() {}

// IdentityFilter: WHERE id = value
type IdentityFilter struct{ ID string }

func (IdentityFilter) condition() {}

// NullFilter: always-false condition (empty result set)
type NullFilter struct{}

func (NullFilter) condition() {}

// AndCond: left AND right
type AndCond struct{ Left, Right Condition }

func (AndCond) condition() {}

// OrCond: left OR right
type OrCond struct{ Left, Right Condition }

func (OrCond) condition() {}

// --- Org hierarchy conditions ---
// These carry resolved path data, not SQL.

// OrgChainUp: ancestor at exactly N levels above target.
type OrgChainUp struct {
	Path  string
	Steps int
}

func (OrgChainUp) condition() {}

// OrgChainDown: descendants at exactly N levels below target.
type OrgChainDown struct {
	Path  string
	Depth int
}

func (OrgChainDown) condition() {}

// OrgChainAll: all ancestors of target (full chain to root).
type OrgChainAll struct{ Path string }

func (OrgChainAll) condition() {}

// OrgSubtree: all descendants of target (any depth).
type OrgSubtree struct{ Path string }

func (OrgSubtree) condition() {}

// SameFieldCond: column = value AND id != excludeID (peers/colleagues).
type SameFieldCond struct {
	Field     string // API name
	Value     string
	ExcludeID string
}

func (SameFieldCond) condition() {}

// ReportsTo: reports_to(., target) inside where — ltree descendant check.
type ReportsTo struct{ TargetPath string }

func (ReportsTo) condition() {}

// SubqueryAgg: correlated subquery like reports(., 1) | count > 0
type SubqueryAgg struct {
	OrgFunc string // "reports"
	Depth   int
	AggFunc string // "count", "sum", etc.
	Op      string // comparison op in outer context
	Value   string // comparison value in outer context
}

func (SubqueryAgg) condition() {}

// --- Helpers ---

func joinChain(chain []string) string {
	return strings.Join(chain, ".")
}

func nlevelFromPath(path string) int {
	if path == "" {
		return 0
	}
	n := 1
	for _, ch := range path {
		if ch == '.' {
			n++
		}
	}
	return n
}
