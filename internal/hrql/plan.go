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
	BoolCondition Condition // deferred to SQL execution
}

// OrderBy specifies sort order for a list result.
type OrderBy struct {
	Field string
	Desc  bool
}

// EmployeeRef is an unresolved reference to an employee or a derived value.
// The pg backend resolves it to SQL at translation time.
type EmployeeRef struct {
	ID    string   // base UUID (selfID or literal)
	Chain []string // optional field chain: ["manager"] for self.manager
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

// FieldCmpRef: .field == self.field (comparison with an unresolved employee ref)
type FieldCmpRef struct {
	Field []string    // API name chain on the left
	Op    string      // comparison operator
	Ref   EmployeeRef // unresolved reference on the right
}

func (FieldCmpRef) condition() {}

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
// These carry unresolved EmployeeRef data, not resolved paths.

// OrgChainUp: ancestor at exactly N levels above target.
type OrgChainUp struct {
	Emp   EmployeeRef
	Steps int
}

func (OrgChainUp) condition() {}

// OrgChainDown: descendants at exactly N levels below target.
type OrgChainDown struct {
	Emp   EmployeeRef
	Depth int
}

func (OrgChainDown) condition() {}

// OrgChainAll: all ancestors of target (full chain to root).
type OrgChainAll struct{ Emp EmployeeRef }

func (OrgChainAll) condition() {}

// OrgSubtree: all descendants of target (any depth).
type OrgSubtree struct{ Emp EmployeeRef }

func (OrgSubtree) condition() {}

// SameFieldCond: column = (SELECT field FROM emp WHERE id = ref.ID) AND id != ref.ID
type SameFieldCond struct {
	Field string      // API name
	Emp   EmployeeRef // employee whose field value to match; Emp.ID used for exclude
}

func (SameFieldCond) condition() {}

// ReportsTo: reports_to(., target) inside where — ltree descendant check.
type ReportsTo struct{ Target EmployeeRef }

func (ReportsTo) condition() {}

// ReportsToCheck: top-level reports_to(emp, target) — produces a boolean via SQL.
type ReportsToCheck struct {
	Emp    EmployeeRef
	Target EmployeeRef
}

func (ReportsToCheck) condition() {}

// SubqueryAgg: correlated subquery like reports(., 1) | count > 0
type SubqueryAgg struct {
	OrgFunc string // "reports"
	Depth   int
	AggFunc string // "count", "sum", etc.
	Op      string // comparison op in outer context
	Value   string // comparison value in outer context
}

func (SubqueryAgg) condition() {}

// --- REST API filter conditions ---

// InFilter: field IN (values)
type InFilter struct {
	Field  []string
	Values []string
}

func (InFilter) condition() {}

// IsNullFilter: field IS NULL / IS NOT NULL
type IsNullFilter struct {
	Field  []string
	IsNull bool
}

func (IsNullFilter) condition() {}

// LikeFilter: field LIKE/ILIKE pattern (raw SQL LIKE syntax from REST API)
type LikeFilter struct {
	Field           []string
	Pattern         string
	CaseInsensitive bool
}

func (LikeFilter) condition() {}

// --- Helpers ---

func joinChain(chain []string) string {
	return strings.Join(chain, ".")
}
