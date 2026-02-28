package parser

// ValueKind classifies what a function produces.
type ValueKind int

const (
	KindList      ValueKind = iota // produces a list of records
	KindScalar                     // produces a single value
	KindBoolean                    // produces true/false
	KindTransform                  // passthrough transform (unique, upper, lower)
)

// ArgKind classifies a function argument.
type ArgKind int

const (
	ArgEmployee ArgKind = iota // self, UUID literal, self.field
	ArgInt                     // integer literal
	ArgField                   // .field access
	ArgString                  // string literal
	ArgAny                     // unconstrained
)

// FuncDef describes a registered HRQL call-style function.
type FuncDef struct {
	Name       string
	ArgTypes   []ArgKind
	Variadic   int       // 0=fixed, N=N optional trailing args
	ReturnKind ValueKind
}

// Functions is the canonical registry of all HRQL call-style functions.
// Aggregation operators (count, sum, avg, min, max) and special-syntax forms
// (where, sort_by, first, last, nth) are NOT included — they have dedicated AST nodes.
var Functions = map[string]*FuncDef{
	// Org-tree traversal
	"chain":   {Name: "chain", ArgTypes: []ArgKind{ArgEmployee, ArgInt}, Variadic: 1, ReturnKind: KindList},
	"reports": {Name: "reports", ArgTypes: []ArgKind{ArgEmployee, ArgInt}, Variadic: 1, ReturnKind: KindList},
	"peers":   {Name: "peers", ArgTypes: []ArgKind{ArgEmployee}, ReturnKind: KindList},
	"colleagues": {Name: "colleagues", ArgTypes: []ArgKind{ArgEmployee, ArgField}, ReturnKind: KindList},

	// Boolean predicate
	"reports_to": {Name: "reports_to", ArgTypes: []ArgKind{ArgAny, ArgEmployee}, ReturnKind: KindBoolean},

	// String operations
	"contains":    {Name: "contains", ArgTypes: []ArgKind{ArgString}, ReturnKind: KindBoolean},
	"starts_with": {Name: "starts_with", ArgTypes: []ArgKind{ArgString}, ReturnKind: KindBoolean},
	"ends_with":   {Name: "ends_with", ArgTypes: []ArgKind{ArgString}, ReturnKind: KindBoolean},

	// Transforms (zero-arg, used without parens in pipe position)
	"unique": {Name: "unique", ReturnKind: KindTransform},
	"upper":  {Name: "upper", ReturnKind: KindTransform},
	"lower":  {Name: "lower", ReturnKind: KindTransform},

	// Scalar (zero-arg)
	"length": {Name: "length", ReturnKind: KindScalar},
}

// GetFunction returns the FuncDef for name and whether it was found.
func GetFunction(name string) (*FuncDef, bool) {
	f, ok := Functions[name]
	return f, ok
}
