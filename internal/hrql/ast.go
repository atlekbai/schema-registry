package hrql

// Node is the interface all AST nodes implement.
type Node interface {
	node() // marker method
}

// PipeExpr represents a pipeline: step | step | step
// Steps[0] is the source, Steps[1:] are pipe operations.
type PipeExpr struct {
	Steps []Node
}

// FieldAccess represents dot-notation field access: .field or .field.subfield
type FieldAccess struct {
	Chain []string // e.g. ["department", "title"]
}

// SelfExpr represents the `self` pronoun.
type SelfExpr struct{}

// DotExpr represents the `.` pronoun (current pipe item).
type DotExpr struct{}

// IdentExpr represents a bare identifier like `employees`.
type IdentExpr struct {
	Name string
}

// FuncCall represents a function call: name(arg1, arg2, ...)
type FuncCall struct {
	Name string
	Args []Node
}

// WhereExpr represents where(condition).
type WhereExpr struct {
	Cond Node
}

// BinaryOp represents a binary operation: left op right.
type BinaryOp struct {
	Op    string // "==", "!=", ">", ">=", "<", "<=", "and", "or", "+", "-", "*", "/"
	Left  Node
	Right Node
}

// UnaryMinus represents negation: -expr.
type UnaryMinus struct {
	Expr Node
}

// Literal represents a string, number, or boolean literal.
type Literal struct {
	Kind  TokenKind // TokString, TokNumber, TokTrue, TokFalse
	Value string
}

// SortExpr represents sort_by(.field, asc/desc).
type SortExpr struct {
	Field *FieldAccess
	Desc  bool
}

// PickExpr represents first, last, or nth(n).
type PickExpr struct {
	Op string // "first", "last", "nth"
	N  int    // 1-indexed, only meaningful for "nth"
}

// AggExpr represents count, sum, avg, min, or max.
type AggExpr struct {
	Op string // "count", "sum", "avg", "min", "max"
}

func (*PipeExpr) node()    {}
func (*FieldAccess) node() {}
func (*SelfExpr) node()    {}
func (*DotExpr) node()     {}
func (*IdentExpr) node()   {}
func (*FuncCall) node()    {}
func (*WhereExpr) node()   {}
func (*BinaryOp) node()    {}
func (*UnaryMinus) node()  {}
func (*Literal) node()     {}
func (*SortExpr) node()    {}
func (*PickExpr) node()    {}
func (*AggExpr) node()     {}
