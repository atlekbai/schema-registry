package query

import (
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
)

type FilterOp string

const (
	OpEq    FilterOp = "eq"
	OpNeq   FilterOp = "neq"
	OpGt    FilterOp = "gt"
	OpGte   FilterOp = "gte"
	OpLt    FilterOp = "lt"
	OpLte   FilterOp = "lte"
	OpLike  FilterOp = "like"
	OpIlike FilterOp = "ilike"
	OpIn    FilterOp = "in"
	OpIs    FilterOp = "is"
)

var validOps = map[FilterOp]bool{
	OpEq: true, OpNeq: true, OpGt: true, OpGte: true,
	OpLt: true, OpLte: true, OpLike: true, OpIlike: true,
	OpIn: true, OpIs: true,
}

type Filter struct {
	FieldAPIName string
	Op           FilterOp
	Value        string
}

// ParseFilter parses a filter value like "eq.hello" into op + value.
func ParseFilter(raw string) (FilterOp, string, error) {
	before, after, ok := strings.Cut(raw, ".")
	if !ok {
		return "", "", fmt.Errorf("invalid filter format %q, expected op.value", raw)
	}

	op := FilterOp(before)
	if !validOps[op] {
		return "", "", fmt.Errorf("unknown filter operator %q", op)
	}

	value := after
	if op == OpIs && value != "null" && value != "not_null" {
		return "", "", fmt.Errorf("is operator only accepts null or not_null, got %q", value)
	}

	return op, value, nil
}

// InValues splits a comma-separated "in" filter value into individual values.
func InValues(value string) []string {
	return strings.Split(value, ",")
}

// SQLOp returns the SQL operator string for a FilterOp.
func SQLOp(op FilterOp) string {
	switch op {
	case OpEq:
		return "="
	case OpNeq:
		return "!="
	case OpGt:
		return ">"
	case OpGte:
		return ">="
	case OpLt:
		return "<"
	case OpLte:
		return "<="
	case OpLike:
		return "LIKE"
	case OpIlike:
		return "ILIKE"
	default:
		return "="
	}
}

// applyFilter adds a single filter condition to the query builder.
func applyFilter(qb sq.SelectBuilder, col string, f Filter) sq.SelectBuilder {
	switch f.Op {
	case OpIn:
		// Use = ANY($1) instead of IN ($1,$2,...) for stable prepared statements.
		qb = qb.Where(fmt.Sprintf(`%s = ANY(?)`, col), InValues(f.Value))
	case OpIs:
		if f.Value == "null" {
			qb = qb.Where(sq.Eq{col: nil})
		} else {
			qb = qb.Where(sq.NotEq{col: nil})
		}
	default:
		qb = qb.Where(fmt.Sprintf(`%s %s ?`, col, SQLOp(f.Op)), f.Value)
	}
	return qb
}
