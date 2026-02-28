package pg

import (
	"fmt"
	"strings"

	"github.com/atlekbai/schema_registry/internal/hrql"
)

// filterOp is a REST API filter operator parsed from "op.value" strings.
type filterOp string

const (
	opEq    filterOp = "eq"
	opNeq   filterOp = "neq"
	opGt    filterOp = "gt"
	opGte   filterOp = "gte"
	opLt    filterOp = "lt"
	opLte   filterOp = "lte"
	opLike  filterOp = "like"
	opIlike filterOp = "ilike"
	opIn    filterOp = "in"
	opIs    filterOp = "is"
)

var validOps = map[filterOp]bool{
	opEq: true, opNeq: true, opGt: true, opGte: true,
	opLt: true, opLte: true, opLike: true, opIlike: true,
	opIn: true, opIs: true,
}

// ParseFilterCondition parses a REST API filter string like "eq.hello" and returns
// a storage-agnostic hrql.Condition for the given field.
func ParseFilterCondition(fieldAPIName, raw string) (hrql.Condition, error) {
	before, after, ok := strings.Cut(raw, ".")
	if !ok {
		return nil, fmt.Errorf("invalid filter format %q, expected op.value", raw)
	}

	op := filterOp(before)
	if !validOps[op] {
		return nil, fmt.Errorf("unknown filter operator %q", op)
	}

	value := after
	if op == opIs && value != "null" && value != "not_null" {
		return nil, fmt.Errorf("is operator only accepts null or not_null, got %q", value)
	}

	field := []string{fieldAPIName}

	switch op {
	case opEq:
		return hrql.FieldCmp{Field: field, Op: "==", Value: value}, nil
	case opNeq:
		return hrql.FieldCmp{Field: field, Op: "!=", Value: value}, nil
	case opGt:
		return hrql.FieldCmp{Field: field, Op: ">", Value: value}, nil
	case opGte:
		return hrql.FieldCmp{Field: field, Op: ">=", Value: value}, nil
	case opLt:
		return hrql.FieldCmp{Field: field, Op: "<", Value: value}, nil
	case opLte:
		return hrql.FieldCmp{Field: field, Op: "<=", Value: value}, nil
	case opLike:
		return hrql.LikeFilter{Field: field, Pattern: value, CaseInsensitive: false}, nil
	case opIlike:
		return hrql.LikeFilter{Field: field, Pattern: value, CaseInsensitive: true}, nil
	case opIn:
		return hrql.InFilter{Field: field, Values: strings.Split(value, ",")}, nil
	case opIs:
		return hrql.IsNullFilter{Field: field, IsNull: value == "null"}, nil
	default:
		return nil, fmt.Errorf("unsupported filter operator %q", op)
	}
}
