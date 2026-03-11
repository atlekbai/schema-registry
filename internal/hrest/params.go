package hrest

import (
	"encoding/json"

	"github.com/atlekbai/schema_registry/internal/hrql"
)

// Params holds parsed, transport-agnostic query parameters for REST-style list queries.
type Params struct {
	Select     []string
	Expand     []string
	Conditions []hrql.Condition
	Order      *hrql.OrderBy
	Limit      int
	SkipCount  bool
}

// ListResult is the output of a REST-style list query.
type ListResult struct {
	ObjectAPIName string
	Rows          []json.RawMessage
	TotalCount    int64
}

// QueryOpts groups raw REST query parameters for ParseParams.
type QueryOpts struct {
	Sel     string
	Expand  string
	Order   string
	Limit   int32
	Filters map[string]string
}
