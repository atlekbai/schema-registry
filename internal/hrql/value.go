package hrql

import (
	"encoding/json"
	"fmt"
)

// Value is the result of executing an HRQL query.
type Value interface {
	resultTag()
	String() string
}

// List is a set of records from a query.
type List struct {
	ObjectAPIName string
	Rows          []json.RawMessage
	TotalCount    int64
}

func (List) resultTag()      {}
func (l List) String() string { return fmt.Sprintf("list(%d rows)", len(l.Rows)) }

// Scalar is a single aggregated value.
type Scalar struct {
	ObjectAPIName string
	Value         *string
}

func (Scalar) resultTag() {}
func (s Scalar) String() string {
	if s.Value == nil {
		return "scalar(nil)"
	}
	return fmt.Sprintf("scalar(%s)", *s.Value)
}

