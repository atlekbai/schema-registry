package hrql

import (
	"encoding/json"
	"fmt"
)

// ResultType classifies the output of an HRQL query execution.
type ResultType string

const (
	ResultList    ResultType = "list"
	ResultScalar  ResultType = "scalar"
	ResultBoolean ResultType = "boolean"
)

// Value is the result of executing an HRQL query.
type Value interface {
	Type() ResultType
	String() string
}

// List is a set of records from a query.
type List struct {
	ObjectAPIName string
	Rows          []json.RawMessage
	TotalCount    int64
	NextCursor    *string
}

func (List) Type() ResultType { return ResultList }
func (l List) String() string { return fmt.Sprintf("list(%d rows)", len(l.Rows)) }

// Scalar is a single aggregated value.
type Scalar struct {
	ObjectAPIName string
	Value         *string
}

func (Scalar) Type() ResultType { return ResultScalar }
func (s Scalar) String() string {
	if s.Value == nil {
		return "scalar(nil)"
	}
	return fmt.Sprintf("scalar(%s)", *s.Value)
}

// Boolean is a true/false result (e.g. reports_to).
type Boolean struct {
	ObjectAPIName string
	Value         *bool
}

func (Boolean) Type() ResultType { return ResultBoolean }
func (b Boolean) String() string {
	if b.Value == nil {
		return "boolean(nil)"
	}
	return fmt.Sprintf("boolean(%v)", *b.Value)
}

