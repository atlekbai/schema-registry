package schema

import (
	"encoding/json"
	"strings"

	"github.com/google/uuid"
)

// QuoteIdent quotes a SQL identifier, escaping embedded double quotes.
func QuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

type FieldType string

const (
	FieldText        FieldType = "TEXT"
	FieldNumber      FieldType = "NUMBER"
	FieldCurrency    FieldType = "CURRENCY"
	FieldPercentage  FieldType = "PERCENTAGE"
	FieldDate        FieldType = "DATE"
	FieldDatetime    FieldType = "DATETIME"
	FieldBoolean     FieldType = "BOOLEAN"
	FieldChoice      FieldType = "CHOICE"
	FieldMultichoice FieldType = "MULTICHOICE"
	FieldEmail       FieldType = "EMAIL"
	FieldURL         FieldType = "URL"
	FieldPhone       FieldType = "PHONE"
	FieldLookup      FieldType = "LOOKUP"
	FieldFormula     FieldType = "FORMULA"
)

type FieldDef struct {
	ID             uuid.UUID
	ObjectID       uuid.UUID
	APIName        string
	Title          string
	Type           FieldType
	TypeConfig     json.RawMessage
	IsRequired     bool
	IsUnique       bool
	IsStandard     bool
	StorageColumn  *string
	LookupObjectID *uuid.UUID
}

// IsNumeric returns true if the field type requires numeric casting in queries.
func (f *FieldDef) IsNumeric() bool {
	return f.Type == FieldNumber || f.Type == FieldCurrency || f.Type == FieldPercentage
}

type ObjectDef struct {
	ID                   uuid.UUID
	APIName              string
	Title                string
	PluralTitle          string
	IsStandard           bool
	StorageSchema        *string
	StorageTable         *string
	SupportsCustomFields bool
	Fields               []FieldDef
	FieldsByAPIName      map[string]*FieldDef
}

// TableName returns the fully qualified, quoted table name for standard objects.
func (o *ObjectDef) TableName() string {
	if o.StorageSchema != nil && o.StorageTable != nil {
		return QuoteIdent(*o.StorageSchema) + "." + QuoteIdent(*o.StorageTable)
	}
	return ""
}
