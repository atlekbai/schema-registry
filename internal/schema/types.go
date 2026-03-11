package schema

import (
	"encoding/json"

	"github.com/google/uuid"
)

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

