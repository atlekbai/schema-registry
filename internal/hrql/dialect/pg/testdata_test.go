package pg

import (
	"github.com/atlekbai/schema_registry/internal/schema"
	"github.com/google/uuid"
)

var testObj = &schema.ObjectDef{
	ID:            uuid.MustParse("00000000-0000-0000-0000-000000000001"),
	APIName:       "employees",
	IsStandard:    true,
	StorageSchema: new("core"),
	StorageTable:  new("employees"),
	FieldsByAPIName: map[string]*schema.FieldDef{
		"manager": {
			APIName:       "manager",
			Type:          schema.FieldLookup,
			StorageColumn: new("manager_id"),
		},
		"department": {
			APIName:       "department",
			Type:          schema.FieldLookup,
			StorageColumn: new("department_id"),
		},
	},
}
