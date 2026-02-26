package query

import (
	"github.com/atlekbai/schema_registry/internal/schema"
	"github.com/google/uuid"
)

// Builder generates SQL queries for a given object definition.
type Builder interface {
	BuildList(obj *schema.ObjectDef, params *QueryParams) (string, []any, error)
	BuildGetByID(obj *schema.ObjectDef, id uuid.UUID, params *QueryParams) (string, []any, error)
	BuildCount(obj *schema.ObjectDef, params *QueryParams) (string, []any, error)
	// BuildEstimate returns SELECT 1 FROM ... WHERE ... for use with EXPLAIN (FORMAT JSON).
	BuildEstimate(obj *schema.ObjectDef, params *QueryParams) (string, []any, error)
}

// isSystemField returns true for system fields (id, created_at, updated_at)
// that are always emitted by jsonObject and should be skipped in the field loop.
func isSystemField(apiName string) bool {
	return apiName == "id" || apiName == "created_at" || apiName == "updated_at"
}

// NewBuilder returns the appropriate query builder for the object type.
func NewBuilder(obj *schema.ObjectDef) Builder {
	if obj.IsStandard {
		return &StandardBuilder{}
	}
	return &CustomBuilder{}
}
