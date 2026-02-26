package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const loadQuery = `
SELECT
	o.id, o.api_name, o.title, o.plural_title,
	o.is_standard, o.storage_schema, o.storage_table, o.supports_custom_fields,
	f.id, f.api_name, f.title, f.type, f.type_config,
	f.is_required, f.is_unique, f.is_standard,
	f.storage_column, f.lookup_object_id
FROM metadata.objects o
LEFT JOIN metadata.fields f ON f.object_id = o.id
ORDER BY o.api_name, f.created_at
`

type Cache struct {
	mu      sync.RWMutex
	objects map[string]*ObjectDef
	byID    map[uuid.UUID]*ObjectDef
}

func NewCache() *Cache {
	return &Cache{
		objects: make(map[string]*ObjectDef),
		byID:    make(map[uuid.UUID]*ObjectDef),
	}
}

func (c *Cache) Load(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, loadQuery)
	if err != nil {
		return fmt.Errorf("schema cache load: %w", err)
	}
	defer rows.Close()

	objects := make(map[string]*ObjectDef)

	for rows.Next() {
		var (
			oID             uuid.UUID
			oAPIName        string
			oTitle          string
			oPluralTitle    string
			oIsStandard     bool
			oStorageSchema  *string
			oStorageTable   *string
			oSupportsCustom bool
			fID             *uuid.UUID
			fAPIName        *string
			fTitle          *string
			fType           *string
			fTypeConfig     json.RawMessage
			fIsRequired     *bool
			fIsUnique       *bool
			fIsStandard     *bool
			fStorageColumn  *string
			fLookupObjectID *uuid.UUID
		)

		err := rows.Scan(
			&oID, &oAPIName, &oTitle, &oPluralTitle,
			&oIsStandard, &oStorageSchema, &oStorageTable, &oSupportsCustom,
			&fID, &fAPIName, &fTitle, &fType, &fTypeConfig,
			&fIsRequired, &fIsUnique, &fIsStandard,
			&fStorageColumn, &fLookupObjectID,
		)
		if err != nil {
			return fmt.Errorf("schema cache scan: %w", err)
		}

		obj, exists := objects[oAPIName]
		if !exists {
			obj = &ObjectDef{
				ID:                   oID,
				APIName:              oAPIName,
				Title:                oTitle,
				PluralTitle:          oPluralTitle,
				IsStandard:           oIsStandard,
				StorageSchema:        oStorageSchema,
				StorageTable:         oStorageTable,
				SupportsCustomFields: oSupportsCustom,
				FieldsByAPIName:      make(map[string]*FieldDef),
			}
			objects[oAPIName] = obj
		}

		if fID != nil {
			field := FieldDef{
				ID:             *fID,
				ObjectID:       oID,
				APIName:        *fAPIName,
				Title:          *fTitle,
				Type:           FieldType(*fType),
				TypeConfig:     fTypeConfig,
				IsRequired:     *fIsRequired,
				IsUnique:       *fIsUnique,
				IsStandard:     *fIsStandard,
				StorageColumn:  fStorageColumn,
				LookupObjectID: fLookupObjectID,
			}
			obj.Fields = append(obj.Fields, field)
			obj.FieldsByAPIName[field.APIName] = &obj.Fields[len(obj.Fields)-1]
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("schema cache rows: %w", err)
	}

	byID := make(map[uuid.UUID]*ObjectDef, len(objects))
	for _, obj := range objects {
		byID[obj.ID] = obj
	}

	c.mu.Lock()
	c.objects = objects
	c.byID = byID
	c.mu.Unlock()

	return nil
}

func (c *Cache) Get(apiName string) *ObjectDef {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.objects[apiName]
}

// GetByID finds an object definition by its UUID.
func (c *Cache) GetByID(id uuid.UUID) *ObjectDef {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.byID[id]
}

// ObjectCount returns the number of loaded objects.
func (c *Cache) ObjectCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.objects)
}
