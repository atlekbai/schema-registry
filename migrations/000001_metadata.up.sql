begin;

-- Enable UUID v7 extension
CREATE EXTENSION IF NOT EXISTS pg_uuidv7;

-- Create metadata schema
CREATE SCHEMA IF NOT EXISTS metadata;

-- Object categories
CREATE TABLE metadata.object_categories (
	"id"			UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
	"created_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),
	"updated_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),
	"title"			TEXT NOT NULL,
	"description"	TEXT
);

-- Seed object categories
INSERT INTO metadata.object_categories ("title", "description") VALUES
	('HR',		'Human Resources related objects'),
	('IT',		'Information Technology related objects'),
	('Finance',	'Finance and accounting related objects'),
	('Custom',	'Custom user-defined objects')
;

-- Objects (like a table catalog)
CREATE TABLE metadata.objects (
	"id"			UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
	"created_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),
	"updated_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),

	-- For POC purposes this is commented.
	-- 
	-- "tenant_id"	UUID NOT NULL REFERENCES "tenants" ("id"),
	-- "owner_id"	UUID REFERENCES "users" ("id"),

	-- Identity (api_name is immutable, name can change for UI)
	"api_name"		TEXT NOT NULL UNIQUE CHECK (api_name ~ '^[A-Za-z][A-Za-z0-9_]*(__c)?$'),
	"category_id"	UUID REFERENCES metadata.object_categories ("id") ON DELETE RESTRICT,
	"title"			TEXT NOT NULL,
	"plural_title"	TEXT NOT NULL,
	"description"	TEXT,

	-- Physical storage mapping
	-- -- TRUE for native objects, e.g. "Employee", "Device", etc.
	"is_standard"		BOOLEAN NOT NULL DEFAULT FALSE,
	"storage_schema"	TEXT,  -- Actual PostgreSQL schema (required for standard objects)
	"storage_table"		TEXT,  -- Actual PostgreSQL table (required for standard objects)

	-- Features
	"supports_custom_fields"	BOOLEAN NOT NULL DEFAULT TRUE,

	UNIQUE ("storage_schema", "storage_table"),

	-- Data integrity constraints
	-- Standard objects must have both storage_schema and storage_table
	CONSTRAINT chk_objects_standard_storage CHECK (
		("is_standard" = TRUE) = ("storage_schema" IS NOT NULL AND "storage_table" IS NOT NULL)
	)
);

-- Fields (like a column catalog)
CREATE TABLE metadata.fields (
	"id"			UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
	"created_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),
	"updated_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),

	-- For POC purposes this is commented.
	-- 
	-- "tenant_id"	UUID NOT NULL REFERENCES "tenants" ("id"),
	-- "owner_id"	UUID REFERENCES "users" ("id"),

	-- Identity
	"object_id"		UUID NOT NULL REFERENCES metadata.objects(id) ON DELETE CASCADE,
	"api_name"		TEXT NOT NULL CHECK (api_name ~ '^[A-Za-z][A-Za-z0-9_]*(__c)?$'),
	"title"			TEXT NOT NULL,
	"description"	TEXT,

	-- Type
	"type" TEXT NOT NULL CHECK (
		"type" IN (
			-- Basic
			'TEXT', 'NUMBER', 'CURRENCY', 'PERCENTAGE', 'DATE', 'DATETIME',
			'BOOLEAN', 'CHOICE', 'MULTICHOICE', 'EMAIL', 'URL', 'PHONE',
			-- Relationship
			'LOOKUP',
			-- Computed
			'FORMULA', 'SIMPLE_FORMULA', 'SUMMARY'
		)
	),
	-- Type-specific: {max_length, precision, options[], etc.}
	"type_config"	JSONB NOT NULL DEFAULT '{}',
	"default_value"	JSONB,

	-- Type specific columns:
	-- Lookup field config (when type = 'LOOKUP')
	"lookup_object_id"	UUID REFERENCES metadata.objects("id") ON DELETE RESTRICT,
	-- Formula query (when type = 'FORMULA')
	"formula_query"		TEXT,

	-- Constraints
	"is_required"	BOOLEAN NOT NULL DEFAULT FALSE,
	"is_unique"		BOOLEAN NOT NULL DEFAULT FALSE,

	-- Physical storage
	"is_standard"		BOOLEAN NOT NULL DEFAULT FALSE,  -- Standard vs custom field
	"storage_column"	TEXT,  -- NULL for custom/computed fields

	-- Data integrity constraints
	UNIQUE ("object_id", "api_name"),
	CONSTRAINT chk_fields_standard_storage CHECK (("is_standard" = TRUE) = ("storage_column" IS NOT NULL)),
	CONSTRAINT chk_fields_lookup_object CHECK (("type" = 'LOOKUP') = ("lookup_object_id" IS NOT NULL)),
	CONSTRAINT chk_fields_formula_query CHECK (("type" = 'FORMULA') = ("formula_query" IS NOT NULL))
);

-- Records table (stores actual data for custom objects)
CREATE TABLE metadata.records (
	"id"			UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
	"created_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),
	"updated_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),

	-- For POC purposes this is commented.
	-- 
	-- "tenant_id"	UUID NOT NULL REFERENCES "tenants" ("id"),
	-- "owner_id"	UUID REFERENCES "users" ("id"),

	-- Reference to the object definition
	"object_id"		UUID NOT NULL REFERENCES metadata.objects("id") ON DELETE CASCADE,

	-- Actual record data stored as JSONB
	"data"			JSONB NOT NULL DEFAULT '{}'
) WITH (fillfactor = 90);  -- Leave 10% free space for HOT updates

-- Indexes for performance
CREATE INDEX idx_objects_category_id ON metadata.objects("category_id");
CREATE INDEX idx_objects_storage ON metadata.objects("storage_schema", "storage_table");
CREATE INDEX idx_objects_is_standard ON metadata.objects("is_standard") WHERE "is_standard" = TRUE;

CREATE INDEX idx_fields_object_id ON metadata.fields("object_id");
CREATE INDEX idx_fields_type ON metadata.fields("type");
CREATE INDEX idx_fields_lookup_object_id ON metadata.fields("lookup_object_id") WHERE "lookup_object_id" IS NOT NULL;

CREATE INDEX idx_records_object_id ON metadata.records("object_id");
CREATE INDEX idx_records_data ON metadata.records USING GIN ("data" jsonb_path_ops);

-- Table comments for documentation
COMMENT ON TABLE metadata.object_categories IS 'Predefined categories for organizing custom objects (HR, IT, Finance, Custom)';
COMMENT ON TABLE metadata.objects IS 'Object definitions - metadata catalog describing custom object types (like table definitions)';
COMMENT ON TABLE metadata.fields IS 'Field definitions - metadata catalog describing fields/columns for each object type';
COMMENT ON TABLE metadata.records IS 'Actual record data for custom objects stored in JSONB format';

COMMENT ON COLUMN metadata.records.data IS 'JSONB column containing all field values for this record according to its object definition';
COMMENT ON COLUMN metadata.objects.storage_schema IS 'Physical PostgreSQL schema name where standard object data is stored';
COMMENT ON COLUMN metadata.objects.storage_table IS 'Physical PostgreSQL table name where standard object data is stored';

commit;
