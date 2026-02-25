begin;

-- Drop indexes (CASCADE on tables will handle this, but being explicit)
DROP INDEX IF EXISTS metadata.idx_records_data;
DROP INDEX IF EXISTS metadata.idx_records_object_id;
DROP INDEX IF EXISTS metadata.idx_fields_lookup_object_id;
DROP INDEX IF EXISTS metadata.idx_fields_type;
DROP INDEX IF EXISTS metadata.idx_fields_object_id;
DROP INDEX IF EXISTS metadata.idx_objects_is_standard;
DROP INDEX IF EXISTS metadata.idx_objects_storage;
DROP INDEX IF EXISTS metadata.idx_objects_category_id;

-- Drop tables
DROP TABLE IF EXISTS metadata.records CASCADE;
DROP TABLE IF EXISTS metadata.fields CASCADE;
DROP TABLE IF EXISTS metadata.objects CASCADE;
DROP TABLE IF EXISTS metadata.object_categories CASCADE;

-- Drop schema and extension
DROP SCHEMA IF EXISTS metadata CASCADE;
DROP EXTENSION IF EXISTS pg_uuidv7;

commit;
