begin;

-- Register a new object in the metadata schema registry.
-- Looks up category by title. Returns the new object's UUID.
CREATE FUNCTION metadata.register_object(
	p_category_title			TEXT,
	p_api_name					TEXT,
	p_title						TEXT,
	p_plural_title				TEXT,
	p_description				TEXT DEFAULT NULL,
	p_is_standard				BOOLEAN DEFAULT FALSE,
	p_storage_schema			TEXT DEFAULT NULL,
	p_storage_table				TEXT DEFAULT NULL,
	p_supports_custom_fields	BOOLEAN DEFAULT TRUE
) RETURNS UUID
LANGUAGE plpgsql AS $$
DECLARE
	v_category_id UUID;
	v_object_id UUID;
BEGIN
	SELECT "id" INTO v_category_id
	FROM metadata.object_categories
	WHERE "title" = p_category_title;

	IF v_category_id IS NULL THEN
		RAISE EXCEPTION 'Category "%" not found', p_category_title;
	END IF;

	INSERT INTO metadata.objects (
		"category_id", "api_name", "title", "plural_title", "description",
		"is_standard", "storage_schema", "storage_table", "supports_custom_fields"
	) VALUES (
		v_category_id, p_api_name, p_title, p_plural_title, p_description,
		p_is_standard, p_storage_schema, p_storage_table, p_supports_custom_fields
	)
	RETURNING "id" INTO v_object_id;

	-- Auto-register system fields present on every table
	INSERT INTO metadata.fields ("object_id", "api_name", "title", "type", "is_required", "is_standard", "storage_column")
	VALUES
		(v_object_id, 'id', 'ID', 'TEXT', TRUE, TRUE, 'id'),
		(v_object_id, 'created_at', 'Created At', 'DATETIME', TRUE, TRUE, 'created_at'),
		(v_object_id, 'updated_at', 'Updated At', 'DATETIME', TRUE, TRUE, 'updated_at');

	RETURN v_object_id;
END;
$$;

-- Add a field to a registered object.
-- Looks up object and optional lookup target by api_name. Returns the new field's UUID.
CREATE FUNCTION metadata.add_field(
	p_object_api_name			TEXT,
	p_api_name					TEXT,
	p_title						TEXT,
	p_description				TEXT DEFAULT NULL,
	p_type						TEXT DEFAULT 'TEXT',
	p_type_config				JSONB DEFAULT '{}',
	p_is_required				BOOLEAN DEFAULT FALSE,
	p_is_unique					BOOLEAN DEFAULT FALSE,
	p_is_standard				BOOLEAN DEFAULT FALSE,
	p_storage_column			TEXT DEFAULT NULL,
	p_lookup_object_api_name	TEXT DEFAULT NULL
) RETURNS UUID
LANGUAGE plpgsql AS $$
DECLARE
	v_object_id UUID;
	v_lookup_object_id UUID;
	v_field_id UUID;
BEGIN
	SELECT "id" INTO v_object_id
	FROM metadata.objects
	WHERE "api_name" = p_object_api_name;

	IF v_object_id IS NULL THEN
		RAISE EXCEPTION 'Object "%" not found', p_object_api_name;
	END IF;

	IF p_lookup_object_api_name IS NOT NULL THEN
		SELECT "id" INTO v_lookup_object_id
		FROM metadata.objects
		WHERE "api_name" = p_lookup_object_api_name;

		IF v_lookup_object_id IS NULL THEN
			RAISE EXCEPTION 'Lookup object "%" not found', p_lookup_object_api_name;
		END IF;
	END IF;

	INSERT INTO metadata.fields (
		"object_id", "api_name", "title", "description",
		"type", "type_config", "is_required", "is_unique",
		"is_standard", "storage_column", "lookup_object_id"
	) VALUES (
		v_object_id, p_api_name, p_title, p_description,
		p_type, p_type_config, p_is_required, p_is_unique,
		p_is_standard, p_storage_column, v_lookup_object_id
	)
	RETURNING "id" INTO v_field_id;

	RETURN v_field_id;
END;
$$;

COMMENT ON FUNCTION metadata.register_object IS 'Register a new object in the metadata schema registry. Looks up category by title.';
COMMENT ON FUNCTION metadata.add_field IS 'Add a field to a registered object. Looks up object and optional lookup target by api_name.';

commit;
