begin;

-- Delete fields (will cascade from objects, but being explicit)
DELETE FROM metadata.fields
WHERE "object_id" IN (
	SELECT "id" FROM metadata.objects
	WHERE "api_name" IN ('User', 'Organization', 'Department', 'Individual', 'Employee')
);

-- Delete objects (CASCADE will handle fields)
DELETE FROM metadata.objects
WHERE "api_name" IN ('User', 'Organization', 'Department', 'Individual', 'Employee');

commit;
