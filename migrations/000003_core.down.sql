begin;

-- Drop indexes
DROP INDEX IF EXISTS core.idx_employees_custom_fields;
DROP INDEX IF EXISTS core.idx_employees_employment_type;
DROP INDEX IF EXISTS core.idx_employees_manager_id;
DROP INDEX IF EXISTS core.idx_employees_department_id;
DROP INDEX IF EXISTS core.idx_employees_organization_id;
DROP INDEX IF EXISTS core.idx_employees_individual_id;
DROP INDEX IF EXISTS core.idx_employees_user_id;

DROP INDEX IF EXISTS core.idx_individuals_custom_fields;
DROP INDEX IF EXISTS core.idx_individuals_email;

DROP INDEX IF EXISTS core.idx_departments_custom_fields;
DROP INDEX IF EXISTS core.idx_departments_parent_id;
DROP INDEX IF EXISTS core.idx_departments_organization_id;

DROP INDEX IF EXISTS core.idx_organizations_custom_fields;

-- Drop tables in reverse dependency order
DROP TABLE IF EXISTS core.employees CASCADE;
DROP TABLE IF EXISTS core.individuals CASCADE;
DROP TABLE IF EXISTS core.departments CASCADE;
DROP TABLE IF EXISTS core.organizations CASCADE;
DROP TABLE IF EXISTS core.users CASCADE;

-- Drop schema
DROP SCHEMA IF EXISTS core CASCADE;

commit;
