BEGIN;

DROP TRIGGER IF EXISTS trg_employees_manager_path_after ON core.employees;
DROP TRIGGER IF EXISTS trg_employees_manager_path_before ON core.employees;
DROP FUNCTION IF EXISTS core.trg_employees_manager_path_after();
DROP FUNCTION IF EXISTS core.trg_employees_manager_path_before();
DROP INDEX IF EXISTS core.idx_employees_manager_path;
ALTER TABLE core.employees DROP COLUMN IF EXISTS "manager_path";
DROP FUNCTION IF EXISTS core.uuid_to_ltree_label(UUID);

COMMIT;
