BEGIN;

CREATE OR REPLACE FUNCTION core.uuid_to_ltree_label(id UUID)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
	SELECT replace(id::text, '-', '');
$$;

-- Table is empty at migration time; for populated tables use NOT VALID + backfill + VALIDATE.
ALTER TABLE core.employees ADD COLUMN "manager_path" ltree NOT NULL DEFAULT ''::ltree;
ALTER TABLE core.employees ADD CONSTRAINT chk_employees_manager_path
	CHECK (nlevel("manager_path") >= 1);
CREATE INDEX idx_employees_manager_path ON core.employees USING SPGIST ("manager_path");

-- BEFORE: compute this row's path from its manager's path.
CREATE OR REPLACE FUNCTION core.trg_employees_manager_path_before()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
	parent_path ltree;
	self_label  ltree := text2ltree(core.uuid_to_ltree_label(NEW."id"));
BEGIN
	-- Cycle detection
	IF TG_OP = 'UPDATE' AND NEW."manager_id" IS NOT NULL AND EXISTS (
		SELECT 1 FROM core.employees
		WHERE "id" = NEW."manager_id" AND "manager_path" <@ OLD."manager_path"
	) THEN
		RAISE EXCEPTION 'Cycle detected: % is a descendant of %', NEW."manager_id", NEW."id";
	END IF;

	-- Если нет руководителя то просто оставляем самого себя
	-- Иначе вытаскиваем руководителя и соедияем manager_path
	IF NEW."manager_id" IS NULL THEN
		NEW."manager_path" := self_label;
	ELSE
		SELECT "manager_path" INTO STRICT parent_path
		FROM core.employees WHERE "id" = NEW."manager_id";
		NEW."manager_path" := parent_path || self_label;
	END IF;

	RETURN NEW;
END;
$$;

CREATE TRIGGER trg_employees_manager_path_before
	BEFORE INSERT OR UPDATE OF "manager_id" ON core.employees
	FOR EACH ROW
	EXECUTE FUNCTION core.trg_employees_manager_path_before();

-- AFTER: cascade prefix swap to descendants.
-- Only touches manager_path (not manager_id), so the BEFORE trigger won't re-fire.
CREATE OR REPLACE FUNCTION core.trg_employees_manager_path_after()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	-- Защита от рекурсивного вызова: этот UPDATE меняет только manager_path,
	-- но на всякий случай пропускаем если триггер вызван изнутри другого триггера.
	IF pg_trigger_depth() > 1 THEN RETURN NULL; END IF;

	-- Если путь изменился — заменяем старый префикс на новый у всех потомков.
	-- subpath отрезает старый префикс, затем приклеиваем новый.
	IF OLD."manager_path" IS DISTINCT FROM NEW."manager_path" THEN
		UPDATE core.employees
		SET "manager_path" = NEW."manager_path" || subpath("manager_path", nlevel(OLD."manager_path"))
		WHERE "manager_path" <@ OLD."manager_path"
		  AND "id" != NEW."id";
	END IF;

	RETURN NULL;
END;
$$;

CREATE TRIGGER trg_employees_manager_path_after
	AFTER UPDATE OF "manager_id" ON core.employees
	FOR EACH ROW
	EXECUTE FUNCTION core.trg_employees_manager_path_after();

COMMIT;
