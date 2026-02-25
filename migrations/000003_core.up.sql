begin;

-- Create core schema
CREATE SCHEMA IF NOT EXISTS core;

-- Users (authentication identity, can be linked to individuals/employees)
CREATE TABLE core.users (
	"id"			UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
	"created_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),
	"updated_at"	TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Organizations (business units within a tenant)
CREATE TABLE core.organizations (
	"id"			UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
	"created_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),
	"updated_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),
	"title"			TEXT NOT NULL,
	"custom_fields"	JSONB NOT NULL DEFAULT '{}'
);

-- Departments (can be nested - recursive hierarchy)
CREATE TABLE core.departments (
	"id"				UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
	"created_at"		TIMESTAMPTZ NOT NULL DEFAULT now(),
	"updated_at"		TIMESTAMPTZ NOT NULL DEFAULT now(),
	"organization_id"	UUID NOT NULL REFERENCES core.organizations("id") ON DELETE CASCADE,
	"parent_id"			UUID REFERENCES core.departments("id") ON DELETE CASCADE,
	"title"				TEXT NOT NULL,
	"custom_fields"		JSONB NOT NULL DEFAULT '{}'
);

-- Individuals (person data - PII)
CREATE TABLE core.individuals (
	"id"			UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
	"created_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),
	"updated_at"	TIMESTAMPTZ NOT NULL DEFAULT now(),
	"email"			TEXT NOT NULL,
	"first_name"	TEXT NOT NULL,
	"last_name"		TEXT NOT NULL,
	"custom_fields"	JSONB NOT NULL DEFAULT '{}'
);

-- Employees (HR record, the protected resource)
CREATE TABLE core.employees (
	"id"				UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
	"created_at"		TIMESTAMPTZ NOT NULL DEFAULT now(),
	"updated_at"		TIMESTAMPTZ NOT NULL DEFAULT now(),
	"user_id"			UUID REFERENCES core.users("id") ON DELETE SET NULL,
	"individual_id"		UUID NOT NULL REFERENCES core.individuals("id") ON DELETE RESTRICT,
	"organization_id"	UUID NOT NULL REFERENCES core.organizations("id") ON DELETE RESTRICT,
	"department_id"		UUID NOT NULL REFERENCES core.departments("id") ON DELETE RESTRICT,
	"manager_id"		UUID REFERENCES core.employees("id") ON DELETE SET NULL,
	"employee_number"	TEXT NOT NULL,
	"employment_type"	TEXT NOT NULL DEFAULT 'FULL_TIME',
	"start_date"		DATE NOT NULL,
	"end_date"			DATE,
	"custom_fields"		JSONB NOT NULL DEFAULT '{}',

	-- Constraints
	UNIQUE ("employee_number"),
	CONSTRAINT chk_employees_employment_type CHECK (
		"employment_type" IN ('FULL_TIME', 'PART_TIME', 'CONTRACTOR', 'INTERN')
	),
	CONSTRAINT chk_employees_end_date CHECK (
		"end_date" IS NULL OR "end_date" >= "start_date"
	)
);

-- Indexes for performance
CREATE INDEX idx_organizations_custom_fields ON core.organizations USING GIN ("custom_fields" jsonb_path_ops);

CREATE INDEX idx_departments_organization_id ON core.departments("organization_id");
CREATE INDEX idx_departments_parent_id ON core.departments("parent_id") WHERE "parent_id" IS NOT NULL;
CREATE INDEX idx_departments_custom_fields ON core.departments USING GIN ("custom_fields" jsonb_path_ops);

CREATE INDEX idx_individuals_email ON core.individuals("email");
CREATE INDEX idx_individuals_custom_fields ON core.individuals USING GIN ("custom_fields" jsonb_path_ops);

CREATE INDEX idx_employees_user_id ON core.employees("user_id") WHERE "user_id" IS NOT NULL;
CREATE INDEX idx_employees_individual_id ON core.employees("individual_id");
CREATE INDEX idx_employees_organization_id ON core.employees("organization_id");
CREATE INDEX idx_employees_department_id ON core.employees("department_id");
CREATE INDEX idx_employees_manager_id ON core.employees("manager_id") WHERE "manager_id" IS NOT NULL;
CREATE INDEX idx_employees_employment_type ON core.employees("employment_type");
CREATE INDEX idx_employees_custom_fields ON core.employees USING GIN ("custom_fields" jsonb_path_ops);

-- Table comments for documentation
COMMENT ON TABLE core.users IS 'Authentication identities - can be linked to individuals and employees';
COMMENT ON TABLE core.organizations IS 'Business units or organizational entities within a tenant';
COMMENT ON TABLE core.departments IS 'Departments within organizations - supports recursive hierarchy via parent_id';
COMMENT ON TABLE core.individuals IS 'Person records containing PII - separated from employee records for data governance';
COMMENT ON TABLE core.employees IS 'HR employee records - the protected resource linking individuals to organizational structure';

COMMENT ON COLUMN core.departments.parent_id IS 'Self-referential foreign key enabling nested department hierarchies';
COMMENT ON COLUMN core.employees.user_id IS 'Optional link to authentication identity - NULL if individual has no system access';
COMMENT ON COLUMN core.employees.individual_id IS 'Required link to person data (PII) - employees must have associated individual record';
COMMENT ON COLUMN core.employees.manager_id IS 'Self-referential foreign key for manager-employee reporting relationships';
COMMENT ON COLUMN core.employees.employee_number IS 'Unique identifier for employee within organization (e.g., badge number, HR ID)';
COMMENT ON COLUMN core.employees.employment_type IS 'Type of employment relationship: FULL_TIME, PART_TIME, CONTRACTOR, or INTERN';

commit;
