begin;

-- Register standard objects
SELECT metadata.register_object('IT', 'users', 'User', 'Users',
	'Authentication identity - can be linked to individuals and employees',
	TRUE, 'core', 'users', FALSE);

SELECT metadata.register_object('HR', 'organizations', 'Organization', 'Organizations',
	'Business units or organizational entities',
	TRUE, 'core', 'organizations', TRUE);

SELECT metadata.register_object('HR', 'departments', 'Department', 'Departments',
	'Departments within organizations - supports recursive hierarchy',
	TRUE, 'core', 'departments', TRUE);

SELECT metadata.register_object('HR', 'individuals', 'Individual', 'Individuals',
	'Person records containing PII',
	TRUE, 'core', 'individuals', TRUE);

SELECT metadata.register_object('HR', 'employees', 'Employee', 'Employees',
	'HR employee records linking individuals to organizational structure',
	TRUE, 'core', 'employees', TRUE);

-- Organization fields
SELECT metadata.add_field('organizations', 'title', 'Title', 'Organization name',
	'TEXT', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'title');

-- Department fields
SELECT metadata.add_field('departments', 'title', 'Title', 'Department name',
	'TEXT', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'title');

SELECT metadata.add_field('departments', 'organization', 'Organization', 'Parent organization',
	'LOOKUP', p_is_required := TRUE, p_is_standard := TRUE,
	p_storage_column := 'organization_id', p_lookup_object_api_name := 'organizations');

SELECT metadata.add_field('departments', 'parent', 'Parent', 'Parent department for hierarchy',
	'LOOKUP', p_is_standard := TRUE,
	p_storage_column := 'parent_id', p_lookup_object_api_name := 'departments');

-- Individual fields
SELECT metadata.add_field('individuals', 'email', 'Email', 'Email address',
	'EMAIL', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'email');

SELECT metadata.add_field('individuals', 'first_name', 'First Name', 'Person first name',
	'TEXT', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'first_name');

SELECT metadata.add_field('individuals', 'last_name', 'Last Name', 'Person last name',
	'TEXT', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'last_name');

-- Employee fields
SELECT metadata.add_field('employees', 'user', 'User', 'Linked authentication identity',
	'LOOKUP', p_is_standard := TRUE,
	p_storage_column := 'user_id', p_lookup_object_api_name := 'users');

SELECT metadata.add_field('employees', 'individual', 'Individual', 'Person record (PII)',
	'LOOKUP', p_is_required := TRUE, p_is_standard := TRUE,
	p_storage_column := 'individual_id', p_lookup_object_api_name := 'individuals');

SELECT metadata.add_field('employees', 'organization', 'Organization', 'Employee organization',
	'LOOKUP', p_is_required := TRUE, p_is_standard := TRUE,
	p_storage_column := 'organization_id', p_lookup_object_api_name := 'organizations');

SELECT metadata.add_field('employees', 'department', 'Department', 'Employee department',
	'LOOKUP', p_is_required := TRUE, p_is_standard := TRUE,
	p_storage_column := 'department_id', p_lookup_object_api_name := 'departments');

SELECT metadata.add_field('employees', 'manager', 'Manager', 'Reporting manager',
	'LOOKUP', p_is_standard := TRUE,
	p_storage_column := 'manager_id', p_lookup_object_api_name := 'employees');

SELECT metadata.add_field('employees', 'employee_number', 'Employee Number', 'Unique employee identifier',
	'TEXT', p_is_required := TRUE, p_is_unique := TRUE, p_is_standard := TRUE,
	p_storage_column := 'employee_number');

SELECT metadata.add_field('employees', 'employment_type', 'Employment Type', 'Type of employment relationship',
	'CHOICE', '{"options": ["FULL_TIME", "PART_TIME", "CONTRACTOR", "INTERN"]}',
	p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'employment_type');

SELECT metadata.add_field('employees', 'start_date', 'Start Date', 'Employment start date',
	'DATE', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'start_date');

SELECT metadata.add_field('employees', 'end_date', 'End Date', 'Employment end date',
	'DATE', p_is_standard := TRUE, p_storage_column := 'end_date');

commit;
