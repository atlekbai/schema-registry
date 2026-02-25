begin;

-- Register standard objects
SELECT metadata.register_object('IT', 'User', 'User', 'Users',
	'Authentication identity - can be linked to individuals and employees',
	TRUE, 'core', 'users', FALSE);

SELECT metadata.register_object('HR', 'Organization', 'Organization', 'Organizations',
	'Business units or organizational entities',
	TRUE, 'core', 'organizations', TRUE);

SELECT metadata.register_object('HR', 'Department', 'Department', 'Departments',
	'Departments within organizations - supports recursive hierarchy',
	TRUE, 'core', 'departments', TRUE);

SELECT metadata.register_object('HR', 'Individual', 'Individual', 'Individuals',
	'Person records containing PII',
	TRUE, 'core', 'individuals', TRUE);

SELECT metadata.register_object('HR', 'Employee', 'Employee', 'Employees',
	'HR employee records linking individuals to organizational structure',
	TRUE, 'core', 'employees', TRUE);

-- Organization fields
SELECT metadata.add_field('Organization', 'Title', 'Title', 'Organization name',
	'TEXT', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'title');

-- Department fields
SELECT metadata.add_field('Department', 'Title', 'Title', 'Department name',
	'TEXT', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'title');

SELECT metadata.add_field('Department', 'Organization', 'Organization', 'Parent organization',
	'LOOKUP', p_is_required := TRUE, p_is_standard := TRUE,
	p_storage_column := 'organization_id', p_lookup_object_api_name := 'Organization');

SELECT metadata.add_field('Department', 'Parent', 'Parent', 'Parent department for hierarchy',
	'LOOKUP', p_is_standard := TRUE,
	p_storage_column := 'parent_id', p_lookup_object_api_name := 'Department');

-- Individual fields
SELECT metadata.add_field('Individual', 'Email', 'Email', 'Email address',
	'EMAIL', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'email');

SELECT metadata.add_field('Individual', 'FirstName', 'First Name', 'Person first name',
	'TEXT', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'first_name');

SELECT metadata.add_field('Individual', 'LastName', 'Last Name', 'Person last name',
	'TEXT', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'last_name');

-- Employee fields
SELECT metadata.add_field('Employee', 'User', 'User', 'Linked authentication identity',
	'LOOKUP', p_is_standard := TRUE,
	p_storage_column := 'user_id', p_lookup_object_api_name := 'User');

SELECT metadata.add_field('Employee', 'Individual', 'Individual', 'Person record (PII)',
	'LOOKUP', p_is_required := TRUE, p_is_standard := TRUE,
	p_storage_column := 'individual_id', p_lookup_object_api_name := 'Individual');

SELECT metadata.add_field('Employee', 'Organization', 'Organization', 'Employee organization',
	'LOOKUP', p_is_required := TRUE, p_is_standard := TRUE,
	p_storage_column := 'organization_id', p_lookup_object_api_name := 'Organization');

SELECT metadata.add_field('Employee', 'Department', 'Department', 'Employee department',
	'LOOKUP', p_is_required := TRUE, p_is_standard := TRUE,
	p_storage_column := 'department_id', p_lookup_object_api_name := 'Department');

SELECT metadata.add_field('Employee', 'Manager', 'Manager', 'Reporting manager',
	'LOOKUP', p_is_standard := TRUE,
	p_storage_column := 'manager_id', p_lookup_object_api_name := 'Employee');

SELECT metadata.add_field('Employee', 'EmployeeNumber', 'Employee Number', 'Unique employee identifier',
	'TEXT', p_is_required := TRUE, p_is_unique := TRUE, p_is_standard := TRUE,
	p_storage_column := 'employee_number');

SELECT metadata.add_field('Employee', 'EmploymentType', 'Employment Type', 'Type of employment relationship',
	'CHOICE', '{"options": ["FULL_TIME", "PART_TIME", "CONTRACTOR", "INTERN"]}',
	p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'employment_type');

SELECT metadata.add_field('Employee', 'StartDate', 'Start Date', 'Employment start date',
	'DATE', p_is_required := TRUE, p_is_standard := TRUE, p_storage_column := 'start_date');

SELECT metadata.add_field('Employee', 'EndDate', 'End Date', 'Employment end date',
	'DATE', p_is_standard := TRUE, p_storage_column := 'end_date');

commit;
