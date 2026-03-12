package pg

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atlekbai/schema_registry/internal/schema"
)

var (
	deptObjID = uuid.MustParse("00000000-0000-0000-0000-000000000002")
	empObjID  = uuid.MustParse("00000000-0000-0000-0000-000000000001")
)

func testDeptObj() *schema.ObjectDef {
	obj := &schema.ObjectDef{
		ID: deptObjID, APIName: "departments",
		IsStandard: true, StorageSchema: new("core"), StorageTable: new("departments"),
		FieldsByAPIName: make(map[string]*schema.FieldDef),
	}
	obj.Fields = []schema.FieldDef{
		{ID: uuid.New(), APIName: "title", Type: schema.FieldText, IsStandard: true, StorageColumn: new("title")},
	}
	for i := range obj.Fields {
		obj.FieldsByAPIName[obj.Fields[i].APIName] = &obj.Fields[i]
	}
	return obj
}

func testEmpObj() *schema.ObjectDef {
	obj := &schema.ObjectDef{
		ID: empObjID, APIName: "employees",
		IsStandard: true, StorageSchema: new("core"), StorageTable: new("employees"),
		FieldsByAPIName: make(map[string]*schema.FieldDef),
	}
	obj.Fields = []schema.FieldDef{
		{ID: uuid.New(), APIName: "employee_number", Type: schema.FieldText, IsStandard: true, StorageColumn: new("employee_number")},
		{ID: uuid.New(), APIName: "department", Type: schema.FieldLookup, IsStandard: true, StorageColumn: new("department_id"), LookupObjectID: &deptObjID},
		{ID: uuid.New(), APIName: "manager", Type: schema.FieldLookup, IsStandard: true, StorageColumn: new("manager_id"), LookupObjectID: &empObjID},
	}
	for i := range obj.Fields {
		obj.FieldsByAPIName[obj.Fields[i].APIName] = &obj.Fields[i]
	}
	return obj
}

func TestExpandCase(t *testing.T) {
	sql, args, err := expandCase("_xp_department").ToSql()
	require.NoError(t, err)

	assert.Equal(t,
		`CASE WHEN "_xp_department"."id" IS NOT NULL THEN to_jsonb("_xp_department".*) ELSE NULL END`,
		sql)
	assert.Empty(t, args)
}

func TestLateralJoinSimple(t *testing.T) {
	dept := testDeptObj()
	ep := &ExpandPlan{
		FieldName: "department",
		Field:     testEmpObj().FieldsByAPIName["department"],
		Target:    dept,
	}

	join := lateralJoin(ep, `"_e"."department_id"`, "", 0)
	sql, args, err := join.ToSql()
	require.NoError(t, err)

	assert.Equal(t,
		`LEFT JOIN LATERAL (`+
			`SELECT "_xp_department_t"."id", "_xp_department_t"."created_at", "_xp_department_t"."updated_at", `+
			`("_xp_department_t"."title") AS "title" `+
			`FROM "core"."departments" "_xp_department_t" `+
			`WHERE "_xp_department_t"."id" = "_e"."department_id"`+
			`) "_xp_department" ON TRUE`,
		sql)
	assert.Empty(t, args)
}

func TestLateralJoinNested(t *testing.T) {
	dept := testDeptObj()
	emp := testEmpObj()
	ep := &ExpandPlan{
		FieldName: "manager",
		Field:     emp.FieldsByAPIName["manager"],
		Target:    emp,
		Children: []ExpandPlan{
			{
				FieldName: "department",
				Field:     emp.FieldsByAPIName["department"],
				Target:    dept,
			},
		},
	}

	join := lateralJoin(ep, `"_e"."manager_id"`, "", 0)
	sql, args, err := join.ToSql()
	require.NoError(t, err)

	// The nested expand should produce a CASE WHEN for department
	// and a nested LEFT JOIN LATERAL for the child.
	assert.Contains(t, sql, `LEFT JOIN LATERAL (SELECT`)
	assert.Contains(t, sql, `"_xp_manager_t"."id" = "_e"."manager_id"`)
	assert.Contains(t, sql, `"_xp_manager__department_t"."id" = "_xp_manager_t"."department_id"`)
	assert.Contains(t, sql, `CASE WHEN "_xp_manager__department"."id" IS NOT NULL`)
	assert.Contains(t, sql, `) "_xp_manager" ON TRUE`)
	assert.Empty(t, args)
}

func TestLateralJoinMaxDepth(t *testing.T) {
	dept := testDeptObj()
	emp := testEmpObj()

	// depth=1 with maxExpandDepth=2: children should NOT be expanded (depth >= maxExpandDepth-1)
	ep := &ExpandPlan{
		FieldName: "manager",
		Field:     emp.FieldsByAPIName["manager"],
		Target:    emp,
		Children: []ExpandPlan{
			{
				FieldName: "department",
				Field:     emp.FieldsByAPIName["department"],
				Target:    dept,
			},
		},
	}

	join := lateralJoin(ep, `"_e"."manager_id"`, "", 1)
	sql, _, err := join.ToSql()
	require.NoError(t, err)

	// At depth=1, children are NOT expanded — department appears as a plain column, not a CASE/lateral
	assert.NotContains(t, sql, `CASE WHEN`)
	assert.NotContains(t, sql, `_xp_manager__department`)
	assert.Contains(t, sql, `"_xp_manager_t"."department_id"`)
}

func TestAddLateralJoins(t *testing.T) {
	dept := testDeptObj()
	emp := testEmpObj()
	params := &QueryParams{
		ExpandPlans: []ExpandPlan{
			{
				FieldName: "department",
				Field:     emp.FieldsByAPIName["department"],
				Target:    dept,
			},
		},
	}

	qb := addLateralJoins(
		sq.Select("1").From(`"core"."employees" "_e"`),
		params.ExpandPlans,
	)

	sql, args, err := qb.ToSql()
	require.NoError(t, err)

	assert.Contains(t, sql, `LEFT JOIN LATERAL`)
	assert.Contains(t, sql, `"_xp_department_t"."id" = "_e"."department_id"`)
	assert.Contains(t, sql, `) "_xp_department" ON TRUE`)
	assert.Empty(t, args)
}
