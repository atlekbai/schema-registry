package e2e_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/hrql/dialect/pg"
	"github.com/atlekbai/schema_registry/internal/hrql/parser"
	"github.com/atlekbai/schema_registry/internal/schema"
	"github.com/google/uuid"
)

// Stable UUIDs for predictable SQL output.
var (
	empObjID   = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	deptObjID  = uuid.MustParse("00000000-0000-0000-0000-000000000002")
	selfUUID   = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	targetUUID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
)

var testCache *schema.Cache

func TestMain(m *testing.M) {
	testCache = buildCache()
	os.Exit(m.Run())
}

func buildCache() *schema.Cache {
	// departments object (lookup target for employees.department)
	deptObj := &schema.ObjectDef{
		ID:              deptObjID,
		APIName:         "departments",
		Title:           "Department",
		PluralTitle:     "Departments",
		IsStandard:      true,
		StorageSchema:   new("core"),
		StorageTable:    new("departments"),
		FieldsByAPIName: make(map[string]*schema.FieldDef),
	}
	deptObj.Fields = []schema.FieldDef{
		{ID: uuid.New(), APIName: "title", Title: "Title", Type: schema.FieldText, IsStandard: true, StorageColumn: new("title")},
	}
	for i := range deptObj.Fields {
		deptObj.FieldsByAPIName[deptObj.Fields[i].APIName] = &deptObj.Fields[i]
	}

	empObj := &schema.ObjectDef{
		ID:              empObjID,
		APIName:         "employees",
		Title:           "Employee",
		PluralTitle:     "Employees",
		IsStandard:      true,
		StorageSchema:   new("core"),
		StorageTable:    new("employees"),
		FieldsByAPIName: make(map[string]*schema.FieldDef),
	}
	empObj.Fields = []schema.FieldDef{
		{ID: uuid.New(), APIName: "employee_number", Title: "Employee Number", Type: schema.FieldText, IsStandard: true, StorageColumn: new("employee_number")},
		{ID: uuid.New(), APIName: "employment_type", Title: "Employment Type", Type: schema.FieldText, IsStandard: true, StorageColumn: new("employment_type")},
		{ID: uuid.New(), APIName: "start_date", Title: "Start Date", Type: schema.FieldDate, IsStandard: true, StorageColumn: new("start_date")},
		{ID: uuid.New(), APIName: "manager", Title: "Manager", Type: schema.FieldLookup, IsStandard: true, StorageColumn: new("manager_id"), LookupObjectID: &empObjID},
		{ID: uuid.New(), APIName: "department", Title: "Department", Type: schema.FieldLookup, IsStandard: true, StorageColumn: new("department_id"), LookupObjectID: &deptObjID},
	}
	for i := range empObj.Fields {
		empObj.FieldsByAPIName[empObj.Fields[i].APIName] = &empObj.Fields[i]
	}

	return schema.NewCacheFromObjects(empObj, deptObj)
}

// pipeline runs Parse → Compile → Translate and returns the intermediate results.
func pipeline(t *testing.T, input, selfID string, selfObject ...string) (*hrql.Plan, *pg.SQLResult, string, []any) {
	t.Helper()

	var selfObj *schema.ObjectDef
	if len(selfObject) > 0 {
		selfObj = testCache.Get(selfObject[0])
	}

	ast, err := parser.Parse(input)
	require.NoError(t, err, "parse %q", input)

	comp := hrql.NewCompiler(testCache, selfID, selfObj)
	plan, err := comp.Compile(ast)
	require.NoError(t, err, "compile %q", input)

	obj := testCache.Get(plan.ObjectAPIName)
	if obj == nil {
		obj = testCache.Get("employees")
	}

	result, err := pg.Translate(plan, obj, testCache)
	require.NoError(t, err, "translate %q", input)

	// For scalar plans, serialize the Scalar sqlizer into sql+args.
	if plan.Kind == hrql.PlanScalar && result.Scalar != nil {
		sql, args, err := pg.PG.Select().Column(result.Scalar).ToSql()
		require.NoError(t, err, "scalar to sql %q", input)
		return plan, result, sql, args
	}

	return plan, result, "", nil
}

// pipelineErr runs the pipeline expecting an error.
func pipelineErr(input, selfID string, selfObject ...string) error {
	var selfObj *schema.ObjectDef
	if len(selfObject) > 0 {
		selfObj = testCache.Get(selfObject[0])
	}

	ast, err := parser.Parse(input)
	if err != nil {
		return err
	}

	comp := hrql.NewCompiler(testCache, selfID, selfObj)
	plan, err := comp.Compile(ast)
	if err != nil {
		return err
	}

	obj := testCache.Get(plan.ObjectAPIName)
	if obj == nil {
		obj = testCache.Get("employees")
	}

	_, err = pg.Translate(plan, obj, testCache)
	return err
}

// condToSQL extracts SQL and args from a single condition.
func condToSQL(t *testing.T, cond interface{ ToSql() (string, []any, error) }) (string, []any) {
	t.Helper()
	sql, args, err := cond.ToSql()
	require.NoError(t, err, "condition ToSql")
	return sql, args
}

// --- Test: basic list queries ---

func TestListFullScan(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees`, "")

	require.Equal(t, hrql.PlanList, plan.Kind)
	assert.Empty(t, result.Conditions)
}

func TestListSelf(t *testing.T) {
	plan, result, _, _ := pipeline(t, `self`, selfUUID)

	require.Equal(t, hrql.PlanList, plan.Kind)
	assert.Equal(t, 1, plan.Limit)
	require.Len(t, result.Conditions, 1)

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `"_e"."id" = ?`, sql)
	assert.Equal(t, []any{selfUUID}, args)
}

// --- Test: where conditions ---

func TestWhereFieldEquals(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type == "full_time")`, "")

	require.Len(t, result.Conditions, 1)

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `"_e"."employment_type" = ?`, sql)
	assert.Equal(t, []any{"full_time"}, args)
}

func TestWhereFieldNotEquals(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employee_number != "123")`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `"_e"."employee_number" <> ?`, sql)
	assert.Equal(t, []any{"123"}, args)
}

func TestWhereFieldGreaterThan(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.start_date > "2024-01-01")`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `"_e"."start_date" > ?`, sql)
	assert.Equal(t, []any{"2024-01-01"}, args)
}

func TestWhereAnd(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type == "full_time" and .start_date > "2024-01-01")`, "")

	require.Len(t, result.Conditions, 1)

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `("_e"."employment_type" = ? AND "_e"."start_date" > ?)`, sql)
	assert.Equal(t, []any{"full_time", "2024-01-01"}, args)
}

func TestWhereOr(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type == "full_time" or .employment_type == "part_time")`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `("_e"."employment_type" = ? OR "_e"."employment_type" = ?)`, sql)
	assert.Equal(t, []any{"full_time", "part_time"}, args)
}

// --- Test: string match operations ---

func TestWhereContains(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type | contains("full"))`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `"_e"."employment_type" ILIKE ?`, sql)
	assert.Equal(t, []any{"%full%"}, args)
}

func TestWhereStartsWith(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type | starts_with("full"))`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `"_e"."employment_type" ILIKE ?`, sql)
	assert.Equal(t, []any{"full%"}, args)
}

func TestWhereEndsWith(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type | ends_with("time"))`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `"_e"."employment_type" ILIKE ?`, sql)
	assert.Equal(t, []any{"%time"}, args)
}

// --- Test: sort and pick ---

func TestSortByAsc(t *testing.T) {
	plan, _, _, _ := pipeline(t, `employees | sort_by(.employee_number, asc)`, "")

	require.NotNil(t, plan.OrderBy)
	assert.Equal(t, "employee_number", plan.OrderBy.Field)
	assert.False(t, plan.OrderBy.Desc)
	assert.Equal(t, 0, plan.Limit)
}

func TestSortByDesc(t *testing.T) {
	plan, _, _, _ := pipeline(t, `employees | sort_by(.start_date, desc)`, "")

	require.NotNil(t, plan.OrderBy)
	assert.Equal(t, "start_date", plan.OrderBy.Field)
	assert.True(t, plan.OrderBy.Desc)
}

func TestPickFirst(t *testing.T) {
	plan, _, _, _ := pipeline(t, `employees | sort_by(.employee_number, asc) | first`, "")

	assert.Equal(t, 1, plan.Limit)
	assert.Equal(t, "first", plan.PickOp)
	require.NotNil(t, plan.OrderBy)
	assert.False(t, plan.OrderBy.Desc)
}

func TestPickLast(t *testing.T) {
	plan, _, _, _ := pipeline(t, `employees | sort_by(.employee_number, asc) | last`, "")

	assert.Equal(t, 1, plan.Limit)
	assert.Equal(t, "last", plan.PickOp)
	// `last` flips the sort order
	require.NotNil(t, plan.OrderBy)
	assert.True(t, plan.OrderBy.Desc)
}

func TestPickLastNoSort(t *testing.T) {
	plan, _, _, _ := pipeline(t, `employees | last`, "")

	assert.Equal(t, "last", plan.PickOp)
	// Without explicit sort, `last` adds ORDER BY id DESC
	require.NotNil(t, plan.OrderBy)
	assert.Equal(t, "id", plan.OrderBy.Field)
	assert.True(t, plan.OrderBy.Desc)
}

// --- Test: aggregation (PlanScalar) ---

func TestCountAll(t *testing.T) {
	plan, _, sql, args := pipeline(t, `employees | count`, "")

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t, "count", plan.AggFunc)
	assert.Equal(t, `SELECT (SELECT count(*) FROM "core"."employees" "_e")`, sql)
	assert.Empty(t, args)
}

func TestCountWithFilter(t *testing.T) {
	plan, _, sql, args := pipeline(t, `employees | where(.employment_type == "full_time") | count`, "")

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t, `SELECT (SELECT count(*) FROM "core"."employees" "_e" WHERE "_e"."employment_type" = $1)`, sql)
	assert.Equal(t, []any{"full_time"}, args)
}

func TestMinOnField(t *testing.T) {
	plan, _, sql, args := pipeline(t, `employees | .start_date | min`, "")

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t, "min", plan.AggFunc)
	assert.Equal(t, "start_date", plan.AggField)
	assert.Equal(t, `SELECT (SELECT min("_e"."start_date") FROM "core"."employees" "_e")`, sql)
	assert.Empty(t, args)
}

func TestMaxOnField(t *testing.T) {
	plan, _, sql, args := pipeline(t, `employees | .employee_number | max`, "")

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t, "max", plan.AggFunc)
	assert.Equal(t, `SELECT (SELECT max("_e"."employee_number") FROM "core"."employees" "_e")`, sql)
	assert.Empty(t, args)
}

func TestAvgOnDateField(t *testing.T) {
	plan, _, sql, args := pipeline(t, `employees | .start_date | avg`, "")

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t, "avg", plan.AggFunc)
	assert.Equal(t, "start_date", plan.AggField)
	assert.Equal(t, `SELECT (SELECT to_timestamp(avg(extract(epoch from "_e"."start_date")))::text FROM "core"."employees" "_e")`, sql)
	assert.Empty(t, args)
}

func TestLengthAsCount(t *testing.T) {
	plan, _, sql, args := pipeline(t, `employees | length`, "")

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t, "count", plan.AggFunc)
	assert.Equal(t, `SELECT (SELECT count(*) FROM "core"."employees" "_e")`, sql)
	assert.Empty(t, args)
}

// --- Test: org functions ---

func TestChainAll(t *testing.T) {
	plan, result, _, _ := pipeline(t, fmt.Sprintf(`chain("%s")`, targetUUID), "")

	require.Equal(t, hrql.PlanList, plan.Kind)
	require.Len(t, result.Conditions, 1)

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`(("_e"."manager_path") @> (SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?)) AND ("_e"."id") != (?))`,
		sql)
	assert.Equal(t, []any{targetUUID, targetUUID}, args)
}

func TestChainWithDepth(t *testing.T) {
	_, result, _, _ := pipeline(t, fmt.Sprintf(`chain("%s", 2)`, targetUUID), "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`("_e"."manager_path") = subpath(SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?), 0, GREATEST(nlevel(SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?)) - ?, 0))`,
		sql)
	assert.Equal(t, []any{targetUUID, targetUUID, 2}, args)
}

func TestReportsAll(t *testing.T) {
	_, result, _, _ := pipeline(t, fmt.Sprintf(`reports("%s")`, targetUUID), "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`(("_e"."manager_path") <@ (SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?)) AND ("_e"."manager_path") != (SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?)))`,
		sql)
	assert.Equal(t, []any{targetUUID, targetUUID}, args)
}

func TestReportsDirectDepth1(t *testing.T) {
	_, result, _, _ := pipeline(t, fmt.Sprintf(`reports("%s", 1)`, targetUUID), "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`(("_e"."manager_path") <@ (SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?)) AND nlevel("_e"."manager_path") = nlevel(SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?)) + ?)`,
		sql)
	assert.Equal(t, []any{targetUUID, targetUUID, 1}, args)
}

func TestPeers(t *testing.T) {
	_, result, _, _ := pipeline(t, `peers(self)`, selfUUID)

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`("_e"."manager_id" = (SELECT "manager_id" FROM "core"."employees" WHERE "id" = (?)) AND (SELECT "manager_id" FROM "core"."employees" WHERE "id" = (?)) IS NOT NULL AND ("_e"."id") != (?))`,
		sql)
	assert.Equal(t, []any{selfUUID, selfUUID, selfUUID}, args)
}

func TestColleagues(t *testing.T) {
	_, result, _, _ := pipeline(t, `colleagues(self, .department)`, selfUUID)

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`("_e"."department_id" = (SELECT "department_id" FROM "core"."employees" WHERE "id" = (?)) AND (SELECT "department_id" FROM "core"."employees" WHERE "id" = (?)) IS NOT NULL AND ("_e"."id") != (?))`,
		sql)
	assert.Equal(t, []any{selfUUID, selfUUID, selfUUID}, args)
}

// --- Test: reports_to (boolean → scalar) ---

func TestReportsToBoolean(t *testing.T) {
	plan, _, sql, args := pipeline(t, fmt.Sprintf(`reports_to(self, "%s")`, targetUUID), selfUUID)

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t,
		`SELECT ((SELECT "manager_path" FROM "core"."employees" WHERE "id" = ($1)) <@ (SELECT "manager_path" FROM "core"."employees" WHERE "id" = ($2)) AND (SELECT "manager_path" FROM "core"."employees" WHERE "id" = ($3)) != (SELECT "manager_path" FROM "core"."employees" WHERE "id" = ($4)))`,
		sql)
	assert.Equal(t, []any{selfUUID, targetUUID, selfUUID, targetUUID}, args)
}

func TestReportsToInWhere(t *testing.T) {
	_, result, _, _ := pipeline(t, fmt.Sprintf(`employees | where(reports_to(., "%s"))`, targetUUID), "")

	require.Len(t, result.Conditions, 1)

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`(("_e"."manager_path") <@ (SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?)) AND ("_e"."manager_path") != (SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?)))`,
		sql)
	assert.Equal(t, []any{targetUUID, targetUUID}, args)
}

// --- Test: self field references ---

func TestWhereFieldEqualsSelfField(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.department == self.department)`, selfUUID)

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`"_e"."department_id" = (SELECT "department_id" FROM "core"."employees" WHERE "id" = (?))`,
		sql)
	assert.Equal(t, []any{selfUUID}, args)
}

func TestChainWithSelfManager(t *testing.T) {
	_, result, _, _ := pipeline(t, `chain(self.manager)`, selfUUID)

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`(("_e"."manager_path") @> (SELECT "manager_path" FROM "core"."employees" WHERE "id" = (SELECT "manager_id" FROM "core"."employees" WHERE "id" = (?))) AND ("_e"."id") != (SELECT "manager_id" FROM "core"."employees" WHERE "id" = (?)))`,
		sql)
	assert.Equal(t, []any{selfUUID, selfUUID}, args)
}

// --- Test: lookup chain (cross-object field comparison) ---

func TestWhereLookupChain(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.department.title == "Engineering")`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`(SELECT "_sub"."title" FROM "core"."departments" "_sub" WHERE "_sub"."id" = "_e"."department_id") = ?`,
		sql)
	assert.Equal(t, []any{"Engineering"}, args)
}

// --- Test: subquery aggregate in where ---

func TestWhereSubqueryAgg(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(reports(., 1) | count > 0)`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`(SELECT count(*) FROM "core"."employees" "_sub_e" WHERE ("_sub_e"."manager_path") <@ ("_e"."manager_path") AND nlevel("_sub_e"."manager_path") = nlevel("_e"."manager_path") + ?) > ?`,
		sql)
	assert.Equal(t, []any{1, "0"}, args)
}

func TestWhereSubqueryAggAllReports(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(reports(.) | count > 5)`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t,
		`(SELECT count(*) FROM "core"."employees" "_sub_e" WHERE ("_sub_e"."manager_path") <@ ("_e"."manager_path") AND ("_sub_e"."manager_path") != ("_e"."manager_path")) > ?`,
		sql)
	assert.Equal(t, []any{"5"}, args)
}

// --- Test: combined pipeline (where + sort + pick + aggregate) ---

func TestFilterSortFirst(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | where(.employment_type == "full_time") | sort_by(.start_date, asc) | first`, "")

	require.Equal(t, hrql.PlanList, plan.Kind)
	assert.Equal(t, 1, plan.Limit)
	assert.Equal(t, "first", plan.PickOp)
	require.NotNil(t, plan.OrderBy)
	assert.False(t, plan.OrderBy.Desc)
	assert.Equal(t, "start_date", plan.OrderBy.Field)

	require.Len(t, result.Conditions, 1)
	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `"_e"."employment_type" = ?`, sql)
	assert.Equal(t, []any{"full_time"}, args)
}

func TestFilterThenCount(t *testing.T) {
	plan, _, sql, args := pipeline(t, `reports(self) | where(.employment_type == "full_time") | count`, selfUUID)

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t,
		`SELECT (SELECT count(*) FROM "core"."employees" "_e" WHERE (("_e"."manager_path") <@ (SELECT "manager_path" FROM "core"."employees" WHERE "id" = ($1)) AND ("_e"."manager_path") != (SELECT "manager_path" FROM "core"."employees" WHERE "id" = ($2))) AND "_e"."employment_type" = $3)`,
		sql)
	assert.Equal(t, []any{selfUUID, selfUUID, "full_time"}, args)
}

// --- Test: multiple where clauses ---

func TestMultipleWheres(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type == "full_time") | where(.start_date > "2024-01-01")`, "")

	require.Len(t, result.Conditions, 2)

	sql0, args0 := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `"_e"."employment_type" = ?`, sql0)
	assert.Equal(t, []any{"full_time"}, args0)

	sql1, args1 := condToSQL(t, result.Conditions[1])
	assert.Equal(t, `"_e"."start_date" > ?`, sql1)
	assert.Equal(t, []any{"2024-01-01"}, args1)
}

// --- Test: error cases ---

func TestErrors(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		selfID     string
		wantSubstr string
	}{
		{"no self_id", `self`, "", "self_id"},
		{"unknown field", `employees | where(.nonexistent == "val")`, "", "nonexistent"},
		{"unknown identifier", `nonexistent_obj`, "", "unknown object"},
		{"sort unknown field", `employees | sort_by(.nonexistent, asc)`, "", "nonexistent"},
		{"field access no source", `.employment_type`, "", ""},
		{"contains outside where", `employees | contains("test")`, "", "where"},
		{"peers without self", `peers(self)`, "", "self_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pipelineErr(tt.input, tt.selfID)
			require.Error(t, err)
			if tt.wantSubstr != "" {
				assert.Contains(t, err.Error(), tt.wantSubstr)
			}
		})
	}
}

// --- Test: passthrough pipe functions ---

func TestUniquePassthrough(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | unique`, "")

	require.Equal(t, hrql.PlanList, plan.Kind)
	assert.Empty(t, result.Conditions)
}

// --- Test: arithmetic expressions ---

func TestArithPureLiterals(t *testing.T) {
	plan, _, sql, args := pipeline(t, `1 + 2`, "")

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	require.NotNil(t, plan.ScalarExpr)
	assert.Equal(t, `SELECT ($1::numeric + $2::numeric)`, sql)
	assert.Equal(t, []any{"1", "2"}, args)
}

func TestArithLiteralPlusCount(t *testing.T) {
	plan, _, sql, args := pipeline(t, `1 + (employees | count)`, "")

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t, `SELECT ($1::numeric + (SELECT count(*) FROM "core"."employees" "_e"))`, sql)
	assert.Equal(t, []any{"1"}, args)
}

func TestArithCountTimesLiteral(t *testing.T) {
	plan, _, sql, args := pipeline(t, `(employees | count) * 2`, "")

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t, `SELECT ((SELECT count(*) FROM "core"."employees" "_e") * $1::numeric)`, sql)
	assert.Equal(t, []any{"2"}, args)
}

func TestArithReportsCount(t *testing.T) {
	plan, _, sql, args := pipeline(t, `1 + (reports(self, 0) | count)`, selfUUID)

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t,
		`SELECT ($1::numeric + (SELECT count(*) FROM "core"."employees" "_e" WHERE (("_e"."manager_path") <@ (SELECT "manager_path" FROM "core"."employees" WHERE "id" = ($2)) AND ("_e"."manager_path") != (SELECT "manager_path" FROM "core"."employees" WHERE "id" = ($3)))))`,
		sql)
	assert.Equal(t, []any{"1", selfUUID, selfUUID}, args)
}

func TestArithTwoSubqueries(t *testing.T) {
	plan, _, sql, args := pipeline(t, `(employees | count) + (reports(self, 0) | count)`, selfUUID)

	require.Equal(t, hrql.PlanScalar, plan.Kind)
	assert.Equal(t,
		`SELECT ((SELECT count(*) FROM "core"."employees" "_e") + (SELECT count(*) FROM "core"."employees" "_e" WHERE (("_e"."manager_path") <@ (SELECT "manager_path" FROM "core"."employees" WHERE "id" = ($1)) AND ("_e"."manager_path") != (SELECT "manager_path" FROM "core"."employees" WHERE "id" = ($2)))))`,
		sql)
	assert.Equal(t, []any{selfUUID, selfUUID}, args)
	assert.Equal(t, 2, strings.Count(sql, `count(*)`))
}

// --- Test: literal reversed comparison ---

func TestReversedComparison(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where("2024-01-01" < .start_date)`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assert.Equal(t, `"_e"."start_date" > ?`, sql)
	assert.Equal(t, []any{"2024-01-01"}, args)
}

// --- Test: chained pipeline with pick + re-source ---

func TestChainWhereLastChain(t *testing.T) {
	plan, result, _, _ := pipeline(t, `chain(self) | where(.department == self.department) | last | chain(., 1)`, selfUUID)

	require.Equal(t, hrql.PlanList, plan.Kind)
	require.Equal(t, "employees", plan.ObjectAPIName)

	// chain(., 1) produces a single OrgChainUp condition with SubPlan ref
	require.Len(t, result.Conditions, 1)

	sql, args := condToSQL(t, result.Conditions[0])

	// The inner subquery selects the ID from: chain(self) | where(.department == self.department) | last
	innerSub := `SELECT "_e"."id" FROM "core"."employees" "_e" WHERE (("_e"."manager_path") @> (SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?)) AND ("_e"."id") != (?)) AND "_e"."department_id" = (SELECT "department_id" FROM "core"."employees" WHERE "id" = (?)) ORDER BY "_e"."id" DESC LIMIT 1`
	pathSub := `SELECT "manager_path" FROM "core"."employees" WHERE "id" = (` + innerSub + `)`

	assert.Equal(t,
		`("_e"."manager_path") = subpath(`+pathSub+`, 0, GREATEST(nlevel(`+pathSub+`) - ?, 0))`,
		sql)
	assert.Equal(t, []any{selfUUID, selfUUID, selfUUID, selfUUID, selfUUID, selfUUID, 1}, args)
}

// --- Test: generic object support ---

func TestDepartmentsFullScan(t *testing.T) {
	plan, result, _, _ := pipeline(t, `departments`, "")

	require.Equal(t, hrql.PlanList, plan.Kind)
	assert.Equal(t, "departments", plan.ObjectAPIName)
	assert.Empty(t, result.Conditions)
}
