package e2e_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/hrql/parser"
	"github.com/atlekbai/schema_registry/internal/hrql/pg"
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

	// employees object
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
		{ID: uuid.New(), APIName: "employment_type", Title: "Employment Type", Type: schema.FieldChoice, IsStandard: true, StorageColumn: new("employment_type")},
		{ID: uuid.New(), APIName: "start_date", Title: "Start Date", Type: schema.FieldDate, IsStandard: true, StorageColumn: new("start_date")},
		{ID: uuid.New(), APIName: "end_date", Title: "End Date", Type: schema.FieldDate, IsStandard: true, StorageColumn: new("end_date")},
		{ID: uuid.New(), APIName: "manager", Title: "Manager", Type: schema.FieldLookup, IsStandard: true, StorageColumn: new("manager_id"), LookupObjectID: new(empObjID)},
		{ID: uuid.New(), APIName: "department", Title: "Department", Type: schema.FieldLookup, IsStandard: true, StorageColumn: new("department_id"), LookupObjectID: new(deptObjID)},
	}
	for i := range empObj.Fields {
		empObj.FieldsByAPIName[empObj.Fields[i].APIName] = &empObj.Fields[i]
	}

	return schema.NewCacheFromObjects(deptObj, empObj)
}

// pipeline runs the full HRQL pipeline: Parse → Compile → Translate.
// Returns plan, SQLResult (for list/scalar), or boolSQL+boolArgs (for boolean).
func pipeline(t *testing.T, input, selfID string) (*hrql.Plan, *pg.SQLResult, string, []any) {
	t.Helper()

	ast, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("parse %q: %v", input, err)
	}

	comp := hrql.NewCompiler(testCache, selfID)
	plan, err := comp.Compile(ast)
	if err != nil {
		t.Fatalf("compile %q: %v", input, err)
	}

	empObj := testCache.Get("employees")

	if plan.Kind == hrql.PlanBoolean {
		sql, args, err := pg.TranslateBooleanPlan(plan, empObj)
		if err != nil {
			t.Fatalf("translate boolean %q: %v", input, err)
		}
		return plan, nil, sql, args
	}

	result, err := pg.Translate(plan, empObj, testCache)
	if err != nil {
		t.Fatalf("translate %q: %v", input, err)
	}
	return plan, result, "", nil
}

// pipelineErr runs the pipeline expecting an error.
func pipelineErr(input, selfID string) error {
	ast, err := parser.Parse(input)
	if err != nil {
		return err
	}

	comp := hrql.NewCompiler(testCache, selfID)
	plan, err := comp.Compile(ast)
	if err != nil {
		return err
	}

	empObj := testCache.Get("employees")

	if plan.Kind == hrql.PlanBoolean {
		_, _, err = pg.TranslateBooleanPlan(plan, empObj)
		return err
	}

	_, err = pg.Translate(plan, empObj, testCache)
	return err
}

// condToSQL extracts SQL and args from a single condition.
func condToSQL(t *testing.T, cond interface{ ToSql() (string, []any, error) }) (string, []any) {
	t.Helper()
	sql, args, err := cond.ToSql()
	if err != nil {
		t.Fatalf("condition ToSql: %v", err)
	}
	return sql, args
}

// assertContains checks that sql contains the substring.
func assertContains(t *testing.T, sql, substr string) {
	t.Helper()
	if !strings.Contains(sql, substr) {
		t.Errorf("SQL %q does not contain %q", sql, substr)
	}
}

// assertArgCount checks that args has the expected length.
func assertArgCount(t *testing.T, args []any, want int) {
	t.Helper()
	if len(args) != want {
		t.Errorf("expected %d args, got %d: %v", want, len(args), args)
	}
}

// assertArgEquals checks that args[i] string-equals the expected value.
func assertArgEquals(t *testing.T, args []any, i int, want any) {
	t.Helper()
	if i >= len(args) {
		t.Fatalf("arg index %d out of range (len=%d)", i, len(args))
	}
	if fmt.Sprintf("%v", args[i]) != fmt.Sprintf("%v", want) {
		t.Errorf("args[%d] = %v, want %v", i, args[i], want)
	}
}

// --- Test: basic list queries ---

func TestListFullScan(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees`, "")

	if plan.Kind != hrql.PlanList {
		t.Fatalf("expected PlanList, got %v", plan.Kind)
	}
	if len(result.Conditions) != 0 {
		t.Fatalf("expected 0 conditions, got %d", len(result.Conditions))
	}
}

func TestListSelf(t *testing.T) {
	plan, result, _, _ := pipeline(t, `self`, selfUUID)

	if plan.Kind != hrql.PlanList {
		t.Fatalf("expected PlanList, got %v", plan.Kind)
	}
	if plan.Limit != 1 {
		t.Fatalf("expected Limit=1, got %d", plan.Limit)
	}
	if len(result.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result.Conditions))
	}

	sql, args := condToSQL(t, result.Conditions[0])
	assertContains(t, sql, `"_e"."id"`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, selfUUID)
}

// --- Test: where conditions ---

func TestWhereFieldEquals(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type == "full_time")`, "")

	if len(result.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result.Conditions))
	}

	sql, args := condToSQL(t, result.Conditions[0])
	assertContains(t, sql, `"_e"."employment_type"`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, "full_time")
}

func TestWhereFieldNotEquals(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employee_number != "123")`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assertContains(t, sql, `"_e"."employee_number"`)
	assertContains(t, sql, `<>`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, "123")
}

func TestWhereFieldGreaterThan(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.start_date > "2024-01-01")`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assertContains(t, sql, `"_e"."start_date"`)
	assertContains(t, sql, `>`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, "2024-01-01")
}

func TestWhereAnd(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type == "full_time" and .start_date > "2024-01-01")`, "")

	if len(result.Conditions) != 1 {
		t.Fatalf("expected 1 (AND) condition, got %d", len(result.Conditions))
	}

	sql, args := condToSQL(t, result.Conditions[0])
	assertContains(t, sql, `"_e"."employment_type"`)
	assertContains(t, sql, `"_e"."start_date"`)
	assertArgCount(t, args, 2)
	assertArgEquals(t, args, 0, "full_time")
	assertArgEquals(t, args, 1, "2024-01-01")
}

func TestWhereOr(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type == "full_time" or .employment_type == "part_time")`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assertContains(t, sql, `"_e"."employment_type"`)
	assertArgCount(t, args, 2)
	assertArgEquals(t, args, 0, "full_time")
	assertArgEquals(t, args, 1, "part_time")
}

// --- Test: string match operations ---

func TestWhereContains(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type | contains("full"))`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assertContains(t, sql, `"_e"."employment_type" ILIKE`)
	assertContains(t, sql, `'%' || ? || '%'`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, "full")
}

func TestWhereStartsWith(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type | starts_with("full"))`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assertContains(t, sql, `ILIKE ? || '%'`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, "full")
}

func TestWhereEndsWith(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type | ends_with("time"))`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	assertContains(t, sql, `ILIKE '%' || ?`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, "time")
}

// --- Test: sort and pick ---

func TestSortByAsc(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | sort_by(.employee_number, asc)`, "")

	if result.OrderBy == nil {
		t.Fatal("expected OrderBy, got nil")
	}
	if result.OrderBy.FieldAPIName != "employee_number" {
		t.Errorf("expected order field employee_number, got %q", result.OrderBy.FieldAPIName)
	}
	if result.OrderBy.Desc {
		t.Error("expected ascending order")
	}
	if plan.Limit != 0 {
		t.Errorf("expected no limit, got %d", plan.Limit)
	}
}

func TestSortByDesc(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | sort_by(.start_date, desc)`, "")

	if result.OrderBy == nil {
		t.Fatal("expected OrderBy, got nil")
	}
	if result.OrderBy.FieldAPIName != "start_date" {
		t.Errorf("expected order field start_date, got %q", result.OrderBy.FieldAPIName)
	}
	if !result.OrderBy.Desc {
		t.Error("expected descending order")
	}
}

func TestPickFirst(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | sort_by(.employee_number, asc) | first`, "")

	if plan.Limit != 1 {
		t.Errorf("expected Limit=1, got %d", plan.Limit)
	}
	if result.PickOp != "first" {
		t.Errorf("expected PickOp=first, got %q", result.PickOp)
	}
	if result.OrderBy == nil || result.OrderBy.Desc {
		t.Error("expected ascending order for first")
	}
}

func TestPickLast(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | sort_by(.employee_number, asc) | last`, "")

	if plan.Limit != 1 {
		t.Errorf("expected Limit=1, got %d", plan.Limit)
	}
	if result.PickOp != "last" {
		t.Errorf("expected PickOp=last, got %q", result.PickOp)
	}
	// `last` flips the sort order
	if result.OrderBy == nil || !result.OrderBy.Desc {
		t.Error("expected descending order for last (flipped)")
	}
}

func TestPickLastNoSort(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | last`, "")

	if result.PickOp != "last" {
		t.Errorf("expected PickOp=last, got %q", result.PickOp)
	}
	// Without explicit sort, `last` adds ORDER BY id DESC
	if result.OrderBy == nil {
		t.Fatal("expected OrderBy, got nil")
	}
	if result.OrderBy.FieldAPIName != "id" {
		t.Errorf("expected order by id, got %q", result.OrderBy.FieldAPIName)
	}
	if !result.OrderBy.Desc {
		t.Error("expected descending order")
	}
}

// --- Test: aggregation (PlanScalar) ---

func TestCountAll(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | count`, "")

	if plan.Kind != hrql.PlanScalar {
		t.Fatalf("expected PlanScalar, got %v", plan.Kind)
	}
	if plan.AggFunc != "count" {
		t.Errorf("expected AggFunc=count, got %q", plan.AggFunc)
	}

	assertContains(t, result.AggSQL, `count(*)`)
	assertContains(t, result.AggSQL, `"core"."employees"`)
}

func TestCountWithFilter(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | where(.employment_type == "full_time") | count`, "")

	if plan.Kind != hrql.PlanScalar {
		t.Fatalf("expected PlanScalar, got %v", plan.Kind)
	}

	assertContains(t, result.AggSQL, `count(*)`)
	assertContains(t, result.AggSQL, `"_e"."employment_type"`)
	if len(result.AggArgs) != 1 {
		t.Fatalf("expected 1 agg arg, got %d", len(result.AggArgs))
	}
	assertArgEquals(t, result.AggArgs, 0, "full_time")
}

func TestMinOnField(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | .start_date | min`, "")

	if plan.Kind != hrql.PlanScalar {
		t.Fatalf("expected PlanScalar, got %v", plan.Kind)
	}
	if plan.AggFunc != "min" {
		t.Errorf("expected AggFunc=min, got %q", plan.AggFunc)
	}
	if plan.AggField != "start_date" {
		t.Errorf("expected AggField=start_date, got %q", plan.AggField)
	}

	assertContains(t, result.AggSQL, `min(`)
	assertContains(t, result.AggSQL, `"_e"."start_date"`)
}

func TestMaxOnField(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | .employee_number | max`, "")

	if plan.Kind != hrql.PlanScalar {
		t.Fatalf("expected PlanScalar, got %v", plan.Kind)
	}
	if plan.AggFunc != "max" {
		t.Errorf("expected AggFunc=max, got %q", plan.AggFunc)
	}

	assertContains(t, result.AggSQL, `max(`)
	assertContains(t, result.AggSQL, `"_e"."employee_number"`)
}

func TestLengthAsCount(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | length`, "")

	if plan.Kind != hrql.PlanScalar {
		t.Fatalf("expected PlanScalar, got %v", plan.Kind)
	}
	if plan.AggFunc != "count" {
		t.Errorf("expected AggFunc=count, got %q", plan.AggFunc)
	}

	assertContains(t, result.AggSQL, `count(*)`)
}

// --- Test: org functions ---

func TestChainAll(t *testing.T) {
	plan, result, _, _ := pipeline(t, fmt.Sprintf(`chain("%s")`, targetUUID), "")

	if plan.Kind != hrql.PlanList {
		t.Fatalf("expected PlanList, got %v", plan.Kind)
	}
	if len(result.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result.Conditions))
	}

	sql, args := condToSQL(t, result.Conditions[0])
	// ChainAll: manager_path @> PathSubquery AND id != RefToSQL
	assertContains(t, sql, `"_e"."manager_path" @>`)
	assertContains(t, sql, `"_e"."id" !=`)
	assertContains(t, sql, `SELECT "manager_path"`)
	if len(args) < 2 {
		t.Fatalf("expected at least 2 args, got %d", len(args))
	}
	assertArgEquals(t, args, 0, targetUUID)
	assertArgEquals(t, args, 1, targetUUID)
}

func TestChainWithDepth(t *testing.T) {
	_, result, _, _ := pipeline(t, fmt.Sprintf(`chain("%s", 2)`, targetUUID), "")

	sql, args := condToSQL(t, result.Conditions[0])
	// ChainUp: manager_path = subpath(PathSubquery, 0, GREATEST(nlevel - steps, 0))
	assertContains(t, sql, `"_e"."manager_path" = subpath`)
	assertContains(t, sql, `GREATEST`)
	assertContains(t, sql, `nlevel`)
	assertArgEquals(t, args, len(args)-1, 2)
}

func TestReportsAll(t *testing.T) {
	_, result, _, _ := pipeline(t, fmt.Sprintf(`reports("%s")`, targetUUID), "")

	sql, args := condToSQL(t, result.Conditions[0])
	// Subtree: manager_path <@ PathSubquery AND manager_path != PathSubquery
	assertContains(t, sql, `"_e"."manager_path" <@`)
	assertContains(t, sql, `"_e"."manager_path" !=`)
	assertContains(t, sql, `SELECT "manager_path"`)
	assertArgEquals(t, args, 0, targetUUID)
}

func TestReportsDirectDepth1(t *testing.T) {
	_, result, _, _ := pipeline(t, fmt.Sprintf(`reports("%s", 1)`, targetUUID), "")

	sql, args := condToSQL(t, result.Conditions[0])
	// ChainDown: manager_path <@ PathSubquery AND nlevel = nlevel(PathSubquery) + depth
	assertContains(t, sql, `"_e"."manager_path" <@`)
	assertContains(t, sql, `nlevel`)
	assertArgEquals(t, args, len(args)-1, 1)
}

func TestPeers(t *testing.T) {
	_, result, _, _ := pipeline(t, `peers(self)`, selfUUID)

	sql, args := condToSQL(t, result.Conditions[0])
	// SameField: _e.manager_id = (SELECT manager_id ... WHERE id = ?) AND ... IS NOT NULL AND _e.id != ?
	assertContains(t, sql, `"_e"."manager_id"`)
	assertContains(t, sql, `SELECT "manager_id"`)
	assertContains(t, sql, `IS NOT NULL`)
	assertContains(t, sql, `"_e"."id" !=`)
	assertArgCount(t, args, 3)
	for i := range args {
		assertArgEquals(t, args, i, selfUUID)
	}
}

func TestColleagues(t *testing.T) {
	_, result, _, _ := pipeline(t, `colleagues(self, .department)`, selfUUID)

	sql, args := condToSQL(t, result.Conditions[0])
	// SameField on department_id
	assertContains(t, sql, `"_e"."department_id"`)
	assertContains(t, sql, `SELECT "department_id"`)
	assertContains(t, sql, `IS NOT NULL`)
	assertContains(t, sql, `"_e"."id" !=`)
	assertArgCount(t, args, 3)
	for i := range args {
		assertArgEquals(t, args, i, selfUUID)
	}
}

// --- Test: reports_to (boolean) ---

func TestReportsToBoolean(t *testing.T) {
	plan, _, sql, args := pipeline(t, fmt.Sprintf(`reports_to(self, "%s")`, targetUUID), selfUUID)

	if plan.Kind != hrql.PlanBoolean {
		t.Fatalf("expected PlanBoolean, got %v", plan.Kind)
	}

	// SELECT (empPath <@ tgtPath AND empPath != tgtPath)
	assertContains(t, sql, `SELECT`)
	assertContains(t, sql, `<@`)
	assertContains(t, sql, `SELECT "manager_path"`)
	if len(args) < 2 {
		t.Fatalf("expected at least 2 args, got %d", len(args))
	}
}

func TestReportsToInWhere(t *testing.T) {
	_, result, _, _ := pipeline(t, fmt.Sprintf(`employees | where(reports_to(., "%s"))`, targetUUID), "")

	if len(result.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result.Conditions))
	}

	sql, args := condToSQL(t, result.Conditions[0])
	// ReportsTo in where = Subtree: manager_path <@ PathSubquery AND manager_path != PathSubquery
	assertContains(t, sql, `"_e"."manager_path" <@`)
	assertContains(t, sql, `"_e"."manager_path" !=`)
	assertArgEquals(t, args, 0, targetUUID)
}

// --- Test: self field references ---

func TestWhereFieldEqualsSelfField(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.department == self.department)`, selfUUID)

	sql, args := condToSQL(t, result.Conditions[0])
	// FieldCmpRef: _e.department_id = (SELECT department_id FROM ... WHERE id = ?)
	assertContains(t, sql, `"_e"."department_id"`)
	assertContains(t, sql, `SELECT "department_id"`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, selfUUID)
}

func TestChainWithSelfManager(t *testing.T) {
	_, result, _, _ := pipeline(t, `chain(self.manager)`, selfUUID)

	sql, args := condToSQL(t, result.Conditions[0])
	// ChainAll with ref that has a chain: RefToSQL wraps in subquery
	assertContains(t, sql, `"_e"."manager_path" @>`)
	assertContains(t, sql, `SELECT "manager_id"`)
	assertArgEquals(t, args, 0, selfUUID)
}

// --- Test: lookup chain (cross-object field comparison) ---

func TestWhereLookupChain(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.department.title == "Engineering")`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	// lookupChainToSQL: (SELECT col FROM target WHERE id = fk_ref) = ?
	assertContains(t, sql, `SELECT`)
	assertContains(t, sql, `"_sub"."title"`)
	assertContains(t, sql, `"core"."departments"`)
	assertContains(t, sql, `"_e"."department_id"`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, "Engineering")
}

// --- Test: subquery aggregate in where ---

func TestWhereSubqueryAgg(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(reports(., 1) | count > 0)`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	// SubqueryAgg: (SELECT count(*) FROM ... WHERE manager_path <@ outer AND nlevel = nlevel + 1) > ?
	assertContains(t, sql, `count(*)`)
	assertContains(t, sql, `"_sub_e"."manager_path"`)
	assertContains(t, sql, `nlevel`)
	assertContains(t, sql, `>`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, "0")
}

func TestWhereSubqueryAggAllReports(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(reports(.) | count > 5)`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	// depth=0 → subtree: manager_path <@ outer AND manager_path != outer
	assertContains(t, sql, `count(*)`)
	assertContains(t, sql, `"_sub_e"."manager_path" <@`)
	assertContains(t, sql, `"_sub_e"."manager_path" !=`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, "5")
}

// --- Test: combined pipeline (where + sort + pick + aggregate) ---

func TestFilterSortFirst(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | where(.employment_type == "full_time") | sort_by(.start_date, asc) | first`, "")

	if plan.Kind != hrql.PlanList {
		t.Fatalf("expected PlanList, got %v", plan.Kind)
	}
	if plan.Limit != 1 {
		t.Errorf("expected Limit=1, got %d", plan.Limit)
	}
	if result.PickOp != "first" {
		t.Errorf("expected PickOp=first, got %q", result.PickOp)
	}
	if result.OrderBy == nil || result.OrderBy.Desc {
		t.Error("expected ascending order")
	}
	if result.OrderBy.FieldAPIName != "start_date" {
		t.Errorf("expected order by start_date, got %q", result.OrderBy.FieldAPIName)
	}

	if len(result.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result.Conditions))
	}
	sql, _ := condToSQL(t, result.Conditions[0])
	assertContains(t, sql, `"_e"."employment_type"`)
}

func TestFilterThenCount(t *testing.T) {
	plan, result, _, _ := pipeline(t, `reports(self) | where(.employment_type == "full_time") | count`, selfUUID)

	if plan.Kind != hrql.PlanScalar {
		t.Fatalf("expected PlanScalar, got %v", plan.Kind)
	}

	// AggSQL should include both the org condition and the where filter
	assertContains(t, result.AggSQL, `count(*)`)
	assertContains(t, result.AggSQL, `"_e"."manager_path"`)
	assertContains(t, result.AggSQL, `"_e"."employment_type"`)
}

// --- Test: multiple where clauses ---

func TestMultipleWheres(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where(.employment_type == "full_time") | where(.start_date > "2024-01-01")`, "")

	if len(result.Conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(result.Conditions))
	}

	sql0, _ := condToSQL(t, result.Conditions[0])
	assertContains(t, sql0, `"_e"."employment_type"`)

	sql1, _ := condToSQL(t, result.Conditions[1])
	assertContains(t, sql1, `"_e"."start_date"`)
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
		{"unknown identifier", `departments`, "", "departments"},
		{"sort unknown field", `employees | sort_by(.nonexistent, asc)`, "", "nonexistent"},
		{"field access no source", `.employment_type`, "", ""},
		{"contains outside where", `employees | contains("test")`, "", "where"},
		{"peers without self", `peers(self)`, "", "self_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pipelineErr(tt.input, tt.selfID)
			if err == nil {
				t.Fatal("expected error")
			}
			if tt.wantSubstr != "" {
				assertContains(t, err.Error(), tt.wantSubstr)
			}
		})
	}
}

// --- Test: passthrough pipe functions ---

func TestUniquePassthrough(t *testing.T) {
	plan, result, _, _ := pipeline(t, `employees | unique`, "")

	if plan.Kind != hrql.PlanList {
		t.Fatalf("expected PlanList, got %v", plan.Kind)
	}
	if len(result.Conditions) != 0 {
		t.Fatalf("expected 0 conditions, got %d", len(result.Conditions))
	}
}

// --- Test: literal reversed comparison ---

func TestReversedComparison(t *testing.T) {
	_, result, _, _ := pipeline(t, `employees | where("2024-01-01" < .start_date)`, "")

	sql, args := condToSQL(t, result.Conditions[0])
	// Reversed: "literal < .field" becomes ".field > literal"
	assertContains(t, sql, `"_e"."start_date"`)
	assertContains(t, sql, `>`)
	assertArgCount(t, args, 1)
	assertArgEquals(t, args, 0, "2024-01-01")
}
