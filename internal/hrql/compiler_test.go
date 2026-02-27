package hrql

import (
	"strings"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/atlekbai/schema_registry/internal/query"
	"github.com/atlekbai/schema_registry/internal/schema"
	"github.com/google/uuid"
)

// --- Helper constructors ---

func testEmployeesObj() *schema.ObjectDef {
	storageSchema := "core"
	storageTable := "employees"

	mkField := func(apiName string, typ schema.FieldType, col string) schema.FieldDef {
		c := col
		return schema.FieldDef{
			ID:            uuid.New(),
			APIName:       apiName,
			Title:         apiName,
			Type:          typ,
			IsStandard:    true,
			StorageColumn: &c,
		}
	}

	mkLookupField := func(apiName string, col string, targetID uuid.UUID) schema.FieldDef {
		c := col
		return schema.FieldDef{
			ID:             uuid.New(),
			APIName:        apiName,
			Title:          apiName,
			Type:           schema.FieldLookup,
			IsStandard:     true,
			StorageColumn:  &c,
			LookupObjectID: &targetID,
		}
	}

	deptID := uuid.New()

	fields := []schema.FieldDef{
		mkField("employee_number", schema.FieldText, "employee_number"),
		mkField("employment_type", schema.FieldChoice, "employment_type"),
		mkField("start_date", schema.FieldDate, "start_date"),
		mkField("end_date", schema.FieldDate, "end_date"),
		mkLookupField("manager", "manager_id", uuid.New()),
		mkLookupField("department", "department_id", deptID),
		mkLookupField("organization", "organization_id", uuid.New()),
		mkLookupField("individual", "individual_id", uuid.New()),
		mkLookupField("user", "user_id", uuid.New()),
	}

	obj := &schema.ObjectDef{
		ID:              uuid.New(),
		APIName:         "employees",
		Title:           "Employee",
		PluralTitle:     "Employees",
		IsStandard:      true,
		StorageSchema:   &storageSchema,
		StorageTable:    &storageTable,
		FieldsByAPIName: make(map[string]*schema.FieldDef),
		Fields:          fields,
	}

	for i := range obj.Fields {
		obj.FieldsByAPIName[obj.Fields[i].APIName] = &obj.Fields[i]
	}

	return obj
}

// --- ChainAll tests ---

func TestChainAllRootNode(t *testing.T) {
	// Single label = root, no ancestors.
	cond := query.ChainAll("abc123")
	sql, _, err := condToSQL(cond)
	if err != nil {
		t.Fatal(err)
	}
	// Should produce id IS NULL (no results).
	if !strings.Contains(sql, "IS NULL") {
		t.Fatalf("expected IS NULL for root node, got %q", sql)
	}
}

func TestChainAllMultipleAncestors(t *testing.T) {
	// 3 labels: grandparent.parent.self → should return [grandparent, parent] UUIDs
	path := "aabbccdd11223344556677889900aabb.11223344556677889900aabbccddeeff.deadbeef12345678abcdef0123456789"
	cond := query.ChainAll(path)
	sql, args, err := condToSQL(cond)
	if err != nil {
		t.Fatal(err)
	}

	// Should contain 2 UUIDs (ancestors excluding self).
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d: %v", len(args), args)
	}
	// Check UUID format restoration.
	uuid1, ok := args[0].(string)
	if !ok {
		t.Fatalf("arg 0: expected string, got %T", args[0])
	}
	if !strings.Contains(uuid1, "-") {
		t.Fatalf("expected UUID with hyphens, got %q", uuid1)
	}

	// SQL should reference _e.id.
	if !strings.Contains(sql, `"id"`) {
		t.Fatalf("expected id reference, got %q", sql)
	}
}

// --- ltreeLabelToUUID tests ---

func TestLtreeLabelToUUID(t *testing.T) {
	// 32-char hex → UUID format
	label := "550e8400e29b41d4a716446655440000"
	got := query.LtreeLabelToUUID(label)
	want := "550e8400-e29b-41d4-a716-446655440000"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestLtreeLabelToUUIDShort(t *testing.T) {
	// Non-32 char → returned as-is.
	label := "short"
	got := query.LtreeLabelToUUID(label)
	if got != label {
		t.Fatalf("expected %q, got %q", label, got)
	}
}

// --- nlevelFromPath tests ---

func TestNlevelFromPath(t *testing.T) {
	tests := []struct {
		path string
		want int
	}{
		{"", 0},
		{"abc", 1},
		{"a.b", 2},
		{"a.b.c", 3},
		{"a.b.c.d.e", 5},
	}
	for _, tt := range tests {
		got := nlevelFromPath(tt.path)
		if got != tt.want {
			t.Errorf("nlevelFromPath(%q): expected %d, got %d", tt.path, tt.want, got)
		}
	}
}

// --- Where condition compilation helpers ---

func TestComparisonExpr(t *testing.T) {
	tests := []struct {
		op      string
		wantSQL string
	}{
		{"==", `= $1`},
		{"!=", `<> $1`},
		{">", `> $1`},
		{">=", `>= $1`},
		{"<", `< $1`},
		{"<=", `<= $1`},
	}
	for _, tt := range tests {
		cond := comparisonExpr(`"_e"."employment_type"`, tt.op, "FULL_TIME")
		sql, args, err := condToSQL(cond)
		if err != nil {
			t.Errorf("op %q: %v", tt.op, err)
			continue
		}
		if !strings.Contains(sql, `"_e"."employment_type"`) {
			t.Errorf("op %q: expected column ref, got %q", tt.op, sql)
		}
		if len(args) == 0 {
			t.Errorf("op %q: expected args, got none", tt.op)
		}
		_ = args
	}
}

func TestSqlOp(t *testing.T) {
	tests := map[string]string{
		"==": "=",
		"!=": "!=",
		">":  ">",
		">=": ">=",
		"<":  "<",
		"<=": "<=",
	}
	for input, want := range tests {
		got := sqlOp(input)
		if got != want {
			t.Errorf("sqlOp(%q): expected %q, got %q", input, want, got)
		}
	}
}

func TestReverseOp(t *testing.T) {
	tests := map[string]string{
		">":  "<",
		">=": "<=",
		"<":  ">",
		"<=": ">=",
		"==": "==",
	}
	for input, want := range tests {
		got := reverseOp(input)
		if got != want {
			t.Errorf("reverseOp(%q): expected %q, got %q", input, want, got)
		}
	}
}

// --- BuildAggregate tests ---

func TestBuildAggregateCount(t *testing.T) {
	obj := testEmployeesObj()
	sql, args, err := BuildAggregate(obj, "count", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "count(*)") {
		t.Fatalf("expected count(*), got %q", sql)
	}
	if !strings.Contains(sql, `"core"."employees"`) {
		t.Fatalf("expected table ref, got %q", sql)
	}
	if len(args) != 0 {
		t.Fatalf("expected 0 args, got %d", len(args))
	}
}

func TestBuildAggregateWithConditions(t *testing.T) {
	obj := testEmployeesObj()
	cond := sq.Eq{`"_e"."employment_type"`: "CONTRACTOR"}
	sql, args, err := BuildAggregate(obj, "count", nil, []sq.Sqlizer{cond})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "count(*)") {
		t.Fatalf("expected count(*), got %q", sql)
	}
	if !strings.Contains(sql, "employment_type") {
		t.Fatalf("expected employment_type in WHERE, got %q", sql)
	}
	if len(args) != 1 || args[0] != "CONTRACTOR" {
		t.Fatalf("expected args [CONTRACTOR], got %v", args)
	}
}

func TestBuildAggregateAvgField(t *testing.T) {
	obj := testEmployeesObj()
	// Use start_date as a stand-in (no numeric fields in our test fixture).
	fd := obj.FieldsByAPIName["start_date"]
	sql, _, err := BuildAggregate(obj, "avg", fd, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "avg(") {
		t.Fatalf("expected avg(), got %q", sql)
	}
}

// --- Result kind tests ---

func TestResultKindDefaults(t *testing.T) {
	r := &Result{}
	if r.Kind != KindList {
		t.Fatalf("expected default KindList, got %v", r.Kind)
	}
}

// --- joinChain tests ---

func TestJoinChain(t *testing.T) {
	tests := []struct {
		chain []string
		want  string
	}{
		{nil, ""},
		{[]string{"a"}, "a"},
		{[]string{"a", "b", "c"}, "a.b.c"},
	}
	for _, tt := range tests {
		got := joinChain(tt.chain)
		if got != tt.want {
			t.Errorf("joinChain(%v): expected %q, got %q", tt.chain, tt.want, got)
		}
	}
}

// --- tryCompileStringOp tests ---

func TestTryCompileStringOp(t *testing.T) {
	obj := testEmployeesObj()
	cache := &schema.Cache{}
	c := &Compiler{cache: cache, empObj: obj}

	tests := []struct {
		name    string
		fnName  string
		arg     string
		wantSQL string
	}{
		{"contains", "contains", "test", "ILIKE"},
		{"starts_with", "starts_with", "test", "ILIKE"},
		{"ends_with", "ends_with", "test", "ILIKE"},
	}
	for _, tt := range tests {
		pipe := &PipeExpr{Steps: []Node{
			&FieldAccess{Chain: []string{"employment_type"}},
			&FuncCall{Name: tt.fnName, Args: []Node{&Literal{Kind: TokString, Value: tt.arg}}},
		}}
		cond, ok := c.tryCompileStringOp(pipe)
		if !ok {
			t.Errorf("%s: expected match, got false", tt.name)
			continue
		}
		sql, _, err := condToSQL(cond)
		if err != nil {
			t.Errorf("%s: %v", tt.name, err)
			continue
		}
		if !strings.Contains(sql, tt.wantSQL) {
			t.Errorf("%s: expected %q in SQL, got %q", tt.name, tt.wantSQL, sql)
		}
	}
}

func TestTryCompileStringOpNoMatch(t *testing.T) {
	obj := testEmployeesObj()
	c := &Compiler{empObj: obj}

	// Not a string op — should return false.
	pipe := &PipeExpr{Steps: []Node{
		&FieldAccess{Chain: []string{"employment_type"}},
		&AggExpr{Op: "count"},
	}}
	_, ok := c.tryCompileStringOp(pipe)
	if ok {
		t.Fatal("expected no match for non-string-op pipe")
	}
}

// --- Helpers ---

func condToSQL(cond sq.Sqlizer) (string, []any, error) {
	// Wrap in a SELECT to get valid SQL.
	qb := sq.Select("1").Where(cond).PlaceholderFormat(sq.Dollar)
	sql, args, err := qb.ToSql()
	return sql, args, err
}
