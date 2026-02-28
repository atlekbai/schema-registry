package hrql

import (
	"testing"

	"github.com/atlekbai/schema_registry/internal/hrql/parser"
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

// --- Plan condition tests ---

func TestOrgChainAllRootNode(t *testing.T) {
	// Single label = root → OrgChainAll with single-label path.
	cond := OrgChainAll{Path: "abc123"}
	_ = cond // Plan condition is a value type — no SQL to check here.
	// The SQL translation is tested in pg/ package.
}

func TestLtreeLabelToUUID(t *testing.T) {
	label := "550e8400e29b41d4a716446655440000"
	got := LtreeLabelToUUID(label)
	want := "550e8400-e29b-41d4-a716-446655440000"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestLtreeLabelToUUIDShort(t *testing.T) {
	label := "short"
	got := LtreeLabelToUUID(label)
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

// --- reverseOp tests ---

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

// --- Plan kind tests ---

func TestPlanKindDefaults(t *testing.T) {
	p := &Plan{}
	if p.Kind != PlanList {
		t.Fatalf("expected default PlanList, got %v", p.Kind)
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

// --- tryCompileStringOp tests (now produces Plan conditions) ---

func TestTryCompileStringOp(t *testing.T) {
	obj := testEmployeesObj()
	cache := &schema.Cache{}
	c := &Compiler{cache: cache, empObj: obj}

	tests := []struct {
		name   string
		fnName string
		arg    string
		wantOp string
	}{
		{"contains", "contains", "test", "contains"},
		{"starts_with", "starts_with", "test", "starts_with"},
		{"ends_with", "ends_with", "test", "ends_with"},
	}
	for _, tt := range tests {
		pipe := &parser.PipeExpr{Steps: []parser.Node{
			&parser.FieldAccess{Chain: []string{"employment_type"}},
			&parser.FuncCall{Name: tt.fnName, Args: []parser.Node{&parser.Literal{Kind: parser.TokString, Value: tt.arg}}},
		}}
		cond, ok := c.tryCompileStringOp(pipe)
		if !ok {
			t.Errorf("%s: expected match, got false", tt.name)
			continue
		}
		sm, ok := cond.(StringMatch)
		if !ok {
			t.Errorf("%s: expected StringMatch, got %T", tt.name, cond)
			continue
		}
		if sm.Op != tt.wantOp {
			t.Errorf("%s: expected op %q, got %q", tt.name, tt.wantOp, sm.Op)
		}
		if sm.Pattern != tt.arg {
			t.Errorf("%s: expected pattern %q, got %q", tt.name, tt.arg, sm.Pattern)
		}
		if len(sm.Field) != 1 || sm.Field[0] != "employment_type" {
			t.Errorf("%s: expected field [employment_type], got %v", tt.name, sm.Field)
		}
	}
}

func TestTryCompileStringOpNoMatch(t *testing.T) {
	obj := testEmployeesObj()
	c := &Compiler{empObj: obj}

	pipe := &parser.PipeExpr{Steps: []parser.Node{
		&parser.FieldAccess{Chain: []string{"employment_type"}},
		&parser.AggExpr{Op: "count"},
	}}
	_, ok := c.tryCompileStringOp(pipe)
	if ok {
		t.Fatal("expected no match for non-string-op pipe")
	}
}

// --- isDescendant tests ---

func TestIsDescendant(t *testing.T) {
	tests := []struct {
		emp, tgt string
		want     bool
	}{
		{"a.b.c", "a.b", true},
		{"a.b", "a.b", false},
		{"a.b", "a.b.c", false},
		{"a.b.c", "x.y", false},
	}
	for _, tt := range tests {
		got := isDescendant(tt.emp, tt.tgt)
		if got != tt.want {
			t.Errorf("isDescendant(%q, %q): expected %v, got %v", tt.emp, tt.tgt, tt.want, got)
		}
	}
}

// --- Condition type assertions ---

func TestConditionTypes(t *testing.T) {
	// Verify all condition types implement the Condition interface.
	var _ Condition = IdentityFilter{}
	var _ Condition = NullFilter{}
	var _ Condition = FieldCmp{}
	var _ Condition = StringMatch{}
	var _ Condition = AndCond{}
	var _ Condition = OrCond{}
	var _ Condition = OrgChainUp{}
	var _ Condition = OrgChainDown{}
	var _ Condition = OrgChainAll{}
	var _ Condition = OrgSubtree{}
	var _ Condition = SameFieldCond{}
	var _ Condition = ReportsTo{}
	var _ Condition = SubqueryAgg{}
}
