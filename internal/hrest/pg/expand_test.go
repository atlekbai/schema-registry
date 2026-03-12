package pg

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/atlekbai/schema_registry/internal/schema"
)

// expandNames returns a flat list of field names from expand plans, e.g. ["manager", "department"].
func expandNames(plans []ExpandPlan) []string {
	var names []string
	for _, p := range plans {
		names = append(names, p.FieldName)
	}
	return names
}

// expandTree returns a nested slice representing the expand tree structure.
// Each level is a []string of field names. e.g. [["manager"], ["department"]]
func expandTree(plans []ExpandPlan) [][]string {
	var levels [][]string
	for cur := plans; len(cur) > 0; {
		names := expandNames(cur)
		levels = append(levels, names)
		var next []ExpandPlan
		for _, p := range cur {
			next = append(next, p.Children...)
		}
		cur = next
	}
	return levels
}

func TestResolveExpands_Empty(t *testing.T) {
	dept := testDeptObj()
	cache := schema.NewCacheFromObjects(dept)

	assert.Nil(t, ResolveExpands(nil, dept, cache, 2))
	assert.Nil(t, ResolveExpands([]string{}, dept, cache, 2))
	assert.Nil(t, ResolveExpands([]string{"title"}, dept, cache, 0))
}

func TestResolveExpands_SingleLevel(t *testing.T) {
	dept := testDeptObj()
	emp := testEmpObj()
	cache := schema.NewCacheFromObjects(dept, emp)

	plans := ResolveExpands([]string{"department"}, emp, cache, 2)
	assert.Equal(t, [][]string{{"department"}}, expandTree(plans))
	assert.Equal(t, dept.ID, plans[0].Target.ID)
}

func TestResolveExpands_SkipsNonLookup(t *testing.T) {
	dept := testDeptObj()
	emp := testEmpObj()
	cache := schema.NewCacheFromObjects(dept, emp)

	plans := ResolveExpands([]string{"employee_number"}, emp, cache, 2)
	assert.Empty(t, plans)
}

func TestResolveExpands_TwoLevels(t *testing.T) {
	dept := testDeptObj()
	emp := testEmpObj()
	cache := schema.NewCacheFromObjects(dept, emp)

	plans := ResolveExpands([]string{"manager.department"}, emp, cache, 2)
	assert.Equal(t, [][]string{{"manager"}, {"department"}}, expandTree(plans))
	assert.Equal(t, dept.ID, plans[0].Children[0].Target.ID)
}

func TestResolveExpands_DepthLimit(t *testing.T) {
	dept := testDeptObj()
	emp := testEmpObj()
	cache := schema.NewCacheFromObjects(dept, emp)

	// maxDepth=1: only top-level, children are pruned
	plans := ResolveExpands([]string{"manager.department"}, emp, cache, 1)
	assert.Equal(t, [][]string{{"manager"}}, expandTree(plans))
}

func TestResolveExpands_DeduplicatesParent(t *testing.T) {
	dept := testDeptObj()
	emp := testEmpObj()
	cache := schema.NewCacheFromObjects(dept, emp)

	// "manager" appears both standalone and as prefix — single plan with children
	plans := ResolveExpands([]string{"manager", "manager.department"}, emp, cache, 2)
	assert.Equal(t, [][]string{{"manager"}, {"department"}}, expandTree(plans))
}

func TestResolveExpands_MultipleTopLevel(t *testing.T) {
	dept := testDeptObj()
	emp := testEmpObj()
	cache := schema.NewCacheFromObjects(dept, emp)

	plans := ResolveExpands([]string{"department", "manager"}, emp, cache, 2)
	assert.Equal(t, []string{"department", "manager"}, expandNames(plans))
}

func TestResolveExpands_ThreeLevels(t *testing.T) {
	dept := testDeptObj()
	emp := testEmpObj()
	cache := schema.NewCacheFromObjects(dept, emp)

	plans := ResolveExpands([]string{"manager.manager.department"}, emp, cache, 3)
	assert.Equal(t, [][]string{{"manager"}, {"manager"}, {"department"}}, expandTree(plans))
}

func TestResolveExpands_ThreeLevelsTruncatedByDepth(t *testing.T) {
	dept := testDeptObj()
	emp := testEmpObj()
	cache := schema.NewCacheFromObjects(dept, emp)

	// maxDepth=2: third level is pruned
	plans := ResolveExpands([]string{"manager.manager.department"}, emp, cache, 2)
	assert.Equal(t, [][]string{{"manager"}, {"manager"}}, expandTree(plans))
}
