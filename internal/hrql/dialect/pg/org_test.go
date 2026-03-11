package pg

import (
	"testing"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/stretchr/testify/require"
)

var ref = hrql.EmployeeRef{ID: "abc-123"}

const pathSub = `SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?)`

func TestChainUp(t *testing.T) {
	sql, args, err := chainUp(ref, 2, testObj).ToSql()
	require.NoError(t, err)
	require.Equal(t,
		`("_e"."manager_path") = subpath(`+pathSub+`, 0, GREATEST(nlevel(`+pathSub+`) - ?, 0))`,
		sql)
	require.Equal(t, []any{"abc-123", "abc-123", 2}, args)
}

func TestChainDown(t *testing.T) {
	sql, args, err := chainDown(ref, 1, testObj).ToSql()
	require.NoError(t, err)
	require.Equal(t,
		`(("_e"."manager_path") <@ (`+pathSub+`) AND nlevel("_e"."manager_path") = nlevel(`+pathSub+`) + ?)`,
		sql)
	require.Equal(t, []any{"abc-123", "abc-123", 1}, args)
}

func TestSubtree(t *testing.T) {
	sql, args, err := subtree(ref, testObj).ToSql()
	require.NoError(t, err)
	require.Equal(t,
		`(("_e"."manager_path") <@ (`+pathSub+`) AND ("_e"."manager_path") != (`+pathSub+`))`,
		sql)
	require.Equal(t, []any{"abc-123", "abc-123"}, args)
}

func TestChainAll(t *testing.T) {
	sql, args, err := chainAll(ref, testObj).ToSql()
	require.NoError(t, err)
	require.Equal(t,
		`(("_e"."manager_path") @> (`+pathSub+`) AND ("_e"."id") != (?))`,
		sql)
	require.Equal(t, []any{"abc-123", "abc-123"}, args)
}

func TestSameField(t *testing.T) {
	fieldSub := `SELECT "department_id" FROM "core"."employees" WHERE "id" = (?)`
	sql, args, err := sameField("department", ref, testObj).ToSql()
	require.NoError(t, err)
	require.Equal(t,
		`("_e"."department_id" = (`+fieldSub+`) AND (`+fieldSub+`) IS NOT NULL AND ("_e"."id") != (?))`,
		sql)
	require.Equal(t, []any{"abc-123", "abc-123", "abc-123"}, args)
}

func TestReportsToCheck(t *testing.T) {
	emp := hrql.EmployeeRef{ID: "emp-1"}
	target := hrql.EmployeeRef{ID: "tgt-1"}
	sql, args, err := reportsToCheck(emp, target, testObj).ToSql()
	require.NoError(t, err)
	require.Equal(t,
		`((`+pathSub+`) <@ (`+pathSub+`) AND (`+pathSub+`) != (`+pathSub+`))`,
		sql)
	require.Equal(t, []any{"emp-1", "tgt-1", "emp-1", "tgt-1"}, args)
}

func TestNullCondition(t *testing.T) {
	sql, args, err := nullCondition().ToSql()
	require.NoError(t, err)
	require.Equal(t, `"_e"."id" IS NULL`, sql)
	require.Empty(t, args)
}
