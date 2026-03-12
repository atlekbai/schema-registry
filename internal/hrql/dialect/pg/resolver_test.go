package pg

import (
	"testing"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/stretchr/testify/require"
)

func TestRefToSQL_NoChain(t *testing.T) {
	ref := hrql.EmployeeRef{ID: "abc-123"}
	sql, args, err := refToSQL(ref, testObj, nil).ToSql()
	require.NoError(t, err)
	require.Equal(t, "?", sql)
	require.Equal(t, []any{"abc-123"}, args)
}

func TestRefToSQL_SingleChain(t *testing.T) {
	ref := hrql.EmployeeRef{ID: "abc-123", Chain: []string{"manager"}}
	sql, args, err := refToSQL(ref, testObj, nil).ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT "manager_id" FROM "core"."employees" WHERE "id" = (?)`, sql)
	require.Equal(t, []any{"abc-123"}, args)
}

func TestRefToSQL_DoubleChain(t *testing.T) {
	ref := hrql.EmployeeRef{ID: "abc-123", Chain: []string{"manager", "manager"}}
	sql, args, err := refToSQL(ref, testObj, nil).ToSql()
	require.NoError(t, err)
	require.Equal(t,
		`SELECT "manager_id" FROM "core"."employees" WHERE "id" = (SELECT "manager_id" FROM "core"."employees" WHERE "id" = (?))`,
		sql)
	require.Equal(t, []any{"abc-123"}, args)
}

func TestPathSubquery(t *testing.T) {
	ref := hrql.EmployeeRef{ID: "abc-123"}
	sql, args, err := pathSubquery(ref, testObj, nil).ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT "manager_path" FROM "core"."employees" WHERE "id" = (?)`, sql)
	require.Equal(t, []any{"abc-123"}, args)
}

func TestPathSubquery_WithChain(t *testing.T) {
	ref := hrql.EmployeeRef{ID: "abc-123", Chain: []string{"manager"}}
	sql, args, err := pathSubquery(ref, testObj, nil).ToSql()
	require.NoError(t, err)
	require.Equal(t,
		`SELECT "manager_path" FROM "core"."employees" WHERE "id" = (SELECT "manager_id" FROM "core"."employees" WHERE "id" = (?))`,
		sql)
	require.Equal(t, []any{"abc-123"}, args)
}

func TestFieldSubquery(t *testing.T) {
	ref := hrql.EmployeeRef{ID: "abc-123"}
	sql, args, err := fieldSubquery(ref, "manager", testObj, nil).ToSql()
	require.NoError(t, err)
	require.Equal(t, `SELECT "manager_id" FROM "core"."employees" WHERE "id" = (?)`, sql)
	require.Equal(t, []any{"abc-123"}, args)
}
