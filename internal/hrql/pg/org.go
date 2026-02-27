package pg

import (
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"

	"github.com/atlekbai/schema_registry/internal/hrql"
	"github.com/atlekbai/schema_registry/internal/query"
)

// ChainUp returns a condition matching the ancestor at exactly `steps` levels above target.
func ChainUp(targetPath string, steps int) sq.Sqlizer {
	col := fmt.Sprintf(`%s."manager_path"`, query.QI(query.Alias()))
	return sq.Expr(
		fmt.Sprintf(`%s = subpath(?::ltree, 0, nlevel(?::ltree) - ?)`, col),
		targetPath, targetPath, steps,
	)
}

// ChainDown returns a condition matching descendants at exactly `depth` levels below target.
func ChainDown(targetPath string, depth int) sq.Sqlizer {
	col := fmt.Sprintf(`%s."manager_path"`, query.QI(query.Alias()))
	return sq.Expr(
		fmt.Sprintf(`%s <@ ?::ltree AND nlevel(%s) = nlevel(?::ltree) + ?`, col, col),
		targetPath, targetPath, depth,
	)
}

// Subtree returns a condition matching all descendants (any depth), excluding the target itself.
func Subtree(targetPath string) sq.Sqlizer {
	col := fmt.Sprintf(`%s."manager_path"`, query.QI(query.Alias()))
	return sq.Expr(
		fmt.Sprintf(`%s <@ ?::ltree AND %s != ?::ltree`, col, col),
		targetPath, targetPath,
	)
}

// ExcludeSelf returns id != selfID.
func ExcludeSelf(selfID string) sq.Sqlizer {
	return sq.NotEq{fmt.Sprintf(`%s."id"`, query.QI(query.Alias())): selfID}
}

// SameField returns: column = value AND id != selfID.
func SameField(column, value, selfID string) sq.Sqlizer {
	return sq.And{
		sq.Eq{fmt.Sprintf(`%s.%s`, query.QI(query.Alias()), query.QI(column)): value},
		ExcludeSelf(selfID),
	}
}

// ChainAll returns a condition matching ALL ancestors of the target (full chain to root).
func ChainAll(path string) sq.Sqlizer {
	labels := strings.Split(path, ".")
	if len(labels) <= 1 {
		return sq.Eq{fmt.Sprintf(`%s."id"`, query.QI(query.Alias())): nil}
	}
	ancestors := labels[:len(labels)-1]
	uuids := make([]string, len(ancestors))
	for i, label := range ancestors {
		uuids[i] = hrql.LtreeLabelToUUID(label)
	}
	col := fmt.Sprintf(`%s."id"`, query.QI(query.Alias()))
	return sq.Eq{col: uuids}
}

// ReportsToWhere generates a WHERE condition for reports_to(., target) inside where.
func ReportsToWhere(targetPath string) sq.Sqlizer {
	col := fmt.Sprintf(`%s."manager_path"`, query.QI(query.Alias()))
	return sq.Expr(
		fmt.Sprintf(`%s <@ ?::ltree AND %s != ?::ltree`, col, col),
		targetPath, targetPath,
	)
}

// NullCondition returns an always-false condition.
func NullCondition() sq.Sqlizer {
	return sq.Eq{fmt.Sprintf(`%s."id"`, query.QI(query.Alias())): nil}
}
