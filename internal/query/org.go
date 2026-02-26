package query

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
)

// ChainUp returns a condition matching the ancestor at exactly `steps` levels above target.
// e.g. steps=1 → direct manager, steps=2 → skip-level manager.
func ChainUp(targetPath string, steps int) sq.Sqlizer {
	col := fmt.Sprintf(`%s."manager_path"`, qi(qAlias))
	return sq.Expr(
		fmt.Sprintf(`%s = subpath(?::ltree, 0, nlevel(?::ltree) - ?)`, col),
		targetPath, targetPath, steps,
	)
}

// ChainDown returns a condition matching descendants at exactly `depth` levels below target.
// e.g. depth=1 → direct reports, depth=2 → reports of reports.
func ChainDown(targetPath string, depth int) sq.Sqlizer {
	col := fmt.Sprintf(`%s."manager_path"`, qi(qAlias))
	return sq.Expr(
		fmt.Sprintf(`%s <@ ?::ltree AND nlevel(%s) = nlevel(?::ltree) + ?`, col, col),
		targetPath, targetPath, depth,
	)
}

// Subtree returns a condition matching all descendants (any depth), excluding the target itself.
func Subtree(targetPath string) sq.Sqlizer {
	col := fmt.Sprintf(`%s."manager_path"`, qi(qAlias))
	return sq.Expr(
		fmt.Sprintf(`%s <@ ?::ltree AND %s != ?::ltree`, col, col),
		targetPath, targetPath,
	)
}

// ExcludeSelf returns id != selfID.
func ExcludeSelf(selfID string) sq.Sqlizer {
	return sq.NotEq{fmt.Sprintf(`%s."id"`, qi(qAlias)): selfID}
}

// SameField returns: column = value AND id != selfID.
// Used by PEERS to find employees sharing a dimension value.
func SameField(column, value, selfID string) sq.Sqlizer {
	return sq.And{
		sq.Eq{fmt.Sprintf(`%s.%s`, qi(qAlias), qi(column)): value},
		ExcludeSelf(selfID),
	}
}
