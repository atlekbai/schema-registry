package query

import (
	"fmt"
	"strings"

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

// ChainAll returns a condition matching ALL ancestors of the target (full chain to root).
// It extracts UUID labels from the ltree path, excluding the target itself (last label).
func ChainAll(path string) sq.Sqlizer {
	labels := strings.Split(path, ".")
	if len(labels) <= 1 {
		// Root node or single label — no ancestors.
		return sq.Eq{fmt.Sprintf(`%s."id"`, qi(qAlias)): nil}
	}
	// Exclude self (last label), convert ltree labels back to UUIDs.
	ancestors := labels[:len(labels)-1]
	uuids := make([]string, len(ancestors))
	for i, label := range ancestors {
		uuids[i] = LtreeLabelToUUID(label)
	}
	col := fmt.Sprintf(`%s."id"`, qi(qAlias))
	return sq.Eq{col: uuids}
}

// LtreeLabelToUUID converts a 32-char hex ltree label back to UUID format (8-4-4-4-12).
func LtreeLabelToUUID(label string) string {
	if len(label) != 32 {
		return label
	}
	return label[0:8] + "-" + label[8:12] + "-" + label[12:16] + "-" + label[16:20] + "-" + label[20:32]
}
