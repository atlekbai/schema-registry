package hrql

import "strings"

// isDescendant checks if empPath is a strict descendant of tgtPath using ltree semantics.
// empPath <@ tgtPath AND empPath != tgtPath
func isDescendant(empPath, tgtPath string) bool {
	if empPath == tgtPath {
		return false
	}
	return strings.HasPrefix(empPath, tgtPath+".")
}

// LtreeLabelToUUID converts a 32-char hex ltree label back to UUID format (8-4-4-4-12).
func LtreeLabelToUUID(label string) string {
	if len(label) != 32 {
		return label
	}
	return label[0:8] + "-" + label[8:12] + "-" + label[12:16] + "-" + label[16:20] + "-" + label[20:32]
}
