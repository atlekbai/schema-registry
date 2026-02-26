package service

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

type dslCommand struct {
	Op         string // "chain", "peers", "reports", "reportsto"
	EmployeeID string
	Steps      int    // chain only
	Dimension  string // peers only
	Direct     bool   // reports only
	TargetID   string // reportsto only
}

var dslRe = regexp.MustCompile(`(?i)^\s*(\w+)\s*\(\s*(.+)\s*\)\s*$`)

func parseDSL(input string) (*dslCommand, error) {
	m := dslRe.FindStringSubmatch(input)
	if m == nil {
		return nil, fmt.Errorf("invalid syntax, expected: FUNC(arg1, arg2, ...)")
	}

	op := strings.ToLower(m[1])
	rawArgs := splitArgs(m[2])

	switch op {
	case "chain":
		if len(rawArgs) != 2 {
			return nil, fmt.Errorf("CHAIN requires 2 arguments: CHAIN(employee_id, steps)")
		}
		id, err := parseUUID(rawArgs[0])
		if err != nil {
			return nil, fmt.Errorf("CHAIN arg 1: %w", err)
		}
		steps, err := strconv.Atoi(rawArgs[1])
		if err != nil || steps == 0 {
			return nil, fmt.Errorf("CHAIN arg 2: steps must be a non-zero integer")
		}
		return &dslCommand{Op: "chain", EmployeeID: id, Steps: steps}, nil

	case "peers":
		if len(rawArgs) != 2 {
			return nil, fmt.Errorf("PEERS requires 2 arguments: PEERS(employee_id, dimension)")
		}
		id, err := parseUUID(rawArgs[0])
		if err != nil {
			return nil, fmt.Errorf("PEERS arg 1: %w", err)
		}
		dim := strings.ToLower(rawArgs[1])
		return &dslCommand{Op: "peers", EmployeeID: id, Dimension: dim}, nil

	case "reports":
		if len(rawArgs) < 1 || len(rawArgs) > 2 {
			return nil, fmt.Errorf("REPORTS requires 1-2 arguments: REPORTS(employee_id [, true])")
		}
		id, err := parseUUID(rawArgs[0])
		if err != nil {
			return nil, fmt.Errorf("REPORTS arg 1: %w", err)
		}
		direct := false
		if len(rawArgs) == 2 {
			direct, err = strconv.ParseBool(rawArgs[1])
			if err != nil {
				return nil, fmt.Errorf("REPORTS arg 2: expected true or false")
			}
		}
		return &dslCommand{Op: "reports", EmployeeID: id, Direct: direct}, nil

	case "reportsto":
		if len(rawArgs) != 2 {
			return nil, fmt.Errorf("REPORTSTO requires 2 arguments: REPORTSTO(employee_id, target_id)")
		}
		id, err := parseUUID(rawArgs[0])
		if err != nil {
			return nil, fmt.Errorf("REPORTSTO arg 1: %w", err)
		}
		tid, err := parseUUID(rawArgs[1])
		if err != nil {
			return nil, fmt.Errorf("REPORTSTO arg 2: %w", err)
		}
		return &dslCommand{Op: "reportsto", EmployeeID: id, TargetID: tid}, nil

	default:
		return nil, fmt.Errorf("unknown function %q, expected: CHAIN, PEERS, REPORTS, REPORTSTO", op)
	}
}

func splitArgs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseUUID(s string) (string, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return "", fmt.Errorf("invalid UUID %q", s)
	}
	return u.String(), nil
}
