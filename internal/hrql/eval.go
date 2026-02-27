package hrql

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/atlekbai/schema_registry/internal/query"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// BuildAggregate builds a SQL query for a terminal aggregation.
// Returns: SELECT agg(col) FROM table WHERE conditions
func BuildAggregate(
	obj *schema.ObjectDef,
	aggFunc string,
	aggField *schema.FieldDef,
	conditions []sq.Sqlizer,
) (string, []any, error) {
	alias := query.Alias()
	from, baseWhere := query.TableSource(obj, alias)

	var col string
	switch {
	case aggFunc == "count" && aggField == nil:
		col = "*"
	case aggField != nil:
		col = query.FilterExpr(alias, aggField)
	default:
		col = "*"
	}

	selectExpr := fmt.Sprintf(`%s(%s)`, aggFunc, col)
	qb := sq.Select(selectExpr).From(from).PlaceholderFormat(sq.Dollar)

	if baseWhere != nil {
		qb = qb.Where(baseWhere)
	}
	for _, cond := range conditions {
		qb = qb.Where(cond)
	}

	return qb.ToSql()
}
