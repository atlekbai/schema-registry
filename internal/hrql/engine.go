package hrql

import (
	"context"
	"errors"

	"github.com/atlekbai/schema_registry/internal/hrql/parser"
	"github.com/atlekbai/schema_registry/internal/schema"
)

// InputError wraps parse/compile errors to distinguish from execution errors.
type InputError struct{ Err error }

func (e *InputError) Error() string { return e.Err.Error() }
func (e *InputError) Unwrap() error { return e.Err }

// IsInputError reports whether err is a user input error (parse/compile).
func IsInputError(err error) bool {
	var ie *InputError
	return errors.As(err, &ie)
}

// Queryable creates a Querier for executing HRQL plans.
type Queryable interface {
	Querier(ctx context.Context) (Querier, error)
}

// Querier executes compiled HRQL plans. Single Execute method, returns typed Value.
type Querier interface {
	Execute(ctx context.Context, plan *Plan, opts QueryOpts) (Value, error)
	Close() error
}

// QueryOpts carries pagination/selection options from the request (used for PlanList).
type QueryOpts struct {
	Select  string
	Expand  string
	Order   string
	Limit   int32
	Cursor  string
	Filters map[string]string // REST-style filters (field -> "op.value")
}

// QueryRequest bundles all input needed for an HRQL query.
type QueryRequest struct {
	Query      string
	SelfID     string
	SelfObject string
	Opts       QueryOpts
}

// Engine parses and compiles HRQL expressions, then executes them via a Queryable.
type Engine struct {
	cache *schema.Cache
}

// NewEngine creates a new HRQL engine.
func NewEngine(cache *schema.Cache) *Engine {
	return &Engine{cache: cache}
}

// Query parses, compiles, and executes an HRQL expression.
func (e *Engine) Query(ctx context.Context, q Queryable, req QueryRequest) (Value, error) {
	ast, err := parser.Parse(req.Query)
	if err != nil {
		return nil, &InputError{Err: err}
	}

	compiler := NewCompiler(e.cache, req.SelfID, req.SelfObject)
	plan, err := compiler.Compile(ast)
	if err != nil {
		return nil, &InputError{Err: err}
	}

	return e.execute(ctx, q, plan, req.Opts)
}

// List executes a list query without an HRQL expression (REST-style).
func (e *Engine) List(ctx context.Context, q Queryable, objectName string, opts QueryOpts) (Value, error) {
	plan := &Plan{Kind: PlanList, ObjectAPIName: objectName}
	val, err := e.execute(ctx, q, plan, opts)
	if err != nil {
		return nil, &InputError{Err: err}
	}
	return val, nil
}

func (e *Engine) execute(ctx context.Context, q Queryable, plan *Plan, opts QueryOpts) (Value, error) {
	querier, err := q.Querier(ctx)
	if err != nil {
		return nil, err
	}
	defer querier.Close()
	return querier.Execute(ctx, plan, opts)
}
