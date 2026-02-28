# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Database (requires Docker)
task up              # start postgres
task reset           # wipe volumes + start + run all migrations
task migrate-up      # apply migrations (cached by Taskfile sources: — rename files to force rerun)
task psql            # interactive psql session

# Backend
go run ./cmd/server  # start server (PORT=8080, DATABASE_URL=postgresql://postgres:postgres@localhost:5432/main)

# Proto generation
task proto           # runs buf generate → gen/ (protoc-gen-go, protoc-gen-connect-go, protoc-gen-openapiv2)

# Frontend
cd frontend && bun install && bun run dev   # vite dev server, proxies /api → localhost:8080
cd frontend && bun run build                # tsc + vite build

# Tests
go test ./internal/hrql/...  # lexer, parser, compiler unit tests + e2e (no DB required)
go fix ./...                 # must run after writing new Go code (inlines helpers, modernizes syntax)
```

## Architecture

**Transport**: ConnectRPC + Vanguard REST transcoder. REST routes defined via `google.api.http` annotations in `proto/registry/v1/*_service.proto`. Single Vanguard transcoder handles all routes on `/`. Each service implements `server.ConnectService` interface and is registered in `cmd/server/main.go`.

**Schema Cache** (`internal/schema/cache.go`): In-memory map of `ObjectDef`/`FieldDef` loaded at startup from `metadata.objects` JOIN `metadata.fields`. Used by query layer to validate params and build SQL. Reloaded (best-effort) after metadata mutations. `NewCacheFromObjects(objs...)` builds a pre-loaded cache for tests.

**Query Builder** (`internal/query/`): `NewBuilder(obj)` returns a `QueryBuilder` for both standard (real `core.*` tables) and custom (JSONB `metadata.records`) objects. Uses Squirrel with `sq.Dollar` placeholders. Expansion via LEFT JOIN LATERAL. Keyset pagination with base64url cursor. `QueryParams.ExtraConditions` allows injecting raw `sq.Sqlizer` WHERE clauses (used by OrgService for ltree filters). SQL expression helpers (`QI`, `FilterExpr`, `SelectFieldExpr`, `TableSource`, `QuoteLit`) are public and used by the `hrql/pg` backend.

**HRQL** (`internal/hrql/`): Pipe-based query language for HR data, fully decoupled from SQL. Single `POST /api/org/query` endpoint accepts an HRQL expression + optional `self_id` (UUID of the `self` pronoun). The package has zero SQL imports — it produces a storage-agnostic `Plan` (with `Condition` interface types) that a backend translates to SQL. Pipeline: Parse → AST → Compile → Plan → (backend) → SQL. File layout: `parser/` (tokenizer + recursive descent parser → AST), `plan.go` (Plan/Condition types: `FieldCmp`, `StringMatch`, `OrgChainUp`, `OrgSubtree`, `SameFieldCond`, `SubqueryAgg`, + `ScalarExpr` interface for arithmetic), `compiler.go` (AST → Plan dispatch + step appliers + `compileScalarExpr` for arithmetic), `functions.go` (source/pipe function registry: chain, reports, peers, colleagues, reports_to), `compile_where.go` (where condition compilation → Plan conditions), `resolve.go` (argument resolution helpers), `org.go` (pure helpers: `isDescendant`, `LtreeLabelToUUID`). The compiler is pure (zero I/O): `NewCompiler(cache, selfID)` produces a `Plan` with unresolved `EmployeeRef` values that the pg backend resolves at SQL translation time. Arithmetic expressions (`+`, `-`, `*`, `/`) are supported at the top level and produce `PlanScalar` with a `ScalarExpr` tree (`ScalarLiteral`, `ScalarArith`, `ScalarSubquery`). Operands can be number literals or parenthesized pipes ending in aggregation, e.g. `1 + (reports(self, 0) | count)`. The parser uses standard precedence (`*`/`/` bind tighter than `+`/`-`). Named employee references are NOT supported — frontend resolves names to UUIDs before sending. Language spec: `docs/adr/001-HRQL.md`. Data model mapping: `docs/adr/002-HRQL-data-model-mapping.md`. E2e tests: `internal/hrql/e2e/` (full Parse → Compile → Translate pipeline, no DB required).

**HRQL PostgreSQL backend** (`internal/hrql/pg/`): Translates HRQL `Plan` → SQL. `translate.go` converts `Plan` conditions to `sq.Sqlizer` expressions and builds aggregate queries. For arithmetic plans (`Plan.ScalarExpr != nil`), `scalarExprToSQL` recursively translates the `ScalarExpr` tree to SQL with `?` placeholders, then `buildArithmeticQuery` wraps in `SELECT` and converts to `$N` via `sq.Dollar.ReplacePlaceholders`. `buildAggregateBuilder` is the shared Squirrel builder (without `PlaceholderFormat`) used by both simple aggregates and arithmetic subqueries. `org.go` has ltree condition builders (`ChainUp`, `ChainDown`, `ChainAll`, `Subtree`, `SameField`) using `concatArgs` for safe arg slice concatenation. `resolver.go` has `RefToSQL`, `PathSubquery`, `FieldSubquery` — emit SQL subqueries from `EmployeeRef`. Service calls `pg.Translate(plan, obj, cache)` to get `SQLResult` with conditions, ordering, and optional aggregate SQL. `TranslateBooleanPlan` handles `PlanBoolean` (reports_to).

**Database**: PostgreSQL 16 with `pg_uuidv7` and `ltree` extensions. Two schemas: `metadata` (object/field registry + JSONB records) and `core` (real application tables). `core.employees.manager_path` is a materialized ltree path maintained by BEFORE/AFTER triggers on `manager_id`. SP-GiST index for `<@`/`@>` queries. Migrations are plain SQL files run via `psql` pipe in Taskfile.

**Frontend**: React 19 + Vite + TypeScript. Plain `fetch` calls (no codegen from proto). Vanguard returns camelCase JSON. `@glideapps/glide-data-grid` for data explorer. State-based routing via discriminated union, no router library. Org Chart page (`OrgPage.tsx`) has a DSL text input with employee picker and function template buttons. **Note**: Frontend still uses old DSL syntax — needs update for HRQL.

## Key Conventions

- SQL identifiers always quoted via `schema.QuoteIdent()` — escapes embedded `"`
- pgx v5: cast UUID/timestamp to `::text` in SQL when scanning into Go `string` fields; never use `rows.Values()` (returns `pgtype.UUID`, not `uuid.UUID`)
- Connect errors: always use typed codes (`connect.CodeNotFound`, `connect.CodeInvalidArgument`, `connect.CodeInternal`)
- Proto: messages in `registry.proto`/`metadata.proto`, services in `*_service.proto`; UUID fields validated with `(buf.validate.field).string.uuid = true`
- Migrations wrapped in `BEGIN;`/`COMMIT;`, applied with `ON_ERROR_STOP=1`
- `api_name` regex: `^[A-Za-z][A-Za-z0-9_]*(__c)?$` — `__c` suffix for custom objects
