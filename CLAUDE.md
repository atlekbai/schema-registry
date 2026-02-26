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

# No tests exist yet
```

## Architecture

**Transport**: ConnectRPC + Vanguard REST transcoder. REST routes defined via `google.api.http` annotations in `proto/registry/v1/*_service.proto`. Single Vanguard transcoder handles all routes on `/`. Each service implements `server.ConnectService` interface and is registered in `cmd/server/main.go`.

**Schema Cache** (`internal/schema/cache.go`): In-memory map of `ObjectDef`/`FieldDef` loaded at startup from `metadata.objects` JOIN `metadata.fields`. Used by query layer to validate params and build SQL. Reloaded (best-effort) after metadata mutations.

**Query Builder** (`internal/query/`): `NewBuilder(obj)` dispatches to `StandardBuilder` (real `core.*` tables) or `CustomBuilder` (JSONB `metadata.records`). Uses Squirrel with `sq.Dollar` placeholders. Expansion via LEFT JOIN LATERAL. Keyset pagination with base64url cursor. `QueryParams.ExtraConditions` allows injecting raw `sq.Sqlizer` WHERE clauses (used by OrgService for ltree filters).

**Org Chart** (`internal/service/org.go`, `internal/query/org.go`): `OrgService` provides 4 RPCs for org-tree queries using ltree on `core.employees.manager_path`. Condition builders in `query/org.go` (`ChainUp`, `ChainDown`, `Subtree`, `SameField`) produce ltree WHERE clauses injected via `ExtraConditions`. Routes: `/api/org/chain/{id}`, `/api/org/peers/{id}`, `/api/org/reports/{id}`, `/api/org/reports-to/{id}/{target_id}`.

**Database**: PostgreSQL 16 with `pg_uuidv7` and `ltree` extensions. Two schemas: `metadata` (object/field registry + JSONB records) and `core` (real application tables). `core.employees.manager_path` is a materialized ltree path maintained by BEFORE/AFTER triggers on `manager_id`. SP-GiST index for `<@`/`@>` queries. Migrations are plain SQL files run via `psql` pipe in Taskfile.

**Frontend**: React 19 + Vite + TypeScript. Plain `fetch` calls (no codegen from proto). Vanguard returns camelCase JSON. `@glideapps/glide-data-grid` for data explorer. State-based routing via discriminated union, no router library. Org Chart page (`OrgPage.tsx`) exposes all 4 org operations with tabbed UI.

## Key Conventions

- SQL identifiers always quoted via `schema.QuoteIdent()` — escapes embedded `"`
- pgx v5: cast UUID/timestamp to `::text` in SQL when scanning into Go `string` fields; never use `rows.Values()` (returns `pgtype.UUID`, not `uuid.UUID`)
- Connect errors: always use typed codes (`connect.CodeNotFound`, `connect.CodeInvalidArgument`, `connect.CodeInternal`)
- Proto: messages in `registry.proto`/`metadata.proto`, services in `*_service.proto`; UUID fields validated with `(buf.validate.field).string.uuid = true`
- Migrations wrapped in `BEGIN;`/`COMMIT;`, applied with `ON_ERROR_STOP=1`
- `api_name` regex: `^[A-Za-z][A-Za-z0-9_]*(__c)?$` — `__c` suffix for custom objects
