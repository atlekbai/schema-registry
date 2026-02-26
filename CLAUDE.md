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

**Query Builder** (`internal/query/`): `NewBuilder(obj)` dispatches to `StandardBuilder` (real `core.*` tables) or `CustomBuilder` (JSONB `metadata.records`). Uses Squirrel with `sq.Dollar` placeholders. Expansion via LEFT JOIN LATERAL. Keyset pagination with base64url cursor.

**Database**: PostgreSQL 16 with `pg_uuidv7` extension (NOT `uuidv7`). Two schemas: `metadata` (object/field registry + JSONB records) and `core` (real application tables). Migrations are plain SQL files run via `psql` pipe in Taskfile.

**Frontend**: React 19 + Vite + TypeScript. Plain `fetch` calls (no codegen from proto). Vanguard returns camelCase JSON. `@glideapps/glide-data-grid` for data explorer. State-based routing via discriminated union, no router library.

## Key Conventions

- SQL identifiers always quoted via `schema.QuoteIdent()` — escapes embedded `"`
- pgx v5: cast UUID/timestamp to `::text` in SQL when scanning into Go `string` fields; never use `rows.Values()` (returns `pgtype.UUID`, not `uuid.UUID`)
- Connect errors: always use typed codes (`connect.CodeNotFound`, `connect.CodeInvalidArgument`, `connect.CodeInternal`)
- Proto: messages in `registry.proto`/`metadata.proto`, services in `*_service.proto`; UUID fields validated with `(buf.validate.field).string.uuid = true`
- Migrations wrapped in `BEGIN;`/`COMMIT;`, applied with `ON_ERROR_STOP=1`
- `api_name` regex: `^[A-Za-z][A-Za-z0-9_]*(__c)?$` — `__c` suffix for custom objects
