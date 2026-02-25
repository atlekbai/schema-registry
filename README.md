# Schema Registry

Custom object and field registry system for PostgreSQL using UUID v7 and JSONB storage.

## Prerequisites

- Docker
- Docker Compose
- [Task](https://taskfile.dev/) (optional, for convenience commands)

## Quick Start

1. Start the database:
   ```bash
   task up
   # or
   docker compose up -d
   ```

2. Run migrations:
   ```bash
   task migrate-up
   ```

3. Connect to the database:
   ```bash
   task psql
   # or
   docker compose exec postgres psql -U postgres -d main
   ```

## Available Commands

```bash
task --list        # Show all available commands
task up            # Start the database
task down          # Stop the database
task restart       # Restart the database
task logs          # Show database logs
task psql          # Connect to the database
task migrate-up    # Run all migrations
task migrate-down  # Rollback all migrations
task clean         # Remove containers and volumes
task reset         # Clean and restart with migrations
```

## Database Schema

The schema registry consists of two main schemas:

### 1. `metadata` Schema
Metadata registry for custom objects and fields:
- `object_categories` - Predefined categories (HR, IT, Finance, Custom)
- `objects` - Object definitions (table metadata)
- `fields` - Field definitions (column metadata)
- `records` - Actual data for custom objects (JSONB storage)

### 2. `core` Schema
Standard application tables:
- `users` - Authentication identities
- `organizations` - Business units
- `departments` - Organizational departments (recursive hierarchy)
- `individuals` - Person records (PII)
- `employees` - HR employee records

## Migrations

Migrations are located in the `migrations/` directory:

1. `000001_metadata.up.sql` - Creates metadata schema and tables
2. `000002_core.up.sql` - Creates core application tables
3. `000003_metadata_core.up.sql` - Populates metadata for core tables

Each migration has a corresponding `.down.sql` file for rollback.

## Features

- **UUID v7**: Time-ordered, globally unique identifiers
- **JSONB Storage**: Flexible schema with high performance indexing
- **GIN Indexes**: Optimized JSONB queries with `jsonb_path_ops`
- **Partial Indexes**: Selective indexing for performance
- **Named Constraints**: Clear error messages
- **Standard vs Custom Objects**: Distinction between application and user-defined objects

## Connection Details

- **Host**: localhost
- **Port**: 5432
- **Database**: main
- **User**: postgres
- **Password**: postgres

Connection string:
```
postgresql://postgres:postgres@localhost:5432/main
```

## Development

To reset the database:
```bash
make clean
make up
make migrate-up
```

To connect and inspect the schema:
```bash
make psql
\dt metadata.*
\dt core.*
SELECT * FROM metadata.objects;
```
