# ADR-002: HRQL Data Model Mapping

**Status:** Draft
**Author:** Ali / Doodocs Platform Team
**Created:** 2026-02-27
**Last Updated:** 2026-02-27
**Supersedes:** Adapts ADR-001 (HRQL language design) to the actual schema registry data model

---

## 1. Summary

This ADR maps the HRQL language design (ADR-001) to the concrete schema registry implementation: `core.*` tables, `metadata.objects`/`metadata.fields` registry, ltree hierarchy, and the existing query builder infrastructure.

---

## 2. Data Model Mapping

### 2.1 Object → Table Mapping

| HRQL identifier | `metadata.objects.api_name` | Physical table           | Standard? |
| --------------- | --------------------------- | ------------------------ | --------- |
| `employees`     | `employees`                 | `core.employees`         | Yes       |
| `individuals`   | `individuals`               | `core.individuals`       | Yes       |
| `departments`   | `departments`               | `core.departments`       | Yes       |
| `organizations` | `organizations`             | `core.organizations`     | Yes       |
| `users`         | `users`                     | `core.users`             | Yes       |
| `*__c`          | user-defined                | `metadata.records` JSONB | No        |

### 2.2 Field → Column Mapping (employees)

| HRQL field         | `api_name`        | Type     | Storage column    | LOOKUP target   |
| ------------------ | ----------------- | -------- | ----------------- | --------------- |
| `.employee_number` | `employee_number` | `TEXT`   | `employee_number` | —               |
| `.employment_type` | `employment_type` | `CHOICE` | `employment_type` | —               |
| `.start_date`      | `start_date`      | `DATE`   | `start_date`      | —               |
| `.end_date`        | `end_date`        | `DATE`   | `end_date`        | —               |
| `.manager`         | `manager`         | `LOOKUP` | `manager_id`      | `employees`     |
| `.department`      | `department`      | `LOOKUP` | `department_id`   | `departments`   |
| `.organization`    | `organization`    | `LOOKUP` | `organization_id` | `organizations` |
| `.individual`      | `individual`      | `LOOKUP` | `individual_id`   | `individuals`   |
| `.user`            | `user`            | `LOOKUP` | `user_id`         | `users`         |

### 2.3 Field → Column Mapping (other objects)

**individuals:**

| HRQL field    | `api_name`   | Type    | Storage column |
| ------------- | ------------ | ------- | -------------- |
| `.email`      | `email`      | `EMAIL` | `email`        |
| `.first_name` | `first_name` | `TEXT`  | `first_name`   |
| `.last_name`  | `last_name`  | `TEXT`  | `last_name`    |

**departments:**

| HRQL field      | `api_name`     | Type     | Storage column    | LOOKUP target   |
| --------------- | -------------- | -------- | ----------------- | --------------- |
| `.title`        | `title`        | `TEXT`   | `title`           | —               |
| `.organization` | `organization` | `LOOKUP` | `organization_id` | `organizations` |
| `.parent`       | `parent`       | `LOOKUP` | `parent_id`       | `departments`   |

**organizations:**

| HRQL field | `api_name` | Type   | Storage column |
| ---------- | ---------- | ------ | -------------- |
| `.title`   | `title`    | `TEXT` | `title`        |

### 2.4 Hidden Columns (Not Exposed as HRQL Fields)

| Column          | Table            | Purpose                                   | Used by                          |
| --------------- | ---------------- | ----------------------------------------- | -------------------------------- |
| `manager_path`  | `core.employees` | Materialized ltree for hierarchy queries  | `chain`, `reports`, `reports_to` |
| `custom_fields` | `core.*`         | JSONB storage for custom field values     | Custom field access              |
| `id`            | all              | Primary key (auto-exposed on all objects) | Record identity                  |
| `created_at`    | all              | Auto-timestamp (auto-exposed)             | Audit                            |
| `updated_at`    | all              | Auto-timestamp (auto-exposed)             | Audit                            |

---

## 3. Field Access Adaptation

### 3.1 Standard Fields (via metadata.fields)

HRQL field names map directly to `api_name` values registered in `metadata.fields`. The schema cache (`internal/schema/cache.go`) resolves `api_name → storage_column` at runtime.

```jq
self.employee_number         // "EMP-005" (TEXT, storage: employee_number)
self.employment_type         // "FULL_TIME" (CHOICE, storage: employment_type)
self.start_date              // 2020-01-10 (DATE, storage: start_date)
self.end_date                // null (DATE, storage: end_date)
```

### 3.2 LOOKUP Traversal

Dot notation chains through LOOKUP fields to access the target object's fields. The query builder generates LEFT JOIN LATERAL (existing expand infrastructure).

```jq
self.individual.first_name   // "Alex" (individuals.first_name)
self.individual.last_name    // "Petrov" (individuals.last_name)
self.individual.email        // "alex.petrov@acme.com" (individuals.email)
self.department.title        // "Backend" (departments.title)
self.department.organization.title  // "Acme Corp" (organizations.title)
self.department.parent.title // "Engineering" (departments.title via parent LOOKUP)
self.manager.individual.first_name  // manager's first name
self.manager.department.title       // manager's department name
```

**SQL generation** for `self.department.title`:

```sql
SELECT d."title"
FROM core.employees e
LEFT JOIN LATERAL (
  SELECT * FROM core.departments WHERE id = e."department_id"
) d ON true
WHERE e."id" = $1
```

Deeper chains like `self.department.parent.title` add nested joins:

```sql
SELECT p."title"
FROM core.employees e
LEFT JOIN LATERAL (
  SELECT * FROM core.departments WHERE id = e."department_id"
) d ON true
LEFT JOIN LATERAL (
  SELECT * FROM core.departments WHERE id = d."parent_id"
) p ON true
WHERE e."id" = $1
```

### 3.3 Custom Fields

Objects with `supports_custom_fields = true` store extra data in a JSONB `custom_fields` column. Custom fields registered in `metadata.fields` (with `is_standard = false`, `storage_column = NULL`) are accessed via the same dot notation:

```jq
self.salary__c               // custom NUMBER field (stored in custom_fields JSONB)
self.title__c                // custom TEXT field
self.level__c                // custom CHOICE field
```

> **Convention:** Custom field `api_name` values end with `__c`. The query builder resolves them to `custom_fields->>'api_name'` for standard objects or `data->>'api_name'` for custom objects (`metadata.records`).

### 3.4 Fields the ADR-001 Examples Assumed But Don't Exist

ADR-001 used fictional fields from The Office. Here is the mapping to our actual model:

| ADR-001 field     | Status         | Actual equivalent                                                                                                                  |
| ----------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `self.name`       | Does not exist | `self.individual.first_name` + `self.individual.last_name` or `concat(self.individual.first_name, " ", self.individual.last_name)` |
| `self.salary`     | Does not exist | Requires custom field: `self.salary__c`                                                                                            |
| `self.title`      | Does not exist | Requires custom field: `self.title__c`                                                                                             |
| `self.level`      | Does not exist | Requires custom field: `self.level__c`                                                                                             |
| `self.location`   | Does not exist | Requires custom field: `self.location__c`                                                                                          |
| `self.department` | Exists (FK)    | Returns department record, not string. Use `.department.title` for the name                                                        |

---

## 4. Org Function Backend Mapping

### 4.1 Existing Infrastructure

The hierarchy is powered by:

- **`core.employees.manager_path`** — ltree column, materialized by triggers on `manager_id`
- **SP-GiST index** on `manager_path` — enables efficient `<@`/`@>` queries
- **Condition builders** in `internal/query/org.go`: `ChainUp`, `ChainDown`, `Subtree`, `ExcludeSelf`, `SameField`
- **Current DSL** in `internal/service/orgdsl.go`: `CHAIN(id, steps)`, `PEERS(id, dim)`, `REPORTS(id [, true])`, `REPORTSTO(id1, id2)`

### 4.2 HRQL Function → SQL Mapping

| HRQL function                      | SQL condition (via `internal/query/org.go`)                              |
| ---------------------------------- | ------------------------------------------------------------------------ |
| `chain(emp, N)`                    | `ChainUp(path, N)` → `manager_path = subpath(path, 0, nlevel(path) - N)` |
| `chain(emp)` / `chain(emp, 0)`     | Walk full path — extract each ancestor UUID from ltree labels            |
| `reports(emp, 1)`                  | `ChainDown(path, 1)` → `<@ path AND nlevel = nlevel(path)+1`             |
| `reports(emp, N)`                  | `ChainDown(path, N)` → `<@ path AND nlevel = nlevel(path)+N`             |
| `reports(emp)` / `reports(emp, 0)` | `Subtree(path)` → `<@ path AND != path`                                  |
| `peers(emp)`                       | `SameField("manager_id", val, id)`                                       |
| `colleagues(emp, .field)`          | `SameField(column, val, id)` — resolves field to storage column          |
| `reports_to(emp, person)`          | `manager_path <@ person_path`                                            |

### 4.3 Current DSL → HRQL Migration

| Current DSL               | HRQL                           |
| ------------------------- | ------------------------------ |
| `CHAIN(uuid, +1)`         | `chain(emp, 1)`                |
| `CHAIN(uuid, +2)`         | `chain(emp, 2)`                |
| `CHAIN(uuid, -1)`         | `reports(emp, 1)`              |
| `CHAIN(uuid, -2)`         | `reports(emp, 2)`              |
| `PEERS(uuid, manager)`    | `peers(emp)`                   |
| `PEERS(uuid, department)` | `colleagues(emp, .department)` |
| `REPORTS(uuid)`           | `reports(emp)`                 |
| `REPORTS(uuid, true)`     | `reports(emp, 1)`              |
| `REPORTSTO(uuid1, uuid2)` | `reports_to(emp, target)`      |

### 4.4 What HRQL Adds Over Current DSL

1. **Pipes** — results compose: `reports(self, 1) | where(.employment_type == "FULL_TIME") | count`
2. **where** — arbitrary filtering on any field, not just fixed dimensions
3. **Aggregation** — `count`, `avg`, `sum`, `min`, `max` over results
4. **Cross-object traversal** — `self.department.title`, `self.individual.email`
5. **Employee search** — `employees | where(...)` for cross-org queries
6. **Sorting/picking** — `sort_by`, `first`, `last`, `nth`
7. **self/dot pronouns** — unambiguous context in nested expressions

---

## 5. Concrete Examples (Using Seed Data)

The seed data (`migrations/000006_seed.up.sql`) contains ~48 employees at Acme Corp:

```
Sarah Chen (CEO, EMP-001, Executive)
├── James Okafor (CTO, EMP-002, Executive)
│   ├── Alex Petrov (Backend Lead, EMP-005, Backend)
│   │   ├── Priya Sharma (EMP-006)
│   │   ├── Omar Hassan (EMP-007)
│   │   ├── Lisa Mueller (EMP-008)
│   │   ├── Raj Patel (EMP-009, PART_TIME)
│   │   └── Emma Larsson (EMP-010, CONTRACTOR)
│   ├── Jun Tanaka (Frontend Lead, EMP-011, Frontend)
│   │   ├── Anna Kowalski
│   │   ├── Carlos Rivera
│   │   ├── Mei Wong
│   │   └── Daniel Berg (CONTRACTOR)
│   ├── Yuki Sato (Infra Lead, EMP-016, Infrastructure)
│   │   ├── Ivan Popov
│   │   ├── Fatima Ali
│   │   └── Lucas Martin
│   └── Nina Volkov (QA Lead, EMP-020, QA)
│       ├── Tom Andersen
│       └── Aisha Diallo
├── Maria Santos (VP Sales, EMP-003, Executive)
│   ├── Michael Brown (Sales Mgr, EMP-030, Sales)
│   │   ├── Jessica Taylor
│   │   ├── Kevin Nguyen
│   │   ├── Rachel Lee
│   │   ├── Adam White
│   │   └── Diana Costa
│   └── Olivia Mitchell (Marketing Lead, EMP-036, Marketing)
│       ├── Liam Campbell
│       ├── Zara Hussain
│       └── Ethan Clark (PART_TIME)
├── David Kim (CFO, EMP-004, Executive)
│   └── Julia Wright (Finance Mgr, EMP-040, Finance)
│       ├── Ben Suzuki
│       ├── Amara Osei
│       └── Max Hoffman
├── Sophie Dubois (Head of Product, EMP-023, Product)
│   ├── Ryan O'Connor
│   ├── Hana Yoshida
│   ├── Marco Rossi
│   └── Elena Garcia (Design Lead, EMP-027, Design)
│       ├── Noah Fischer
│       └── Lila Jensen (CONTRACTOR)
└── Clara Moreno (HR Lead, EMP-044, People & HR)
    └── Sam Johansson
```

### 5.1 Basic field access

```jq
// Priya's employee number
self.employee_number
// "EMP-006"

// Priya's full name
concat(self.individual.first_name, " ", self.individual.last_name)
// "Priya Sharma"

// Priya's department
self.department.title
// "Backend"

// Priya's parent department
self.department.parent.title
// "Engineering"

// Priya's manager's name
self.manager.individual.first_name
// "Alex"
```

### 5.2 Org function examples

```jq
// Priya's chain to CEO
chain(self)
// [Alex Petrov, James Okafor, Sarah Chen]

chain(self, 1) | first | .individual.first_name
// "Alex"

chain(self, 2) | last | .individual.first_name
// "James"

// Alex's direct reports
reports(alex, 1) | .individual.first_name
// ["Priya", "Omar", "Lisa", "Raj", "Emma"]

// All reports under James (CTO), recursive
reports(james, 0) | count
// 22 (all Engineering employees)

// Alex's full-time direct reports
reports(alex, 1) | where(.employment_type == "FULL_TIME")
// [Priya, Omar, Lisa]

// Priya's peers (same manager = Alex)
peers(self) | .individual.first_name
// ["Omar", "Lisa", "Raj", "Emma"]

// Colleagues in same department
colleagues(self, .department) | .individual.first_name
// ["Alex", "Omar", "Lisa", "Raj", "Emma"]  (Backend, excluding Priya)

// Does Priya report to Sarah?
reports_to(self, sarah)
// true
```

### 5.3 Search and filtering

```jq
// All contractors
employees | where(.employment_type == "CONTRACTOR")
// [Emma Larsson, Daniel Berg, Lila Jensen]

// All interns
employees | where(.employment_type == "INTERN")
// [Alex Thompson, Maya Gupta, Jake Robinson]

// Employees in Sales department
employees | where(.department.title == "Sales") | count
// 6

// People managers (have direct reports) under the CTO
reports(james) | where(reports(., 1) | count > 0) | .individual.first_name
// ["Alex", "Jun", "Yuki", "Nina"]

// Employees in Engineering sub-departments
employees | where(.department.parent.title == "Engineering") | count
// 22
```

### 5.4 Approval routing

```jq
// Find the HR Lead (has reports in People & HR)
employees | where(.department.title == "People & HR" and reports(., 1) | count > 0) | first
// Clara Moreno

// Nearest manager above me with >3 direct reports
chain(self) | where(reports(., 1) | count > 3) | first
// Alex Petrov (has 5 direct reports)

// CFO (most senior person in Finance by start_date)
employees | where(.department.title == "Finance") | sort_by(.start_date, asc) | first
// Julia Wright
```

### 5.5 Analytics

```jq
// Count of my direct reports
reports(self, 1) | count

// Team members hired after 2023
reports(self) | where(.start_date > date(2023, 1, 1)) | count

// All departments (via employees)
employees | .department.title | unique

// Contractors per department (would need group_by — see open questions)
employees | where(.employment_type == "CONTRACTOR") | count
// 3
```

---

## 6. Implementation Considerations

### 6.1 Schema Cache as Source of Truth

The HRQL parser resolves field names by looking up `api_name` in the schema cache (`ObjectDef.FieldsByAPIName`). This provides:

- **Validation** — unknown field names are caught at parse time
- **Type information** — the parser knows field types for comparison validation
- **Storage resolution** — `FieldDef.StorageColumn` for standard fields, JSONB path for custom

### 6.2 Query Builder Integration

HRQL expressions compile to Squirrel SQL builders via the existing `query.NewBuilder(obj)` dispatch:

- **StandardBuilder** — for `core.*` tables (employees, departments, etc.)
- **CustomBuilder** — for `metadata.records` JSONB
- **ExtraConditions** (`[]sq.Sqlizer`) — org functions inject ltree WHERE clauses

### 6.3 LOOKUP Traversal Reuses Expand

Chained dot access (`self.department.title`) reuses the LEFT JOIN LATERAL mechanism already built for the `expand` query parameter. The HRQL compiler walks the dot chain, resolving each LOOKUP field's target object, and generates the same join structure.

### 6.4 manager_path Is Not a Field

`manager_path` is intentionally not registered in `metadata.fields`. It's an internal optimization for ltree queries. HRQL never exposes it directly — org functions (`chain`, `reports`, `reports_to`) use it internally via the condition builders in `internal/query/org.go`.

---

## 7. Resolved Questions

1. **Error handling** — `first` on empty list returns `null`. Same for `last`, `nth` out of bounds.

2. **Null propagation** — PostgreSQL-style: `self.manager.manager.individual.first_name` returns `null` if any link in the chain is NULL. Same as `jsonb->'a'->'b'->'c'` — short-circuits to `null`, no error.

3. **Identity comparison** — `employees | where(. != self)` compares by `id` UUID.

4. **Performance of `reports_to` in where** — Push down to SQL: `employees | where(reports_to(., target))` compiles to `WHERE manager_path <@ target_path`. No per-row evaluation.

5. **`group_by`** — Not needed for now. Can revisit later.

6. **Custom object traversal** — `self.performance_review__c.rating__c` resolves the same as any other LOOKUP field. The schema cache knows the target object; the query builder generates the appropriate join (LEFT JOIN LATERAL on `metadata.records` filtered by `object_id`).

7. **Sandbox/permissions** — No restrictions for now. `employees` is available everywhere. Prototyping stage.

8. **Depth semantics migration** — Not a concern. Single developer, no existing frontend users to migrate.

---

## 8. References

- [ADR-001: HRQL Language Design](001-HRQL.md) — Language specification, grammar, design decisions
- `internal/schema/cache.go` — Schema cache: `ObjectDef`, `FieldDef`, field resolution
- `internal/schema/types.go` — `FieldType` enum, `FieldDef` struct, `QuoteIdent()`
- `internal/query/org.go` — ltree condition builders: `ChainUp`, `ChainDown`, `Subtree`, `SameField`
- `internal/service/orgdsl.go` — Current org DSL parser (to be superseded)
- `internal/query/builder.go` — Query builder dispatch: `StandardBuilder` vs `CustomBuilder`
- `migrations/000003_core.up.sql` — Core table schemas
- `migrations/000004_metadata_core.up.sql` — Metadata field registrations
- `migrations/000005_employees_ltree.up.sql` — ltree column, triggers, SP-GiST index
- `migrations/000006_seed.up.sql` — Seed data (~48 employees)
