# RFC: HRQL — A Pipe-Based Query Language for HR Data

**Status:** Draft
**Author:** Ali / Doodocs Platform Team
**Created:** 2026-02-27
**Last Updated:** 2026-02-27

---

## 1. Summary

This RFC proposes **HRQL** (Human Resources Query Language) — a formula language for querying, filtering, and computing over HR data. HRQL combines the approachability of Excel-style functions with the composability of jq-style pipelines to create a language that HR administrators can learn in minutes but that scales to complex compensation models, approval routing, and organizational analytics.

---

## 2. Motivation

### 2.1 The Problem

Existing HR formula languages (e.g., Rippling's RQL) suffer from a common design flaw: **overloaded god functions** that combine multiple behaviors behind polymorphic argument patterns.

Consider Rippling's `ORG` function, which handles 12 distinct operations through a single signature:

```
ORG(Employee, Manager)              → one person
ORG(Employee, Peers)                → list of people
ORG(Employee, Department)           → group by attribute
ORG(Employee, Title lookup, "VP")   → graph search
ORG(Employee, -2)                   → depth traversal
```

This design violates fundamental principles of good language design:

- **No type predictability** — the same function returns a scalar, a list, or a boolean depending on arguments
- **No composability** — results can't be piped into further operations without nesting
- **No discoverability** — users must memorize argument patterns rather than discovering functions through autocomplete
- **No extensibility** — adding a new relationship type means modifying the god function's argument parser

### 2.2 The Gap

Excel nailed approachability but hit a wall at filtering — every aggregation needs its own `*IF` variant (`SUMIF`, `COUNTIF`, `AVERAGEIF`, etc.). SQL nailed querying but alienated non-developers. The gap between them is underexplored.

### 2.3 The Goal

Design a language where:

- Every function does one thing
- Filtering is a first-class operation, not bolted onto each aggregation
- Complex queries stay flat and readable instead of becoming nested function calls
- The language is extensible by registering new functions without modifying existing ones

---

## 3. Design Principles

### 3.1 Everything Is a Pipeline

Data flows left to right through a chain of operations separated by `|`. Each step receives the output of the previous step. There is no distinction between "filter," "navigate," and "transform" — they're all operations that data flows through.

```jq
input | operation | operation | operation
```

### 3.2 Stored Fields Use Dot Access, Computed Values Use Functions

If a value is stored on the record, access it with `.field`. If it's computed, call a function. This distinction is absolute — there are no exceptions.

```jq
// Stored — actual fields on the employee record
self.manager
self.name
self.salary
self.department

// Computed — always a function call
reports(self, 1)
peers(self)
chain(self)
```

**Rationale:** When a user types `self.` and sees autocomplete suggestions, they see exactly what's in the database. Computed relationships live in the function namespace and are discovered separately.

### 3.3 Functions Declare Their Inputs

Every function takes an explicit first argument — typically the employee it operates on. There is no implicit receiver.

```jq
// Explicit — always unambiguous
peers(self)
peers(self.manager)
peers(andy)

// Mid-pipe, no confusion
self.manager | reports(., 1) | where(.level == self.level)
```

### 3.4 Two Pronouns: `.` and `self`

| Pronoun | Meaning                                          | Scope                        |
| ------- | ------------------------------------------------ | ---------------------------- |
| `.`     | The current item flowing through the pipe        | Changes with each pipe step  |
| `self`  | The employee this formula is being evaluated for | Fixed for the entire formula |

This resolves the ambiguity in existing systems where `Department = Department` means "this record's department equals the row's department" — a convention that only makes sense if you already know the convention.

```jq
// Clear: . is each employee in the pipe, self is "me"
employees | where(.department == self.department) | .salary | avg
```

---

## 4. Language Specification

### 4.1 Field Access

Access stored fields on any record using dot notation.

```jq
self.name                    // "Andy Bernard"
self.manager                 // Employee record (Jim Halpert)
self.manager.name            // "Jim Halpert"
self.manager.department      // "Sales"
self.salary                  // 65000
self.start_date              // 2008-03-17
self.level                   // "Individual Contributor"
self.title                   // "Regional Director in Charge of Sales"
self.location                // "Scranton"
self.employment_type         // "Full-time"
```

Dot notation chains naturally for relationship traversal:

```jq
self.manager.manager.name    // "Michael Scott"
self.manager.manager.title   // "Regional Manager"
```

### 4.2 The Pipe Operator

The `|` operator passes the result of the left side as input to the right side.

```jq
// Single value flowing through
self.salary | round(., 2)

// List flowing through
reports(self, 1) | where(.department == "Engineering") | .salary | avg
```

When a list flows through `.field`, it maps — each item's field is extracted:

```jq
reports(self, 1) | .name
// ["Dwight Schrute", "Jim Halpert"]

reports(self, 1) | .salary
// [85000, 78000]
```

### 4.3 Filtering with `where`

`where(condition)` keeps items from a list that match the condition. Multiple conditions separated by commas are combined with AND.

```jq
// Single condition
employees | where(.department == "Engineering")

// Multiple conditions (AND)
employees | where(.department == "Engineering" and .level == "Senior")

// OR requires explicit operator
employees | where(.department == "Engineering" or .department == "Sales")

// Nested expressions
employees | where(.start_date > today() - 90 and .salary > 0)
```

Inside `where`, `.` refers to each item being tested. `self` still refers to the formula's row context:

```jq
// "Employees in my department"
employees | where(.department == self.department)

// "Employees at my level who earn more than me"
employees | where(.level == self.level and .salary > self.salary)
```

### 4.4 Sorting and Picking

```jq
// Sort a list
list | sort_by(.field)             // ascending (default)
list | sort_by(.field, asc)        // ascending (explicit)
list | sort_by(.field, desc)       // descending

// Pick from a list
list | first                       // first item
list | last                        // last item
list | nth(3)                      // third item (1-indexed)

// Combined — most common pattern
reports(self, 1) | sort_by(.salary, desc) | first
// → highest-paid direct report
```

### 4.5 Aggregation

Standard aggregation functions receive a list and return a scalar.

```jq
list | count                       // number of items
list | sum                         // sum of numeric values
list | avg                         // arithmetic mean
list | min                         // minimum value
list | max                         // maximum value
```

These compose with everything:

```jq
// Average salary of my peers
peers(self) | .salary | avg

// Count of Engineering employees
employees | where(.department == "Engineering") | count

// Max salary in my department
colleagues(self, .department) | .salary | max
```

### 4.6 String Operations

```jq
value | contains("substring")      // boolean
value | starts_with("prefix")      // boolean
value | ends_with("suffix")        // boolean
value | upper                      // uppercase
value | lower                      // lowercase
value | length                     // character count
```

### 4.7 List Operations

```jq
list | contains(item)              // boolean: is item in list?
list | unique                      // deduplicated list
list | flat_map(.field)            // map + flatten
list | length                      // count (alias for count)
```

---

## 5. Org Functions

### 5.1 Overview

The organizational hierarchy is a tree with one stored relationship — `.manager` — from which all other relationships are computed. HRQL provides five org functions. Each takes an explicit employee as its first argument and returns either a list or a boolean.

| Function                       | Returns | Description                                   |
| ------------------------------ | ------- | --------------------------------------------- |
| `chain(employee, [depth])`     | List    | Managers upward from employee                 |
| `reports(employee, [depth])`   | List    | Employees below in the hierarchy              |
| `peers(employee)`              | List    | Employees sharing the same manager            |
| `colleagues(employee, field)`  | List    | Employees sharing an attribute value          |
| `reports_to(employee, person)` | Boolean | Whether employee reports up through person    |
| `employees \| where(...)`      | List    | Search by any attribute combination (see 5.7) |

### 5.2 `chain(employee, [depth])`

Returns the list of managers above the employee, ordered nearest first.

```jq
chain(employee)              // full chain to root
chain(employee, 0)           // same — full chain (0 = unlimited)
chain(employee, 1)           // immediate manager only
chain(employee, 2)           // manager and their manager
chain(employee, n)           // n levels up
```

**Depth parameter:**

- `0` or omitted: full chain to root (unlimited)
- `n > 0`: exactly n levels up

**Examples:**

Given this reporting tree:

```
Jan Levinson
  → Michael Scott
    → Dwight Schrute
    → Jim Halpert
      → Andy Bernard
      → Phyllis Lapin
      → Stanley Hudson
```

```jq
chain(andy)
// [Jim, Michael, Jan]

chain(andy, 1)
// [Jim]

chain(andy, 2)
// [Jim, Michael]

// Skip-level manager
chain(andy, 2) | last
// Michael

// Full chain to CEO
chain(andy) | last
// Jan

// Nearest Director above me
chain(self) | where(.title | contains("Director")) | first

// ALL Directors above me
chain(self) | where(.title | contains("Director"))

// Nearest Director's salary
chain(self) | where(.title | contains("Director")) | first | .salary

// Nearest Director's direct reports
chain(self) | where(.title | contains("Director")) | first | reports(., 1)
```

### 5.3 `reports(employee, [depth])`

Returns all employees below the given employee in the hierarchy.

```jq
reports(employee)            // all reports, fully recursive (depth = 0)
reports(employee, 0)         // same — all reports
reports(employee, 1)         // direct reports only
reports(employee, 2)         // directs + their directs
reports(employee, n)         // n levels down
```

**Depth parameter:**

- `0` or omitted: all reports recursively (unlimited)
- `n > 0`: exactly n levels down

**Examples:**

```jq
reports(michael, 1)
// [Dwight, Jim]

reports(michael, 2)
// [Dwight, Jim, Andy, Phyllis, Stanley]

reports(michael)
// [Dwight, Jim, Andy, Phyllis, Stanley]

// Count of all my reports
reports(self) | count

// Direct reports in Engineering
reports(self, 1) | where(.department == "Engineering")

// Average salary two levels down
reports(self, 2) | .salary | avg

// All reports who are managers themselves
reports(self) | where(reports(., 1) | count > 0)
```

### 5.4 `peers(employee)`

Returns employees who share the same manager, excluding the given employee.

```jq
peers(stanley)
// [Andy, Phyllis]

peers(self.manager)
// my manager's peers (my "uncle/aunt" managers)

// Peers at my level
peers(self) | where(.level == self.level)

// Average peer salary
peers(self) | .salary | avg

// Peers hired before me
peers(self) | where(.start_date < self.start_date)
```

**Pipeline equivalent (for documentation):**

```jq
peers(employee) = reports(employee.manager, 1) | where(. != employee)
```

### 5.5 `colleagues(employee, field)`

Returns all employees who share the same value for the given field.

```jq
colleagues(self, .department)      // same department
colleagues(self, .level)           // same level
colleagues(self, .location)        // same work location
colleagues(self, .title)           // same title
colleagues(self, .employment_type) // same employment type

// Colleagues in my department at a higher level
colleagues(self, .department) | where(.level > self.level)

// Count of people at my location
colleagues(self, .location) | count

// Average salary in my department
colleagues(self, .department) | .salary | avg
```

**Pipeline equivalent:**

```jq
colleagues(employee, .field) = employees | where(.field == employee.field)
```

### 5.6 `reports_to(employee, person)`

Returns a boolean indicating whether the employee reports up through the given person at any level.

```jq
reports_to(stanley, jan)
// true — Stanley reports to Jim, who reports to Michael, who reports to Jan

reports_to(michael, dwight)
// false — Michael does not report to Dwight

reports_to(jim, dwight)
// false — peers have no reporting relationship

// Filter: only employees who report to Michael
employees | where(reports_to(., michael))

// Conditional logic
if(reports_to(self, Jan Levinson), "Levinson Org", "Other")
```

**Pipeline equivalent:**

```jq
reports_to(employee, person) = chain(employee) | contains(person)
```

### 5.7 Employee Search (No Dedicated Function)

A common need is finding employees by department, title, level, or any combination of attributes — for example, routing an approval to the HR Manager or finding all Senior Engineers in Sales. HRQL handles this entirely through `employees | where(...)` with no dedicated search function.

**Why no `find_employee` function?** A function like `find_employee(.department, "HR", .title, CONTAINS, "Manager")` would bake `where` and `first` into positional arguments — the same Excel-style trap we eliminated with `chain_find` (see Section 13.5). The pipe version composes freely: drop `first` to get all matches, add `sort_by` to rank them, add `count` to tally them.

**Basic patterns:**

```jq
// All Directors in Engineering
employees | where(.department == "Engineering" and .title | contains("Director"))

// First match (e.g., for approval routing)
employees | where(.department == "Engineering" and .title | contains("Director")) | first

// Their name
employees | where(.department == "Engineering" and .title | contains("Director")) | first | .name
```

**Approval routing:**

```jq
// Route to the HR Manager
employees | where(.department == "HR" and .title | contains("Manager")) | first

// Route to the CFO
employees | where(.department == "Finance" and .level >= "C-Level") | first
```

**Team discovery:**

```jq
// Senior Engineers in my department
employees | where(.department == self.department and (.title | contains("Senior Engineer")))

// All people managers in Sales
employees | where(.department == "Sales" and (reports(., 1) | count > 0))

// Onboarding buddy — same department, not my manager, not me
employees | where(.department == self.department and . != self.manager and . != self) | first
```

**Analytics over search results:**

```jq
// Count of Directors in Engineering
employees | where(.department == "Engineering" and (.title | contains("Director"))) | count

// Average salary of Senior Engineers across the company
employees | where(.title | contains("Senior Engineer")) | .salary | avg

// Highest-paid person in Legal
employees | where(.department == "Legal") | sort_by(.salary, desc) | first
```

Every variant uses the same building blocks — `where`, `sort_by`, `first`, `count`, `avg` — that the user already knows. No new function to learn, no new signature to memorize.

---

## 6. Excel-Compatible Functions

HRQL retains Excel-style function syntax for standard operations. These functions work identically to their Excel counterparts and can be used standalone or within pipes.

### 6.1 Aggregation

```jq
avg(list)                          // arithmetic mean
sum(list)                          // total
count(list)                        // number of items
min(list)                          // minimum
max(list)                          // maximum
```

Both call styles are equivalent:

```jq
// Functional
avg(reports(self, 1) | .salary)

// Pipe
reports(self, 1) | .salary | avg
```

### 6.2 Math

```jq
round(value, decimals)
abs(value)
floor(value)
ceiling(value)
mod(value, divisor)
power(base, exponent)
```

### 6.3 Date and Time

```jq
today()                            // current date
now()                              // current datetime
date(year, month, day)             // construct a date
year(date)                         // extract year
month(date)                        // extract month
day(date)                          // extract day
datedif(start, end, unit)          // difference between dates
```

### 6.4 Text

```jq
concat(value1, value2, ...)        // join strings
left(text, n)                      // first n characters
right(text, n)                     // last n characters
mid(text, start, length)           // substring
trim(text)                         // remove whitespace
substitute(text, old, new)         // replace text
```

### 6.5 Logic

```jq
if(condition, then, else)
and(condition1, condition2, ...)
or(condition1, condition2, ...)
not(condition)
switch(value, case1, result1, case2, result2, ..., default)
```

### 6.6 Statistical

```jq
percentrank(list, value, buckets)
```

Example:

```jq
// Salary decile vs department peers
percentrank(
  colleagues(self, .department) | .salary,
  self.salary,
  10
)

// Equity percentile vs recent hires
percentrank(
  employees | where(.start_date > today() - 90 and .equity_grant > 0) | .equity_grant,
  self.equity_grant,
  100
)
```

---

## 7. Temporal Functions

### 7.1 `history(field)`

Returns the change history of a field as a list of change records. Each record has `.date`, `.old_value`, `.new_value`, and `.effective_from`.

> **Availability:** Reports only.

```jq
// All title changes
history(self.title)

// Most recent title change date
history(self.title) | last | .date

// When was I last promoted?
history(self.level) | last | .date

// How long have I had my current title?
today() - (history(self.title) | last | .date)

// When did title change from Senior Manager to Director?
history(self.title)
  | where(.new_value == "Director" and .old_value == "Senior Manager")
  | first
  | .date

// Level changes after Dec 1, 2024
history(self.level)
  | where(.effective_from > date(2024, 12, 1))
  | last
  | .date
```

### 7.2 `value_as_of(field, date)`

Returns the value a field contained on a given past date.

> **Availability:** Reports only.

```jq
// Employment status on Jan 1, 2024
value_as_of(self.employment_status, date(2024, 1, 1))

// Title on start date
value_as_of(self.title, self.start_date)

// Manager on a leave start date
value_as_of(self.manager, self.leave_start_date)

// Salary one year ago
value_as_of(self.salary, today() - 365)
```

### 7.3 `prior_value(field)`

Returns the value of a field before the proposed change. Only available during record save (validations and rules).

> **Availability:** Custom object validations and rules only.

```jq
// Prevent salary increase over $50,000
(self.salary - prior_value(self.salary)) > 50000

// Ensure task status follows workflow
self.task_status == "COMPLETED"
  and prior_value(self.task_status) != "IN_PROGRESS"

// Log the previous department on transfer
prior_value(self.department)
```

---

## 9. Complete Example: Compensation Analysis Report

A realistic example combining multiple HRQL features:

```jq
// Average salary in my department
colleagues(self, .department) | .salary | avg

// My salary percentile within my level
percentrank(
  colleagues(self, .level) | .salary,
  self.salary,
  100
)

// My salary vs department average (ratio)
self.salary / (colleagues(self, .department) | .salary | avg)

// Highest-paid direct report
reports(self, 1) | sort_by(.salary, desc) | first | .name

// Pay period with highest gross pay
self.pay_periods | sort_by(.gross_pay, desc) | first

// Team members hired in last 90 days
reports(self) | where(.start_date > today() - 90) | count

// Approval routing: nearest VP above me
chain(self) | where(.title | contains("VP")) | first

// Approval routing: HR Manager (cross-org search)
employees | where(.department == "HR" and .title | contains("Manager")) | first

// People promoted in the last year
employees
  | where(history(.level) | last | .date > today() - 365)
  | count
```

---

## 10. Grammar (EBNF)

```ebnf
expression     = pipe_expr ;
pipe_expr      = primary { "|" pipe_step } ;

pipe_step      = field_access
               | function_call
               | where_clause
               | sort_clause
               | pick_operation
               | aggregation ;

primary        = "self"
               | identifier
               | literal
               | "(" expression ")"
               | function_call ;

field_access   = "." identifier { "." identifier } ;

function_call  = identifier "(" [ arg_list ] ")" ;
arg_list       = argument { "," argument } ;
argument       = expression ;

where_clause   = "where" "(" bool_expr ")" ;
bool_expr      = bool_term { "or" bool_term } ;
bool_term      = bool_factor { "and" bool_factor } ;
bool_factor    = comparison
               | "(" bool_expr ")"
               | expression ;
comparison     = expression comparator expression ;
comparator     = "==" | "!=" | ">" | ">=" | "<" | "<=" ;

sort_clause    = "sort_by" "(" field_access [ "," sort_order ] ")" ;
sort_order     = "asc" | "desc" ;

pick_operation = "first" | "last" | "nth" "(" integer ")" ;
aggregation    = "avg" | "sum" | "count" | "min" | "max" ;

literal        = string | number | boolean | date_literal ;
string         = '"' { character } '"' ;
number         = digit { digit } [ "." digit { digit } ] ;
boolean        = "true" | "false" ;

identifier     = letter { letter | digit | "_" } ;
```

---

## 11. Function Registry and Extensibility

### 11.1 The Extensibility Contract

Every function registered in HRQL must satisfy three rules:

1. **Explicit input** — the first argument declares what the function operates on (never implicit)
2. **Pipeline equivalent** — documentation shows the pipe expression it expands to
3. **Composable output** — returns either a single item or a list, always pipeable

### 11.2 Adding New Functions

New functions are registered without modifying existing ones. Example — adding dotted-line manager support:

**Step 1:** Add a stored field (if needed):

```jq
self.dotted_managers         // new field: list of dotted-line manager FKs
```

**Step 2:** Register a computed function:

```jq
matrix_peers(employee)
  = employee.dotted_managers | flat_map(reports(., 1)) | where(. != employee)
```

**Step 3:** Immediately composable — no other changes needed:

```jq
matrix_peers(self) | where(.department == "Legal") | first
matrix_peers(self) | .salary | avg
```

### 11.3 Built-in Function Reference

| Function      | Signature                           | Returns | Pipeline Equivalent                                    |
| ------------- | ----------------------------------- | ------- | ------------------------------------------------------ |
| `chain`       | `chain(employee, [depth])`          | List    | Walk `.manager` upward                                 |
| `reports`     | `reports(employee, [depth])`        | List    | Recursive find where `.manager == employee`            |
| `peers`       | `peers(employee)`                   | List    | `reports(employee.manager, 1) \| where(. != employee)` |
| `colleagues`  | `colleagues(employee, field)`       | List    | `employees \| where(.field == employee.field)`         |
| `reports_to`  | `reports_to(employee, person)`      | Boolean | `chain(employee) \| contains(person)`                  |
| `history`     | `history(field)`                    | List    | Change log for a field                                 |
| `value_as_of` | `value_as_of(field, date)`          | Value   | Snapshot of field at date                              |
| `prior_value` | `prior_value(field)`                | Value   | Field value before proposed change                     |
| `percentrank` | `percentrank(list, value, buckets)` | Integer | Quantile ranking                                       |

---

## 12. Migration from RQL

### 12.1 ORG Function Mapping

| RQL                                       | HRQL                                                            |
| ----------------------------------------- | --------------------------------------------------------------- |
| `ORG(Employee, Manager)`                  | `self.manager`                                                  |
| `ORG(Employee, 1)`                        | `chain(self, 1) \| first`                                       |
| `ORG(Employee, 2)`                        | `chain(self, 2) \| last`                                        |
| `ORG(Employee, -1)`                       | `reports(self, 1)`                                              |
| `ORG(Employee, -2)`                       | `reports(self, 2)`                                              |
| `ORG(Employee, 0)`                        | `peers(self)`                                                   |
| `ORG(Employee, Peers)`                    | `peers(self)`                                                   |
| `ORG(Employee, Direct reports)`           | `reports(self, 1)`                                              |
| `ORG(Employee, All reports)`              | `reports(self)`                                                 |
| `ORG(Employee, Self)`                     | `self`                                                          |
| `ORG(Employee, Department)`               | `colleagues(self, .department)`                                 |
| `ORG(Employee, Location)`                 | `colleagues(self, .location)`                                   |
| `ORG(Employee, Team)`                     | `colleagues(self, .team)`                                       |
| `ORG(Employee, Level)`                    | `colleagues(self, .level)`                                      |
| `ORG(Employee, Title)`                    | `colleagues(self, .title)`                                      |
| `ORG(Employee, Employment type)`          | `colleagues(self, .employment_type)`                            |
| `ORG(Employee, Title lookup, "Director")` | `chain(self) \| where(.title \| contains("Director")) \| first` |
| `ORG(Employee, Level lookup, X)`          | `chain(self) \| where(.level >= "X") \| first`                  |

### 12.2 SELECT Mapping

| RQL                                                                   | HRQL                                                           |
| --------------------------------------------------------------------- | -------------------------------------------------------------- |
| `AVG(SELECT(All Rows, Annual compensation, Department = Department))` | `colleagues(self, .department) \| .annual_compensation \| avg` |
| `SELECT(All Rows, Pay period, Employee = Employee)`                   | `self.pay_periods`                                             |

### 12.3 EXTRACT Mapping

| RQL                                             | HRQL                                               |
| ----------------------------------------------- | -------------------------------------------------- |
| `EXTRACT(list, Total gross pay, Descending, 1)` | `list \| sort_by(.total_gross_pay, desc) \| first` |

### 12.4 PERCENTRANK Mapping

| RQL                                                             | HRQL                                                                                          |
| --------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `PERCENTRANK(All Rows, Annual compensation, Level = Level, 10)` | `percentrank(colleagues(self, .level) \| .annual_compensation, self.annual_compensation, 10)` |

### 12.5 Temporal Function Mapping

| RQL                                            | HRQL                                                    |
| ---------------------------------------------- | ------------------------------------------------------- |
| `DATEOFCHANGE(Level name, LAST)`               | `history(self.level) \| last \| .date`                  |
| `DATEOFCHANGE(Title, FIRST, ...)`              | `history(self.title) \| where(...) \| first \| .date`   |
| `TODAY() - DATEOFCHANGE(Title, LAST)`          | `today() - (history(self.title) \| last \| .date)`      |
| `VALUEASOF(Employment status, DATE(2024,1,1))` | `value_as_of(self.employment_status, date(2024, 1, 1))` |
| `PRIORVALUE(Salary)`                           | `prior_value(self.salary)`                              |

---

## 13. Design Decisions Log

### 13.1 Why pipes over nested functions?

Nested function calls grow inward and must be read inside-out:

```
AVG(SELECT(All Rows, Salary, Department = Department))
```

Pipes grow rightward and are read left-to-right:

```jq
colleagues(self, .department) | .salary | avg
```

Left-to-right matches natural language: _"take my colleagues in the same department, get their salaries, average them."_

### 13.2 Why `self` and `.` instead of implicit context?

Implicit context creates ambiguity in pipes. When `peers()` follows a pipe, does it operate on the pipe's current value or the row? Explicit `self` (row context) and `.` (pipe context) eliminate this entirely.

### 13.3 Why explicit first argument on functions?

`peers(self)` is always unambiguous. `peers(self.manager)` clearly means "my manager's peers." With an implicit receiver, `self.manager | peers()` is ambiguous — whose peers?

### 13.4 Why `depth=0` means unlimited?

Follows the convention of `timeout=0` (no timeout) and `LIMIT 0` in many systems — zero means "no constraint." The most common use case (full traversal) gets the shortest syntax: `reports(self)` and `chain(self)`.

### 13.5 Why no `chain_find` function?

`chain_find(self, .title, CONTAINS, "Director")` is Excel-style argument encoding dressed up as a function. It embeds `where` and `first` into positional arguments. The pipe equivalent is strictly superior:

```jq
chain(self) | where(.title | contains("Director")) | first
```

This composes freely (drop `first` to get ALL directors, pipe into `.salary`, etc.), while `chain_find` would need separate variants for each use case.

### 13.6 Why stored fields use dot and computed use functions?

This makes the language predictable. Autocomplete after `self.` shows the database schema. Functions show computed relationships. Users never wonder "is `.directs` a real field?" — it's a function, so it's clearly computed.

### 13.7 Why no `find_employee` function for cross-org search?

Searching for employees by department and title (e.g., "find the HR Manager") is a common need, especially for approval routing. A dedicated function like `find_employee(.department, "HR", .title, CONTAINS, "Manager")` would seem convenient but repeats the `chain_find` mistake — it embeds `where` and `first` into positional arguments.

The pipe version `employees | where(.department == "HR" and .title | contains("Manager")) | first` is marginally more verbose but infinitely more flexible. Users can drop `first` to get all matches, add `sort_by` to rank them, pipe into `count`, or chain further filters — all without learning a new function. If the pattern feels verbose, that's a tooling problem (autocomplete templates, saved snippets) not a language problem.

---

## 14. Open Questions

1. **Error handling** — What happens when `chain(self) | where(.title | contains("VP")) | first` matches nothing? Return `null`? Empty? Throw?

2. **Multi-field sort** — Should `sort_by` accept multiple fields? E.g., `sort_by(.department, asc, .salary, desc)`

3. **Null propagation** — Does `self.manager.manager.title` return `null` if there's no skip-level manager, or error?

4. **Sandbox** — Should `employees` (the full dataset) be available everywhere, or only in reports? Allowing it in validations could enable expensive queries.

5. **Custom fields** — How do user-defined fields on custom objects interact with dot access? E.g., `self.custom_objects.performance_review.rating`

---

## 15. References

- [jq Manual](https://stedolan.github.io/jq/manual/) — Pipeline and filter language for JSON
- [Excel LAMBDA and LET](https://support.microsoft.com/en-us/office/lambda-function) — Excel's move toward composability
- [Rippling RQL Documentation](https://www.rippling.com/blog/rql) — Existing HR formula language (motivation for this RFC)
