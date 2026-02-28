import { useState, useCallback, useEffect, useRef, useMemo, memo } from 'react'
import { orgQuery, type OrgQueryResponse } from './api'

const EXPAND = 'individual,department,manager'
const SELECT = 'employee_number,employment_type,individual,department,manager,start_date'

// ── types ────────────────────────────────────────────────────────

interface Employee {
  id: string
  employee_number: string
  employment_type: string
  individual: { first_name: string; last_name: string; email?: string } | null
  department: { id: string; title: string } | null
  manager: { id: string } | null
  start_date: string | null
}

interface TreeEmployee extends Employee {
  children: TreeEmployee[]
}

interface Suggestion {
  label: string
  insert: string
  detail?: string
  kind: 'function' | 'field' | 'keyword' | 'employee' | 'snippet'
}

// ── HRQL suggestion data ─────────────────────────────────────────

const FN_SUGGESTIONS: Suggestion[] = [
  { label: 'chain', insert: 'chain(', detail: 'employee, [depth]', kind: 'function' },
  { label: 'reports', insert: 'reports(', detail: 'employee, [depth]', kind: 'function' },
  { label: 'peers', insert: 'peers(', detail: 'employee', kind: 'function' },
  { label: 'colleagues', insert: 'colleagues(', detail: 'employee, .field', kind: 'function' },
  { label: 'reports_to', insert: 'reports_to(', detail: 'employee, person', kind: 'function' },
  { label: 'employees', insert: 'employees', detail: 'all employees', kind: 'function' },
]

const PIPE_SUGGESTIONS: Suggestion[] = [
  { label: 'where', insert: 'where(', detail: 'condition', kind: 'function' },
  { label: 'sort_by', insert: 'sort_by(', detail: '.field, asc|desc', kind: 'function' },
  { label: 'count', insert: 'count', detail: 'aggregate', kind: 'keyword' },
  { label: 'sum', insert: 'sum', detail: 'aggregate', kind: 'keyword' },
  { label: 'avg', insert: 'avg', detail: 'aggregate', kind: 'keyword' },
  { label: 'min', insert: 'min', detail: 'aggregate', kind: 'keyword' },
  { label: 'max', insert: 'max', detail: 'aggregate', kind: 'keyword' },
  { label: 'first', insert: 'first', detail: 'pick', kind: 'keyword' },
  { label: 'last', insert: 'last', detail: 'pick', kind: 'keyword' },
  { label: 'unique', insert: 'unique', detail: 'deduplicate', kind: 'keyword' },
  { label: 'length', insert: 'length', detail: 'count/length', kind: 'keyword' },
  { label: 'contains', insert: 'contains(', detail: 'string/list check', kind: 'function' },
  { label: 'starts_with', insert: 'starts_with(', detail: 'string prefix', kind: 'function' },
  { label: 'upper', insert: 'upper', detail: 'uppercase', kind: 'keyword' },
  { label: 'lower', insert: 'lower', detail: 'lowercase', kind: 'keyword' },
]

const FIELD_SUGGESTIONS: Suggestion[] = [
  { label: '.employee_number', insert: '.employee_number', kind: 'field' },
  { label: '.employment_type', insert: '.employment_type', kind: 'field' },
  { label: '.start_date', insert: '.start_date', kind: 'field' },
  { label: '.end_date', insert: '.end_date', kind: 'field' },
  { label: '.department', insert: '.department', kind: 'field' },
  { label: '.department.title', insert: '.department.title', kind: 'field' },
  { label: '.manager', insert: '.manager', kind: 'field' },
  { label: '.individual', insert: '.individual', kind: 'field' },
  { label: '.individual.first_name', insert: '.individual.first_name', kind: 'field' },
  { label: '.individual.last_name', insert: '.individual.last_name', kind: 'field' },
  { label: '.individual.email', insert: '.individual.email', kind: 'field' },
  { label: '.organization', insert: '.organization', kind: 'field' },
]

const KEYWORD_SUGGESTIONS: Suggestion[] = [
  { label: 'and', insert: 'and ', kind: 'keyword' },
  { label: 'or', insert: 'or ', kind: 'keyword' },
  { label: 'asc', insert: 'asc', kind: 'keyword' },
  { label: 'desc', insert: 'desc', kind: 'keyword' },
  { label: 'true', insert: 'true', kind: 'keyword' },
  { label: 'false', insert: 'false', kind: 'keyword' },
  { label: 'self', insert: 'self', detail: 'current employee', kind: 'keyword' },
]

const EXAMPLES = [
  { label: 'Direct reports', query: 'reports(self, 1)' },
  { label: 'All reports', query: 'reports(self)' },
  { label: 'Peers', query: 'peers(self)' },
  { label: 'Chain to CEO', query: 'chain(self)' },
  { label: 'Same dept', query: 'colleagues(self, .department)' },
  { label: 'Contractors', query: 'employees | where(.employment_type == "CONTRACTOR")' },
  { label: 'Team count', query: 'reports(self) | count' },
  { label: 'Avg start date dept', query: 'colleagues(self, .department) | .start_date | count' },
]

// ── helpers ──────────────────────────────────────────────────────

function empName(e: Employee): string {
  return e.individual ? `${e.individual.first_name} ${e.individual.last_name}` : e.employee_number
}

function buildTree(employees: Employee[]): TreeEmployee[] {
  const map = new Map<string, TreeEmployee>()
  for (const e of employees) {
    map.set(e.id, { ...e, children: [] })
  }
  const roots: TreeEmployee[] = []
  for (const node of map.values()) {
    if (node.manager?.id) {
      const parent = map.get(node.manager.id)
      if (parent) {
        parent.children.push(node)
        continue
      }
    }
    roots.push(node)
  }
  return roots
}

// ── context detection for autocomplete ──────────────────────────

/** What to append after selecting a suggestion for a given function arg position. */
function argSuffix(fn: string, argIdx: number): string {
  switch (fn) {
    case 'peers': return ')'                                    // 1 arg
    case 'chain': case 'reports': return argIdx === 0 ? ', ' : ')' // optional depth
    case 'colleagues': return argIdx === 0 ? ', ' : ')'         // emp, .field
    case 'reports_to': return argIdx === 0 ? ', ' : ')'          // emp, person
    case 'sort_by': return argIdx === 0 ? ', ' : ')'             // .field, dir
    default: return ''
  }
}

/** Clone suggestions with a suffix appended to each insert text. */
function withSuffix(items: Suggestion[], suffix: string): Suggestion[] {
  if (!suffix) return items
  return items.map(s => ({ ...s, insert: s.insert + suffix }))
}

/** Pipe operator + common pipe steps (for after `)`, keyword, etc.). */
function pipeSuggestions(prefix: string): Suggestion[] {
  const all: Suggestion[] = [
    ...PIPE_SUGGESTIONS.map(s => ({ ...s, insert: ' | ' + s.insert, label: '| ' + s.label })),
    ...FIELD_SUGGESTIONS.map(s => ({ ...s, insert: ' | ' + s.insert, label: '| ' + s.label })),
  ]
  if (!prefix) return all.slice(0, 15)
  const q = prefix.toLowerCase()
  return all.filter(s => s.label.toLowerCase().includes(q))
}

function getSuggestions(text: string, cursor: number, employees: Employee[]): Suggestion[] {
  const before = text.slice(0, cursor)
  const after = text.slice(cursor)
  const nextChar = after.trimStart()[0] ?? ''
  const hasClosingChar = nextChar === ',' || nextChar === ')'

  // After closing paren or completed keyword → suggest pipe steps
  if (before.match(/\)\s*$/) || before.match(/\b(employees|count|sum|avg|min|max|first|last|unique|length|upper|lower)\s+$/)) {
    const partial = before.match(/\)\s+(\w*)$/) ?? before.match(/\s+(\w*)$/)
    return pipeSuggestions(partial?.[1] ?? '')
  }

  // After pipe → pipe step suggestions
  const afterPipe = before.match(/\|\s*(\w*)$/)
  if (afterPipe) {
    const prefix = afterPipe[1].toLowerCase()
    const results = [...PIPE_SUGGESTIONS, ...FIELD_SUGGESTIONS]
    if (!prefix) return results.slice(0, 15)
    return results.filter(s => s.label.toLowerCase().startsWith(prefix) || s.label.toLowerCase().startsWith('.' + prefix))
  }

  // After dot → field suggestions
  const afterDot = before.match(/\.(\w*)$/)
  if (afterDot) {
    const prefix = '.' + afterDot[1].toLowerCase()
    return FIELD_SUGGESTIONS.filter(s => s.label.toLowerCase().startsWith(prefix))
  }

  // Inside function parens — detect which function and arg index
  const parenOpen = findMatchingParen(before)
  if (parenOpen >= 0) {
    const fnMatch = before.slice(0, parenOpen).match(/(\w+)$/)
    const fn = fnMatch?.[1]?.toLowerCase() ?? ''
    const argsStr = before.slice(parenOpen + 1)
    const argIdx = argsStr.split(',').length - 1
    const currentArg = argsStr.split(',').pop()?.trim() ?? ''
    const suffix = hasClosingChar ? '' : argSuffix(fn, argIdx)

    // where() → field + keyword suggestions (no auto-suffix, conditions are complex)
    if (fn === 'where') {
      if (currentArg.startsWith('.')) {
        return FIELD_SUGGESTIONS.filter(s => s.label.toLowerCase().startsWith(currentArg.toLowerCase()))
      }
      const all = [...FIELD_SUGGESTIONS, ...KEYWORD_SUGGESTIONS]
      if (!currentArg) return all.slice(0, 12)
      return all.filter(s => s.label.toLowerCase().startsWith(currentArg.toLowerCase()))
    }

    // sort_by() → fields (arg 0), asc/desc (arg 1)
    if (fn === 'sort_by') {
      if (argIdx === 0) {
        const fields = !currentArg
          ? FIELD_SUGGESTIONS
          : FIELD_SUGGESTIONS.filter(s => s.label.toLowerCase().startsWith(currentArg.toLowerCase()))
        return withSuffix(fields, suffix)
      }
      const dirs = KEYWORD_SUGGESTIONS.filter(s => s.label === 'asc' || s.label === 'desc')
      return withSuffix(dirs, hasClosingChar ? '' : ')')
    }

    // colleagues() — arg 0: employee, arg 1: field
    if (fn === 'colleagues') {
      if (argIdx === 0) {
        return withSuffix(employeeSuggestions(employees, currentArg), suffix)
      }
      const fields = !currentArg
        ? FIELD_SUGGESTIONS
        : FIELD_SUGGESTIONS.filter(s => s.label.toLowerCase().startsWith(currentArg.toLowerCase()))
      return withSuffix(fields, hasClosingChar ? '' : ')')
    }

    // org functions first arg → self + employees
    if (['chain', 'reports', 'peers', 'reports_to'].includes(fn)) {
      if (argIdx === 0) {
        return withSuffix(employeeSuggestions(employees, currentArg), suffix)
      }
      if (fn === 'reports_to' && argIdx === 1) {
        return withSuffix(employeeSuggestions(employees, currentArg), hasClosingChar ? '' : ')')
      }
    }

    return []
  }

  // Top-level — function + keyword suggestions
  const prefix = before.trimStart().toLowerCase()
  const all = [...FN_SUGGESTIONS, ...KEYWORD_SUGGESTIONS.filter(s => s.label === 'self')]
  if (!prefix) return all
  return all.filter(s => s.label.toLowerCase().startsWith(prefix))
}

function employeeSuggestions(employees: Employee[], prefix: string): Suggestion[] {
  const selfSuggestion: Suggestion = { label: 'self', insert: 'self', detail: 'current employee', kind: 'keyword' }
  const empSugs: Suggestion[] = employees.map(e => ({
    label: empName(e),
    insert: empName(e),
    detail: e.employee_number,
    kind: 'employee' as const,
  }))
  const all = [selfSuggestion, ...empSugs]
  if (!prefix) return all.slice(0, 12)
  const q = prefix.toLowerCase()
  return all.filter(s => s.label.toLowerCase().includes(q) || (s.detail?.toLowerCase().includes(q) ?? false)).slice(0, 12)
}

function findMatchingParen(text: string): number {
  let depth = 0
  for (let i = text.length - 1; i >= 0; i--) {
    if (text[i] === ')') depth++
    if (text[i] === '(') {
      if (depth === 0) return i
      depth--
    }
  }
  return -1
}

/** Replace human-readable employee names with UUIDs in the query. */
function resolveNames(input: string, employees: Employee[]): string {
  const byName = new Map<string, string>()
  for (const e of employees) {
    byName.set(empName(e).toLowerCase(), e.id)
    byName.set(e.employee_number.toLowerCase(), e.id)
  }
  // Replace names inside function arguments — match names that look like "First Last"
  let result = input
  // Sort by length descending to replace longer names first
  const names = [...byName.entries()].sort((a, b) => b[0].length - a[0].length)
  for (const [name, id] of names) {
    // Only replace inside parens, not field names or keywords
    const regex = new RegExp(`(?<=[(,]\\s*)${escapeRegex(name)}(?=\\s*[,)])`, 'gi')
    result = result.replace(regex, `"${id}"`)

  }
  return result
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

// ── TreeNode ─────────────────────────────────────────────────────

const TreeNode = memo(function TreeNode({
  node,
  depth,
  selfId,
  onSelectSelf,
}: {
  node: TreeEmployee
  depth: number
  selfId: string | null
  onSelectSelf: (e: Employee) => void
}) {
  const [expanded, setExpanded] = useState(depth < 2)
  const hasChildren = node.children.length > 0
  const isSelected = selfId === node.id

  return (
    <div className="tree-node">
      <div
        className={`tree-row${isSelected ? ' tree-selected' : ''}`}
        onClick={() => onSelectSelf(node)}
      >
        <span
          className="tree-toggle"
          onClick={(e) => {
            e.stopPropagation()
            if (hasChildren) setExpanded(!expanded)
          }}
        >
          {hasChildren ? (expanded ? '\u25BE' : '\u25B8') : '\u00A0'}
        </span>
        <span className="tree-name">{empName(node)}</span>
        {node.department && (
          <span className="tree-dept">{node.department.title}</span>
        )}
      </div>
      {expanded && hasChildren && (
        <div className="tree-children">
          {node.children.map(child => (
            <TreeNode
              key={child.id}
              node={child}
              depth={depth + 1}
              selfId={selfId}
              onSelectSelf={onSelectSelf}
            />
          ))}
        </div>
      )}
    </div>
  )
})

// ── OrgPage ─────────────────────────────────────────────────────

export default function OrgPage() {
  const [input, setInput] = useState('')
  const [results, setResults] = useState<Record<string, unknown>[]>([])
  const [totalCount, setTotalCount] = useState<number | null>(null)
  const [boolResult, setBoolResult] = useState<boolean | null>(null)
  const [scalarResult, setScalarResult] = useState<number | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [employees, setEmployees] = useState<Employee[]>([])
  const [selfEmployee, setSelfEmployee] = useState<Employee | null>(null)

  const tree = useMemo(() => buildTree(employees), [employees])
  const selfId = selfEmployee?.id ?? null

  const [showSuggestions, setShowSuggestions] = useState(false)
  const [selectedIdx, setSelectedIdx] = useState(0)
  const [suggestionKey, setSuggestionKey] = useState(0)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const suggestionsRef = useRef<HTMLDivElement>(null)

  // Load employees on mount
  useEffect(() => {
    fetch(`/api/employees?limit=200&select=${SELECT}&expand=${EXPAND}`)
      .then(r => r.json())
      .then(data => {
        setEmployees(data.results ?? [])
      })
      .catch(() => {})
  }, [])

  // Compute suggestions — read cursor from DOM every render to avoid stale state
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const suggestions = useMemo<Suggestion[]>(() => {
    const cursor = textareaRef.current?.selectionStart ?? input.length
    return getSuggestions(input, cursor, employees)
  }, [input, employees, suggestionKey])

  const handleSelectSelf = useCallback((e: Employee) => {
    setSelfEmployee(e)
  }, [])

  const clearSelf = useCallback(() => {
    setSelfEmployee(null)
  }, [])

  // Apply a suggestion
  const applySuggestion = useCallback((s: Suggestion) => {
    const el = textareaRef.current
    if (!el) return
    const cursor = el.selectionStart ?? input.length
    const before = input.slice(0, cursor)
    const after = input.slice(cursor)

    // Find the start of the current token
    let tokenStart = cursor
    // Walk back to find where the current token starts
    for (let i = cursor - 1; i >= 0; i--) {
      const ch = before[i]
      if (' \t\n|,()'.includes(ch)) {
        tokenStart = i + 1
        break
      }
      if (i === 0) tokenStart = 0
    }

    // For field suggestions starting with '.', include the dot
    if (s.kind === 'field' && tokenStart > 0 && before[tokenStart - 1] === '.') {
      tokenStart--
    }

    const newBefore = input.slice(0, tokenStart)
    const newInput = newBefore + s.insert + after
    const newCursor = newBefore.length + s.insert.length
    setInput(newInput)
    setSelectedIdx(0)
    setShowSuggestions(true)
    setTimeout(() => {
      el.setSelectionRange(newCursor, newCursor)
      el.focus()
      setSuggestionKey(k => k + 1)
    })
  }, [input])

  const handleRun = useCallback(async () => {
    const q = input.trim()
    if (!q) return
    setShowSuggestions(false)
    setLoading(true)
    setError(null)
    setResults([])
    setBoolResult(null)
    setScalarResult(null)
    try {
      const resolved = resolveNames(q, employees)
      const data: OrgQueryResponse = await orgQuery(resolved, {
        select: SELECT,
        expand: EXPAND,
        limit: 50,
        selfId: selfId ?? undefined,
      })
      if (data.reportsTo !== undefined) {
        setBoolResult(data.reportsTo ?? null)
      }
      if (data.scalar !== undefined && data.scalar !== null) {
        setScalarResult(data.scalar)
      }
      if (data.results && data.results.length > 0) {
        setResults(data.results)
        setTotalCount(data.totalCount ?? null)
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [input, employees, selfId])

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (showSuggestions && suggestions.length > 0) {
      if (e.key === 'ArrowDown') {
        e.preventDefault()
        setSelectedIdx(i => (i + 1) % suggestions.length)
        return
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault()
        setSelectedIdx(i => (i - 1 + suggestions.length) % suggestions.length)
        return
      }
      if (e.key === 'Tab') {
        e.preventDefault()
        applySuggestion(suggestions[selectedIdx])
        return
      }
      if (e.key === 'Escape') {
        e.preventDefault()
        setShowSuggestions(false)
        return
      }
    }
    // Ctrl+Enter or Cmd+Enter to run
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault()
      handleRun()
    }
  }, [showSuggestions, suggestions, selectedIdx, applySuggestion, handleRun])

  const handleChange = useCallback((e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInput(e.target.value)
    setShowSuggestions(true)
    setSelectedIdx(0)
    setSuggestionKey(k => k + 1)
  }, [])

  const handleExample = useCallback((query: string) => {
    setInput(query)
    setShowSuggestions(false)
    textareaRef.current?.focus()
  }, [])

  // Scroll selected suggestion into view
  useEffect(() => {
    if (!suggestionsRef.current) return
    const items = suggestionsRef.current.children
    if (items[selectedIdx]) {
      (items[selectedIdx] as HTMLElement).scrollIntoView({ block: 'nearest' })
    }
  }, [selectedIdx])

  // Close suggestions on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (
        suggestionsRef.current &&
        !suggestionsRef.current.contains(e.target as Node) &&
        textareaRef.current !== e.target
      ) {
        setShowSuggestions(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const hasResult = results.length > 0 || boolResult !== null || scalarResult !== null

  return (
    <div className="org-layout">
      {/* ── Left: Org Tree ── */}
      <div className="org-tree-panel">
        <div className="org-tree-header">
          <h3>Organization</h3>
          <span className="org-tree-count">{employees.length}</span>
        </div>
        <div className="org-tree-scroll">
          {tree.map(root => (
            <TreeNode
              key={root.id}
              node={root}
              depth={0}
              selfId={selfId}
              onSelectSelf={handleSelectSelf}
            />
          ))}
          {tree.length === 0 && (
            <div className="org-tree-empty">Loading...</div>
          )}
        </div>
      </div>

      {/* ── Right: Editor + Results ── */}
      <div className="org-main">
        {/* Self picker */}
        {selfEmployee && (
          <div className="org-self-bar">
            <span className="org-self-label">self =</span>
            <span className="org-self-name">{empName(selfEmployee)}</span>
            <span className="org-self-emp">{selfEmployee.employee_number}</span>
            <button className="org-self-clear" onClick={clearSelf}>&times;</button>
          </div>
        )}
        {!selfEmployee && (
          <div className="org-self-bar org-self-hint">
            Click an employee in the tree to set <code>self</code>
          </div>
        )}

        {/* Editor */}
        <div className="org-editor-wrap">
          <div className="org-editor-box" style={{ position: 'relative' }}>
            <textarea
              ref={textareaRef}
              className="org-textarea"
              placeholder='reports(self, 1) | where(.employment_type == "FULL_TIME")'
              value={input}
              onChange={handleChange}
              onKeyDown={handleKeyDown}
              onFocus={() => setShowSuggestions(true)}
              rows={3}
              spellCheck={false}
              autoComplete="off"
            />
            <button
              className="org-run-btn"
              onClick={handleRun}
              disabled={loading || !input.trim()}
            >
              {loading ? 'Running...' : 'Run'}
            </button>

            {showSuggestions && suggestions.length > 0 && (
              <div className="ac-dropdown" ref={suggestionsRef}>
                {suggestions.map((s, i) => (
                  <div
                    key={s.label + i}
                    className={i === selectedIdx ? 'ac-item selected' : 'ac-item'}
                    onMouseDown={(ev) => { ev.preventDefault(); applySuggestion(s) }}
                    onMouseEnter={() => setSelectedIdx(i)}
                  >
                    <span className={`ac-icon ac-${s.kind}`}>
                      {s.kind === 'function' ? 'f' : s.kind === 'field' ? '.' : s.kind === 'employee' ? 'E' : 'k'}
                    </span>
                    <span className="ac-label">{s.label}</span>
                    {s.detail && <span className="ac-detail">{s.detail}</span>}
                  </div>
                ))}
              </div>
            )}
          </div>
          <div className="org-shortcuts">
            <span className="org-shortcut-hint">Ctrl+Enter to run</span>
          </div>
        </div>

        {/* Example snippets */}
        <div className="org-examples">
          {EXAMPLES.map(ex => (
            <button
              key={ex.label}
              className="org-example-btn"
              onClick={() => handleExample(ex.query)}
            >
              {ex.label}
            </button>
          ))}
        </div>

        {/* Error */}
        {error && <div className="error">{error}</div>}

        {/* Results */}
        {scalarResult !== null && (
          <div className="org-scalar-result">
            <div className="org-scalar-value">{scalarResult}</div>
            <div className="org-scalar-label">Result</div>
          </div>
        )}

        {boolResult !== null && (
          <div className="org-bool-result">
            <strong>Result:</strong>{' '}
            <span className={boolResult ? 'org-true' : 'org-false'}>
              {boolResult ? 'Yes' : 'No'}
            </span>
          </div>
        )}

        {results.length > 0 && (
          <>
            <div className="info-bar">
              Showing <strong>{results.length}</strong>
              {totalCount !== null ? <> of <strong>{totalCount}</strong></> : null}{' '}
              results
            </div>
            <ResultsTable rows={results} />
          </>
        )}

        {!loading && !error && !hasResult && (
          <div className="empty-state">
            Write an HRQL query above and press <strong>Ctrl+Enter</strong> to run.
          </div>
        )}
      </div>
    </div>
  )
}

// ── ResultsTable ────────────────────────────────────────────────

function ResultsTable({ rows }: { rows: Record<string, unknown>[] }) {
  if (rows.length === 0) return null
  const keys = Object.keys(rows[0])

  return (
    <div style={{ overflowX: 'auto', flex: 1 }}>
      <table className="meta-table">
        <thead>
          <tr>
            {keys.map(k => <th key={k}>{k}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              {keys.map(k => <td key={k}>{formatCell(row[k])}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function formatCell(val: unknown): string {
  if (val === null || val === undefined) return ''
  if (typeof val === 'object') return JSON.stringify(val)
  return String(val)
}
