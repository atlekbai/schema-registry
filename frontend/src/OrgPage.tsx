import { useState, useCallback, useEffect, useRef, useMemo } from 'react'
import { orgQuery, type OrgQueryResponse } from './api'

const EXPAND = 'individual,department,manager'
const SELECT = 'employee_number,employment_type,individual,department,manager,start_date'

interface Employee {
  id: string
  employee_number: string
  individual: { first_name: string; last_name: string } | null
}

// ── suggestion types ─────────────────────────────────────────────

interface Suggestion {
  label: string      // display text
  insert: string     // text to insert
  detail?: string    // secondary text (right-aligned)
  kind: 'function' | 'employee' | 'keyword'
}

const FN_SUGGESTIONS: Suggestion[] = [
  { label: 'CHAIN', insert: 'CHAIN(', detail: 'id, steps', kind: 'function' },
  { label: 'PEERS', insert: 'PEERS(', detail: 'id, dimension', kind: 'function' },
  { label: 'REPORTS', insert: 'REPORTS(', detail: 'id [, true]', kind: 'function' },
  { label: 'REPORTSTO', insert: 'REPORTSTO(', detail: 'id, id', kind: 'function' },
]

const DIMENSION_SUGGESTIONS: Suggestion[] = [
  { label: 'manager', insert: 'manager', kind: 'keyword' },
  { label: 'department', insert: 'department', kind: 'keyword' },
]

const BOOL_SUGGESTIONS: Suggestion[] = [
  { label: 'true', insert: 'true', detail: 'direct only', kind: 'keyword' },
  { label: 'false', insert: 'false', detail: 'all reports', kind: 'keyword' },
]

// ── context detection ────────────────────────────────────────────

type ArgContext =
  | { pos: 'function'; prefix: string }
  | { pos: 'arg'; fn: string; argIdx: number; prefix: string }
  | null

function detectContext(text: string, cursor: number): ArgContext {
  const before = text.slice(0, cursor)

  // inside parens?
  const parenOpen = before.lastIndexOf('(')
  if (parenOpen === -1) {
    // no paren → typing function name
    return { pos: 'function', prefix: before.trim() }
  }

  const fn = before.slice(0, parenOpen).trim().toUpperCase()
  const argsStr = before.slice(parenOpen + 1)
  const argIdx = argsStr.split(',').length - 1
  const currentArg = argsStr.split(',').pop()?.trim() ?? ''

  return { pos: 'arg', fn, argIdx, prefix: currentArg }
}

// ── name resolution ──────────────────────────────────────────────

function empLabel(e: Employee): string {
  const ind = e.individual
  return ind ? `${ind.first_name} ${ind.last_name}` : e.employee_number
}

function empDisplay(e: Employee): string {
  const ind = e.individual
  if (!ind) return e.employee_number
  return `${ind.first_name} ${ind.last_name} | ${e.employee_number}`
}

/** Replace human-readable employee names with UUIDs in the DSL string. */
function resolveNames(input: string, employees: Employee[]): string {
  // build lookup: lowercase name → id, lowercase emp number → id
  const byName = new Map<string, string>()
  for (const e of employees) {
    byName.set(empLabel(e).toLowerCase(), e.id)
    byName.set(e.employee_number.toLowerCase(), e.id)
  }

  // match FUNC( args )
  return input.replace(/^(\w+)\((.+)\)$/i, (_match, fn, argsStr) => {
    const args = argsStr.split(',').map((a: string) => {
      const trimmed = a.trim()
      const resolved = byName.get(trimmed.toLowerCase())
      return resolved ?? trimmed
    })
    return `${fn}(${args.join(', ')})`
  })
}

// ── component ────────────────────────────────────────────────────

export default function OrgPage() {
  const [input, setInput] = useState('')
  const [results, setResults] = useState<Record<string, unknown>[]>([])
  const [totalCount, setTotalCount] = useState('')
  const [boolResult, setBoolResult] = useState<boolean | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [employees, setEmployees] = useState<Employee[]>([])
  const [showSuggestions, setShowSuggestions] = useState(false)
  const [selectedIdx, setSelectedIdx] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const suggestionsRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    fetch('/api/employees?limit=200&select=employee_number&expand=individual')
      .then((r) => r.json())
      .then((data) => setEmployees(data.results ?? []))
      .catch(() => {})
  }, [])

  const empSuggestions = useMemo<Suggestion[]>(
    () =>
      employees.map((e) => ({
        label: empDisplay(e),
        insert: empLabel(e),
        kind: 'employee' as const,
      })),
    [employees],
  )

  // compute suggestions based on cursor context
  const suggestions = useMemo<Suggestion[]>(() => {
    const el = inputRef.current
    const cursor = el?.selectionStart ?? input.length
    const ctx = detectContext(input, cursor)
    if (!ctx) return []

    if (ctx.pos === 'function') {
      const q = ctx.prefix.toLowerCase()
      return FN_SUGGESTIONS.filter((s) => s.label.toLowerCase().startsWith(q))
    }

    // arg position
    if (ctx.argIdx === 0) {
      // first arg is always an employee
      const q = ctx.prefix.toLowerCase()
      if (!q) return empSuggestions.slice(0, 10)
      return empSuggestions.filter((s) => s.label.toLowerCase().includes(q)).slice(0, 10)
    }

    if (ctx.argIdx === 1) {
      if (ctx.fn === 'PEERS') {
        const q = ctx.prefix.toLowerCase()
        return DIMENSION_SUGGESTIONS.filter((s) => s.label.startsWith(q))
      }
      if (ctx.fn === 'REPORTS') {
        return BOOL_SUGGESTIONS
      }
      if (ctx.fn === 'REPORTSTO') {
        const q = ctx.prefix.toLowerCase()
        if (!q) return empSuggestions.slice(0, 10)
        return empSuggestions.filter((s) => s.label.toLowerCase().includes(q)).slice(0, 10)
      }
    }

    return []
  }, [input, empSuggestions])

  // apply a suggestion at cursor
  const applySuggestion = useCallback(
    (s: Suggestion) => {
      const el = inputRef.current
      if (!el) return
      const cursor = el.selectionStart ?? input.length
      const ctx = detectContext(input, cursor)
      if (!ctx) return

      let before: string
      let after: string

      if (ctx.pos === 'function') {
        before = ''
        after = input.slice(cursor)
        // remove any partial function name
        const trimmedAfter = after.replace(/^\w*/, '')
        setInput(s.insert + trimmedAfter)
        const newCursor = s.insert.length
        setTimeout(() => {
          el.setSelectionRange(newCursor, newCursor)
          el.focus()
        })
      } else {
        // find start of current arg
        const parenOpen = input.lastIndexOf('(', cursor)
        const argsStr = input.slice(parenOpen + 1, cursor)
        const lastComma = argsStr.lastIndexOf(',')
        const argStart = parenOpen + 1 + (lastComma === -1 ? 0 : lastComma + 1)
        // preserve spacing: if there's a space after comma, keep it
        const rawBefore = input.slice(argStart, cursor)
        const leadingSpace = rawBefore.match(/^\s*/)?.[0] ?? ''

        before = input.slice(0, argStart) + leadingSpace
        after = input.slice(cursor)
        // remove rest of current token from after
        const tokenRest = after.match(/^[^,)]*/)
        after = after.slice(tokenRest?.[0].length ?? 0)

        setInput(before + s.insert + after)
        const newCursor = before.length + s.insert.length
        setTimeout(() => {
          el.setSelectionRange(newCursor, newCursor)
          el.focus()
        })
      }

      setShowSuggestions(false)
      setSelectedIdx(0)
    },
    [input],
  )

  const handleRun = useCallback(async () => {
    const q = input.trim()
    if (!q) return
    setShowSuggestions(false)
    setLoading(true)
    setError(null)
    setResults([])
    setBoolResult(null)
    try {
      const resolved = resolveNames(q, employees)
      const data: OrgQueryResponse = await orgQuery(resolved, {
        select: SELECT,
        expand: EXPAND,
        limit: 50,
      })
      if (data.reportsTo !== undefined) {
        setBoolResult(data.reportsTo)
      }
      if (data.results && data.results.length > 0) {
        setResults(data.results)
        setTotalCount(data.totalCount != null ? String(data.totalCount) : '')
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [input, employees])

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (showSuggestions && suggestions.length > 0) {
        if (e.key === 'ArrowDown') {
          e.preventDefault()
          setSelectedIdx((i) => (i + 1) % suggestions.length)
          return
        }
        if (e.key === 'ArrowUp') {
          e.preventDefault()
          setSelectedIdx((i) => (i - 1 + suggestions.length) % suggestions.length)
          return
        }
        if (e.key === 'Tab' || (e.key === 'Enter' && suggestions.length > 0)) {
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
      if (e.key === 'Enter' && !showSuggestions) {
        handleRun()
      }
    },
    [showSuggestions, suggestions, selectedIdx, applySuggestion, handleRun],
  )

  const handleChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setInput(e.target.value)
    setShowSuggestions(true)
    setSelectedIdx(0)
  }, [])

  // scroll selected suggestion into view
  useEffect(() => {
    if (!suggestionsRef.current) return
    const items = suggestionsRef.current.children
    if (items[selectedIdx]) {
      (items[selectedIdx] as HTMLElement).scrollIntoView({ block: 'nearest' })
    }
  }, [selectedIdx])

  // close suggestions on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (
        suggestionsRef.current &&
        !suggestionsRef.current.contains(e.target as Node) &&
        inputRef.current !== e.target
      ) {
        setShowSuggestions(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  return (
    <div className="meta-page">
      <div className="meta-header">
        <h2>Org Chart</h2>
      </div>

      <div className="org-controls">
        <div className="org-form">
          <div className="form-row" style={{ position: 'relative' }}>
            <input
              ref={inputRef}
              type="text"
              placeholder="CHAIN(Sarah Chen, -1)"
              value={input}
              onChange={handleChange}
              onKeyDown={handleKeyDown}
              onFocus={() => setShowSuggestions(true)}
              style={{ flex: 1 }}
              autoComplete="off"
            />
            <button onClick={handleRun} disabled={loading || !input.trim()}>
              {loading ? 'Running...' : 'Run'}
            </button>

            {showSuggestions && suggestions.length > 0 && (
              <div className="ac-dropdown" ref={suggestionsRef}>
                {suggestions.map((s, i) => (
                  <div
                    key={s.label + i}
                    className={i === selectedIdx ? 'ac-item selected' : 'ac-item'}
                    onMouseDown={(e) => {
                      e.preventDefault()
                      applySuggestion(s)
                    }}
                    onMouseEnter={() => setSelectedIdx(i)}
                  >
                    <span className={`ac-icon ac-${s.kind}`}>
                      {s.kind === 'function' ? 'f' : s.kind === 'employee' ? 'E' : 'k'}
                    </span>
                    <span className="ac-label">{s.label}</span>
                    {s.detail && <span className="ac-detail">{s.detail}</span>}
                  </div>
                ))}
              </div>
            )}
          </div>
          <div className="meta-desc">
            <code>CHAIN(name, steps)</code> · <code>PEERS(name, manager|department)</code> · <code>REPORTS(name [, true])</code> · <code>REPORTSTO(name, name)</code>
          </div>
        </div>
      </div>

      {error && <div className="error">{error}</div>}

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
            {totalCount ? (
              <>
                {' '}of <strong>{totalCount}</strong>
              </>
            ) : null}{' '}
            results
          </div>
          <ResultsTable rows={results} />
        </>
      )}

      {!loading && !error && results.length === 0 && boolResult === null && (
        <div className="empty-state">
          Start typing a function name to see suggestions.
        </div>
      )}
    </div>
  )
}

function ResultsTable({ rows }: { rows: Record<string, unknown>[] }) {
  if (rows.length === 0) return null
  const keys = Object.keys(rows[0])

  return (
    <div style={{ overflowX: 'auto', flex: 1 }}>
      <table className="meta-table">
        <thead>
          <tr>
            {keys.map((k) => (
              <th key={k}>{k}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              {keys.map((k) => (
                <td key={k}>{formatCell(row[k])}</td>
              ))}
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
