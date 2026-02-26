import { useState, useCallback } from 'react'
import { orgQuery, type OrgQueryResponse } from './api'

const EXPAND = 'individual,department,manager'
const SELECT = 'employee_number,employment_type,individual,department,manager,start_date'

export default function OrgPage() {
  const [input, setInput] = useState('')
  const [results, setResults] = useState<Record<string, unknown>[]>([])
  const [totalCount, setTotalCount] = useState('')
  const [boolResult, setBoolResult] = useState<boolean | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleRun = useCallback(async () => {
    const q = input.trim()
    if (!q) return
    setLoading(true)
    setError(null)
    setResults([])
    setBoolResult(null)
    try {
      const data: OrgQueryResponse = await orgQuery(q, {
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
  }, [input])

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Enter') handleRun()
    },
    [handleRun],
  )

  return (
    <div className="meta-page">
      <div className="meta-header">
        <h2>Org Chart</h2>
      </div>

      <div className="org-controls">
        <div className="org-form">
          <div className="form-row">
            <input
              type="text"
              placeholder="CHAIN(id, 1), PEERS(id, manager), REPORTS(id), REPORTSTO(a, b)"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              style={{ flex: 1 }}
            />
            <button onClick={handleRun} disabled={loading || !input.trim()}>
              {loading ? 'Running...' : 'Run'}
            </button>
          </div>
          <div className="meta-desc">
            <code>CHAIN(id, steps)</code> · <code>PEERS(id, manager|department)</code> · <code>REPORTS(id [, true])</code> · <code>REPORTSTO(id, id)</code>
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
          Type a DSL expression above and press <strong>Enter</strong>.
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
