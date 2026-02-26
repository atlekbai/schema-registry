import { useState, useCallback, useRef } from 'react'
import DataEditor, {
  type GridColumn,
  type GridCell,
  GridCellKind,
  type Item,
} from '@glideapps/glide-data-grid'
import '@glideapps/glide-data-grid/dist/index.css'
import { fetchObjects, type ListResponse, type ObjectMeta } from './api'
import ObjectsPage from './ObjectsPage'
import ObjectDetail from './ObjectDetail'
import './App.css'

type Page = { kind: 'explorer' } | { kind: 'objects' } | { kind: 'object-detail'; objectId: string }

function App() {
  const [page, setPage] = useState<Page>({ kind: 'explorer' })

  return (
    <div className="app">
      <header className="header">
        <h1>Schema Registry</h1>
        <nav className="tabs">
          <button
            className={page.kind === 'explorer' ? 'tab active' : 'tab'}
            onClick={() => setPage({ kind: 'explorer' })}
          >
            Data Explorer
          </button>
          <button
            className={page.kind === 'objects' || page.kind === 'object-detail' ? 'tab active' : 'tab'}
            onClick={() => setPage({ kind: 'objects' })}
          >
            Objects
          </button>
        </nav>
      </header>

      {page.kind === 'explorer' && <DataExplorer />}
      {page.kind === 'objects' && (
        <ObjectsPage
          onSelectObject={(obj: ObjectMeta) => setPage({ kind: 'object-detail', objectId: obj.id })}
        />
      )}
      {page.kind === 'object-detail' && (
        <ObjectDetail
          objectId={page.objectId}
          onBack={() => setPage({ kind: 'objects' })}
        />
      )}
    </div>
  )
}

function DataExplorer() {
  const [objectName, setObjectName] = useState('')
  const [inputValue, setInputValue] = useState('')
  const [rows, setRows] = useState<Record<string, unknown>[]>([])
  const [columns, setColumns] = useState<GridColumn[]>([])
  const [columnKeys, setColumnKeys] = useState<string[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [totalCount, setTotalCount] = useState<string>('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const rowsRef = useRef(rows)
  rowsRef.current = rows

  const columnKeysRef = useRef(columnKeys)
  columnKeysRef.current = columnKeys

  const applyResponse = useCallback(
    (data: ListResponse, append: boolean) => {
      const newRows = data.results ?? []
      const allRows = append ? [...rowsRef.current, ...newRows] : newRows
      setRows(allRows)
      setNextCursor(data.nextCursor ?? null)
      setTotalCount(data.totalCount ?? '')

      if (!append && newRows.length > 0) {
        const keys = Object.keys(newRows[0])
        setColumnKeys(keys)
        setColumns(
          keys.map((k) => ({
            title: k,
            id: k,
            width: Math.max(120, Math.min(300, k.length * 10 + 40)),
          })),
        )
      }
    },
    [],
  )

  const handleLoad = useCallback(async () => {
    const name = inputValue.trim()
    if (!name) return
    setObjectName(name)
    setLoading(true)
    setError(null)
    try {
      const data = await fetchObjects(name)
      applyResponse(data, false)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setRows([])
      setColumns([])
      setColumnKeys([])
    } finally {
      setLoading(false)
    }
  }, [inputValue, applyResponse])

  const handleLoadMore = useCallback(async () => {
    if (!nextCursor || !objectName) return
    setLoading(true)
    setError(null)
    try {
      const data = await fetchObjects(objectName, nextCursor)
      applyResponse(data, true)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [nextCursor, objectName, applyResponse])

  const getCellContent = useCallback((cell: Item): GridCell => {
    const [col, row] = cell
    const rowData = rowsRef.current[row]
    const key = columnKeysRef.current[col]
    if (!rowData || !key) {
      return { kind: GridCellKind.Text, data: '', displayData: '', allowOverlay: false }
    }
    const val = rowData[key]
    const display =
      val === null || val === undefined
        ? ''
        : typeof val === 'object'
          ? JSON.stringify(val)
          : String(val)
    return {
      kind: GridCellKind.Text,
      data: display,
      displayData: display,
      allowOverlay: true,
    }
  }, [])

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Enter') {
        handleLoad()
      }
    },
    [handleLoad],
  )

  return (
    <>
      <div className="controls">
        <input
          type="text"
          placeholder="Object name (e.g. employees)"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyDown={handleKeyDown}
          className="object-input"
        />
        <button onClick={handleLoad} disabled={loading || !inputValue.trim()}>
          {loading ? 'Loading...' : 'Load'}
        </button>
      </div>

      {error && <div className="error">{error}</div>}

      {columns.length > 0 && (
        <>
          <div className="info-bar">
            <span>
              Showing <strong>{rows.length}</strong>
              {totalCount ? (
                <>
                  {' '}
                  of <strong>{totalCount}</strong>
                </>
              ) : null}{' '}
              records
              {objectName ? (
                <>
                  {' '}
                  from <code>{objectName}</code>
                </>
              ) : null}
            </span>
          </div>
          <div className="grid-wrapper">
            <DataEditor
              getCellContent={getCellContent}
              columns={columns}
              rows={rows.length}
              width="100%"
              height="100%"
              smoothScrollX
              smoothScrollY
              getCellsForSelection={true}
            />
          </div>
          {nextCursor && (
            <div className="pagination">
              <button onClick={handleLoadMore} disabled={loading}>
                {loading ? 'Loading...' : 'Load More'}
              </button>
            </div>
          )}
        </>
      )}

      {!loading && !error && columns.length === 0 && (
        <div className="empty-state">
          Enter an object name above and click <strong>Load</strong> to browse records.
        </div>
      )}
    </>
  )
}

export default App
