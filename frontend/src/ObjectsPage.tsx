import { useState, useEffect, useCallback } from 'react'
import {
  listMetaObjects,
  createMetaObject,
  deleteMetaObject,
  type ObjectMeta,
} from './api'

interface Props {
  onSelectObject: (obj: ObjectMeta) => void
}

export default function ObjectsPage({ onSelectObject }: Props) {
  const [objects, setObjects] = useState<ObjectMeta[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [showForm, setShowForm] = useState(false)
  const [formData, setFormData] = useState({
    apiName: '',
    title: '',
    pluralTitle: '',
    description: '',
    supportsCustomFields: true,
  })
  const [saving, setSaving] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      setObjects(await listMetaObjects())
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()
  }, [load])

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    setSaving(true)
    setError(null)
    try {
      await createMetaObject(formData)
      setShowForm(false)
      setFormData({ apiName: '', title: '', pluralTitle: '', description: '', supportsCustomFields: true })
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setSaving(false)
    }
  }

  const handleDelete = async (id: string) => {
    if (!confirm('Delete this object and all its fields?')) return
    setError(null)
    try {
      await deleteMetaObject(id)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div className="meta-page">
      <div className="meta-header">
        <h2>Objects</h2>
        <button onClick={() => setShowForm(!showForm)}>
          {showForm ? 'Cancel' : 'New Object'}
        </button>
      </div>

      {error && <div className="error">{error}</div>}

      {showForm && (
        <form className="meta-form" onSubmit={handleCreate}>
          <div className="form-row">
            <label>API Name</label>
            <input
              required
              placeholder="my_object"
              value={formData.apiName}
              onChange={(e) => setFormData({ ...formData, apiName: e.target.value })}
            />
          </div>
          <div className="form-row">
            <label>Title</label>
            <input
              required
              placeholder="My Object"
              value={formData.title}
              onChange={(e) => setFormData({ ...formData, title: e.target.value })}
            />
          </div>
          <div className="form-row">
            <label>Plural Title</label>
            <input
              required
              placeholder="My Objects"
              value={formData.pluralTitle}
              onChange={(e) => setFormData({ ...formData, pluralTitle: e.target.value })}
            />
          </div>
          <div className="form-row">
            <label>Description</label>
            <input
              placeholder="Optional description"
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
            />
          </div>
          <div className="form-row">
            <label>
              <input
                type="checkbox"
                checked={formData.supportsCustomFields}
                onChange={(e) => setFormData({ ...formData, supportsCustomFields: e.target.checked })}
              />
              {' '}Supports custom fields
            </label>
          </div>
          <button type="submit" disabled={saving}>
            {saving ? 'Creating...' : 'Create'}
          </button>
        </form>
      )}

      {loading ? (
        <div className="empty-state">Loading...</div>
      ) : objects.length === 0 ? (
        <div className="empty-state">No objects defined yet.</div>
      ) : (
        <table className="meta-table">
          <thead>
            <tr>
              <th>API Name</th>
              <th>Title</th>
              <th>Standard</th>
              <th>Storage</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {objects.map((obj) => (
              <tr key={obj.id}>
                <td>
                  <button className="link-btn" onClick={() => onSelectObject(obj)}>
                    {obj.apiName}
                  </button>
                </td>
                <td>{obj.title}</td>
                <td>{obj.isStandard ? 'Yes' : 'No'}</td>
                <td>
                  {obj.storageSchema && obj.storageTable
                    ? `${obj.storageSchema}.${obj.storageTable}`
                    : '-'}
                </td>
                <td>
                  {!obj.isStandard && (
                    <button className="danger-btn" onClick={() => handleDelete(obj.id)}>
                      Delete
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}
