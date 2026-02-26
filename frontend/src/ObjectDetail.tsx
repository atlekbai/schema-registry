import { useState, useEffect, useCallback } from 'react'
import {
  getMetaObject,
  updateMetaObject,
  createMetaField,
  updateMetaField,
  deleteMetaField,
  type ObjectMeta,
  type FieldMeta,
} from './api'

const FIELD_TYPES = [
  'TEXT', 'NUMBER', 'CURRENCY', 'PERCENTAGE', 'DATE', 'DATETIME',
  'BOOLEAN', 'CHOICE', 'MULTICHOICE', 'EMAIL', 'URL', 'PHONE', 'LOOKUP',
]

interface Props {
  objectId: string
  onBack: () => void
}

export default function ObjectDetail({ objectId, onBack }: Props) {
  const [obj, setObj] = useState<ObjectMeta | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Object edit state
  const [editing, setEditing] = useState(false)
  const [editData, setEditData] = useState({ title: '', pluralTitle: '', description: '' })
  const [saving, setSaving] = useState(false)

  // Field form state
  const [showFieldForm, setShowFieldForm] = useState(false)
  const [fieldForm, setFieldForm] = useState({
    apiName: '',
    title: '',
    description: '',
    type: 'TEXT',
    isRequired: false,
    isUnique: false,
  })
  const [savingField, setSavingField] = useState(false)

  // Inline field edit
  const [editingFieldId, setEditingFieldId] = useState<string | null>(null)
  const [fieldEditData, setFieldEditData] = useState({ title: '', description: '', isRequired: false, isUnique: false })

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await getMetaObject(objectId)
      setObj(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [objectId])

  useEffect(() => {
    load()
  }, [load])

  const handleUpdateObject = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!obj) return
    setSaving(true)
    setError(null)
    try {
      const updated = await updateMetaObject(obj.id, editData)
      setObj({ ...obj, ...updated })
      setEditing(false)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setSaving(false)
    }
  }

  const handleCreateField = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!obj) return
    setSavingField(true)
    setError(null)
    try {
      await createMetaField(obj.id, fieldForm)
      setShowFieldForm(false)
      setFieldForm({ apiName: '', title: '', description: '', type: 'TEXT', isRequired: false, isUnique: false })
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setSavingField(false)
    }
  }

  const handleUpdateField = async (fieldId: string) => {
    if (!obj) return
    setError(null)
    try {
      await updateMetaField(obj.id, fieldId, fieldEditData)
      setEditingFieldId(null)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const handleDeleteField = async (fieldId: string) => {
    if (!obj) return
    if (!confirm('Delete this field?')) return
    setError(null)
    try {
      await deleteMetaField(obj.id, fieldId)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const startEditField = (f: FieldMeta) => {
    setEditingFieldId(f.id)
    setFieldEditData({
      title: f.title,
      description: f.description,
      isRequired: f.isRequired,
      isUnique: f.isUnique,
    })
  }

  if (loading) return <div className="empty-state">Loading...</div>
  if (!obj) return <div className="empty-state">Object not found.</div>

  const fields = obj.fields ?? []

  return (
    <div className="meta-page">
      <div className="meta-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <button onClick={onBack}>Back</button>
          <h2>{obj.title}</h2>
          <code>{obj.apiName}</code>
          {obj.isStandard && <span className="badge">Standard</span>}
        </div>
        {!editing && (
          <button onClick={() => { setEditing(true); setEditData({ title: obj.title, pluralTitle: obj.pluralTitle, description: obj.description }) }}>
            Edit
          </button>
        )}
      </div>

      {error && <div className="error">{error}</div>}

      {obj.description && <p className="meta-desc">{obj.description}</p>}

      {editing && (
        <form className="meta-form" onSubmit={handleUpdateObject}>
          <div className="form-row">
            <label>Title</label>
            <input value={editData.title} onChange={(e) => setEditData({ ...editData, title: e.target.value })} />
          </div>
          <div className="form-row">
            <label>Plural Title</label>
            <input value={editData.pluralTitle} onChange={(e) => setEditData({ ...editData, pluralTitle: e.target.value })} />
          </div>
          <div className="form-row">
            <label>Description</label>
            <input value={editData.description} onChange={(e) => setEditData({ ...editData, description: e.target.value })} />
          </div>
          <div style={{ display: 'flex', gap: '0.5rem' }}>
            <button type="submit" disabled={saving}>{saving ? 'Saving...' : 'Save'}</button>
            <button type="button" onClick={() => setEditing(false)}>Cancel</button>
          </div>
        </form>
      )}

      <div className="meta-header" style={{ marginTop: '1.5rem' }}>
        <h3>Fields ({fields.length})</h3>
        <button onClick={() => setShowFieldForm(!showFieldForm)}>
          {showFieldForm ? 'Cancel' : 'New Field'}
        </button>
      </div>

      {showFieldForm && (
        <form className="meta-form" onSubmit={handleCreateField}>
          <div className="form-row">
            <label>API Name</label>
            <input
              required
              placeholder="my_field"
              value={fieldForm.apiName}
              onChange={(e) => setFieldForm({ ...fieldForm, apiName: e.target.value })}
            />
          </div>
          <div className="form-row">
            <label>Title</label>
            <input
              required
              placeholder="My Field"
              value={fieldForm.title}
              onChange={(e) => setFieldForm({ ...fieldForm, title: e.target.value })}
            />
          </div>
          <div className="form-row">
            <label>Type</label>
            <select
              value={fieldForm.type}
              onChange={(e) => setFieldForm({ ...fieldForm, type: e.target.value })}
            >
              {FIELD_TYPES.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          </div>
          <div className="form-row">
            <label>Description</label>
            <input
              placeholder="Optional"
              value={fieldForm.description}
              onChange={(e) => setFieldForm({ ...fieldForm, description: e.target.value })}
            />
          </div>
          <div className="form-row">
            <label>
              <input
                type="checkbox"
                checked={fieldForm.isRequired}
                onChange={(e) => setFieldForm({ ...fieldForm, isRequired: e.target.checked })}
              />{' '}Required
            </label>
            <label style={{ marginLeft: '1rem' }}>
              <input
                type="checkbox"
                checked={fieldForm.isUnique}
                onChange={(e) => setFieldForm({ ...fieldForm, isUnique: e.target.checked })}
              />{' '}Unique
            </label>
          </div>
          <button type="submit" disabled={savingField}>
            {savingField ? 'Creating...' : 'Create Field'}
          </button>
        </form>
      )}

      {fields.length === 0 ? (
        <div className="empty-state">No fields defined yet.</div>
      ) : (
        <table className="meta-table">
          <thead>
            <tr>
              <th>API Name</th>
              <th>Title</th>
              <th>Type</th>
              <th>Required</th>
              <th>Unique</th>
              <th>Standard</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {fields.map((f) => (
              <tr key={f.id}>
                <td><code>{f.apiName}</code></td>
                <td>
                  {editingFieldId === f.id ? (
                    <input
                      value={fieldEditData.title}
                      onChange={(e) => setFieldEditData({ ...fieldEditData, title: e.target.value })}
                      style={{ width: '100%' }}
                    />
                  ) : (
                    f.title
                  )}
                </td>
                <td>{f.type}</td>
                <td>
                  {editingFieldId === f.id ? (
                    <input
                      type="checkbox"
                      checked={fieldEditData.isRequired}
                      onChange={(e) => setFieldEditData({ ...fieldEditData, isRequired: e.target.checked })}
                    />
                  ) : (
                    f.isRequired ? 'Yes' : '-'
                  )}
                </td>
                <td>
                  {editingFieldId === f.id ? (
                    <input
                      type="checkbox"
                      checked={fieldEditData.isUnique}
                      onChange={(e) => setFieldEditData({ ...fieldEditData, isUnique: e.target.checked })}
                    />
                  ) : (
                    f.isUnique ? 'Yes' : '-'
                  )}
                </td>
                <td>{f.isStandard ? 'Yes' : 'No'}</td>
                <td>
                  <div style={{ display: 'flex', gap: '0.25rem' }}>
                    {editingFieldId === f.id ? (
                      <>
                        <button onClick={() => handleUpdateField(f.id)}>Save</button>
                        <button onClick={() => setEditingFieldId(null)}>Cancel</button>
                      </>
                    ) : (
                      <>
                        <button onClick={() => startEditField(f)}>Edit</button>
                        {!f.isStandard && (
                          <button className="danger-btn" onClick={() => handleDeleteField(f.id)}>Delete</button>
                        )}
                      </>
                    )}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}
