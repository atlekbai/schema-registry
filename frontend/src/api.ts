export interface ListResponse {
  totalCount: string
  nextCursor: string | null
  results: Record<string, unknown>[]
}

export async function fetchObjects(
  objectName: string,
  cursor?: string | null,
  limit = 50,
): Promise<ListResponse> {
  const params = new URLSearchParams()
  params.set('limit', String(limit))
  if (cursor) {
    params.set('cursor', cursor)
  }

  const res = await fetch(`/api/${objectName}?${params.toString()}`)
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`)
  }
  return res.json()
}

// ── Metadata types ──────────────────────────────────────────────────

export interface ObjectMeta {
  id: string
  apiName: string
  title: string
  pluralTitle: string
  description: string
  isStandard: boolean
  storageSchema: string
  storageTable: string
  supportsCustomFields: boolean
  categoryId: string
  fields?: FieldMeta[]
  createdAt: string
  updatedAt: string
}

export interface FieldMeta {
  id: string
  objectId: string
  apiName: string
  title: string
  description: string
  type: string
  typeConfig: string
  isRequired: boolean
  isUnique: boolean
  isStandard: boolean
  storageColumn: string
  lookupObjectId: string
  createdAt: string
  updatedAt: string
}

// ── Objects API ─────────────────────────────────────────────────────

async function jsonOrThrow<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || `${res.status} ${res.statusText}`)
  }
  return res.json()
}

export async function listMetaObjects(): Promise<ObjectMeta[]> {
  const data = await jsonOrThrow<{ objects: ObjectMeta[] }>(
    await fetch('/api/meta/objects'),
  )
  return data.objects ?? []
}

export async function getMetaObject(id: string): Promise<ObjectMeta> {
  const data = await jsonOrThrow<{ object: ObjectMeta }>(
    await fetch(`/api/meta/objects/${id}`),
  )
  return data.object
}

export async function createMetaObject(
  body: Pick<ObjectMeta, 'apiName' | 'title' | 'pluralTitle' | 'description' | 'supportsCustomFields'>,
): Promise<ObjectMeta> {
  const data = await jsonOrThrow<{ object: ObjectMeta }>(
    await fetch('/api/meta/objects', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
  )
  return data.object
}

export async function updateMetaObject(
  id: string,
  body: Partial<Pick<ObjectMeta, 'title' | 'pluralTitle' | 'description' | 'supportsCustomFields'>>,
): Promise<ObjectMeta> {
  const data = await jsonOrThrow<{ object: ObjectMeta }>(
    await fetch(`/api/meta/objects/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
  )
  return data.object
}

export async function deleteMetaObject(id: string): Promise<void> {
  await jsonOrThrow<Record<string, never>>(
    await fetch(`/api/meta/objects/${id}`, { method: 'DELETE' }),
  )
}

// ── Fields API ──────────────────────────────────────────────────────

export async function listMetaFields(objectId: string): Promise<FieldMeta[]> {
  const data = await jsonOrThrow<{ fields: FieldMeta[] }>(
    await fetch(`/api/meta/objects/${objectId}/fields`),
  )
  return data.fields ?? []
}

export async function createMetaField(
  objectId: string,
  body: Pick<FieldMeta, 'apiName' | 'title' | 'description' | 'type' | 'isRequired' | 'isUnique'>,
): Promise<FieldMeta> {
  const data = await jsonOrThrow<{ field: FieldMeta }>(
    await fetch(`/api/meta/objects/${objectId}/fields`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
  )
  return data.field
}

export async function updateMetaField(
  objectId: string,
  fieldId: string,
  body: Partial<Pick<FieldMeta, 'title' | 'description' | 'isRequired' | 'isUnique'>>,
): Promise<FieldMeta> {
  const data = await jsonOrThrow<{ field: FieldMeta }>(
    await fetch(`/api/meta/objects/${objectId}/fields/${fieldId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }),
  )
  return data.field
}

export async function deleteMetaField(objectId: string, fieldId: string): Promise<void> {
  await jsonOrThrow<Record<string, never>>(
    await fetch(`/api/meta/objects/${objectId}/fields/${fieldId}`, { method: 'DELETE' }),
  )
}

// ── Org API ───────────────────────────────────────────────────────

export interface OrgQueryResponse {
  results?: Record<string, unknown>[]
  totalCount?: number
  nextCursor?: string | null
  reportsTo?: boolean
}

export async function orgQuery(
  query: string,
  opts: { select?: string; expand?: string; limit?: number; cursor?: string } = {},
): Promise<OrgQueryResponse> {
  return jsonOrThrow<OrgQueryResponse>(
    await fetch('/api/org/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query, ...opts }),
    }),
  )
}
