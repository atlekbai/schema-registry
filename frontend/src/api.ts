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
