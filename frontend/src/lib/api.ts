const raw = process.env.NEXT_PUBLIC_API_URL ?? 'http://127.0.0.1:8000'

export const API_BASE_URL = raw.replace(/\/$/, '')

export function authHeaders(): HeadersInit {
  if (typeof window === 'undefined') return {}
  const token = localStorage.getItem('truvexis_token')
  return token ? { Authorization: `Bearer ${token}` } : {}
}
