'use client'

import { useEffect, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import { API_BASE_URL, authHeaders } from '@/lib/api'

// ── Types ─────────────────────────────────────────────────────
type MatchedIndicator = {
    id?: string
    indicator_id: string
    original_header: string
    matched_indicator: string
    confidence: number
    matching_method?: string
    requires_review: boolean
  }

type ValidationResult = {
  result_id: string
  rule_name: string
  severity: string
  message: string
  suggested_fixes?: string[]
  reviewed: boolean
}

const COLORS = {
  copper: '#B87333', gold: '#D4AF37', peach: '#FFD4B8',
  sage: '#C8D5B9', terra: '#E89C7F', brown: '#3E2723',
  mid: '#6D4C41', bg: '#FEFEF8', bgAlt: '#F5EFE6',
  taupe: '#D4C4B0', gray: '#8D8D8D', dark: '#2C1810'
}

const STEPS = ['Upload', 'Match', 'Normalize', 'Validate', 'Generate', 'Export']

const Icons = {
  dashboard: <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>,
  upload:    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>,
  reports:   <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>,
  chat:      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>,
  settings:  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>,
  logout:    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>,
  check:     <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="20 6 9 17 4 12"/></svg>,
  file:      <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>,
  download:  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>,
}

export default function NewReportPage() {
  const router = useRouter()
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [user, setUser] = useState<any>(null)
  const [step, setStep] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  // Form state
  const [file, setFile] = useState<File | null>(null)
  const [dragOver, setDragOver] = useState(false)
  const [facilityName, setFacilityName] = useState('')
  const [industry, setIndustry] = useState('cement')
  const [reportingPeriod, setReportingPeriod] = useState('')

  // Pipeline state
  const [uploadId, setUploadId] = useState('')
  const [matchedIndicators, setMatchedIndicators] = useState<MatchedIndicator[]>([])
  const [normSummary, setNormSummary] = useState<any>(null)
  const [valSummary, setValSummary] = useState<any>(null)
  const [valErrors, setValErrors] = useState<ValidationResult[]>([])
  const [valWarnings, setValWarnings] = useState<ValidationResult[]>([])
  const [genResult, setGenResult] = useState<any>(null)
  const [selectedFrameworks, setSelectedFrameworks] = useState(['BRSR'])
  const [progress, setProgress] = useState('')
  const [pdfLoading, setPdfLoading] = useState(false)

  // Auth
  useEffect(() => {
    const token = localStorage.getItem('truvexis_token')
    if (!token) { router.replace('/'); return }
    fetch(`${API_BASE_URL}/api/v1/auth/me`, { headers: authHeaders() })
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d) setUser(d); else router.replace('/') })
      .catch(() => router.replace('/'))
  }, [router])

  const logout = () => {
    localStorage.removeItem('truvexis_token')
    localStorage.removeItem('truvexis_user')
    router.replace('/')
  }

  // ── Step 1: Upload ─────────────────────────────────────────
  const handleUpload = async () => {
    if (!file) { setError('Please select a file.'); return }
    if (!facilityName.trim()) { setError('Please enter a facility name.'); return }
    setError('')
    setLoading(true)
    setProgress('Uploading file...')
    try {
      const formData = new FormData()
      formData.append('file', file)
      formData.append('facility_name', facilityName)
      formData.append('industry', industry)
      if (reportingPeriod) formData.append('reporting_period', reportingPeriod)

      const res = await fetch(`${API_BASE_URL}/api/v1/ingest/upload`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${localStorage.getItem('truvexis_token')}` },
        body: formData,
      })
      if (!res.ok) { const d = await res.json(); throw new Error(d.detail || 'Upload failed') }
      const data = await res.json()
      setUploadId(data.upload_id || data.id)
      setProgress('Running entity matching...')

      // Auto-run matching
      const matchRes = await fetch(`${API_BASE_URL}/api/v1/matching/${data.upload_id || data.id}`, {
        method: 'POST',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ reviews: [] }),
      })
      if (!matchRes.ok) throw new Error('Matching failed')
      
      // GET the results after POST
      const matchGet = await fetch(`${API_BASE_URL}/api/v1/matching/${data.upload_id || data.id}`, {
        headers: authHeaders(),
      })
      const matchData = await matchGet.json()
      setMatchedIndicators(matchData.results || [])
      setStep(1)
    } catch (e: any) {
      setError(e.message || 'Something went wrong.')
    }
    setLoading(false)
    setProgress('')
  }

  // ── Step 2: Approve matches → Normalize ───────────────────
  const handleNormalize = async () => {
    setError('')
    setLoading(true)
    setProgress('Normalizing data...')
    try {
      const res = await fetch(`${API_BASE_URL}/api/v1/normalization/${uploadId}`, {
        method: 'POST', headers: authHeaders(),
      })
      if (!res.ok) { const d = await res.json(); throw new Error(d.detail || 'Normalization failed') }
      
      // GET results after POST
      const getRes = await fetch(`${API_BASE_URL}/api/v1/normalization/${uploadId}`, {
        headers: authHeaders(),
      })
      const data = await getRes.json()
      setNormSummary(data.summary || data)
      setStep(2)
    } catch (e: any) { setError(e.message) }
    setLoading(false)
    setProgress('')
  }

  // ── Step 3: Normalize done → Validate ─────────────────────
  const handleValidate = async () => {
    setError('')
    setLoading(true)
    setProgress('Validating against industry benchmarks...')
    try {
        const res = await fetch(`${API_BASE_URL}/api/v1/validation/${uploadId}?industry=${industry}`, {
            method: 'POST', headers: authHeaders(),
          })
          if (!res.ok) { const d = await res.json(); throw new Error(d.detail || 'Validation failed') }
          
          const getRes = await fetch(`${API_BASE_URL}/api/v1/validation/${uploadId}`, { 
            headers: authHeaders() 
          })
          const data = await getRes.json()
          setValSummary(data.summary || data)
          setValErrors((data.errors || []).slice(0, 10))
          setValWarnings((data.warnings || []).slice(0, 10))
          setStep(3)
    } catch (e: any) { setError(e.message) }
    setLoading(false)
    setProgress('')
  }

  // ── Step 4: Validate done → Generate ──────────────────────
  const handleGenerate = async () => {
    setError('')
    setLoading(true)
    setProgress('Generating AI narratives... this takes ~60 seconds')
    try {
      const res = await fetch(`${API_BASE_URL}/api/v1/generation/${uploadId}`, {
        method: 'POST',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ framework: selectedFrameworks[0], include_recommendations: true }),
      })
      if (!res.ok) { const d = await res.json(); throw new Error(d.detail || 'Generation failed') }
      const data = await res.json()
      setGenResult(data)
      setStep(4)
    } catch (e: any) { setError(e.message) }
    setLoading(false)
    setProgress('')
  }

  const handleDownloadPdf = async () => {
    if (!uploadId) return
    setPdfLoading(true)
    try {
      const framework = selectedFrameworks[0] || 'BRSR'
      const res = await fetch(
        `${API_BASE_URL}/api/v1/export/${uploadId}/pdf?framework=${framework}`,
        { headers: authHeaders() },
      )
      if (!res.ok) {
        const d = await res.json().catch(() => ({}))
        throw new Error(d.detail || 'PDF download failed')
      }
      const blob = await res.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      const cd = res.headers.get('Content-Disposition') || ''
      const match = cd.match(/filename="?([^"]+)"?/)
      a.download = match ? match[1] : `Truvexis_${framework}_report.pdf`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
    } catch (e: any) {
      setError(e.message)
    }
    setPdfLoading(false)
  }

  // ── File drag/drop ─────────────────────────────────────────
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    setDragOver(false)
    const f = e.dataTransfer.files[0]
    if (f) setFile(f)
  }

  const confidenceColor = (score: number) => {
    if (score >= 0.85) return '#4a7c4a'
    if (score >= 0.6) return '#8a6500'
    return '#a0402a'
  }
  
  const confidenceBg = (score: number) => {
    if (score >= 0.85) return 'rgba(200,213,185,0.25)'
    if (score >= 0.6) return 'rgba(212,175,55,0.15)'
    return 'rgba(232,156,127,0.2)'
  }

  if (!user) return (
    <div style={{ minHeight: '100vh', background: COLORS.bg, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
      <div style={{ width: 28, height: 28, border: `2px solid ${COLORS.taupe}`, borderTopColor: COLORS.copper, borderRadius: '50%', animation: 'spin 0.8s linear infinite' }}/>
      <style>{`@keyframes spin{to{transform:rotate(360deg)}}`}</style>
    </div>
  )

  return (
    <div style={{ display: 'flex', minHeight: '100vh', fontFamily: "'DM Sans',sans-serif", background: COLORS.bgAlt }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,500;0,700;1,400&family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600&display=swap');
        *{box-sizing:border-box;margin:0;padding:0}
        @keyframes spin{to{transform:rotate(360deg)}}
        @keyframes fade-up{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}
        .fade-up{animation:fade-up 0.4s ease forwards}
        .nav-item{display:flex;align-items:center;gap:12px;padding:11px 16px;border-radius:10px;cursor:pointer;transition:all 0.2s ease;color:rgba(254,254,248,0.5);font-size:0.875rem;font-weight:400;border:none;background:transparent;width:100%;text-align:left}
        .nav-item:hover{background:rgba(255,255,255,0.08);color:rgba(254,254,248,0.85)}
        .nav-item.active{background:rgba(184,115,51,0.2);color:#FFD4B8;font-weight:500}
        .input-field{width:100%;padding:11px 14px;border:1.5px solid #D4C4B0;border-radius:10px;font-size:0.9rem;font-family:inherit;color:#3E2723;background:#FEFEF8;outline:none;transition:border-color 0.2s}
        .input-field:focus{border-color:#B87333}
        .btn-primary{background:#3E2723;color:#FEFEF8;border:none;border-radius:10px;padding:12px 28px;font-size:0.9rem;font-weight:500;cursor:pointer;font-family:inherit;transition:all 0.2s;display:flex;align-items:center;gap:8px}
        .btn-primary:hover{background:#6D4C41}
        .btn-primary:disabled{background:#D4C4B0;cursor:not-allowed}
        .btn-secondary{background:transparent;color:#3E2723;border:1.5px solid #D4C4B0;border-radius:10px;padding:12px 24px;font-size:0.9rem;font-weight:400;cursor:pointer;font-family:inherit;transition:all 0.2s}
        .btn-secondary:hover{border-color:#3E2723}
        .card{background:#FEFEF8;border:1px solid #D4C4B0;border-radius:16px;padding:28px}
        .drop-zone{border:2px dashed #D4C4B0;border-radius:16px;padding:48px 24px;text-align:center;cursor:pointer;transition:all 0.25s;background:#FEFEF8}
        .drop-zone:hover,.drop-zone.over{border-color:#B87333;background:rgba(184,115,51,0.04)}
        .table-row{display:grid;gap:12px;padding:12px 16px;border-radius:8px;border:1px solid #EDE4D3;margin-bottom:8px;align-items:center;background:#FEFEF8;transition:border-color 0.2s}
        .table-row:hover{border-color:#D4C4B0}
        .badge{display:inline-flex;align-items:center;padding:3px 10px;border-radius:100px;font-size:0.7rem;font-weight:600;letter-spacing:0.04em;text-transform:uppercase}
        .scrollbar-hide{scrollbar-width:none;-ms-overflow-style:none}
        .scrollbar-hide::-webkit-scrollbar{display:none}
        .fw-check{display:flex;align-items:center;gap:10px;padding:14px 16px;border-radius:10px;border:1.5px solid #D4C4B0;cursor:pointer;transition:all 0.2s;background:#FEFEF8}
        .fw-check.selected{border-color:#B87333;background:rgba(184,115,51,0.06)}
        .step-dot{width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:0.75rem;font-weight:600;flex-shrink:0;transition:all 0.3s}
      `}</style>

      {/* ── SIDEBAR ── */}
      <aside style={{ width: 220, background: COLORS.dark, display: 'flex', flexDirection: 'column', padding: '24px 16px', position: 'fixed', top: 0, left: 0, bottom: 0, zIndex: 100, borderRight: '1px solid rgba(255,255,255,0.06)' }}>
        <div style={{ padding: '8px 8px 24px', borderBottom: '1px solid rgba(255,255,255,0.08)', marginBottom: 20 }}>
          <span style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.25rem', color: '#FEFEF8' }}>Truvexis</span>
          <div style={{ fontSize: '0.68rem', color: 'rgba(254,254,248,0.3)', marginTop: 2, letterSpacing: '0.08em', textTransform: 'uppercase' }}>ESG Platform</div>
        </div>
        <nav style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 4 }}>
          {[
            { id: 'dashboard', label: 'Dashboard', icon: Icons.dashboard, path: '/dashboard' },
            { id: 'upload', label: 'New Report', icon: Icons.upload, path: '/dashboard/upload' },
            { id: 'chat', label: 'Ask AI', icon: Icons.chat, path: '/dashboard/chat' },
          ].map(item => (
            <button key={item.id} className={`nav-item ${item.id === 'upload' ? 'active' : ''}`} onClick={() => router.push(item.path)}>
              {item.icon}{item.label}
            </button>
          ))}
        </nav>
        <div style={{ borderTop: '1px solid rgba(255,255,255,0.07)', paddingTop: 16 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: 8, marginBottom: 8 }}>
            <div style={{ width: 34, height: 34, borderRadius: '50%', background: 'linear-gradient(135deg, #B87333, #D4AF37)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.85rem', fontWeight: 600, color: '#FEFEF8', flexShrink: 0 }}>
              {user.name.charAt(0).toUpperCase()}
            </div>
            <div style={{ overflow: 'hidden' }}>
              <div style={{ fontSize: '0.82rem', color: '#FEFEF8', fontWeight: 500, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{user.name}</div>
              <div style={{ fontSize: '0.7rem', color: 'rgba(254,254,248,0.35)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{user.company || user.email}</div>
            </div>
          </div>
          <button className="nav-item" onClick={logout} style={{ color: 'rgba(232,156,127,0.7)' }}>{Icons.logout} Sign Out</button>
        </div>
      </aside>

      {/* ── MAIN ── */}
      <main style={{ marginLeft: 220, flex: 1, padding: '32px 40px', maxWidth: 'calc(100vw - 220px)' }}>

        {/* Header */}
        <div style={{ marginBottom: 32 }}>
          <button onClick={() => router.push('/dashboard')} style={{ display: 'flex', alignItems: 'center', gap: 6, color: COLORS.gray, fontSize: '0.82rem', background: 'none', border: 'none', cursor: 'pointer', fontFamily: 'inherit', marginBottom: 16, padding: 0 }}>
            ← Back to Dashboard
          </button>
          <h1 style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.75rem', color: COLORS.brown, fontWeight: 500, marginBottom: 4 }}>New Report</h1>
          <p style={{ color: COLORS.gray, fontSize: '0.875rem' }}>Upload your facility data and generate compliant ESG reports automatically.</p>
        </div>

        {/* ── PROGRESS BAR ── */}
        <div style={{ display: 'flex', alignItems: 'center', marginBottom: 40, background: COLORS.bg, borderRadius: 16, padding: '20px 24px', border: `1px solid ${COLORS.taupe}` }}>
          {STEPS.map((s, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', flex: i < STEPS.length - 1 ? 1 : 'none' }}>
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 6 }}>
                <div className="step-dot" style={{
                  background: i < step ? COLORS.copper : i === step ? COLORS.brown : COLORS.bgAlt,
                  color: i <= step ? '#FEFEF8' : COLORS.gray,
                  border: i === step ? `2px solid ${COLORS.copper}` : `2px solid ${i < step ? COLORS.copper : COLORS.taupe}`,
                  boxShadow: i === step ? `0 0 0 4px rgba(184,115,51,0.15)` : 'none',
                }}>
                  {i < step ? Icons.check : <span>{i + 1}</span>}
                </div>
                <span style={{ fontSize: '0.7rem', color: i <= step ? COLORS.brown : COLORS.gray, fontWeight: i === step ? 600 : 400, whiteSpace: 'nowrap' }}>{s}</span>
              </div>
              {i < STEPS.length - 1 && (
                <div style={{ flex: 1, height: 2, background: i < step ? COLORS.copper : COLORS.taupe, margin: '0 8px', marginBottom: 20, transition: 'background 0.3s' }}/>
              )}
            </div>
          ))}
        </div>

        {/* ── ERROR ── */}
        {error && (
          <div style={{ background: '#FFF0F0', border: '1px solid #FFCDD2', borderRadius: 10, padding: '12px 16px', marginBottom: 20, color: '#a0402a', fontSize: '0.875rem', display: 'flex', alignItems: 'center', gap: 8 }}>
            ⚠ {error}
            <button onClick={() => setError('')} style={{ marginLeft: 'auto', background: 'none', border: 'none', cursor: 'pointer', color: '#a0402a', fontSize: '1rem' }}>✕</button>
          </div>
        )}

        {/* ── STEP CONTENT ── */}
        <div className="fade-up">

          {/* STEP 0: Upload */}
          {step === 0 && (
            <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 1fr', gap: 24 }}>
              <div className="card">
                <h2 style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.15rem', color: COLORS.brown, marginBottom: 20 }}>Upload Facility Data</h2>
                <div
                  className={`drop-zone ${dragOver ? 'over' : ''}`}
                  onDragOver={e => { e.preventDefault(); setDragOver(true) }}
                  onDragLeave={() => setDragOver(false)}
                  onDrop={handleDrop}
                  onClick={() => fileInputRef.current?.click()}
                >
                  <input ref={fileInputRef} type="file" accept=".csv,.xlsx,.xls,.pdf" style={{ display: 'none' }} onChange={e => e.target.files?.[0] && setFile(e.target.files[0])}/>
                  {file ? (
                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 10 }}>
                      <div style={{ color: COLORS.copper }}>{Icons.file}</div>
                      <div style={{ fontWeight: 500, color: COLORS.brown }}>{file.name}</div>
                      <div style={{ fontSize: '0.78rem', color: COLORS.gray }}>{(file.size / 1024).toFixed(1)} KB</div>
                      <button onClick={e => { e.stopPropagation(); setFile(null) }} style={{ fontSize: '0.75rem', color: COLORS.gray, background: 'none', border: 'none', cursor: 'pointer', textDecoration: 'underline' }}>Remove</button>
                    </div>
                  ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 10 }}>
                      <div style={{ color: COLORS.taupe }}>{Icons.file}</div>
                      <div style={{ fontWeight: 500, color: COLORS.brown, fontSize: '0.95rem' }}>Drop your file here</div>
                      <div style={{ fontSize: '0.78rem', color: COLORS.gray }}>or click to browse</div>
                      <div style={{ fontSize: '0.72rem', color: COLORS.taupe, marginTop: 4 }}>Supports CSV, Excel (.xlsx), PDF</div>
                    </div>
                  )}
                </div>
              </div>

              <div className="card">
                <h2 style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.15rem', color: COLORS.brown, marginBottom: 20 }}>Report Details</h2>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                  <div>
                    <label style={{ fontSize: '0.72rem', letterSpacing: '0.08em', textTransform: 'uppercase', color: COLORS.gray, display: 'block', marginBottom: 6 }}>Facility Name *</label>
                    <input className="input-field" placeholder="e.g. Plant A, Tata Steel Jamshedpur" value={facilityName} onChange={e => setFacilityName(e.target.value)}/>
                  </div>
                  <div>
                    <label style={{ fontSize: '0.72rem', letterSpacing: '0.08em', textTransform: 'uppercase', color: COLORS.gray, display: 'block', marginBottom: 6 }}>Industry *</label>
                    <select className="input-field" value={industry} onChange={e => setIndustry(e.target.value)} style={{ appearance: 'none' }}>
                      <option value="cement">Cement</option>
                      <option value="steel">Steel</option>
                      <option value="automotive">Automotive</option>
                      <option value="chemical">Chemical</option>
                      <option value="other">Other Manufacturing</option>
                    </select>
                  </div>
                  <div>
                    <label style={{ fontSize: '0.72rem', letterSpacing: '0.08em', textTransform: 'uppercase', color: COLORS.gray, display: 'block', marginBottom: 6 }}>Reporting Period</label>
                    <input className="input-field" placeholder="e.g. 2024-01 or FY 2023-24" value={reportingPeriod} onChange={e => setReportingPeriod(e.target.value)}/>
                  </div>
                  <button className="btn-primary" onClick={handleUpload} disabled={loading || !file} style={{ marginTop: 8, justifyContent: 'center', opacity: loading || !file ? 0.6 : 1 }}>
                    {loading ? <><span style={{ width: 16, height: 16, border: '2px solid rgba(255,255,255,0.3)', borderTopColor: '#fff', borderRadius: '50%', animation: 'spin 0.7s linear infinite' }}/>{progress || 'Processing...'}</> : <>{Icons.upload} Start Pipeline</>}
                  </button>
                </div>
              </div>
            </div>
          )}

          {/* STEP 1: Match results */}
          {step === 1 && (
            <div className="card">
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 24 }}>
                <div>
                  <h2 style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.15rem', color: COLORS.brown, marginBottom: 4 }}>Header Matching Results</h2>
                  <p style={{ fontSize: '0.85rem', color: COLORS.gray }}>{matchedIndicators.length} indicators matched. Review and approve before normalizing.</p>
                </div>
                <div style={{ display: 'flex', gap: 10 }}>
                  <div style={{ fontSize: '0.78rem', padding: '6px 12px', borderRadius: 8, background: 'rgba(200,213,185,0.25)', color: '#4a7c4a' }}>
                    ✓ {matchedIndicators.filter(m => m.confidence >= 0.85).length} auto-approved
                  </div>
                  <div style={{ fontSize: '0.78rem', padding: '6px 12px', borderRadius: 8, background: 'rgba(212,175,55,0.15)', color: '#8a6500' }}>
                    ⚠ {matchedIndicators.filter(m => m.confidence < 0.85).length} need review
                  </div>
                </div>
              </div>

              {/* Table header */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 100px 80px', gap: 12, padding: '8px 16px', fontSize: '0.72rem', letterSpacing: '0.08em', textTransform: 'uppercase', color: COLORS.gray, marginBottom: 8 }}>
                <span>Your Header</span><span>Matched Indicator</span><span>Confidence</span><span>Method</span>
              </div>
              <div className="scrollbar-hide" style={{ maxHeight: 360, overflowY: 'auto' }}>
                {matchedIndicators.map((m, i) => (
                  <div key={m.id || i} className="table-row" style={{ gridTemplateColumns: '1fr 1fr 100px 80px' }}>
                    <span style={{ fontSize: '0.875rem', color: COLORS.brown, fontWeight: 500 }}>{m.original_header}</span>
                    <span style={{ fontSize: '0.875rem', color: COLORS.mid }}>{m.matched_indicator}</span>
                    <span className="badge" style={{ background: confidenceBg(m.confidence), color: confidenceColor(m.confidence) }}>
                      {(m.confidence * 100).toFixed(0)}%
                    </span>
                    <span style={{ fontSize: '0.72rem', color: COLORS.gray, textTransform: 'uppercase', letterSpacing: '0.06em' }}>{m.matching_method}</span>
                  </div>
                ))}
                {matchedIndicators.length === 0 && (
                  <div style={{ textAlign: 'center', padding: '40px 0', color: COLORS.gray, fontSize: '0.875rem' }}>No matches found. Check your file format.</div>
                )}
              </div>

              <div style={{ display: 'flex', gap: 12, marginTop: 24, justifyContent: 'flex-end' }}>
                <button className="btn-secondary" onClick={() => setStep(0)}>← Back</button>
                <button className="btn-primary" onClick={handleNormalize} disabled={loading}>
                  {loading ? <><span style={{ width: 16, height: 16, border: '2px solid rgba(255,255,255,0.3)', borderTopColor: '#fff', borderRadius: '50%', animation: 'spin 0.7s linear infinite' }}/>{progress}</> : 'Approve & Normalize →'}
                </button>
              </div>
            </div>
          )}

          {/* STEP 2: Normalization summary */}
          {step === 2 && (
            <div className="card">
              <h2 style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.15rem', color: COLORS.brown, marginBottom: 24 }}>Normalization Complete</h2>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 16, marginBottom: 28 }}>
                {[
                  { label: 'Total Records', value: (normSummary?.total_records || 0).toLocaleString(), color: COLORS.copper },
                  { label: 'Successfully Normalized', value: (normSummary?.successfully_normalized || 0).toLocaleString(), color: '#4a7c4a' },
                  { label: 'Failed', value: (normSummary?.failed_normalization || 0).toLocaleString(), color: '#a0402a' },
                ].map((s, i) => (
                  <div key={i} style={{ background: COLORS.bgAlt, borderRadius: 12, padding: '20px', border: `1px solid ${COLORS.taupe}` }}>
                    <div style={{ fontSize: '0.72rem', letterSpacing: '0.1em', textTransform: 'uppercase', color: COLORS.gray, marginBottom: 8 }}>{s.label}</div>
                    <div style={{ fontFamily: "'Playfair Display',serif", fontSize: '2rem', color: s.color, fontWeight: 500 }}>{s.value}</div>
                  </div>
                ))}
              </div>
              {normSummary?.unique_units_detected?.length > 0 && (
                <div style={{ marginBottom: 24 }}>
                  <div style={{ fontSize: '0.78rem', color: COLORS.gray, marginBottom: 8 }}>Units detected:</div>
                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    {normSummary.unique_units_detected.map((u: string, i: number) => (
                      <span key={i} style={{ padding: '4px 12px', background: 'rgba(184,115,51,0.1)', color: COLORS.copper, borderRadius: 100, fontSize: '0.78rem', border: `1px solid rgba(184,115,51,0.2)` }}>{u}</span>
                    ))}
                  </div>
                </div>
              )}
              <div style={{ display: 'flex', gap: 12, justifyContent: 'flex-end' }}>
                <button className="btn-secondary" onClick={() => setStep(1)}>← Back</button>
                <button className="btn-primary" onClick={handleValidate} disabled={loading}>
                  {loading ? <><span style={{ width: 16, height: 16, border: '2px solid rgba(255,255,255,0.3)', borderTopColor: '#fff', borderRadius: '50%', animation: 'spin 0.7s linear infinite' }}/>{progress}</> : 'Run Validation →'}
                </button>
              </div>
            </div>
          )}

          {/* STEP 3: Validation results */}
          {step === 3 && (
            <div className="card">
              <h2 style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.15rem', color: COLORS.brown, marginBottom: 20 }}>Validation Results</h2>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 14, marginBottom: 28 }}>
                {[
                  { label: 'Total Records', value: (valSummary?.total_records || 0).toLocaleString(), color: COLORS.copper },
                  { label: 'Valid', value: (valSummary?.valid_records || 0).toLocaleString(), color: '#4a7c4a' },
                  { label: 'Errors', value: (valSummary?.records_with_errors || 0).toLocaleString(), color: '#a0402a' },
                  { label: 'Pass Rate', value: `${(valSummary?.validation_pass_rate || 0).toFixed(1)}%`, color: COLORS.brown },
                ].map((s, i) => (
                  <div key={i} style={{ background: COLORS.bgAlt, borderRadius: 12, padding: '18px', border: `1px solid ${COLORS.taupe}` }}>
                    <div style={{ fontSize: '0.7rem', letterSpacing: '0.1em', textTransform: 'uppercase', color: COLORS.gray, marginBottom: 6 }}>{s.label}</div>
                    <div style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.6rem', color: s.color, fontWeight: 500 }}>{s.value}</div>
                  </div>
                ))}
              </div>

              {valErrors.length > 0 && (
                <div style={{ marginBottom: 20 }}>
                  <div style={{ fontSize: '0.78rem', letterSpacing: '0.08em', textTransform: 'uppercase', color: '#a0402a', marginBottom: 10, fontWeight: 600 }}>Sample Errors (showing {valErrors.length})</div>
                  <div className="scrollbar-hide" style={{ maxHeight: 200, overflowY: 'auto' }}>
                    {valErrors.map((e, i) => (
                      <div key={i} style={{ padding: '10px 14px', background: 'rgba(255,240,240,0.8)', border: '1px solid #FFCDD2', borderRadius: 8, marginBottom: 6 }}>
                        <div style={{ fontSize: '0.82rem', color: '#a0402a', fontWeight: 500, marginBottom: 2 }}>{e.rule_name}</div>
                        <div style={{ fontSize: '0.78rem', color: '#6D4C41' }}>{e.message}</div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              <div style={{ display: 'flex', gap: 12, justifyContent: 'flex-end' }}>
                <button className="btn-secondary" onClick={() => setStep(2)}>← Back</button>
                <button className="btn-primary" onClick={() => setStep(4)} disabled={loading}>
                  Continue to Generate →
                </button>
              </div>
            </div>
          )}

          {/* STEP 4: Generate */}
          {step === 4 && (
            <div className="card">
              <h2 style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.15rem', color: COLORS.brown, marginBottom: 8 }}>Generate ESG Reports</h2>
              <p style={{ fontSize: '0.875rem', color: COLORS.gray, marginBottom: 28 }}>Select frameworks and generate AI-powered narratives grounded in your validated data.</p>

              <div style={{ marginBottom: 28 }}>
                <div style={{ fontSize: '0.72rem', letterSpacing: '0.08em', textTransform: 'uppercase', color: COLORS.gray, marginBottom: 12 }}>Select Frameworks</div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
                  {[
                    { id: 'BRSR', name: 'BRSR Core', desc: 'India SEBI Mandate', flag: '🇮🇳' },
                    { id: 'GRI', name: 'GRI Universal', desc: 'Global Standards', flag: '🌍' },
                    { id: 'CSRD', name: 'CSRD', desc: 'EU Directive', flag: '🇪🇺' },
                  ].map(fw => (
                    <div key={fw.id} className={`fw-check ${selectedFrameworks.includes(fw.id) ? 'selected' : ''}`}
                      onClick={() => setSelectedFrameworks(prev => prev.includes(fw.id) ? prev.filter(f => f !== fw.id) : [...prev, fw.id])}>
                      <div style={{ width: 20, height: 20, borderRadius: 6, border: `2px solid ${selectedFrameworks.includes(fw.id) ? COLORS.copper : COLORS.taupe}`, background: selectedFrameworks.includes(fw.id) ? COLORS.copper : 'transparent', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, transition: 'all 0.2s' }}>
                        {selectedFrameworks.includes(fw.id) && <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="3"><polyline points="20 6 9 17 4 12"/></svg>}
                      </div>
                      <div>
                        <div style={{ fontSize: '0.875rem', fontWeight: 500, color: COLORS.brown }}>{fw.flag} {fw.name}</div>
                        <div style={{ fontSize: '0.72rem', color: COLORS.gray }}>{fw.desc}</div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div style={{ background: COLORS.bgAlt, borderRadius: 12, padding: '16px 20px', border: `1px solid ${COLORS.taupe}`, marginBottom: 28 }}>
                <div style={{ fontSize: '0.82rem', color: COLORS.mid, lineHeight: 1.6 }}>
                  ✦ Narratives will be grounded in your validated facility data via RAG<br/>
                  ✦ 100% citation traceability — every claim traces to a source record<br/>
                  ✦ Generation takes approximately 30–60 seconds
                </div>
              </div>

              <div style={{ display: 'flex', gap: 12, justifyContent: 'flex-end' }}>
                <button className="btn-secondary" onClick={() => setStep(3)}>← Back</button>
                <button className="btn-primary" onClick={handleGenerate} disabled={loading || selectedFrameworks.length === 0} style={{ minWidth: 180, justifyContent: 'center' }}>
                  {loading ? <><span style={{ width: 16, height: 16, border: '2px solid rgba(255,255,255,0.3)', borderTopColor: '#fff', borderRadius: '50%', animation: 'spin 0.7s linear infinite' }}/>{progress || 'Generating...'}</> : '✦ Generate Reports →'}
                </button>
              </div>
            </div>
          )}

          {/* STEP 5: Report Viewer */}
{step === 5 && (
  <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
    
    {/* Header */}
    <div className="card" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
      <div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
          <div style={{ width: 32, height: 32, borderRadius: '50%', background: 'rgba(200,213,185,0.3)', border: '2px solid #C8D5B9', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.9rem' }}>✓</div>
          <h2 style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.25rem', color: COLORS.brown }}>ESG Report Generated</h2>
        </div>
        <p style={{ fontSize: '0.85rem', color: COLORS.gray }}>
          {genResult?.narratives?.length || 0} narratives · {selectedFrameworks.join(', ')} framework · {facilityName}
        </p>
      </div>
      <div style={{ display: 'flex', gap: 10 }}>
        <button onClick={() => router.push('/dashboard')} style={{ padding: '10px 20px', background: 'none', border: `1.5px solid ${COLORS.taupe}`, borderRadius: 10, fontSize: '0.875rem', cursor: 'pointer', fontFamily: 'inherit', color: COLORS.brown }}>
          ← Dashboard
        </button>
        <button
          onClick={handleDownloadPdf}
          disabled={pdfLoading}
          style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '10px 20px', background: COLORS.copper, border: 'none', borderRadius: 10, fontSize: '0.875rem', cursor: pdfLoading ? 'not-allowed' : 'pointer', fontFamily: 'inherit', color: '#fff', opacity: pdfLoading ? 0.7 : 1, transition: 'opacity 0.2s' }}
        >
          {pdfLoading
            ? <><span style={{ width: 14, height: 14, border: '2px solid rgba(255,255,255,0.3)', borderTopColor: '#fff', borderRadius: '50%', animation: 'spin 0.7s linear infinite', display: 'inline-block' }}/> Preparing...</>
            : <>{Icons.download} Download PDF</>}
        </button>
      </div>
    </div>

    {/* Stats row */}
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
      {[
        { label: 'Narratives', value: genResult?.narratives?.length || 0, color: COLORS.copper },
        { label: 'Total Words', value: (genResult?.narratives || []).reduce((a: number, n: any) => a + (n.word_count || 0), 0).toLocaleString(), color: COLORS.brown },
        { label: 'Citations', value: (genResult?.narratives || []).reduce((a: number, n: any) => a + (n.citations?.length || 0), 0), color: COLORS.gold },
        { label: 'Framework', value: selectedFrameworks[0], color: '#4a7c4a' },
      ].map((s, i) => (
        <div key={i} style={{ background: COLORS.bg, border: `1px solid ${COLORS.taupe}`, borderRadius: 12, padding: '16px 20px' }}>
          <div style={{ fontSize: '0.7rem', letterSpacing: '0.1em', textTransform: 'uppercase', color: COLORS.gray, marginBottom: 6 }}>{s.label}</div>
          <div style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.5rem', color: s.color, fontWeight: 500 }}>{s.value}</div>
        </div>
      ))}
    </div>

    {/* Narratives grouped by indicator */}
    {(() => {
      const narratives = genResult?.narratives || []
      const grouped: Record<string, any[]> = {}
      narratives.forEach((n: any) => {
        if (!grouped[n.indicator]) grouped[n.indicator] = []
        grouped[n.indicator].push(n)
      })
      return Object.entries(grouped).map(([indicator, sections], gi) => (
        <div key={gi} className="card">
          {/* Indicator header */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 20, paddingBottom: 16, borderBottom: `1px solid ${COLORS.taupe}` }}>
            <div style={{ width: 36, height: 36, borderRadius: 10, background: `rgba(184,115,51,0.1)`, border: `1px solid rgba(184,115,51,0.2)`, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.8rem', color: COLORS.copper, fontWeight: 600, flexShrink: 0 }}>
              {gi + 1}
            </div>
            <div>
              <div style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.1rem', color: COLORS.brown }}>{indicator}</div>
              <div style={{ fontSize: '0.72rem', color: COLORS.gray, marginTop: 2 }}>{sections.length} section{sections.length > 1 ? 's' : ''}</div>
            </div>
          </div>

          {/* Sections */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            {sections.map((s: any, si: number) => (
              <div key={si} style={{ background: COLORS.bgAlt, borderRadius: 10, padding: '18px 20px', border: `1px solid ${COLORS.taupe}` }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
                  <span style={{ fontSize: '0.72rem', letterSpacing: '0.1em', textTransform: 'uppercase', color: COLORS.copper, fontWeight: 600 }}>
                    {s.section.replace(/_/g, ' ')}
                  </span>
                  <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                    {s.citations?.length > 0 && (
                      <span style={{ fontSize: '0.7rem', color: COLORS.gold, background: 'rgba(212,175,55,0.1)', padding: '2px 8px', borderRadius: 100 }}>
                        {s.citations.length} citation{s.citations.length > 1 ? 's' : ''}
                      </span>
                    )}
                    <span style={{ fontSize: '0.7rem', color: COLORS.gray }}>{s.word_count} words</span>
                  </div>
                </div>
                <p style={{ fontSize: '0.875rem', color: COLORS.mid, lineHeight: 1.8, whiteSpace: 'pre-line' }}>
                  {s.content.replace(/\\n/g, '\n')}
                </p>
                {s.citations?.length > 0 && (
                  <div style={{ marginTop: 12, paddingTop: 10, borderTop: `1px solid ${COLORS.taupe}` }}>
                    <div style={{ fontSize: '0.7rem', color: COLORS.gray, marginBottom: 6 }}>CITATIONS</div>
                    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                      {s.citations.map((c: any, ci: number) => (
                        <span key={ci} style={{ fontSize: '0.72rem', padding: '3px 10px', borderRadius: 100, background: c.verified ? 'rgba(200,213,185,0.3)' : 'rgba(212,196,176,0.3)', color: c.verified ? '#4a7c4a' : COLORS.gray, border: `1px solid ${c.verified ? '#C8D5B9' : COLORS.taupe}` }}>
                          {c.reference} = {c.value} {c.verified ? '✓' : ''}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      ))
    })()}
  </div>
)}

          {/* Auto-advance from step 4 generation to step 5 */}
          {step === 4 && genResult && !loading && (
            <div style={{ display: 'none' }}>{(() => { setStep(5); return null })()}</div>
          )}
        </div>
      </main>
    </div>
  )
}