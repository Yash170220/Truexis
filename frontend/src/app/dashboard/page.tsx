'use client'

import { useEffect, useState, useCallback } from 'react'
import { useRouter } from 'next/navigation'
import { API_BASE_URL, authHeaders } from '@/lib/api'
import {
  BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, Tooltip, ResponsiveContainer, Legend
} from 'recharts'

type User = { id: string; name: string; email: string; company: string | null; industry: string | null }
type Upload = { id: string; filename: string; status: string; upload_time: string }
type ValidationSummary = { total_records: number; valid_records: number; records_with_errors: number; records_with_warnings: number; validation_pass_rate: number }
type NormSummary = { total_records: number; successfully_normalized: number; failed_normalization: number; normalization_rate: number }

const C = { copper: '#B87333', gold: '#D4AF37', peach: '#FFD4B8', sage: '#C8D5B9', brown: '#3E2723', mid: '#6D4C41', bg: '#FEFEF8', bgAlt: '#F5EFE6', taupe: '#D4C4B0', gray: '#8D8D8D', dark: '#2C1810' }
const CHART_COLORS = ['#B87333', '#D4AF37', '#C8D5B9', '#E89C7F']

const Icons = {
  dashboard: <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>,
  upload:    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>,
  reports:   <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>,
  chat:      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>,
  settings:  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>,
  logout:    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>,
}

const NAV_ITEMS = [
  { id: 'dashboard', label: 'Dashboard',  icon: Icons.dashboard, path: '/dashboard' },
  { id: 'upload',    label: 'New Report', icon: Icons.upload,    path: '/dashboard/upload' },
  { id: 'chat',      label: 'Ask AI',     icon: Icons.chat,      path: '/dashboard/chat' },
]

const FRAMEWORKS = [
  { id: 'BRSR',  label: 'BRSR Core',          desc: 'India SEBI Mandate' },
  { id: 'GRI',   label: 'GRI Universal 2021',  desc: 'Global Standards' },
  { id: 'CSRD',  label: 'CSRD',                desc: 'EU Directive' },
]

export default function DashboardPage() {
  const router = useRouter()
  const [user, setUser]           = useState<User | null>(null)
  const [uploads, setUploads]     = useState<Upload[]>([])
  const [latestUpload, setLatest] = useState<Upload | null>(null)
  const [valSummary, setVal]      = useState<ValidationSummary | null>(null)
  const [normSummary, setNorm]    = useState<NormSummary | null>(null)
  const [loading, setLoading]     = useState(true)

  // Download modal
  const [showDlModal, setShowDlModal]   = useState(false)
  const [dlUploadId, setDlUploadId]     = useState('')
  const [dlFramework, setDlFramework]   = useState('BRSR')
  const [dlLoading, setDlLoading]       = useState(false)
  const [dlError, setDlError]           = useState('')

  useEffect(() => {
    const token = localStorage.getItem('truvexis_token')
    if (!token) { router.replace('/'); return }
    fetch(`${API_BASE_URL}/api/v1/auth/me`, { headers: authHeaders() })
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d) setUser(d); else router.replace('/') })
      .catch(() => router.replace('/'))
  }, [router])

  const loadData = useCallback(async () => {
    setLoading(true)
    try {
      const res = await fetch(`${API_BASE_URL}/api/v1/ingest/uploads`, { headers: authHeaders() })
      if (res.ok) {
        const data = await res.json()
        const list: Upload[] = data.uploads || data || []
        setUploads(list)
        const completed = list.filter((u: Upload) => u.status === 'completed')
        if (completed.length > 0) {
          const latest = completed[0]
          setLatest(latest)
          const [vr, nr] = await Promise.all([
            fetch(`${API_BASE_URL}/api/v1/validation/${latest.id}`, { headers: authHeaders() }),
            fetch(`${API_BASE_URL}/api/v1/normalization/${latest.id}`, { headers: authHeaders() }),
          ])
          if (vr.ok) { const d = await vr.json(); setVal(d.summary || d) }
          if (nr.ok) { const d = await nr.json(); setNorm(d.summary || d) }
        }
      }
    } catch (e) { console.error(e) }
    setLoading(false)
  }, [])

  useEffect(() => { if (user) loadData() }, [user, loadData])

  const logout = () => {
    localStorage.removeItem('truvexis_token')
    localStorage.removeItem('truvexis_user')
    router.replace('/')
  }

  const openDlModal = () => {
    const completed = uploads.filter(u => u.status === 'completed')
    setDlUploadId(completed[0]?.id || '')
    setDlFramework('BRSR')
    setDlError('')
    setShowDlModal(true)
  }

  const handleDownload = async () => {
    if (!dlUploadId) { setDlError('Please select a file.'); return }
    setDlLoading(true)
    setDlError('')
    try {
      const res = await fetch(
        `${API_BASE_URL}/api/v1/export/${dlUploadId}/pdf?framework=${dlFramework}`,
        { headers: authHeaders() },
      )
      if (!res.ok) {
        const d = await res.json().catch(() => ({}))
        throw new Error(d.detail || 'Download failed')
      }
      const blob = await res.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      const cd = res.headers.get('Content-Disposition') || ''
      const match = cd.match(/filename="?([^"]+)"?/)
      a.download = match ? match[1] : `Truvexis_${dlFramework}_report.pdf`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
      setShowDlModal(false)
    } catch (e: any) {
      setDlError(e.message)
    }
    setDlLoading(false)
  }

  const passRate = valSummary?.validation_pass_rate ?? 0
  const normRate = normSummary ? (normSummary.successfully_normalized / Math.max(normSummary.total_records, 1)) * 100 : 0

  const pieData = valSummary ? [
    { name: 'Valid',    value: valSummary.valid_records },
    { name: 'Errors',  value: valSummary.records_with_errors },
    { name: 'Warnings',value: valSummary.records_with_warnings },
  ] : []

  const barData = [
    { stage: 'Ingest',  value: normSummary?.total_records || 0 },
    { stage: 'Matched', value: normSummary?.successfully_normalized || 0 },
    { stage: 'Valid',   value: valSummary?.valid_records || 0 },
    { stage: 'Errors',  value: valSummary?.records_with_errors || 0 },
  ]

  if (!user) return (
    <div style={{ minHeight:'100vh', background:C.bg, display:'flex', alignItems:'center', justifyContent:'center', fontFamily:"'DM Sans',sans-serif" }}>
      <div style={{ width:32, height:32, border:`2px solid ${C.taupe}`, borderTopColor:C.copper, borderRadius:'50%', animation:'spin 0.8s linear infinite' }}/>
      <style>{`@keyframes spin{to{transform:rotate(360deg)}}`}</style>
    </div>
  )

  return (
    <div style={{ display:'flex', minHeight:'100vh', fontFamily:"'DM Sans',sans-serif", background:C.bgAlt }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,500;0,700;1,400&family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600&display=swap');
        *{box-sizing:border-box;margin:0;padding:0}
        @keyframes spin{to{transform:rotate(360deg)}}
        @keyframes fu{from{opacity:0;transform:translateY(14px)}to{opacity:1;transform:translateY(0)}}
        .fu{animation:fu 0.5s ease forwards}
        .d1{animation-delay:.05s;opacity:0}.d2{animation-delay:.1s;opacity:0}.d3{animation-delay:.15s;opacity:0}.d4{animation-delay:.2s;opacity:0}.d5{animation-delay:.25s;opacity:0}
        .ni{display:flex;align-items:center;gap:12px;padding:11px 16px;border-radius:10px;cursor:pointer;transition:all .2s;color:rgba(254,254,248,.5);font-size:.875rem;border:none;background:transparent;width:100%;text-align:left;font-family:inherit}
        .ni:hover{background:rgba(255,255,255,.08);color:rgba(254,254,248,.85)}
        .ni.on{background:rgba(184,115,51,.2);color:#FFD4B8;font-weight:500}
        .sc{background:#FEFEF8;border:1px solid #D4C4B0;border-radius:16px;padding:24px;transition:all .3s}
        .sc:hover{transform:translateY(-3px);box-shadow:0 12px 32px rgba(62,39,35,.1)}
        .cc{background:#FEFEF8;border:1px solid #D4C4B0;border-radius:16px;padding:24px}
        .ur{display:flex;align-items:center;gap:12px;padding:14px 16px;border-radius:10px;border:1px solid #EDE4D3;margin-bottom:8px;background:#FEFEF8;transition:all .2s}
        .ur:hover{border-color:#D4C4B0;box-shadow:0 4px 12px rgba(62,39,35,.07)}
        .bdg{display:inline-flex;align-items:center;padding:3px 10px;border-radius:100px;font-size:.7rem;font-weight:600;letter-spacing:.04em;text-transform:uppercase}
        .pb{height:6px;border-radius:100px;background:#EDE4D3;overflow:hidden}
        .pf{height:100%;border-radius:100px;transition:width 1s ease}
        .sh{scrollbar-width:none;-ms-overflow-style:none}
        .sh::-webkit-scrollbar{display:none}
      `}</style>

      {/* SIDEBAR */}
      <aside style={{ width:220, background:C.dark, display:'flex', flexDirection:'column', padding:'24px 16px', position:'fixed', top:0, left:0, bottom:0, zIndex:100, borderRight:'1px solid rgba(255,255,255,.06)' }}>
        <div style={{ padding:'8px 8px 24px', borderBottom:'1px solid rgba(255,255,255,.08)', marginBottom:20 }}>
          <span style={{ fontFamily:"'Playfair Display',serif", fontSize:'1.25rem', color:'#FEFEF8' }}>Truvexis</span>
          <div style={{ fontSize:'.68rem', color:'rgba(254,254,248,.3)', marginTop:2, letterSpacing:'.08em', textTransform:'uppercase' }}>ESG Platform</div>
        </div>

        <nav style={{ flex:1, display:'flex', flexDirection:'column', gap:4 }}>
          {NAV_ITEMS.map(item => (
            <button key={item.id} className={`ni ${item.id === 'dashboard' ? 'on' : ''}`}
              onClick={() => router.push(item.path)}>
              {item.icon}{item.label}
            </button>
          ))}
        </nav>

        <div style={{ borderTop:'1px solid rgba(255,255,255,.07)', paddingTop:16 }}>
          <div style={{ display:'flex', alignItems:'center', gap:10, padding:8, marginBottom:8 }}>
            <div style={{ width:34, height:34, borderRadius:'50%', background:'linear-gradient(135deg,#B87333,#D4AF37)', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'.85rem', fontWeight:600, color:'#FEFEF8', flexShrink:0 }}>
              {user.name.charAt(0).toUpperCase()}
            </div>
            <div style={{ overflow:'hidden' }}>
              <div style={{ fontSize:'.82rem', color:'#FEFEF8', fontWeight:500, whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis' }}>{user.name}</div>
              <div style={{ fontSize:'.7rem', color:'rgba(254,254,248,.35)', whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis' }}>{user.company || user.email}</div>
            </div>
          </div>
          <button className="ni" onClick={logout} style={{ color:'rgba(232,156,127,.7)' }}>
            {Icons.logout} Sign Out
          </button>
        </div>
      </aside>

      {/* MAIN */}
      <main style={{ marginLeft:220, flex:1, padding:'32px 36px', minHeight:'100vh', maxWidth:'calc(100vw - 220px)', overflowX:'hidden' }}>

        {/* Header */}
        <div className="fu d1" style={{ marginBottom:32, display:'flex', alignItems:'flex-start', justifyContent:'space-between' }}>
          <div>
            <h1 style={{ fontFamily:"'Playfair Display',serif", fontSize:'1.75rem', color:C.brown, fontWeight:500, marginBottom:4 }}>
              Good {new Date().getHours() < 12 ? 'morning' : new Date().getHours() < 17 ? 'afternoon' : 'evening'}, {user.name.split(' ')[0]}
            </h1>
            <p style={{ color:C.gray, fontSize:'.875rem' }}>
              {latestUpload ? `Showing data from: ${latestUpload.filename}` : 'No uploads yet. Start a new report to see your data.'}
            </p>
          </div>
          {/* ✅ FIXED: routes to /dashboard/upload */}
          <button
            onClick={() => router.push('/dashboard/upload')}
            style={{ display:'flex', alignItems:'center', gap:8, background:C.brown, color:'#FEFEF8', border:'none', borderRadius:10, padding:'11px 20px', fontSize:'.875rem', fontWeight:500, cursor:'pointer', fontFamily:'inherit', transition:'all .2s' }}
            onMouseEnter={e => (e.currentTarget.style.background = C.mid)}
            onMouseLeave={e => (e.currentTarget.style.background = C.brown)}>
            {Icons.upload} New Report
          </button>
        </div>

        {loading ? (
          <div style={{ display:'flex', alignItems:'center', justifyContent:'center', height:300, gap:12, flexDirection:'column' }}>
            <div style={{ width:28, height:28, border:`2px solid ${C.taupe}`, borderTopColor:C.copper, borderRadius:'50%', animation:'spin 0.8s linear infinite' }}/>
            <p style={{ color:C.gray, fontSize:'.85rem' }}>Loading your data...</p>
          </div>
        ) : (
          <>
            {/* STAT CARDS */}
            <div className="fu d2" style={{ display:'grid', gridTemplateColumns:'repeat(4,1fr)', gap:16, marginBottom:24 }}>
              {[
                { label:'Total Records',  value:(normSummary?.total_records||0).toLocaleString(),         sub:'Normalized data points',  color:C.copper,   icon:'◈' },
                { label:'Pass Rate',      value:`${passRate.toFixed(1)}%`,                                sub:`${(valSummary?.valid_records||0).toLocaleString()} valid`, color:'#4a7c4a', icon:'✓' },
                { label:'Errors Found',   value:(valSummary?.records_with_errors||0).toLocaleString(),    sub:'Require attention',        color:'#a0402a',  icon:'⚠' },
                { label:'Warnings',       value:(valSummary?.records_with_warnings||0).toLocaleString(),  sub:'Need review',              color:C.gold,     icon:'◎' },
              ].map((card, i) => (
                <div key={i} className="sc">
                  <div style={{ display:'flex', alignItems:'flex-start', justifyContent:'space-between', marginBottom:16 }}>
                    <span style={{ fontSize:'.72rem', letterSpacing:'.1em', textTransform:'uppercase', color:C.gray }}>{card.label}</span>
                    <span style={{ fontSize:'1.1rem', color:card.color }}>{card.icon}</span>
                  </div>
                  <div style={{ fontFamily:"'Playfair Display',serif", fontSize:'clamp(1.6rem,3vw,2.2rem)', fontWeight:500, color:card.color, lineHeight:1, marginBottom:8 }}>{card.value}</div>
                  <div style={{ fontSize:'.78rem', color:C.gray }}>{card.sub}</div>
                </div>
              ))}
            </div>

            {/* PROGRESS BARS */}
            <div className="fu d3" style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:16, marginBottom:24 }}>
              <div className="cc">
                <div style={{ fontSize:'.72rem', letterSpacing:'.1em', textTransform:'uppercase', color:C.gray, marginBottom:4 }}>Validation Pass Rate</div>
                <div style={{ fontFamily:"'Playfair Display',serif", fontSize:'1.5rem', color:C.brown, marginBottom:16 }}>{passRate.toFixed(2)}%</div>
                <div className="pb" style={{ marginBottom:12 }}><div className="pf" style={{ width:`${passRate}%`, background:`linear-gradient(to right,${C.sage},#4a7c4a)` }}/></div>
                <div style={{ display:'flex', justifyContent:'space-between', fontSize:'.78rem', color:C.gray }}>
                  <span>✓ {(valSummary?.valid_records||0).toLocaleString()} valid</span>
                  <span>✕ {(valSummary?.records_with_errors||0).toLocaleString()} errors</span>
                </div>
              </div>
              <div className="cc">
                <div style={{ fontSize:'.72rem', letterSpacing:'.1em', textTransform:'uppercase', color:C.gray, marginBottom:4 }}>Normalization Rate</div>
                <div style={{ fontFamily:"'Playfair Display',serif", fontSize:'1.5rem', color:C.brown, marginBottom:16 }}>{normRate.toFixed(1)}%</div>
                <div className="pb" style={{ marginBottom:12 }}><div className="pf" style={{ width:`${normRate}%`, background:`linear-gradient(to right,${C.peach},${C.copper})` }}/></div>
                <div style={{ display:'flex', justifyContent:'space-between', fontSize:'.78rem', color:C.gray }}>
                  <span>✓ {(normSummary?.successfully_normalized||0).toLocaleString()} success</span>
                  <span>✕ {(normSummary?.failed_normalization||0).toLocaleString()} failed</span>
                </div>
              </div>
            </div>

            {/* CHARTS */}
            <div className="fu d4" style={{ display:'grid', gridTemplateColumns:'1.5fr 1fr', gap:16, marginBottom:24 }}>
              <div className="cc">
                <div style={{ fontSize:'.72rem', letterSpacing:'.1em', textTransform:'uppercase', color:C.gray, marginBottom:4 }}>Pipeline Overview</div>
                <div style={{ fontFamily:"'Playfair Display',serif", fontSize:'1rem', color:C.brown, marginBottom:20 }}>Records by Stage</div>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={barData} barSize={32}>
                    <XAxis dataKey="stage" tick={{ fontSize:11, fill:C.gray, fontFamily:'DM Sans' }} axisLine={false} tickLine={false}/>
                    <YAxis tick={{ fontSize:11, fill:C.gray, fontFamily:'DM Sans' }} axisLine={false} tickLine={false} tickFormatter={v => v>=1000?`${(v/1000).toFixed(0)}k`:v}/>
                    <Tooltip contentStyle={{ background:C.brown, border:'none', borderRadius:8, color:'#FEFEF8', fontSize:12, fontFamily:'DM Sans' }} cursor={{ fill:'rgba(212,196,176,.15)' }}/>
                    <Bar dataKey="value" radius={[6,6,0,0]}>
                      {barData.map((_,i) => <Cell key={i} fill={CHART_COLORS[i%CHART_COLORS.length]}/>)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
              <div className="cc">
                <div style={{ fontSize:'.72rem', letterSpacing:'.1em', textTransform:'uppercase', color:C.gray, marginBottom:4 }}>Validation Results</div>
                <div style={{ fontFamily:"'Playfair Display',serif", fontSize:'1rem', color:C.brown, marginBottom:16 }}>Record Distribution</div>
                {pieData.length > 0 ? (
                  <ResponsiveContainer width="100%" height={180}>
                    <PieChart>
                      <Pie data={pieData} cx="50%" cy="50%" innerRadius={50} outerRadius={80} paddingAngle={3} dataKey="value">
                        {pieData.map((_,i) => <Cell key={i} fill={[C.sage,'#a0402a',C.gold][i]}/>)}
                      </Pie>
                      <Tooltip contentStyle={{ background:C.brown, border:'none', borderRadius:8, color:'#FEFEF8', fontSize:12, fontFamily:'DM Sans' }}/>
                      <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize:'.75rem', fontFamily:'DM Sans', color:C.gray }}/>
                    </PieChart>
                  </ResponsiveContainer>
                ) : (
                  <div style={{ height:180, display:'flex', alignItems:'center', justifyContent:'center', color:C.gray, fontSize:'.85rem' }}>No data yet</div>
                )}
              </div>
            </div>

            {/* BOTTOM ROW */}
            <div className="fu d5" style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:16 }}>
              <div className="cc">
                <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:20 }}>
                  <div>
                    <div style={{ fontSize:'.72rem', letterSpacing:'.1em', textTransform:'uppercase', color:C.gray, marginBottom:4 }}>Recent Activity</div>
                    <div style={{ fontFamily:"'Playfair Display',serif", fontSize:'1rem', color:C.brown }}>Uploaded Files</div>
                  </div>
                  {/* ✅ FIXED */}
                  <button onClick={() => router.push('/dashboard/reports')} style={{ fontSize:'.78rem', color:C.copper, background:'none', border:'none', cursor:'pointer', fontFamily:'inherit' }}>View all →</button>
                </div>
                <div className="sh" style={{ maxHeight:240, overflowY:'auto' }}>
                  {uploads.length === 0 ? (
                    <div style={{ textAlign:'center', color:C.gray, fontSize:'.85rem', padding:'40px 0' }}>
                      No uploads yet.<br/>
                      {/* ✅ FIXED */}
                      <button onClick={() => router.push('/dashboard/upload')} style={{ color:C.copper, background:'none', border:'none', cursor:'pointer', fontFamily:'inherit', fontSize:'.85rem', marginTop:8, textDecoration:'underline' }}>
                        Start your first report
                      </button>
                    </div>
                  ) : uploads.slice(0,5).map((u) => (
                    <div key={u.id} className="ur">
                      <div style={{ width:36, height:36, borderRadius:8, background:C.bgAlt, border:`1px solid ${C.taupe}`, display:'flex', alignItems:'center', justifyContent:'center', fontSize:'.8rem', color:C.copper, flexShrink:0 }}>
                        {u.filename.endsWith('.csv')?'CSV':u.filename.endsWith('.pdf')?'PDF':'XLS'}
                      </div>
                      <div style={{ flex:1, overflow:'hidden' }}>
                        <div style={{ fontSize:'.85rem', color:C.brown, fontWeight:500, whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis' }}>{u.filename}</div>
                        <div style={{ fontSize:'.72rem', color:C.gray, marginTop:2 }}>{new Date(u.upload_time).toLocaleDateString('en-IN',{day:'numeric',month:'short',year:'numeric'})}</div>
                      </div>
                      <span className="bdg" style={{ background: u.status==='completed'?'rgba(200,213,185,.3)':u.status==='failed'?'rgba(232,156,127,.2)':'rgba(212,175,55,.15)', color: u.status==='completed'?'#4a7c4a':u.status==='failed'?'#a0402a':'#8a6500' }}>
                        {u.status}
                      </span>
                    </div>
                  ))}
                </div>
              </div>

              <div className="cc">
                <div style={{ fontSize:'.72rem', letterSpacing:'.1em', textTransform:'uppercase', color:C.gray, marginBottom:4 }}>Frameworks Covered</div>
                <div style={{ fontFamily:"'Playfair Display',serif", fontSize:'1rem', color:C.brown, marginBottom:20 }}>Compliance Status</div>
                <div style={{ display:'flex', flexDirection:'column', gap:12 }}>
                  {[
                    { name:'BRSR Core',         desc:'India SEBI Mandate', color:C.copper },
                    { name:'GRI Universal 2021', desc:'Global Standards',  color:C.gold },
                    { name:'CSRD',               desc:'EU Directive',       color:C.sage },
                  ].map((fw,i) => (
                    <div key={i} style={{ display:'flex', alignItems:'center', gap:12, padding:'12px 14px', background:C.bgAlt, borderRadius:10, border:`1px solid ${C.taupe}` }}>
                      <div style={{ width:8, height:8, borderRadius:'50%', background:latestUpload?fw.color:C.taupe, flexShrink:0 }}/>
                      <div style={{ flex:1 }}>
                        <div style={{ fontSize:'.85rem', fontWeight:500, color:C.brown }}>{fw.name}</div>
                        <div style={{ fontSize:'.72rem', color:C.gray }}>{fw.desc}</div>
                      </div>
                      <span style={{ fontSize:'.7rem', color:latestUpload?'#4a7c4a':C.gray, background:latestUpload?'rgba(200,213,185,.3)':'rgba(141,141,141,.1)', padding:'3px 10px', borderRadius:100, fontWeight:600 }}>
                        {latestUpload?'Ready':'No data'}
                      </span>
                    </div>
                  ))}
                </div>
                {latestUpload && (
                  <button onClick={openDlModal}
                    style={{ width:'100%', marginTop:16, padding:'11px', background:C.brown, color:'#FEFEF8', border:'none', borderRadius:10, fontSize:'.875rem', fontWeight:500, cursor:'pointer', fontFamily:'inherit', transition:'all .2s' }}
                    onMouseEnter={e => (e.currentTarget.style.background=C.mid)}
                    onMouseLeave={e => (e.currentTarget.style.background=C.brown)}>
                    Download Reports →
                  </button>
                )}
              </div>
            </div>
          </>
        )}
      </main>

      {/* DOWNLOAD MODAL */}
      {showDlModal && (
        <div
          onClick={() => setShowDlModal(false)}
          style={{ position:'fixed', inset:0, background:'rgba(44,24,16,.55)', zIndex:200, display:'flex', alignItems:'center', justifyContent:'center', backdropFilter:'blur(4px)' }}
        >
          <div
            onClick={e => e.stopPropagation()}
            style={{ background:C.bg, borderRadius:20, padding:'32px 36px', width:480, maxWidth:'calc(100vw - 32px)', boxShadow:'0 24px 64px rgba(44,24,16,.25)', border:`1px solid ${C.taupe}` }}
          >
            {/* Header */}
            <div style={{ marginBottom:24 }}>
              <div style={{ fontSize:'.72rem', letterSpacing:'.12em', textTransform:'uppercase', color:C.copper, marginBottom:6 }}>Export</div>
              <h2 style={{ fontFamily:"'Playfair Display',serif", fontSize:'1.3rem', color:C.brown, fontWeight:500 }}>Download ESG Report</h2>
              <p style={{ fontSize:'.82rem', color:C.gray, marginTop:4 }}>Select a file and compliance framework to download your generated PDF report.</p>
            </div>

            {/* File picker */}
            <div style={{ marginBottom:20 }}>
              <label style={{ display:'block', fontSize:'.72rem', letterSpacing:'.08em', textTransform:'uppercase', color:C.gray, marginBottom:8 }}>Uploaded File</label>
              <select
                value={dlUploadId}
                onChange={e => setDlUploadId(e.target.value)}
                style={{ width:'100%', padding:'10px 14px', border:`1px solid ${C.taupe}`, borderRadius:10, fontSize:'.875rem', color:C.brown, background:C.bgAlt, fontFamily:'inherit', outline:'none', cursor:'pointer' }}
              >
                {uploads.filter(u => u.status === 'completed').length === 0 && (
                  <option value="">No completed uploads</option>
                )}
                {uploads.filter(u => u.status === 'completed').map(u => (
                  <option key={u.id} value={u.id}>{u.filename}</option>
                ))}
              </select>
            </div>

            {/* Framework picker */}
            <div style={{ marginBottom:24 }}>
              <label style={{ display:'block', fontSize:'.72rem', letterSpacing:'.08em', textTransform:'uppercase', color:C.gray, marginBottom:8 }}>Compliance Framework</label>
              <div style={{ display:'flex', flexDirection:'column', gap:8 }}>
                {FRAMEWORKS.map(fw => (
                  <label key={fw.id} style={{ display:'flex', alignItems:'center', gap:12, padding:'11px 14px', borderRadius:10, border:`1.5px solid ${dlFramework === fw.id ? C.copper : C.taupe}`, background:dlFramework === fw.id ? 'rgba(184,115,51,.06)' : C.bgAlt, cursor:'pointer', transition:'all .2s' }}>
                    <input
                      type="radio"
                      name="framework"
                      value={fw.id}
                      checked={dlFramework === fw.id}
                      onChange={() => setDlFramework(fw.id)}
                      style={{ accentColor:C.copper, width:16, height:16 }}
                    />
                    <div>
                      <div style={{ fontSize:'.875rem', fontWeight:500, color:C.brown }}>{fw.label}</div>
                      <div style={{ fontSize:'.72rem', color:C.gray }}>{fw.desc}</div>
                    </div>
                  </label>
                ))}
              </div>
            </div>

            {dlError && (
              <div style={{ marginBottom:16, padding:'10px 14px', background:'rgba(232,156,127,.15)', border:'1px solid rgba(232,156,127,.4)', borderRadius:8, fontSize:'.82rem', color:'#a0402a' }}>
                {dlError}
              </div>
            )}

            {/* Actions */}
            <div style={{ display:'flex', gap:10, justifyContent:'flex-end' }}>
              <button
                onClick={() => setShowDlModal(false)}
                style={{ padding:'10px 20px', background:'none', border:`1.5px solid ${C.taupe}`, borderRadius:10, fontSize:'.875rem', cursor:'pointer', fontFamily:'inherit', color:C.brown }}
              >
                Cancel
              </button>
              <button
                onClick={handleDownload}
                disabled={dlLoading || !dlUploadId}
                style={{ display:'flex', alignItems:'center', gap:8, padding:'10px 22px', background:dlLoading || !dlUploadId ? C.taupe : C.brown, color:'#FEFEF8', border:'none', borderRadius:10, fontSize:'.875rem', fontWeight:500, cursor:dlLoading || !dlUploadId ? 'not-allowed' : 'pointer', fontFamily:'inherit', transition:'all .2s' }}
              >
                {dlLoading
                  ? <><span style={{ width:14, height:14, border:'2px solid rgba(255,255,255,.3)', borderTopColor:'#fff', borderRadius:'50%', animation:'spin 0.7s linear infinite', display:'inline-block' }}/> Preparing...</>
                  : '↓ Download PDF'
                }
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}