'use client'

import { useEffect, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import { API_BASE_URL, authHeaders } from '@/lib/api'

const C = {
  copper: '#B87333', gold: '#D4AF37', peach: '#FFD4B8', sage: '#C8D5B9',
  brown: '#3E2723', mid: '#6D4C41', bg: '#FEFEF8', bgAlt: '#F5EFE6',
  taupe: '#D4C4B0', gray: '#8D8D8D', dark: '#2C1810'
}

const Icons = {
  dashboard: <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>,
  upload:    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>,
  reports:   <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>,
  chat:      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>,
  settings:  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>,
  logout:    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>,
  send:      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>,
}

type Message = {
  id: string
  role: 'user' | 'assistant'
  content: string
  sources?: { indicator: string; value: number; unit: string; period: string; similarity: number }[]
  confidence?: number
  loading?: boolean
}

type Upload = { id: string; filename: string; status: string }

const SUGGESTED = [
  'What are the total Scope 1 GHG emissions?',
  'What is the electricity consumption trend?',
  'How much water was consumed across all facilities?',
  'What is the waste recycling rate?',
  'Show me the safety incident count.',
  'What is the natural gas consumption?',
]

export default function ChatPage() {
  const router = useRouter()
  const [user, setUser] = useState<any>(null)
  const [uploads, setUploads] = useState<Upload[]>([])
  const [selectedUpload, setSelectedUpload] = useState<string>('')
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [sessionId] = useState(() => `session_${Date.now()}`)
  const [loading, setLoading] = useState(false)
  const bottomRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLTextAreaElement>(null)

  useEffect(() => {
    const token = localStorage.getItem('truvexis_token')
    if (!token) { router.replace('/'); return }
    fetch(`${API_BASE_URL}/api/v1/auth/me`, { headers: authHeaders() })
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d) setUser(d); else router.replace('/') })
      .catch(() => router.replace('/'))
  }, [router])

  useEffect(() => {
    if (!user) return
    fetch(`${API_BASE_URL}/api/v1/ingest/uploads`, { headers: authHeaders() })
      .then(r => r.ok ? r.json() : { uploads: [] })
      .then(d => {
        const list = d.uploads || []
        setUploads(list)
        const completed = list.filter((u: Upload) => u.status === 'completed')
        if (completed.length > 0) setSelectedUpload(completed[0].id)
      })
  }, [user])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const logout = () => {
    localStorage.removeItem('truvexis_token')
    localStorage.removeItem('truvexis_user')
    router.replace('/')
  }

  const sendMessage = async (text?: string) => {
    const question = text || input.trim()
    if (!question || loading || !selectedUpload) return
    setInput('')

    const userMsg: Message = { id: Date.now().toString(), role: 'user', content: question }
    const loadingMsg: Message = { id: `loading_${Date.now()}`, role: 'assistant', content: '', loading: true }
    setMessages(prev => [...prev, userMsg, loadingMsg])
    setLoading(true)

    try {
      const res = await fetch(`${API_BASE_URL}/api/v1/chat/${selectedUpload}`, {
        method: 'POST',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ question, session_id: sessionId }),
      })

      if (!res.ok) throw new Error('Failed to get response')
      const data = await res.json()

      const assistantMsg: Message = {
        id: `assistant_${Date.now()}`,
        role: 'assistant',
        content: data.answer,
        sources: data.sources || [],
        confidence: data.confidence,
      }
      setMessages(prev => [...prev.filter(m => !m.loading), assistantMsg])
    } catch {
      setMessages(prev => [...prev.filter(m => !m.loading), {
        id: `error_${Date.now()}`,
        role: 'assistant',
        content: 'Sorry, something went wrong. Please try again.',
      }])
    }
    setLoading(false)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage() }
  }

  if (!user) return (
    <div style={{ minHeight: '100vh', background: C.bg, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
      <div style={{ width: 28, height: 28, border: `2px solid ${C.taupe}`, borderTopColor: C.copper, borderRadius: '50%', animation: 'spin 0.8s linear infinite' }}/>
      <style>{`@keyframes spin{to{transform:rotate(360deg)}}`}</style>
    </div>
  )

  return (
    <div style={{ display: 'flex', minHeight: '100vh', fontFamily: "'DM Sans',sans-serif", background: C.bgAlt }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,500;0,700;1,400&family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600&display=swap');
        *{box-sizing:border-box;margin:0;padding:0}
        @keyframes spin{to{transform:rotate(360deg)}}
        @keyframes fade-up{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:0.4}}
        .ni{display:flex;align-items:center;gap:12px;padding:11px 16px;border-radius:10px;cursor:pointer;transition:all .2s;color:rgba(254,254,248,.5);font-size:.875rem;border:none;background:transparent;width:100%;text-align:left;font-family:inherit}
        .ni:hover{background:rgba(255,255,255,.08);color:rgba(254,254,248,.85)}
        .ni.on{background:rgba(184,115,51,.2);color:#FFD4B8;font-weight:500}
        .msg-appear{animation:fade-up 0.3s ease forwards}
        .dot{width:6px;height:6px;border-radius:50%;background:${C.copper};animation:pulse 1.2s ease infinite}
        .dot:nth-child(2){animation-delay:.2s}
        .dot:nth-child(3){animation-delay:.4s}
        .send-btn{background:${C.brown};color:#FEFEF8;border:none;border-radius:10px;padding:10px 16px;cursor:pointer;font-family:inherit;transition:all .2s;display:flex;align-items:center;justify-content:center;flex-shrink:0}
        .send-btn:hover{background:${C.mid}}
        .send-btn:disabled{background:${C.taupe};cursor:not-allowed}
        .suggestion{background:${C.bg};border:1px solid ${C.taupe};border-radius:10px;padding:10px 14px;font-size:.8rem;color:${C.mid};cursor:pointer;transition:all .2s;text-align:left;font-family:inherit}
        .suggestion:hover{border-color:${C.copper};color:${C.brown};background:rgba(184,115,51,.04)}
        .sh{scrollbar-width:thin;scrollbar-color:${C.taupe} transparent}
        .sh::-webkit-scrollbar{width:4px}
        .sh::-webkit-scrollbar-thumb{background:${C.taupe};border-radius:4px}
        textarea{resize:none;outline:none;border:none;background:transparent;font-family:inherit;font-size:.9rem;color:${C.brown};width:100%;line-height:1.5}
        textarea::placeholder{color:${C.taupe}}
      `}</style>

      {/* SIDEBAR */}
      <aside style={{ width: 220, background: C.dark, display: 'flex', flexDirection: 'column', padding: '24px 16px', position: 'fixed', top: 0, left: 0, bottom: 0, zIndex: 100, borderRight: '1px solid rgba(255,255,255,.06)' }}>
        <div style={{ padding: '8px 8px 24px', borderBottom: '1px solid rgba(255,255,255,.08)', marginBottom: 20 }}>
          <span style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.25rem', color: '#FEFEF8' }}>Truvexis</span>
          <div style={{ fontSize: '.68rem', color: 'rgba(254,254,248,.3)', marginTop: 2, letterSpacing: '.08em', textTransform: 'uppercase' }}>ESG Platform</div>
        </div>
        <nav style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 4 }}>
          {[
            { id: 'dashboard', label: 'Dashboard',  icon: Icons.dashboard, path: '/dashboard' },
            { id: 'upload',    label: 'New Report',  icon: Icons.upload,    path: '/dashboard/upload' },
            { id: 'chat',      label: 'Ask AI',      icon: Icons.chat,      path: '/dashboard/chat' },
          ].map(item => (
            <button key={item.id} className={`ni ${item.id === 'chat' ? 'on' : ''}`} onClick={() => router.push(item.path)}>
              {item.icon}{item.label}
            </button>
          ))}
        </nav>
        <div style={{ borderTop: '1px solid rgba(255,255,255,.07)', paddingTop: 16 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: 8, marginBottom: 8 }}>
            <div style={{ width: 34, height: 34, borderRadius: '50%', background: 'linear-gradient(135deg,#B87333,#D4AF37)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '.85rem', fontWeight: 600, color: '#FEFEF8', flexShrink: 0 }}>
              {user.name.charAt(0).toUpperCase()}
            </div>
            <div style={{ overflow: 'hidden' }}>
              <div style={{ fontSize: '.82rem', color: '#FEFEF8', fontWeight: 500, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{user.name}</div>
              <div style={{ fontSize: '.7rem', color: 'rgba(254,254,248,.35)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{user.company || user.email}</div>
            </div>
          </div>
          <button className="ni" onClick={logout} style={{ color: 'rgba(232,156,127,.7)' }}>{Icons.logout} Sign Out</button>
        </div>
      </aside>

      {/* MAIN */}
      <main style={{ marginLeft: 220, flex: 1, display: 'flex', flexDirection: 'column', height: '100vh', maxWidth: 'calc(100vw - 220px)' }}>

        {/* Top bar */}
        <div style={{ padding: '20px 32px', borderBottom: `1px solid ${C.taupe}`, background: C.bg, display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexShrink: 0 }}>
          <div>
            <h1 style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.4rem', color: C.brown, fontWeight: 500 }}>Ask AI</h1>
            <p style={{ fontSize: '.78rem', color: C.gray, marginTop: 2 }}>Ask questions about your ESG data in plain English</p>
          </div>
          {/* Upload selector */}
          {uploads.length > 0 && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <span style={{ fontSize: '.78rem', color: C.gray }}>Dataset:</span>
              <select value={selectedUpload} onChange={e => { setSelectedUpload(e.target.value); setMessages([]) }}
                style={{ padding: '8px 12px', border: `1.5px solid ${C.taupe}`, borderRadius: 10, fontSize: '.82rem', fontFamily: 'inherit', color: C.brown, background: C.bg, outline: 'none', cursor: 'pointer', maxWidth: 220 }}>
                {uploads.filter(u => u.status === 'completed').map(u => (
                  <option key={u.id} value={u.id}>{u.filename}</option>
                ))}
              </select>
            </div>
          )}
        </div>

        {/* Messages area */}
        <div className="sh" style={{ flex: 1, overflowY: 'auto', padding: '24px 32px', display: 'flex', flexDirection: 'column', gap: 20 }}>

          {/* Empty state */}
          {messages.length === 0 && (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', flex: 1, gap: 32 }}>
              <div style={{ textAlign: 'center' }}>
                <div style={{ width: 56, height: 56, borderRadius: '50%', background: `rgba(184,115,51,0.1)`, border: `2px solid rgba(184,115,51,0.2)`, display: 'flex', alignItems: 'center', justifyContent: 'center', margin: '0 auto 16px', fontSize: '1.5rem' }}>✦</div>
                <h2 style={{ fontFamily: "'Playfair Display',serif", fontSize: '1.25rem', color: C.brown, marginBottom: 8 }}>Ask anything about your ESG data</h2>
                <p style={{ fontSize: '.875rem', color: C.gray, maxWidth: 400 }}>
                  {selectedUpload ? 'Your validated facility data is ready. Ask questions in plain English.' : 'No completed uploads found. Upload a file first.'}
                </p>
              </div>
              {selectedUpload && (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 10, maxWidth: 700, width: '100%' }}>
                  {SUGGESTED.map((s, i) => (
                    <button key={i} className="suggestion" onClick={() => sendMessage(s)}>{s}</button>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Messages */}
          {messages.map(msg => (
            <div key={msg.id} className="msg-appear" style={{ display: 'flex', flexDirection: 'column', alignItems: msg.role === 'user' ? 'flex-end' : 'flex-start', gap: 8 }}>
              {/* Bubble */}
              <div style={{
                maxWidth: '72%',
                padding: '14px 18px',
                borderRadius: msg.role === 'user' ? '16px 16px 4px 16px' : '16px 16px 16px 4px',
                background: msg.role === 'user' ? C.brown : C.bg,
                color: msg.role === 'user' ? '#FEFEF8' : C.brown,
                border: msg.role === 'assistant' ? `1px solid ${C.taupe}` : 'none',
                fontSize: '.9rem',
                lineHeight: 1.7,
              }}>
                {msg.loading ? (
                  <div style={{ display: 'flex', gap: 6, alignItems: 'center', padding: '4px 0' }}>
                    <div className="dot"/>
                    <div className="dot"/>
                    <div className="dot"/>
                  </div>
                ) : (
                  <span style={{ whiteSpace: 'pre-line' }}>{msg.content}</span>
                )}
              </div>

              {/* Sources */}
              {msg.sources && msg.sources.length > 0 && (
                <div style={{ maxWidth: '72%', display: 'flex', flexDirection: 'column', gap: 6 }}>
                  <div style={{ fontSize: '.7rem', color: C.gray, letterSpacing: '.08em', textTransform: 'uppercase' }}>
                    Sources · {(msg.confidence! * 100).toFixed(0)}% confidence
                  </div>
                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    {msg.sources.slice(0, 4).map((src, i) => (
                      <div key={i} style={{ padding: '8px 12px', background: C.bg, border: `1px solid ${C.taupe}`, borderRadius: 8, fontSize: '.75rem' }}>
                        <div style={{ color: C.copper, fontWeight: 500, marginBottom: 2 }}>{src.indicator}</div>
                        <div style={{ color: C.brown }}>{src.value.toLocaleString()} {src.unit}</div>
                        <div style={{ color: C.gray, marginTop: 1 }}>{src.period} · {(src.similarity * 100).toFixed(0)}% match</div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ))}
          <div ref={bottomRef}/>
        </div>

        {/* Input area */}
        <div style={{ padding: '16px 32px 24px', background: C.bg, borderTop: `1px solid ${C.taupe}`, flexShrink: 0 }}>
          <div style={{ display: 'flex', gap: 12, alignItems: 'flex-end', background: C.bgAlt, border: `1.5px solid ${C.taupe}`, borderRadius: 14, padding: '12px 16px', transition: 'border-color .2s' }}
            onFocus={() => {}} >
            <textarea
              ref={inputRef}
              rows={1}
              placeholder={selectedUpload ? 'Ask about your ESG data...' : 'Upload a file first to start chatting'}
              value={input}
              onChange={e => {
                setInput(e.target.value)
                e.target.style.height = 'auto'
                e.target.style.height = Math.min(e.target.scrollHeight, 120) + 'px'
              }}
              onKeyDown={handleKeyDown}
              disabled={!selectedUpload || loading}
              style={{ maxHeight: 120 }}
            />
            <button className="send-btn" onClick={() => sendMessage()} disabled={!input.trim() || loading || !selectedUpload}>
              {Icons.send}
            </button>
          </div>
          <div style={{ fontSize: '.72rem', color: C.taupe, marginTop: 8, textAlign: 'center' }}>
            Press Enter to send · Shift+Enter for new line · Answers grounded in your validated data
          </div>
        </div>
      </main>
    </div>
  )
}