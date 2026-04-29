'use client'
import { useEffect, useRef, useState } from 'react'
import { API_BASE_URL } from '@/lib/api'

export default function Home() {
  const [navSolid, setNavSolid] = useState(false)
  const [authOpen, setAuthOpen] = useState(false)
  const [authTab, setAuthTab] = useState<'signin'|'signup'>('signin')
  const [authForm, setAuthForm] = useState({ name:'', company:'', industry:'', email:'', password:'', confirm:'' })
  const [authError, setAuthError] = useState('')
  const [authLoading, setAuthLoading] = useState(false)

  useEffect(() => {
    const fn = () => setNavSolid(window.scrollY > 60)
    window.addEventListener('scroll', fn, { passive: true })
    return () => window.removeEventListener('scroll', fn)
  }, [])

  useEffect(() => {
    const fn = (e: KeyboardEvent) => { if (e.key === 'Escape') setAuthOpen(false) }
    window.addEventListener('keydown', fn)
    return () => window.removeEventListener('keydown', fn)
  }, [])

  // Check if already logged in
  useEffect(() => {
    const token = localStorage.getItem('truvexis_token')
    if (token) window.location.href = '/dashboard'
  }, [])

  const handleAuth = async () => {
    setAuthError('')

    // Basic validation
    if (!authForm.email || !authForm.password) {
      setAuthError('Email and password are required.')
      return
    }
    if (authTab === 'signup') {
      if (!authForm.name) { setAuthError('Name is required.'); return }
      if (authForm.password !== authForm.confirm) { setAuthError('Passwords do not match.'); return }
      if (authForm.password.length < 6) { setAuthError('Password must be at least 6 characters.'); return }
    }

    setAuthLoading(true)
    try {
      const url = authTab === 'signin'
        ? `${API_BASE_URL}/api/v1/auth/login/json`
        : `${API_BASE_URL}/api/v1/auth/register`

      const body = authTab === 'signin'
        ? { email: authForm.email, password: authForm.password }
        : { name: authForm.name, email: authForm.email, password: authForm.password, company: authForm.company, industry: authForm.industry }

      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })

      const data = await res.json()

      if (!res.ok) {
        setAuthError(data.detail || 'Something went wrong.')
        return
      }

      // Store token and user in localStorage
      localStorage.setItem('truvexis_token', data.access_token)
      localStorage.setItem('truvexis_user', JSON.stringify(data.user))

      // Redirect to dashboard
      window.location.href = '/dashboard'

    } catch {
      setAuthError('Cannot connect to server. Make sure the backend is running.')
    } finally {
      setAuthLoading(false)
    }
  }

  const steps = [
    { n:'01', label:'Ingest', color:'#FFD4B8', bg:'rgba(255,212,184,0.12)', icon:'⬆', desc:'Upload Excel, CSV, or PDF from any facility. Handles merged cells, inconsistent headers, and multi-sheet workbooks — zero manual prep required.' },
    { n:'02', label:'Match', color:'#D4AF37', bg:'rgba(212,175,55,0.1)', icon:'◎', desc:'Hybrid rule + LLM matching maps your headers to standard ESG indicators. 91% auto-approval rate. Ambiguous items flagged with AI reasoning.' },
    { n:'03', label:'Normalize', color:'#C8D5B9', bg:'rgba(200,213,185,0.15)', icon:'⚖', desc:'50+ conversion factors. kWh → MWh, kg CO₂ → tonnes CO₂e. Every conversion source documented for full audit compliance.' },
    { n:'04', label:'Validate', color:'#B8C9A8', bg:'rgba(184,201,168,0.15)', icon:'✓', desc:'Industry-specific rules: cement 800–1,100 kg CO₂/tonne, steel BF-BOF 1,800–2,500. Z-score outlier detection at 87% accuracy.' },
    { n:'05', label:'Generate', color:'#D4AF37', bg:'rgba(212,175,55,0.1)', icon:'✦', desc:'RAG narratives grounded in your actual validated data via Qdrant vector search. 100% citation traceability — zero hallucination guaranteed.' },
    { n:'06', label:'Review', color:'#E89C7F', bg:'rgba(232,156,127,0.12)', icon:'◉', desc:'Human-in-the-loop dashboard surfaces low-confidence matches and flagged items. Approve, correct, or override with full audit logging.' },
    { n:'07', label:'Export', color:'#B87333', bg:'rgba(184,115,51,0.12)', icon:'↓', desc:'BRSR Core, GRI Universal 2021, and CSRD simultaneously. Download DOCX, Excel, or PDF — with W3C PROV audit trail embedded.' },
  ]

  return (
    <main style={{ fontFamily:"'DM Sans',sans-serif", background:'#FEFEF8', color:'#3E2723', overflowX:'hidden' }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,500;0,700;1,400;1,500&family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600&display=swap');
        *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
        html{scroll-behavior:smooth}
        a{text-decoration:none;color:inherit}
        button{cursor:pointer;font-family:inherit}
        img{display:block;max-width:100%}
        .nav{position:fixed;top:0;left:0;right:0;z-index:200;padding:30px 60px;display:flex;align-items:center;justify-content:space-between;transition:all 0.5s cubic-bezier(.4,0,.2,1)}
        .nav.solid{padding:14px 60px;background:rgba(254,254,248,.97);backdrop-filter:blur(24px);border-bottom:1px solid rgba(212,196,176,.3);box-shadow:0 2px 20px rgba(62,39,35,.06)}
        .nav-logo{font-family:'Playfair Display',serif;font-size:1.4rem;letter-spacing:-.01em;transition:color .4s}
        .nav-menu{display:flex;gap:44px;list-style:none}
        .nav-menu a{font-size:.875rem;transition:color .3s}
        .nav-btn{font-size:.875rem;font-weight:500;padding:10px 26px;border-radius:2px;transition:all .3s;border:1.5px solid;background:transparent}
        .hero{position:relative;width:100%;height:100vh;min-height:700px;overflow:hidden;display:flex;align-items:flex-end}
        .hero-bg{position:absolute;inset:0}
        .hero-bg img{width:100%;height:100%;object-fit:cover;animation:zoom 14s ease-out forwards;transform:scale(1.07)}
        @keyframes zoom{to{transform:scale(1)}}
        .hero-ov{position:absolute;inset:0;background:linear-gradient(160deg,rgba(44,24,16,.05) 0%,rgba(44,24,16,.42) 50%,rgba(44,24,16,.9) 100%)}
        .hero-ct{position:relative;z-index:2;padding:0 60px 90px;max-width:800px}
        .hero-ey{font-size:.72rem;letter-spacing:.14em;text-transform:uppercase;color:#FFD4B8;margin-bottom:20px;font-weight:500}
        .hero-h1{font-family:'Playfair Display',serif;font-size:clamp(2.8rem,5vw,4.8rem);line-height:1.09;letter-spacing:-.025em;color:#FEFEF8;margin-bottom:24px}
        .hero-bd{font-size:1rem;color:rgba(254,254,248,.62);line-height:1.78;max-width:460px;margin-bottom:36px;font-weight:300}
        .lk{display:inline-flex;align-items:center;gap:10px;font-size:.9rem;font-weight:500;border-bottom:1px solid;padding-bottom:4px;transition:gap .3s,border-color .3s}
        .lk-lt{color:#FEFEF8;border-color:rgba(254,254,248,.35)}
        .lk-lt:hover{gap:16px;border-color:#FEFEF8}
        .lk-cu{color:#B87333;border-color:rgba(184,115,51,.35)}
        .lk-cu:hover{gap:16px;border-color:#B87333}
        .eye{font-size:.72rem;letter-spacing:.14em;text-transform:uppercase;font-weight:500;margin-bottom:20px}
        .eye-cu{color:#B87333}
        .eye-lt{color:#FFD4B8}
        .h2{font-family:'Playfair Display',serif;font-size:clamp(1.9rem,2.8vw,2.6rem);line-height:1.16;letter-spacing:-.02em;margin-bottom:22px}
        .h2-dk{color:#3E2723}
        .h2-lt{color:#FEFEF8}
        .bd{font-size:.95rem;line-height:1.8;font-weight:300;max-width:420px}
        .bd-dk{color:#6D4C41}
        .bd-lt{color:rgba(254,254,248,.55)}
        .defn{font-size:.82rem;color:#8D8D8D;line-height:1.7;border-left:2px solid #D4C4B0;padding-left:16px;margin:22px 0}
        .pron{font-family:'Playfair Display',serif;font-style:italic;font-size:.95rem;color:#8D8D8D;margin-bottom:4px}
        .intro{display:grid;grid-template-columns:1fr 1fr;min-height:580px}
        @media(max-width:768px){.intro{grid-template-columns:1fr}}
        .intro-l{background:#FEFEF8;padding:100px 80px 100px 60px;display:flex;flex-direction:column;justify-content:center}
        .intro-r{position:relative;overflow:hidden;min-height:480px}
        .intro-r img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;transition:transform .8s ease}
        .intro-r:hover img{transform:scale(1.04)}
        .tc{display:grid;grid-template-columns:1fr 1fr;min-height:520px}
        @media(max-width:768px){.tc{grid-template-columns:1fr}}
        .tc-img{position:relative;overflow:hidden;min-height:420px}
        .tc-img img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;transition:transform .8s ease}
        .tc-img:hover img{transform:scale(1.04)}
        .tc-txt{padding:90px 80px;display:flex;flex-direction:column;justify-content:center}
        .spec-s{display:grid;grid-template-columns:1fr 1fr;background:#F5EFE6}
        @media(max-width:768px){.spec-s{grid-template-columns:1fr}}
        .spec-l{padding:100px 80px 100px 60px;display:flex;flex-direction:column;justify-content:center}
        .spec-r{padding:100px 60px 100px 40px;display:flex;flex-direction:column;justify-content:center}
        .spec-card{border:1px solid #D4C4B0;border-radius:6px;overflow:hidden;background:#FEFEF8;box-shadow:0 8px 40px rgba(62,39,35,.07)}
        .spec-hd{padding:14px 24px;border-bottom:1px solid #D4C4B0;font-size:.72rem;letter-spacing:.1em;text-transform:uppercase;color:#8D8D8D;font-weight:500}
        .spec-row{display:flex;justify-content:space-between;align-items:center;padding:14px 24px;border-bottom:1px solid rgba(212,196,176,.3);font-size:.84rem;transition:background .2s}
        .spec-row:hover{background:#FFF8F5}
        .spec-row:last-child{border-bottom:none}
        .sk{color:#8D8D8D;text-transform:uppercase;letter-spacing:.06em;font-size:.72rem}
        .sv{color:#3E2723;font-weight:500}
        .caps{display:grid;grid-template-columns:repeat(4,1fr);border-top:1px solid #D4C4B0}
        @media(max-width:900px){.caps{grid-template-columns:repeat(2,1fr)}}
        .cap{padding:48px 36px;border-right:1px solid #D4C4B0;transition:background .35s}
        .cap:last-child{border-right:none}
        .cap:hover{background:#F5EFE6}
        .cap-n{font-size:.72rem;letter-spacing:.12em;text-transform:uppercase;color:#B87333;font-weight:600;margin-bottom:22px}
        .cap-t{font-family:'Playfair Display',serif;font-size:1.1rem;color:#3E2723;margin-bottom:14px;line-height:1.3}
        .cap-d{font-size:.82rem;color:#8D8D8D;line-height:1.7}
        .pipe-sec{background:#1C1008;padding:120px 60px;position:relative;overflow:hidden}
        .pipe-sec::before{content:'';position:absolute;inset:0;background:radial-gradient(ellipse 100% 60% at 50% 0%,rgba(255,212,184,.06) 0%,transparent 60%),radial-gradient(ellipse 80% 40% at 50% 100%,rgba(184,115,51,.06) 0%,transparent 60%);pointer-events:none}
        .pipe-inner{max-width:1000px;margin:0 auto}
        .pipe-hd{margin-bottom:80px}
        .timeline{position:relative;padding-left:60px}
        .timeline::before{content:'';position:absolute;left:20px;top:0;bottom:0;width:1px;background:linear-gradient(to bottom,rgba(255,212,184,.15),rgba(184,115,51,.4) 30%,rgba(212,175,55,.5) 60%,rgba(232,156,127,.3) 85%,rgba(255,212,184,.05))}
        .t-step{position:relative;padding:0 0 64px}
        .t-step:last-child{padding-bottom:0}
        .t-dot{position:absolute;left:-48px;top:20px;width:16px;height:16px;border-radius:50%;border:1.5px solid;background:#1C1008;transition:all .5s cubic-bezier(.4,0,.2,1);z-index:2}
        .t-dot.lit{width:20px;height:20px;left:-50px;top:18px}
        .t-tick{position:absolute;left:-40px;top:26px;width:28px;height:1px;background:rgba(255,255,255,.08);transition:background .5s}
        .t-step.lit .t-tick{background:rgba(255,255,255,.2)}
        .t-card{background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.07);border-radius:16px;padding:36px 40px;display:grid;grid-template-columns:auto 1fr;gap:28px;align-items:start;transition:all .5s cubic-bezier(.4,0,.2,1);position:relative;overflow:hidden}
        .t-card::before{content:'';position:absolute;inset:0;background:linear-gradient(135deg,rgba(255,255,255,.03) 0%,transparent 60%);opacity:0;transition:opacity .4s;border-radius:16px}
        .t-step.lit .t-card{background:rgba(255,255,255,.05);border-color:rgba(255,255,255,.12);transform:translateX(8px);box-shadow:0 8px 48px rgba(0,0,0,.3)}
        .t-step.lit .t-card::before{opacity:1}
        .t-icon{width:52px;height:52px;border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:1.3rem;transition:all .4s;flex-shrink:0}
        .t-meta{font-size:.68rem;letter-spacing:.12em;text-transform:uppercase;font-weight:600;margin-bottom:8px;transition:color .4s}
        .t-title{font-family:'Playfair Display',serif;font-size:1.35rem;color:rgba(254,254,248,.9);margin-bottom:10px;line-height:1.25}
        .t-step.lit .t-title{color:#FEFEF8}
        .t-desc{font-size:.875rem;color:rgba(254,254,248,.35);line-height:1.75;font-weight:300;transition:color .5s}
        .t-step.lit .t-desc{color:rgba(254,254,248,.6)}
        .t-badge{position:absolute;right:28px;top:28px;font-family:'Playfair Display',serif;font-size:3.5rem;font-style:italic;color:rgba(255,255,255,.04);line-height:1;letter-spacing:-.04em;pointer-events:none;transition:color .5s;font-weight:700}
        .t-step.lit .t-badge{color:rgba(255,255,255,.07)}
        .stats{display:grid;grid-template-columns:repeat(3,1fr);background:#2C1810}
        @media(max-width:640px){.stats{grid-template-columns:1fr}}
        .stat{padding:72px 60px;border-right:1px solid rgba(255,255,255,.07);transition:background .3s}
        .stat:hover{background:rgba(255,255,255,.03)}
        .stat:last-child{border-right:none}
        .stat-n{font-family:'Playfair Display',serif;font-size:clamp(3rem,5vw,4.5rem);color:#FEFEF8;line-height:1;letter-spacing:-.03em;margin-bottom:8px}
        .stat-n em{font-style:italic;color:#D4AF37}
        .stat-u{font-size:.72rem;letter-spacing:.1em;text-transform:uppercase;color:#B87333;margin-bottom:16px;font-weight:500}
        .stat-d{font-size:.84rem;color:rgba(254,254,248,.35);line-height:1.7}
        .ind-s{background:#F5EFE6;padding:100px 60px}
        .ind-sc{display:flex;gap:24px;margin-top:56px;overflow-x:auto;padding-bottom:20px;scrollbar-width:none}
        .ind-sc::-webkit-scrollbar{display:none}
        .ind-c{flex:0 0 360px;border:1px solid #D4C4B0;border-radius:4px;overflow:hidden;background:#FEFEF8;transition:transform .35s cubic-bezier(.4,0,.2,1),box-shadow .35s}
        .ind-c:hover{transform:translateY(-8px);box-shadow:0 24px 56px rgba(62,39,35,.14)}
        .ind-ci{width:100%;height:230px;object-fit:cover;transition:transform .6s ease}
        .ind-c:hover .ind-ci{transform:scale(1.06)}
        .ind-cb{padding:32px 28px}
        .ind-lb{font-size:.7rem;letter-spacing:.1em;text-transform:uppercase;color:#8D8D8D;margin-bottom:10px}
        .ind-t{font-family:'Playfair Display',serif;font-size:1.05rem;color:#3E2723;margin-bottom:12px;line-height:1.35}
        .ind-d{font-size:.8rem;color:#8D8D8D;line-height:1.65;margin-bottom:22px}
        .ind-lk{display:inline-flex;align-items:center;gap:8px;font-size:.8rem;font-weight:500;color:#B87333;border-bottom:1px solid rgba(184,115,51,.3);padding-bottom:3px;transition:gap .25s,border-color .25s}
        .ind-lk:hover{gap:13px;border-color:#B87333}
        .eaas{display:grid;grid-template-columns:1fr 1fr;min-height:580px}
        @media(max-width:768px){.eaas{grid-template-columns:1fr}}
        .eaas-i{position:relative;overflow:hidden;min-height:480px}
        .eaas-i img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;transition:transform .8s ease}
        .eaas-i:hover img{transform:scale(1.04)}
        .eaas-t{background:#3E2723;padding:100px 80px;display:flex;flex-direction:column;justify-content:center}
        .cta{background:#2C1810;padding:130px 60px;text-align:center;position:relative;overflow:hidden}
        .cta::before{content:'';position:absolute;inset:0;background:radial-gradient(ellipse 70% 60% at 50% 110%,rgba(255,212,184,.09) 0%,transparent 65%);pointer-events:none}
        .cta-h{font-family:'Playfair Display',serif;font-size:clamp(2.4rem,4.5vw,4rem);line-height:1.1;letter-spacing:-.025em;color:#FEFEF8;max-width:640px;margin:0 auto 20px}
        .cta-h em{font-style:italic;color:#D4AF37}
        .cta-b{font-size:.95rem;color:rgba(254,254,248,.35);max-width:400px;margin:0 auto 52px;line-height:1.75;font-weight:300}
        .cta-ac{display:flex;gap:16px;justify-content:center;flex-wrap:wrap}
        .bf{font-size:.9rem;font-weight:500;background:#F5EFE6;color:#3E2723;padding:15px 36px;border-radius:2px;border:none;transition:all .3s}
        .bf:hover{background:#FEFEF8;transform:translateY(-2px);box-shadow:0 12px 32px rgba(0,0,0,.3)}
        .ft{background:#1C0E08;padding:52px 60px;border-top:1px solid rgba(255,255,255,.05)}
        .ft-in{max-width:1200px;margin:0 auto;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:24px}
        .ft-lg{font-family:'Playfair Display',serif;font-size:1.3rem;color:#FEFEF8}
        .ft-md{font-size:.78rem;color:rgba(254,254,248,.2);text-align:center;line-height:1.8}
        .ft-lk{display:flex;gap:24px}
        .ft-lk a{font-size:.78rem;color:rgba(254,254,248,.2);transition:color .25s}
        .ft-lk a:hover{color:rgba(254,254,248,.6)}
        .rv{opacity:0;transform:translateY(28px);transition:opacity .9s cubic-bezier(.4,0,.2,1),transform .9s cubic-bezier(.4,0,.2,1)}
        .rv.go{opacity:1;transform:translateY(0)}
        .rv-l{opacity:0;transform:translateX(-32px);transition:opacity .9s cubic-bezier(.4,0,.2,1),transform .9s cubic-bezier(.4,0,.2,1)}
        .rv-l.go{opacity:1;transform:translateX(0)}
        .rv-r{opacity:0;transform:translateX(32px);transition:opacity .9s cubic-bezier(.4,0,.2,1),transform .9s cubic-bezier(.4,0,.2,1)}
        .rv-r.go{opacity:1;transform:translateX(0)}
        @keyframes h-in{from{opacity:0;transform:translateY(22px)}to{opacity:1;transform:translateY(0)}}
        .ha{animation:h-in 1s cubic-bezier(.4,0,.2,1) forwards;opacity:0}
        .d1{animation-delay:.1s}.d2{animation-delay:.3s}.d3{animation-delay:.5s}.d4{animation-delay:.7s}
        .dv-wh{height:60px;background:linear-gradient(to bottom,#2C1810,#FEFEF8)}
        .dv-dg1{height:60px;background:#FEFEF8;clip-path:polygon(0 0,100% 0,100% 100%,0 40%);margin-bottom:-2px}
        .dv-dg2{height:60px;background:#2C1810;clip-path:polygon(0 0,100% 60%,100% 100%,0 100%);margin-bottom:-2px}
        .dv-dk{height:60px;background:linear-gradient(to bottom,#FEFEF8,#1C1008)}
        .dv-dk2{height:60px;background:linear-gradient(to bottom,#1C1008,#2C1810)}

        /* AUTH MODAL */
        .auth-ov{position:fixed;inset:0;z-index:9000;display:flex;align-items:center;justify-content:center;padding:20px;background:rgba(28,14,8,.75);backdrop-filter:blur(12px);animation:fade-in .22s ease}
        @keyframes fade-in{from{opacity:0}to{opacity:1}}
        .auth-box{background:#FEFEF8;border-radius:4px;width:100%;max-width:520px;overflow:hidden;animation:slide-up .28s cubic-bezier(.4,0,.2,1)}
        @keyframes slide-up{from{opacity:0;transform:translateY(22px)}to{opacity:1;transform:translateY(0)}}
        .auth-hd{padding:36px 40px 0;display:flex;justify-content:space-between;align-items:flex-start}
        .auth-logo{font-family:'Playfair Display',serif;font-size:1.25rem;color:#3E2723}
        .auth-x{background:none;border:none;font-size:1.4rem;color:#8D8D8D;cursor:pointer;line-height:1;padding:4px;transition:color .2s}
        .auth-x:hover{color:#3E2723}
        .auth-tabs{display:flex;margin:28px 40px 0;border-bottom:1px solid #D4C4B0}
        .auth-tab{flex:1;padding:10px 0;font-size:.875rem;font-weight:500;background:none;border:none;color:#8D8D8D;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .2s}
        .auth-tab.active{color:#3E2723;border-bottom-color:#B87333}
        .auth-body{padding:28px 40px 40px;display:flex;flex-direction:column;gap:16px}
        .auth-field{display:flex;flex-direction:column;gap:6px}
        .auth-label{font-size:.78rem;letter-spacing:.06em;text-transform:uppercase;color:#8D8D8D;font-weight:500}
        .auth-input{padding:11px 14px;border:1.5px solid #D4C4B0;border-radius:2px;font-size:.9rem;font-family:inherit;color:#3E2723;background:#FEFEF8;outline:none;transition:border-color .2s}
        .auth-input:focus{border-color:#B87333}
        .auth-select{padding:11px 14px;border:1.5px solid #D4C4B0;border-radius:2px;font-size:.9rem;font-family:inherit;color:#3E2723;background:#FEFEF8;outline:none;transition:border-color .2s;appearance:none}
        .auth-select:focus{border-color:#B87333}
        .auth-error{background:#FFF0F0;border:1px solid #FFCDD2;border-radius:2px;padding:10px 14px;font-size:.82rem;color:#C0392B;line-height:1.5}
        .auth-submit{margin-top:4px;padding:13px;color:#FEFEF8;border:none;border-radius:2px;font-size:.9rem;font-weight:500;cursor:pointer;font-family:inherit;transition:all .25s;display:flex;align-items:center;justify-content:center;gap:8px}
        .auth-submit:disabled{opacity:.6;cursor:not-allowed}
        .auth-switch{text-align:center;font-size:.82rem;color:#8D8D8D}
        .auth-switch button{background:none;border:none;color:#B87333;font-size:.82rem;cursor:pointer;font-family:inherit;text-decoration:underline;padding:0}
        .spin{width:14px;height:14px;border:2px solid rgba(255,255,255,.3);border-top-color:#fff;border-radius:50%;animation:spin .6s linear infinite}
        @keyframes spin{to{transform:rotate(360deg)}}
      `}</style>

      {/* NAV */}
      <nav className={`nav ${navSolid?'solid':''}`}>
        <span className="nav-logo" style={{color:navSolid?'#3E2723':'#FEFEF8'}}>Truvexis</span>
        <ul className="nav-menu">
          {[['#why','Why'],['#how','How It Works'],['#industries','Industries'],['#features','Features']].map(([h,l])=>(
            <li key={h}><a href={h} style={{color:navSolid?'#6D4C41':'rgba(254,254,248,0.7)'}}>{l}</a></li>
          ))}
        </ul>
        <button className="nav-btn"
          style={{borderColor:navSolid?'#3E2723':'rgba(254,254,248,0.45)',color:navSolid?'#3E2723':'#FEFEF8'}}
          onMouseEnter={e=>{const t=e.target as HTMLElement;t.style.background=navSolid?'#3E2723':'rgba(255,255,255,0.1)';t.style.color='#FEFEF8'}}
          onMouseLeave={e=>{const t=e.target as HTMLElement;t.style.background='transparent';t.style.color=navSolid?'#3E2723':'#FEFEF8'}}
          onClick={()=>{setAuthError('');setAuthTab('signin');setAuthOpen(true)}}>
          Sign In
        </button>
      </nav>

      {/* HERO */}
      <section className="hero">
        <div className="hero-bg">
          <img src="https://images.unsplash.com/photo-1473341304170-971dccb5ac1e?w=1920&q=85" alt="Wind turbines"/>
          <div className="hero-ov"/>
        </div>
        <div className="hero-ct">
          <p className="ha d1 hero-ey">AI-Powered ESG Reporting Platform</p>
          <h1 className="ha d2 hero-h1">Automated ESG Reporting<br/>for High-Impact<br/>Manufacturing</h1>
          <p className="ha d3 hero-bd">Truvexis transforms weeks of manual ESG compliance into a fully automated pipeline — from raw facility data to audit-ready reports in under two hours.</p>
          <div className="ha d4" style={{display:'flex',gap:'16px',flexWrap:'wrap'}}>
            <button className="bf" onClick={()=>{setAuthError('');setAuthTab('signup');setAuthOpen(true)}}>Get Started Free →</button>
            <a href="#why" className="lk lk-lt">Learn more</a>
          </div>
        </div>
      </section>

      <div className="dv-wh"/>

      {/* INTRO */}
      <section id="why" className="intro">
        <div className="intro-l">
          <R t="l">
            <p className="eye eye-cu">About Truvexis</p>
            <p className="pron">\ ˈtruː-vek-sis \</p>
            <h2 className="h2 h2-dk">The Name Reflects<br/>Our Mission</h2>
            <p className="bd bd-dk">Truvexis combines <em>true</em> — grounded in verifiable data — and <em>nexis</em> — the connecting thread between source and disclosure.</p>
            <div className="defn">Manufacturing companies spend 5–6 weeks and ₹5,00,000 every year producing ESG reports that often can't be traced. Truvexis solves both problems at once.</div>
            <a href="#how" className="lk lk-cu">Explore the platform →</a>
          </R>
        </div>
        <div className="intro-r">
          <img src="https://images.unsplash.com/photo-1497435334941-8c899ee9e8e9?w=900&q=85" alt="Solar panels"/>
        </div>
      </section>

      <div className="dv-dg1"/>

      {/* TECHNOLOGY */}
      <section style={{background:'#F5EFE6'}}>
        <div className="tc">
          <div className="tc-img">
            <img src="https://images.unsplash.com/photo-1581091226825-a6a2a5aee158?w=900&q=85" alt="Technology"/>
          </div>
          <div className="tc-txt" style={{background:'#F5EFE6'}}>
            <R t="r">
              <p className="eye eye-cu">The Technology</p>
              <h2 className="h2 h2-dk">A Purpose-Built<br/>AI Pipeline</h2>
              <p className="bd bd-dk">We built a 7-stage AI pipeline specifically for manufacturing ESG compliance — handling inconsistent formats, mapping to BRSR and GRI standards, validating against benchmarks, and generating fully cited narratives automatically.</p>
              <a href="#how" className="lk lk-cu" style={{marginTop:'28px'}}>Explore the pipeline →</a>
            </R>
          </div>
        </div>
      </section>

      {/* SPEC */}
      <section className="spec-s">
        <div className="spec-l">
          <R t="l">
            <p className="eye eye-cu">Platform Performance</p>
            <h2 className="h2 h2-dk">Intelligent Systems</h2>
            <p className="bd bd-dk">Groq LLM inference for speed, Qdrant vector search for grounded narratives, W3C PROV-DM for complete data lineage — fast, accurate, fully auditable.</p>
          </R>
        </div>
        <div className="spec-r">
          <R t="r" d={120}>
            <div className="spec-card">
              <div className="spec-hd">Truvexis Platform</div>
              {[['Frameworks','BRSR · GRI · CSRD'],['Pipeline Time','Under 2 hours'],['Matching Accuracy','91% auto-approved'],['Normalization Rate','98% success'],['Validation Detection','87% detection'],['Citation Traceability','100%'],['Audit Standard','W3C PROV-DM']].map(([k,v])=>(
                <div key={k} className="spec-row"><span className="sk">{k}</span><span className="sv">{v}</span></div>
              ))}
            </div>
          </R>
        </div>
      </section>

      {/* 4-COL */}
      <section style={{background:'#FEFEF8',paddingBottom:'0'}}>
        <div style={{maxWidth:'1200px',margin:'0 auto',padding:'100px 60px 48px'}}>
          <R><p className="eye eye-cu">What We Automate</p><h2 className="h2 h2-dk">End-to-End ESG Coverage</h2></R>
        </div>
        <div className="caps">
          {[
            {n:'01',t:'Data Ingestion',d:'Excel, CSV, PDF from any facility. Merged cells, mixed units, multi-sheet workbooks handled automatically.'},
            {n:'02',t:'AI Matching',d:'Rule + LLM hybrid at 91% auto-approval. 50+ unit conversions across energy, emissions, water, waste.'},
            {n:'03',t:'RAG Narratives',d:'Every sentence grounded in your actual data. 100% citation traceability — zero hallucination.'},
            {n:'04',t:'Multi-Framework Export',d:'BRSR, GRI 2021, CSRD simultaneously. DOCX, Excel, PDF with W3C PROV audit trail.'},
          ].map((c,i)=>(
            <R key={i} d={i*80}>
              <div className="cap"><div className="cap-n">{c.n}</div><div className="cap-t">{c.t}</div><p className="cap-d">{c.d}</p></div>
            </R>
          ))}
        </div>
      </section>

      <div className="dv-dk"/>

      {/* PIPELINE */}
      <section id="how" className="pipe-sec">
        <div className="pipe-inner">
          <div className="pipe-hd">
            <R>
              <p className="eye eye-lt">The Pipeline</p>
              <h2 className="h2 h2-lt" style={{maxWidth:'480px'}}>Your Data's Journey<br/>to Compliance</h2>
              <p className="bd bd-lt" style={{marginTop:'8px'}}>Seven automated stages. Scroll through the path your data takes.</p>
            </R>
          </div>
          <div className="timeline">
            {steps.map((s,i)=>(
              <PipeStep key={i} step={s} index={i}/>
            ))}
          </div>
        </div>
      </section>

      <div className="dv-dk2"/>

      {/* STATS */}
      <div className="stats">
        {[
          {n:'99.98',em:'%',u:'Time Reduction',d:'From 240 hours of manual work to 63 seconds of automated pipeline processing.'},
          {n:'3',em:'',u:'Frameworks at Once',d:'BRSR, GRI, and CSRD — one upload, three compliant outputs.'},
          {n:'₹5L',em:'',u:'Saved Annually',d:'Eliminate consultant fees, staff overtime, and rework costs entirely.'},
        ].map((s,i)=>(
          <R key={i} d={i*100}>
            <div className="stat">
              <div className="stat-n">{s.n}<em>{s.em}</em></div>
              <div className="stat-u">{s.u}</div>
              <p className="stat-d">{s.d}</p>
            </div>
          </R>
        ))}
      </div>

      <div className="dv-dg2"/>

      {/* INDUSTRIES */}
      <section id="industries" className="ind-s">
        <div style={{maxWidth:'1200px',margin:'0 auto'}}>
          <R>
            <p className="eye eye-cu">Industries Served</p>
            <h2 className="h2 h2-dk">Built for High-Impact<br/>Manufacturing</h2>
            <p className="bd bd-dk" style={{marginTop:'16px'}}>Sector-specific validation rules, benchmark ranges, and indicator mappings for the industries with the most complex ESG requirements.</p>
          </R>
          <div className="ind-sc">
            {[
              {l:'Cement',t:'Automated ESG for Cement Manufacturing',d:'Scope 1 intensity (800–1,100 kg CO₂/tonne clinker), energy normalization, BRSR Principle 6 narratives.',i:'https://images.unsplash.com/photo-1567521464027-f127ff144326?w=700&q=85'},
              {l:'Steel',t:'Precision Reporting for BF-BOF and EAF Steel',d:'Route-specific validation benchmarked against GRI 305 and BRSR industry averages.',i:'https://images.unsplash.com/photo-1538688525198-9b88f6f53126?w=700&q=85'},
              {l:'Automotive',t:'Vehicle-Level ESG Metrics, Automated',d:'Per-vehicle emission and energy intensity mapped to CSRD for automotive OEMs.',i:'https://images.unsplash.com/photo-1593941707882-a5bba14938c7?w=700&q=85'},
              {l:'Chemical',t:'Multi-Site Chemical Plant Reporting',d:'Consolidated GRI 302, 303, 305, 306 disclosures across multiple facilities.',i:'https://images.unsplash.com/photo-1532187863486-abf9dbad1b69?w=700&q=85'},
            ].map((ind,i)=>(
              <div key={i} className="ind-c">
                <img src={ind.i} alt={ind.l} className="ind-ci"/>
                <div className="ind-cb">
                  <div className="ind-lb">{ind.l}</div>
                  <div className="ind-t">{ind.t}</div>
                  <p className="ind-d">{ind.d}</p>
                  <span className="ind-lk">Learn more →</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* EAAS */}
      <section id="features" className="eaas">
        <div className="eaas-i">
          <img src="https://images.unsplash.com/photo-1473341304170-971dccb5ac1e?w=900&q=85" alt="Wind farm"/>
        </div>
        <div className="eaas-t">
          <R t="r">
            <p className="eye eye-lt">Reporting as a Service</p>
            <h2 className="h2 h2-lt" style={{marginBottom:'22px'}}>We Process the Data.<br/>You Sign the Report.</h2>
            <p className="bd bd-lt">Upload raw facility data. Receive complete, audit-ready ESG disclosures. No consultants. No manual aggregation. No rework cycles.</p>
            <p className="bd bd-lt" style={{marginTop:'16px'}}>Every number traces back to its source through W3C PROV-DM graphs — exportable for independent verification.</p>
            <button className="bf" style={{marginTop:'32px',width:'fit-content'}} onClick={()=>{setAuthError('');setAuthTab('signup');setAuthOpen(true)}}>Get Started Free →</button>
          </R>
        </div>
      </section>

      {/* CTA */}
      <section className="cta">
        <R>
          <h2 className="cta-h">From 6 Weeks to<br/><em>Under 2 Hours.</em></h2>
          <p className="cta-b">Built as a CSUF thesis on AI-driven ESG automation for high-impact manufacturing.</p>
          <div className="cta-ac">
            <button className="bf" onClick={()=>{setAuthError('');setAuthTab('signup');setAuthOpen(true)}}>Start for Free →</button>
          </div>
        </R>
      </section>

      {/* FOOTER */}
      <footer className="ft">
        <div className="ft-in">
          <span className="ft-lg">Truvexis</span>
          <div className="ft-md">Built by Yash Dusane · CSUF CPSC 589 · 2025<br/>Advisor: Prof. Dr. Kenneth Kung</div>
          <div className="ft-lk">
            <a href="#">BRSR</a><a href="#">GRI</a><a href="#">CSRD</a><a href="#">GitHub</a>
          </div>
        </div>
      </footer>

      {/* AUTH MODAL */}
      {authOpen && (
        <div className="auth-ov" onClick={e=>{if(e.target===e.currentTarget){setAuthOpen(false);setAuthError('')}}}>
          <div className="auth-box">
            <div className="auth-hd">
              <span className="auth-logo">Truvexis</span>
              <button className="auth-x" onClick={()=>{setAuthOpen(false);setAuthError('')}}>✕</button>
            </div>
            <div className="auth-tabs">
              <button className={`auth-tab${authTab==='signin'?' active':''}`} onClick={()=>{setAuthTab('signin');setAuthError('')}}>Sign In</button>
              <button className={`auth-tab${authTab==='signup'?' active':''}`} onClick={()=>{setAuthTab('signup');setAuthError('')}}>Create Account</button>
            </div>
            <div className="auth-body">
              {authTab==='signup' && (
                <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:'16px'}}>
                  <div className="auth-field">
                    <label className="auth-label">Full Name</label>
                    <input className="auth-input" type="text" placeholder="Yash Dusane"
                      value={authForm.name} onChange={e=>setAuthForm(f=>({...f,name:e.target.value}))}/>
                  </div>
                  <div className="auth-field">
                    <label className="auth-label">Company</label>
                    <input className="auth-input" type="text" placeholder="Tata Steel Ltd."
                      value={authForm.company} onChange={e=>setAuthForm(f=>({...f,company:e.target.value}))}/>
                  </div>
                </div>
              )}
              {authTab==='signup' && (
                <div className="auth-field">
                  <label className="auth-label">Industry</label>
                  <select className="auth-select" value={authForm.industry}
                    onChange={e=>setAuthForm(f=>({...f,industry:e.target.value}))}>
                    <option value="">Select your industry</option>
                    <option value="cement">Cement</option>
                    <option value="steel">Steel</option>
                    <option value="automotive">Automotive</option>
                    <option value="chemical">Chemical</option>
                    <option value="other">Other Manufacturing</option>
                  </select>
                </div>
              )}
              <div className="auth-field">
                <label className="auth-label">Email</label>
                <input className="auth-input" type="email" placeholder="you@company.com"
                  value={authForm.email} onChange={e=>setAuthForm(f=>({...f,email:e.target.value}))}
                  onKeyDown={e=>{if(e.key==='Enter') handleAuth()}}/>
              </div>
              <div style={authTab==='signup'?{display:'grid',gridTemplateColumns:'1fr 1fr',gap:'16px'}:{}}>
                <div className="auth-field">
                  <label className="auth-label">Password</label>
                  <input className="auth-input" type="password" placeholder="••••••••"
                    value={authForm.password} onChange={e=>setAuthForm(f=>({...f,password:e.target.value}))}
                    onKeyDown={e=>{if(e.key==='Enter') handleAuth()}}/>
                </div>
                {authTab==='signup' && (
                  <div className="auth-field">
                    <label className="auth-label">Confirm Password</label>
                    <input className="auth-input" type="password" placeholder="••••••••"
                      value={authForm.confirm} onChange={e=>setAuthForm(f=>({...f,confirm:e.target.value}))}
                      style={authForm.confirm && authForm.confirm!==authForm.password?{borderColor:'#C0392B'}:{}}
                      onKeyDown={e=>{if(e.key==='Enter') handleAuth()}}/>
                  </div>
                )}
              </div>

              {authError && <div className="auth-error">{authError}</div>}

              <button className="auth-submit" onClick={handleAuth} disabled={authLoading}
                style={{background: authLoading ? '#8D8D8D' : '#3E2723'}}>
                {authLoading && <span className="spin"/>}
                {authLoading ? 'Please wait...' : authTab==='signin' ? 'Sign In →' : 'Create Account →'}
              </button>

              <p className="auth-switch">
                {authTab==='signin'
                  ? <>Don't have an account? <button onClick={()=>{setAuthTab('signup');setAuthError('')}}>Sign up free</button></>
                  : <>Already have an account? <button onClick={()=>{setAuthTab('signin');setAuthError('')}}>Sign in</button></>}
              </p>
            </div>
          </div>
        </div>
      )}
    </main>
  )
}

function PipeStep({ step, index }: { step: any; index: number }) {
  const ref = useRef<HTMLDivElement>(null)
  const [lit, setLit] = useState(false)
  useEffect(() => {
    const obs = new IntersectionObserver(([e]) => { if (e.isIntersecting) setLit(true) }, { threshold: 0.3 })
    if (ref.current) obs.observe(ref.current)
    return () => obs.disconnect()
  }, [])
  return (
    <div ref={ref} className={`t-step ${lit?'lit':''}`} style={{transitionDelay:`${index*60}ms`}}>
      <div className="t-dot" style={{borderColor:lit?step.color:'rgba(255,255,255,0.15)',background:lit?step.color:'#1C1008',boxShadow:lit?`0 0 16px ${step.color}60`:'none'}}/>
      <div className="t-tick"/>
      <div className="t-card" style={{borderColor:lit?`${step.color}25`:'rgba(255,255,255,0.07)'}}>
        <div className="t-badge">{step.n}</div>
        <div className="t-icon" style={{background:lit?step.bg:'rgba(255,255,255,0.04)',color:lit?step.color:'rgba(255,255,255,0.2)',border:`1px solid ${lit?step.color+'40':'rgba(255,255,255,0.08)'}`}}>
          {step.icon}
        </div>
        <div>
          <div className="t-meta" style={{color:lit?step.color:'rgba(255,255,255,0.25)'}}>Step {step.n}</div>
          <div className="t-title">{step.label}</div>
          <p className="t-desc">{step.desc}</p>
        </div>
      </div>
    </div>
  )
}

function R({ children, d=0, t='up' }: { children: React.ReactNode; d?: number; t?: 'up'|'l'|'r' }) {
  const ref = useRef<HTMLDivElement>(null)
  const [go, setGo] = useState(false)
  useEffect(() => {
    const obs = new IntersectionObserver(([e]) => { if (e.isIntersecting) setGo(true) }, { threshold: 0.07 })
    if (ref.current) obs.observe(ref.current)
    return () => obs.disconnect()
  }, [])
  const cls = t==='l'?'rv-l':t==='r'?'rv-r':'rv'
  return (
    <div ref={ref} className={`${cls} ${go?'go':''}`} style={go?{transitionDelay:`${d}ms`}:{}}>
      {children}
    </div>
  )
}