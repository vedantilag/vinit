import React, { useState, useRef, useEffect } from 'react';
import { Upload, Search, Mic, Lock, Eye, FileText, Image, File, Shield, Key, LogOut, Menu, X, AlertCircle, CheckCircle, MicOff, Send, Loader } from 'lucide-react';

const API_BASE = 'http://localhost:5000';

const apiFetch = async (path, options = {}) => {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  const data = await res.json();
  return { ok: res.ok, status: res.status, data };
};

const uploadFilesToBackend = async (files) => {
  const formData = new FormData();
  Array.from(files).forEach(file => formData.append('files', file));

  const res = await fetch(`${API_BASE}/api/upload`, {
    method: 'POST',
    body: formData,
  });

  const data = await res.json();
  return { ok: res.ok, status: res.status, data };
};

// ─── Auth Form ───────────────────────────────────────────────────────────────
const AuthForm = ({ onClose, onLogin }) => {
  const [step, setStep] = useState('login');
  const [form, setForm] = useState({ name: '', email: '', password: '', confirm: '' });
  const [error, setError] = useState('');

  const handleSubmit = () => {
    if (!form.email || !form.password) { setError('Please fill in all fields.'); return; }
    if (step === 'register' && form.password !== form.confirm) { setError('Passwords do not match.'); return; }
    onLogin();
  };

  return (
    <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.5)', display:'flex', alignItems:'center', justifyContent:'center', zIndex:50, padding:'1rem' }}>
      <div style={{ background:'var(--color-background-primary)', borderRadius:'var(--border-radius-lg)', boxShadow:'0 20px 60px rgba(0,0,0,0.3)', maxWidth:420, width:'100%', padding:'2rem' }}>
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:'1.5rem' }}>
          <h2 style={{ margin:0, fontSize:20, fontWeight:500, color:'var(--color-text-primary)' }}>
            {step === 'login' ? 'Sign in' : 'Create account'}
          </h2>
          <button onClick={onClose} style={{ background:'none', border:'none', cursor:'pointer', color:'var(--color-text-secondary)', padding:4 }}><X size={20} /></button>
        </div>

        {error && (
          <div style={{ background:'var(--color-background-danger)', border:'0.5px solid var(--color-border-danger)', borderRadius:'var(--border-radius-md)', padding:'10px 14px', marginBottom:'1rem', color:'var(--color-text-danger)', fontSize:13 }}>{error}</div>
        )}

        <div style={{ display:'flex', flexDirection:'column', gap:12 }}>
          {step === 'register' && (
            <input placeholder="Full name" value={form.name} onChange={e => setForm({...form, name: e.target.value})} style={{ width:'100%', boxSizing:'border-box' }} />
          )}
          <input type="email" placeholder="Email" value={form.email} onChange={e => setForm({...form, email: e.target.value})} style={{ width:'100%', boxSizing:'border-box' }} />
          <input type="password" placeholder="Password" value={form.password} onChange={e => setForm({...form, password: e.target.value})} style={{ width:'100%', boxSizing:'border-box' }} />
          {step === 'register' && (
            <input type="password" placeholder="Confirm password" value={form.confirm} onChange={e => setForm({...form, confirm: e.target.value})} style={{ width:'100%', boxSizing:'border-box' }} />
          )}
          <button onClick={handleSubmit} style={{ width:'100%', padding:'10px 0', background:'#1a56db', color:'#fff', border:'none', borderRadius:'var(--border-radius-md)', fontWeight:500, fontSize:15, cursor:'pointer', marginTop:4 }}>
            {step === 'login' ? 'Sign in' : 'Create account'}
          </button>
        </div>

        <div style={{ textAlign:'center', marginTop:'1rem' }}>
          <button onClick={() => { setStep(step === 'login' ? 'register' : 'login'); setError(''); }}
            style={{ background:'none', border:'none', cursor:'pointer', color:'#1a56db', fontSize:14 }}>
            {step === 'login' ? "Don't have an account? Sign up" : 'Already have an account? Sign in'}
          </button>
        </div>
      </div>
    </div>
  );
};

// ─── PIN Modal ───────────────────────────────────────────────────────────────
const PinModal = ({ onClose, onVerify }) => {
  const [pin, setPin] = useState('');
  return (
    <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.5)', display:'flex', alignItems:'center', justifyContent:'center', zIndex:50, padding:'1rem' }}>
      <div style={{ background:'var(--color-background-primary)', borderRadius:'var(--border-radius-lg)', maxWidth:340, width:'100%', padding:'2rem', textAlign:'center' }}>
        <Shield size={40} style={{ color:'#1a56db', marginBottom:12 }} />
        <h3 style={{ margin:'0 0 8px', fontWeight:500, color:'var(--color-text-primary)' }}>Security verification</h3>
        <p style={{ color:'var(--color-text-secondary)', fontSize:14, marginBottom:'1.25rem' }}>Enter your 4-digit PIN to access this document</p>
        <input type="password" maxLength={4} placeholder="• • • •" value={pin} onChange={e => setPin(e.target.value)}
          style={{ textAlign:'center', letterSpacing:'0.5em', fontSize:20, width:'100%', boxSizing:'border-box', marginBottom:12 }} />
        <div style={{ display:'flex', gap:8 }}>
          <button onClick={onClose} style={{ flex:1, padding:'10px 0' }}>Cancel</button>
          <button onClick={() => onVerify(pin)} style={{ flex:1, padding:'10px 0', background:'#1a56db', color:'#fff', border:'none', borderRadius:'var(--border-radius-md)', fontWeight:500, cursor:'pointer' }}>Verify</button>
        </div>
      </div>
    </div>
  );
};

// ─── Upload Tab ──────────────────────────────────────────────────────────────
const UploadTab = ({ onUpload }) => {
  const [dragging, setDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadStatus, setUploadStatus] = useState(null);
  const fileRef = useRef();

  const handleFiles = async (files) => {
    if (!files || files.length === 0) return;
    setUploading(true);
    setUploadStatus(null);
    await new Promise(r => setTimeout(r, 1200)); // simulate
    const newDocs = Array.from(files).map((f, i) => ({
      id: Date.now() + i,
      name: f.name,
      type: f.name.endsWith('.pdf') ? 'pdf' : 'image',
      size: (f.size / (1024 * 1024)).toFixed(1) + ' MB',
      date: new Date().toISOString().slice(0, 10),
      encrypted: true,
    }));
    onUpload(newDocs);
    setUploading(false);
    setUploadStatus({ ok: true, message: `${newDocs.length} document(s) uploaded and encrypted successfully.` });
  };

  return (
    <div style={{ padding:'2rem', maxWidth:640, margin:'0 auto' }}>
      <div
        onDragOver={e => { e.preventDefault(); setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={e => { e.preventDefault(); setDragging(false); handleFiles(e.dataTransfer.files); }}
        onClick={() => fileRef.current.click()}
        style={{
          border: `2px dashed ${dragging ? '#1a56db' : 'var(--color-border-secondary)'}`,
          borderRadius:'var(--border-radius-lg)',
          padding:'3rem 2rem',
          textAlign:'center',
          cursor:'pointer',
          background: dragging ? 'var(--color-background-info)' : 'var(--color-background-primary)',
          transition:'all 0.15s',
          marginBottom:'1.25rem',
        }}
      >
        <input ref={fileRef} type="file" multiple accept=".pdf,.png,.jpg,.jpeg,.docx" style={{ display:'none' }} onChange={e => handleFiles(e.target.files)} />
        {uploading
          ? <><Loader size={36} style={{ color:'#1a56db', marginBottom:12, animation:'spin 1s linear infinite' }} /><p style={{ color:'var(--color-text-secondary)', margin:0 }}>Encrypting and uploading…</p></>
          : <><Upload size={36} style={{ color:'var(--color-text-secondary)', marginBottom:12 }} /><p style={{ fontWeight:500, margin:'0 0 6px', color:'var(--color-text-primary)' }}>Drop files here or click to browse</p><p style={{ color:'var(--color-text-secondary)', fontSize:13, margin:0 }}>PDF, DOCX, PNG, JPG — max 10 MB each</p></>
        }
      </div>

      {uploadStatus && (
        <div style={{ display:'flex', alignItems:'center', gap:8, padding:'12px 16px', borderRadius:'var(--border-radius-md)', background: uploadStatus.ok ? 'var(--color-background-success)' : 'var(--color-background-danger)', border: `0.5px solid ${uploadStatus.ok ? 'var(--color-border-success)' : 'var(--color-border-danger)'}`, marginBottom:'1rem' }}>
          {uploadStatus.ok ? <CheckCircle size={16} style={{ color:'var(--color-text-success)', flexShrink:0 }} /> : <AlertCircle size={16} style={{ color:'var(--color-text-danger)', flexShrink:0 }} />}
          <span style={{ fontSize:14, color: uploadStatus.ok ? 'var(--color-text-success)' : 'var(--color-text-danger)' }}>{uploadStatus.message}</span>
        </div>
      )}

      <div style={{ padding:'1rem 1.25rem', background:'var(--color-background-info)', border:'0.5px solid var(--color-border-info)', borderRadius:'var(--border-radius-md)', display:'flex', gap:10 }}>
        <AlertCircle size={16} style={{ color:'var(--color-text-info)', flexShrink:0, marginTop:2 }} />
        <div>
          <p style={{ margin:'0 0 4px', fontWeight:500, color:'var(--color-text-info)', fontSize:14 }}>AES-256 encryption</p>
          <p style={{ margin:0, color:'var(--color-text-info)', fontSize:13 }}>All documents are encrypted before storage. OCR text extraction is applied automatically to images and scanned PDFs.</p>
        </div>
      </div>
    </div>
  );
};

// ─── Chat / AI Assistant Tab ─────────────────────────────────────────────────
const ChatTab = () => {
  const [messages, setMessages] = useState([
    { type: 'bot', text: 'Hello! I can search your documents. Try asking "Show my address from Aadhar card" or "What is my date of birth?"' }
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [recording, setRecording] = useState(false);
  const [backendStatus, setBackendStatus] = useState(null);
  const bottomRef = useRef();
  const recognitionRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  useEffect(() => {
    apiFetch('/api/health').then(({ ok, data }) => {
      setBackendStatus(ok ? 'connected' : 'error');
    }).catch(() => setBackendStatus('error'));
  }, []);

  const sendMessage = async (text) => {
    const q = (text || input).trim();
    if (!q || loading) return;
    setInput('');
    setMessages(prev => [...prev, { type: 'user', text: q }]);
    setLoading(true);

    try {
      const { ok, data } = await apiFetch('/api/query', {
        method: 'POST',
        body: JSON.stringify({ query: q }),
      });

      if (ok) {
        const citations = data.citations || [];
        setMessages(prev => [...prev, {
          type: 'bot',
          text: data.answer || 'No answer returned.',
          citations,
        }]);
      } else {
        setMessages(prev => [...prev, {
          type: 'bot',
          text: `Error: ${data.error || 'Request failed'}`,
          isError: true,
        }]);
      }
    } catch (err) {
      setMessages(prev => [...prev, {
        type: 'bot',
        text: 'Could not reach the backend. Make sure the Flask server is running on port 5000.',
        isError: true,
      }]);
    } finally {
      setLoading(false);
    }
  };

  const toggleVoice = () => {
    if (!('webkitSpeechRecognition' in window || 'SpeechRecognition' in window)) {
      alert('Voice recognition is not supported in this browser. Try Chrome.');
      return;
    }
    if (recording) {
      recognitionRef.current?.stop();
      setRecording(false);
      return;
    }
    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    const rec = new SpeechRecognition();
    rec.lang = 'en-IN';
    rec.interimResults = false;
    rec.onresult = (e) => {
      const transcript = e.results[0][0].transcript;
      sendMessage(transcript);
    };
    rec.onend = () => setRecording(false);
    rec.onerror = () => setRecording(false);
    recognitionRef.current = rec;
    rec.start();
    setRecording(true);
  };

  return (
    <div style={{ display:'flex', flexDirection:'column', height:'calc(100vh - 64px)' }}>
      {backendStatus && (
        <div style={{ padding:'6px 1.5rem', background: backendStatus === 'connected' ? 'var(--color-background-success)' : 'var(--color-background-danger)', borderBottom:'0.5px solid var(--color-border-tertiary)', display:'flex', alignItems:'center', gap:6 }}>
          <div style={{ width:7, height:7, borderRadius:'50%', background: backendStatus === 'connected' ? 'var(--color-text-success)' : 'var(--color-text-danger)' }} />
          <span style={{ fontSize:13, color: backendStatus === 'connected' ? 'var(--color-text-success)' : 'var(--color-text-danger)' }}>
            {backendStatus === 'connected' ? 'Backend connected (AWS Bedrock Knowledge Base)' : 'Backend offline — start Flask server on port 5000'}
          </span>
        </div>
      )}

      <div style={{ flex:1, overflowY:'auto', padding:'1.5rem', display:'flex', flexDirection:'column', gap:12 }}>
        {messages.map((msg, i) => (
          <div key={i} style={{ display:'flex', justifyContent: msg.type === 'user' ? 'flex-end' : 'flex-start' }}>
            <div style={{
              maxWidth:'72%',
              padding:'10px 14px',
              borderRadius: msg.type === 'user' ? '12px 12px 4px 12px' : '12px 12px 12px 4px',
              background: msg.type === 'user' ? '#1a56db' : msg.isError ? 'var(--color-background-danger)' : 'var(--color-background-secondary)',
              color: msg.type === 'user' ? '#fff' : msg.isError ? 'var(--color-text-danger)' : 'var(--color-text-primary)',
              fontSize:14,
              lineHeight:1.6,
              border: msg.isError ? '0.5px solid var(--color-border-danger)' : 'none',
            }}>
              {msg.text}
              {msg.citations && msg.citations.length > 0 && (
                <div style={{ marginTop:8, paddingTop:8, borderTop:'0.5px solid rgba(0,0,0,0.1)' }}>
                  <p style={{ margin:'0 0 4px', fontSize:12, fontWeight:500, opacity:0.7 }}>Sources ({msg.citations.length})</p>
                  {msg.citations.slice(0, 3).map((c, ci) => (
                    <div key={ci} style={{ fontSize:12, opacity:0.75, marginTop:2, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                      {c.text?.slice(0, 80)}…
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        ))}
        {loading && (
          <div style={{ display:'flex', justifyContent:'flex-start' }}>
            <div style={{ padding:'10px 16px', borderRadius:'12px 12px 12px 4px', background:'var(--color-background-secondary)', display:'flex', alignItems:'center', gap:8 }}>
              <Loader size={14} style={{ animation:'spin 1s linear infinite', color:'var(--color-text-secondary)' }} />
              <span style={{ fontSize:13, color:'var(--color-text-secondary)' }}>Searching knowledge base…</span>
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      <div style={{ padding:'1rem 1.5rem', borderTop:'0.5px solid var(--color-border-tertiary)', display:'flex', gap:8, alignItems:'center', background:'var(--color-background-primary)' }}>
        <input
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && sendMessage()}
          placeholder="Ask about your documents…"
          disabled={loading}
          style={{ flex:1 }}
        />
        <button onClick={toggleVoice} title={recording ? 'Stop recording' : 'Start voice input'}
          style={{ padding:'8px', borderRadius:'var(--border-radius-md)', border: recording ? '1.5px solid var(--color-border-danger)' : '0.5px solid var(--color-border-secondary)', background: recording ? 'var(--color-background-danger)' : 'transparent', cursor:'pointer', display:'flex', alignItems:'center' }}>
          {recording ? <MicOff size={18} style={{ color:'var(--color-text-danger)' }} /> : <Mic size={18} style={{ color:'var(--color-text-secondary)' }} />}
        </button>
        <button onClick={() => sendMessage()} disabled={loading || !input.trim()}
          style={{ padding:'8px 16px', background:'#1a56db', color:'#fff', border:'none', borderRadius:'var(--border-radius-md)', fontWeight:500, cursor:'pointer', display:'flex', alignItems:'center', gap:6, opacity: loading || !input.trim() ? 0.5 : 1 }}>
          <Send size={16} /> Send
        </button>
      </div>

      <style>{`@keyframes spin { from{transform:rotate(0deg)} to{transform:rotate(360deg)} }`}</style>
    </div>
  );
};

// ─── Main App ────────────────────────────────────────────────────────────────
export default function App() {
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [showAuth, setShowAuth] = useState(false);
  const [authMode, setAuthMode] = useState('login');
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [activeTab, setActiveTab] = useState('dashboard');
  const [documents, setDocuments] = useState([
    { id: 1, name: 'Aadhar_Card.pdf', type: 'pdf', size: '2.4 MB', date: '2024-11-15', encrypted: true },
    { id: 2, name: 'PAN_Card.jpg', type: 'image', size: '1.2 MB', date: '2024-11-10', encrypted: true },
    { id: 3, name: 'Certificate.pdf', type: 'pdf', size: '850 KB', date: '2024-11-05', encrypted: true },
  ]);
  const [pinModal, setPinModal] = useState({ open: false, docId: null });
  const [searchQuery, setSearchQuery] = useState('');

  const filteredDocs = documents.filter(d => d.name.toLowerCase().includes(searchQuery.toLowerCase()));

  if (!isLoggedIn) {
    return (
      <div style={{ minHeight:'100vh', background:'var(--color-background-tertiary)' }}>
        {showAuth && <AuthForm onClose={() => setShowAuth(false)} onLogin={() => { setIsLoggedIn(true); setShowAuth(false); }} />}

        <nav style={{ background:'var(--color-background-primary)', borderBottom:'0.5px solid var(--color-border-tertiary)', padding:'1rem 2rem', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
          <div style={{ display:'flex', alignItems:'center', gap:8 }}>
            <Shield size={28} style={{ color:'#1a56db' }} />
            <span style={{ fontSize:20, fontWeight:500, color:'var(--color-text-primary)' }}>SecureVault</span>
          </div>
          <button onClick={() => { setShowAuth(true); setAuthMode('login'); }}
            style={{ padding:'8px 20px', background:'#1a56db', color:'#fff', border:'none', borderRadius:'var(--border-radius-md)', fontWeight:500, cursor:'pointer' }}>
            Sign in
          </button>
        </nav>

        <div style={{ maxWidth:960, margin:'0 auto', padding:'4rem 2rem' }}>
          <div style={{ textAlign:'center', marginBottom:'4rem' }}>
            <h1 style={{ fontSize:'clamp(2rem, 5vw, 3.5rem)', fontWeight:500, color:'var(--color-text-primary)', marginBottom:'1rem', lineHeight:1.2 }}>
              Privacy-preserving document vault
            </h1>
            <p style={{ fontSize:18, color:'var(--color-text-secondary)', marginBottom:'2rem', maxWidth:580, margin:'0 auto 2rem' }}>
              Store, search, and interact with your personal documents using AI-powered retrieval. All data encrypted, all queries private.
            </p>
            <button onClick={() => { setShowAuth(true); setAuthMode('register'); }}
              style={{ padding:'12px 32px', background:'#1a56db', color:'#fff', border:'none', borderRadius:'var(--border-radius-lg)', fontWeight:500, fontSize:16, cursor:'pointer' }}>
              Get started free
            </button>
          </div>

          <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(260px, 1fr))', gap:16 }}>
            {[
              { icon: Lock, label: 'End-to-end encryption', desc: 'AES-256/RSA hybrid encryption. Secure at rest and in transit.' },
              { icon: Search, label: 'AI-powered search', desc: 'RAG with vector database for semantic search across documents.' },
              { icon: Mic, label: 'Voice search', desc: 'Ask questions naturally using voice. Instant answers from your files.' },
              { icon: Shield, label: 'Multi-layer auth', desc: 'PIN verification for sensitive document access.' },
              { icon: FileText, label: 'OCR extraction', desc: 'Automatic text extraction from images and scanned PDFs.' },
              { icon: Key, label: 'AWS KMS security', desc: 'Enterprise-grade key management for your encryption keys.' },
            ].map(({ icon: Icon, label, desc }) => (
              <div key={label} style={{ background:'var(--color-background-primary)', border:'0.5px solid var(--color-border-tertiary)', borderRadius:'var(--border-radius-lg)', padding:'1.5rem' }}>
                <Icon size={24} style={{ color:'#1a56db', marginBottom:12 }} />
                <h3 style={{ margin:'0 0 8px', fontWeight:500, fontSize:15, color:'var(--color-text-primary)' }}>{label}</h3>
                <p style={{ margin:0, fontSize:14, color:'var(--color-text-secondary)', lineHeight:1.6 }}>{desc}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div style={{ minHeight:'100vh', display:'flex', background:'var(--color-background-tertiary)' }}>
      {pinModal.open && <PinModal onClose={() => setPinModal({ open: false })} onVerify={() => { setPinModal({ open: false }); }} />}

      {/* Sidebar */}
      <div style={{ width: sidebarOpen ? 220 : 60, background:'var(--color-background-primary)', borderRight:'0.5px solid var(--color-border-tertiary)', display:'flex', flexDirection:'column', transition:'width 0.2s', flexShrink:0, overflow:'hidden' }}>
        <div style={{ padding:'1rem', display:'flex', alignItems:'center', justifyContent: sidebarOpen ? 'space-between' : 'center', borderBottom:'0.5px solid var(--color-border-tertiary)' }}>
          {sidebarOpen && <div style={{ display:'flex', alignItems:'center', gap:8 }}><Shield size={22} style={{ color:'#1a56db' }} /><span style={{ fontWeight:500, fontSize:15, color:'var(--color-text-primary)' }}>SecureVault</span></div>}
          <button onClick={() => setSidebarOpen(s => !s)} style={{ background:'none', border:'none', cursor:'pointer', color:'var(--color-text-secondary)', padding:4 }}>
            {sidebarOpen ? <X size={18} /> : <Menu size={18} />}
          </button>
        </div>

        <nav style={{ flex:1, padding:'8px' }}>
          {[
            { id: 'dashboard', icon: FileText, label: 'Dashboard' },
            { id: 'documents', icon: File, label: 'My Documents' },
            { id: 'upload', icon: Upload, label: 'Upload' },
            { id: 'chat', icon: Search, label: 'AI Assistant' },
          ].map(({ id, icon: Icon, label }) => (
            <button key={id} onClick={() => setActiveTab(id)}
              style={{ width:'100%', display:'flex', alignItems:'center', gap:10, padding:'10px 12px', borderRadius:'var(--border-radius-md)', border:'none', background: activeTab === id ? 'var(--color-background-info)' : 'transparent', color: activeTab === id ? '#1a56db' : 'var(--color-text-secondary)', cursor:'pointer', fontWeight: activeTab === id ? 500 : 400, fontSize:14, marginBottom:2, justifyContent: sidebarOpen ? 'flex-start' : 'center', whiteSpace:'nowrap', overflow:'hidden' }}>
              <Icon size={18} style={{ flexShrink:0 }} />
              {sidebarOpen && label}
            </button>
          ))}
        </nav>

        <button onClick={() => setIsLoggedIn(false)}
          style={{ margin:'8px', display:'flex', alignItems:'center', gap:10, padding:'10px 12px', borderRadius:'var(--border-radius-md)', border:'none', background:'transparent', color:'var(--color-text-secondary)', cursor:'pointer', fontSize:14, justifyContent: sidebarOpen ? 'flex-start' : 'center' }}>
          <LogOut size={18} style={{ flexShrink:0 }} />
          {sidebarOpen && 'Sign out'}
        </button>
      </div>

      {/* Main content */}
      <div style={{ flex:1, overflow:'auto', display:'flex', flexDirection:'column' }}>
        <div style={{ background:'var(--color-background-primary)', borderBottom:'0.5px solid var(--color-border-tertiary)', padding:'1rem 1.5rem', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
          <h1 style={{ margin:0, fontWeight:500, fontSize:18, color:'var(--color-text-primary)' }}>
            {{ dashboard:'Dashboard', documents:'My Documents', upload:'Upload', chat:'AI Assistant' }[activeTab]}
          </h1>
          <div style={{ display:'flex', alignItems:'center', gap:12 }}>
            <div style={{ display:'flex', alignItems:'center', gap:6, fontSize:13, color:'var(--color-text-success)' }}>
              <Lock size={14} /><span>Secured</span>
            </div>
            <div style={{ width:36, height:36, borderRadius:'50%', background:'#1a56db', color:'#fff', display:'flex', alignItems:'center', justifyContent:'center', fontSize:14, fontWeight:500 }}>VI</div>
          </div>
        </div>

        {/* Dashboard */}
        {activeTab === 'dashboard' && (
          <div style={{ padding:'1.5rem' }}>
            <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(160px, 1fr))', gap:12, marginBottom:'1.5rem' }}>
              {[
                { label:'Total documents', value: documents.length, icon: FileText },
                { label:'Storage used', value: '4.5 GB', icon: Image },
                { label:'Encrypted', value: '100%', icon: Lock },
                { label:'Queries today', value: 12, icon: Search },
              ].map(({ label, value, icon: Icon }) => (
                <div key={label} style={{ background:'var(--color-background-primary)', border:'0.5px solid var(--color-border-tertiary)', borderRadius:'var(--border-radius-lg)', padding:'1rem 1.25rem' }}>
                  <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:8 }}>
                    <span style={{ fontSize:13, color:'var(--color-text-secondary)' }}>{label}</span>
                    <Icon size={16} style={{ color:'var(--color-text-secondary)' }} />
                  </div>
                  <p style={{ margin:0, fontSize:24, fontWeight:500, color:'var(--color-text-primary)' }}>{value}</p>
                </div>
              ))}
            </div>

            <div style={{ background:'var(--color-background-primary)', border:'0.5px solid var(--color-border-tertiary)', borderRadius:'var(--border-radius-lg)', padding:'1.25rem', marginBottom:'1.25rem' }}>
              <h2 style={{ margin:'0 0 1rem', fontWeight:500, fontSize:15, color:'var(--color-text-primary)' }}>Recent documents</h2>
              {documents.slice(0, 3).map(doc => (
                <div key={doc.id} style={{ display:'flex', alignItems:'center', justifyContent:'space-between', padding:'12px 0', borderBottom:'0.5px solid var(--color-border-tertiary)' }}>
                  <div style={{ display:'flex', alignItems:'center', gap:12 }}>
                    <div style={{ width:40, height:40, borderRadius:'var(--border-radius-md)', background:'var(--color-background-info)', display:'flex', alignItems:'center', justifyContent:'center' }}>
                      {doc.type === 'pdf' ? <FileText size={18} style={{ color:'#1a56db' }} /> : <Image size={18} style={{ color:'#1a56db' }} />}
                    </div>
                    <div>
                      <p style={{ margin:0, fontWeight:500, fontSize:14, color:'var(--color-text-primary)' }}>{doc.name}</p>
                      <p style={{ margin:0, fontSize:12, color:'var(--color-text-secondary)' }}>{doc.size} · {doc.date}</p>
                    </div>
                  </div>
                  <button onClick={() => setPinModal({ open: true, docId: doc.id })}
                    style={{ padding:'6px 14px', fontSize:13, color:'#1a56db', background:'transparent', border:'0.5px solid var(--color-border-info)', borderRadius:'var(--border-radius-md)', cursor:'pointer' }}>
                    View
                  </button>
                </div>
              ))}
            </div>

            <div style={{ background:'#1a56db', borderRadius:'var(--border-radius-lg)', padding:'1.5rem', color:'#fff' }}>
              <h2 style={{ margin:'0 0 8px', fontWeight:500, fontSize:18 }}>Try the AI Assistant</h2>
              <p style={{ margin:'0 0 1rem', opacity:0.85, fontSize:14 }}>Ask "Show my address from Aadhar card" or "What's my PAN number?" — queries go directly to your AWS Bedrock Knowledge Base.</p>
              <button onClick={() => setActiveTab('chat')} style={{ padding:'8px 20px', background:'rgba(255,255,255,0.15)', color:'#fff', border:'1px solid rgba(255,255,255,0.4)', borderRadius:'var(--border-radius-md)', cursor:'pointer', fontWeight:500, fontSize:14 }}>
                Open AI Assistant
              </button>
            </div>
          </div>
        )}

        {/* Documents */}
        {activeTab === 'documents' && (
          <div style={{ padding:'1.5rem' }}>
            <div style={{ display:'flex', gap:8, marginBottom:'1.25rem' }}>
              <input value={searchQuery} onChange={e => setSearchQuery(e.target.value)} placeholder="Search documents…" style={{ flex:1 }} />
              <button style={{ padding:'8px 16px', display:'flex', alignItems:'center', gap:6, fontSize:14 }}><Search size={16} />Search</button>
            </div>
            <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fill, minmax(220px, 1fr))', gap:12 }}>
              {filteredDocs.map(doc => (
                <div key={doc.id} style={{ background:'var(--color-background-primary)', border:'0.5px solid var(--color-border-tertiary)', borderRadius:'var(--border-radius-lg)', padding:'1.25rem' }}>
                  <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:12 }}>
                    <div style={{ width:44, height:44, borderRadius:'var(--border-radius-md)', background:'var(--color-background-info)', display:'flex', alignItems:'center', justifyContent:'center' }}>
                      {doc.type === 'pdf' ? <FileText size={22} style={{ color:'#1a56db' }} /> : <Image size={22} style={{ color:'#1a56db' }} />}
                    </div>
                    <div style={{ display:'flex', alignItems:'center', gap:4, fontSize:12, color:'var(--color-text-success)' }}>
                      <Lock size={12} /><span>Encrypted</span>
                    </div>
                  </div>
                  <p style={{ margin:'0 0 4px', fontWeight:500, fontSize:14, color:'var(--color-text-primary)', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{doc.name}</p>
                  <p style={{ margin:'0 0 12px', fontSize:12, color:'var(--color-text-secondary)' }}>{doc.size} · {doc.date}</p>
                  <button onClick={() => setPinModal({ open: true, docId: doc.id })}
                    style={{ width:'100%', padding:'8px 0', background:'#1a56db', color:'#fff', border:'none', borderRadius:'var(--border-radius-md)', cursor:'pointer', fontWeight:500, fontSize:14 }}>
                    View document
                  </button>
                </div>
              ))}
              {filteredDocs.length === 0 && (
                <p style={{ color:'var(--color-text-secondary)', fontSize:14 }}>No documents match "{searchQuery}"</p>
              )}
            </div>
          </div>
        )}

        {activeTab === 'upload' && <UploadTab onUpload={newDocs => setDocuments(prev => [...prev, ...newDocs])} />}
        {activeTab === 'chat' && <ChatTab />}
      </div>
    </div>
  );
}