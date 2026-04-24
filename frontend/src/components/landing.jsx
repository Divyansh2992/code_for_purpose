import { useRef, useState, useEffect } from "react";
import { Upload, ArrowRight, Sparkles, Database, Zap } from "lucide-react";
import { uploadCSV } from "../api/client";

/* Animated number counter */
function Counter({ end, suffix = "" }) {
  const [val, setVal] = useState(0);
  useEffect(() => {
    let start = 0;
    const step = Math.ceil(end / 40);
    const timer = setInterval(() => {
      start += step;
      if (start >= end) { setVal(end); clearInterval(timer); }
      else setVal(start);
    }, 30);
    return () => clearInterval(timer);
  }, [end]);
  return <>{val.toLocaleString()}{suffix}</>;
}

export default function LandingPage({ onUpload }) {
  const inputRef = useRef();
  const [dragOver, setDragOver] = useState(false);
  const [loading, setLoading] = useState(false);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    const t = setTimeout(() => setMounted(true), 60);
    return () => clearTimeout(t);
  }, []);

  const handleFile = async (file) => {
    if (!file || !file.name.toLowerCase().endsWith(".csv")) return;
    setLoading(true);
    try {
      const data = await uploadCSV(file);
      onUpload(data);
    } catch (e) {
      console.error("Upload failed:", e);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={s.page}>
      {/* ── Particle grid bg ── */}
      <svg style={s.gridSvg} xmlns="http://www.w3.org/2000/svg">
        <defs>
          <pattern id="grid" width="50" height="50" patternUnits="userSpaceOnUse">
            <path d="M 50 0 L 0 0 0 50" fill="none" stroke="rgba(139,92,246,0.07)" strokeWidth="1"/>
          </pattern>
        </defs>
        <rect width="100%" height="100%" fill="url(#grid)" />
      </svg>

      {/* ── Ambient orbs ── */}
      <div style={s.orb1} />
      <div style={s.orb2} />
      <div style={s.orb3} />

      {/* ── Navbar ── */}
      <nav style={s.navbar}>
        <div style={s.logoWrap}>
          <div style={s.logoIcon}>
            <Database size={18} color="#fff" />
          </div>
          <span style={s.logoText}>DataLens</span>
        </div>
        <div style={s.navRight}>
          <span style={s.navTag}>v2.0</span>
          <div style={s.navPill}>
            <span style={s.navDot} />
            AI-Ready
          </div>
        </div>
      </nav>

      {/* ── Main content ── */}
      <main style={s.main}>

        {/* Eyebrow */}
        <div style={{ ...s.eyebrow, opacity: mounted ? 1 : 0, transform: mounted ? 'none' : 'translateY(16px)', transition: 'all 0.6s cubic-bezier(0.23,1,0.32,1)' }}>
          
        </div>

        {/* Hero headline */}
        <h1 style={{ ...s.headline, opacity: mounted ? 1 : 0, transform: mounted ? 'none' : 'translateY(24px)', transition: 'all 0.7s 0.1s cubic-bezier(0.23,1,0.32,1)' }}>
          Your Data<br />
          <span style={s.headlineAccent}>Speaks Now</span>
        </h1>

        {/* Subline */}
        <p style={{ ...s.sub, opacity: mounted ? 1 : 0, transform: mounted ? 'none' : 'translateY(20px)', transition: 'all 0.7s 0.2s cubic-bezier(0.23,1,0.32,1)' }}>
          Upload a CSV. Ask anything in plain English.<br />
          Get charts, SQL, and insights — instantly.
        </p>

        {/* Capability chips */}
        <div style={{ ...s.chips, opacity: mounted ? 1 : 0, transition: 'all 0.7s 0.3s cubic-bezier(0.23,1,0.32,1)' }}>
          {[
            { icon: "⚡", label: "DuckDB Engine" },
            { icon: "🤖", label: "Groq AI" },
            { icon: "📊", label: "Auto Visualize" },
            { icon: "🔍", label: "SQL Explain" },
          ].map((c, i) => (
            <div key={i} style={{ ...s.chip, animationDelay: `${i * 0.08}s` }}>
              <span>{c.icon}</span>
              <span style={s.chipLabel}>{c.label}</span>
            </div>
          ))}
        </div>

        {/* Upload zone */}
        <div
          style={{
            ...s.uploadZone,
            ...(dragOver ? s.uploadZoneActive : {}),
            opacity: mounted ? 1 : 0,
            transform: mounted ? 'none' : 'translateY(20px) scale(0.98)',
            transition: 'opacity 0.7s 0.35s cubic-bezier(0.23,1,0.32,1), transform 0.7s 0.35s cubic-bezier(0.23,1,0.32,1), border-color 0.22s, box-shadow 0.22s',
          }}
          onClick={() => !loading && inputRef.current.click()}
          onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
          onDragLeave={() => setDragOver(false)}
          onDrop={(e) => { e.preventDefault(); setDragOver(false); handleFile(e.dataTransfer.files[0]); }}
        >
          <input ref={inputRef} type="file" accept=".csv" style={{ display: "none" }} onChange={(e) => handleFile(e.target.files[0])} />

          {/* Upload icon */}
          <div style={{ ...s.uploadIconRing, ...(dragOver ? s.uploadIconRingActive : {}) }}>
            {loading
              ? <div style={s.spinner} />
              : <Upload size={28} color={dragOver ? "#fff" : "#8b5cf6"} />
            }
          </div>

          <div style={s.uploadMain}>
            {loading
              ? <span style={s.uploadTitle}>Processing your data<span style={s.ellipsis}>...</span></span>
              : dragOver
              ? <span style={{ ...s.uploadTitle, color: '#c4b5fd' }}>Release to upload</span>
              : <span style={s.uploadTitle}>Drop your CSV here</span>
            }
            <span style={s.uploadSub}>or click to browse · .csv files only</span>
          </div>

          {/* Corner shimmer */}
          {dragOver && <div style={s.shimmer} />}
        </div>

        {/* CTA row */}
        <div style={{ ...s.ctaRow, opacity: mounted ? 1 : 0, transition: 'all 0.7s 0.45s cubic-bezier(0.23,1,0.32,1)' }}>
          <button style={s.ctaBtn} onClick={() => inputRef.current.click()} disabled={loading}>
            {loading ? "Analyzing…" : "Get Started"}
            <ArrowRight size={16} />
          </button>
          <span style={s.ctaHint}>No account needed · No SQL required</span>
        </div>

        {/* Stats row */}
        <div style={{ ...s.statsRow, opacity: mounted ? 1 : 0, transition: 'all 0.7s 0.55s cubic-bezier(0.23,1,0.32,1)' }}>
          {[
            { n: 10, s: "M+", label: "Rows Processed" },
            { n: 3,  s: " modes", label: "Query Engines" },
            { n: 100, s: "ms", label: "Avg Response" },
          ].map((st, i) => (
            <div key={i} style={s.statItem}>
              <div style={s.statNum}>
                {mounted && <Counter end={st.n} suffix={st.s} />}
              </div>
              <div style={s.statLabel}>{st.label}</div>
            </div>
          ))}
        </div>

      </main>

      {/* Bottom tagline */}
      <footer style={s.footer}>
        <span style={s.footerTag}>Powered by</span>
        {["DuckDB", "Groq", "FastAPI", "React"].map((t, i) => (
          <span key={i} style={s.footerPill}>{t}</span>
        ))}
      </footer>
    </div>
  );
}

/* ════════════════════════════════════ STYLES */
const s = {
  page: {
    height: "100vh",            
    width: "100%",
    background: "#02030a",
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    justifyContent: "flex-start", 
    position: "relative",
    overflow: "hidden",
    fontFamily: "'Outfit', system-ui, sans-serif",
    color: "#eef2ff",
    padding: "0 20px",
  },
  gridSvg: {
    position: "absolute",
    inset: 0,
    width: "100%", height: "100%",
    zIndex: 0,
    pointerEvents: "none",
  },
  orb1: {
    position: "absolute",
    width: 480, height: 480,
    top: "5%", left: "5%",
    background: "radial-gradient(circle, rgba(139,92,246,0.18) 0%, transparent 70%)",
    filter: "blur(60px)",
    zIndex: 0, pointerEvents: "none",
  },
  orb2: {
    position: "absolute",
    width: 380, height: 380,
    bottom: "5%", right: "8%",
    background: "radial-gradient(circle, rgba(34,211,238,0.15) 0%, transparent 70%)",
    filter: "blur(60px)",
    zIndex: 0, pointerEvents: "none",
  },
  orb3: {
    position: "absolute",
    width: 240, height: 240,
    top: "55%", left: "40%",
    background: "radial-gradient(circle, rgba(52,211,153,0.08) 0%, transparent 70%)",
    filter: "blur(40px)",
    zIndex: 0, pointerEvents: "none",
  },

  /* Navbar */
  navbar: {
    position: "absolute",
    top: 28, left: "50%",
    transform: "translateX(-50%)",
    width: "92%", maxWidth: 1100,
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    padding: "12px 20px",
    background: "rgba(8,12,24,0.7)",
    backdropFilter: "blur(20px)",
    border: "1px solid rgba(255,255,255,0.07)",
    borderRadius: 9999,
    zIndex: 10,
  },
  logoWrap: { display: "flex", alignItems: "center", gap: 10, },
  logoIcon: {
    width: 34, height: 34,
    background: "linear-gradient(135deg, #8b5cf6, #22d3ee)",
    borderRadius: 10,
    display: "flex", alignItems: "center", justifyContent: "center",
    boxShadow: "0 4px 14px rgba(139,92,246,0.4)",
  },
  logoText: {
    fontFamily: "'Syne', sans-serif",
    fontSize: 16, fontWeight: 800,
    background: "linear-gradient(135deg, #8b5cf6, #22d3ee)",
    WebkitBackgroundClip: "text",
    WebkitTextFillColor: "transparent",
    letterSpacing: "-0.02em",
  },
  navRight: { display: "flex", alignItems: "center", gap: 10 },
  navTag: {
    fontFamily: "'IBM Plex Mono', monospace",
    fontSize: 10, fontWeight: 600,
    color: "rgba(139,92,246,0.8)",
    letterSpacing: "0.08em",
  },
  navPill: {
    display: "flex", alignItems: "center", gap: 6,
    fontSize: 11, fontWeight: 600,
    padding: "4px 12px",
    background: "rgba(52,211,153,0.08)",
    border: "1px solid rgba(52,211,153,0.2)",
    borderRadius: 9999,
    color: "#34d399",
    fontFamily: "'IBM Plex Mono', monospace",
    letterSpacing: "0.04em",
  },
  navDot: {
    width: 6, height: 6,
    borderRadius: "50%",
    background: "#34d399",
    boxShadow: "0 0 6px #34d399",
    animation: "pulse 2s ease-in-out infinite",
  },

  /* Main */
  main: {
    position: "relative",
    zIndex: 2,
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    gap: 24,
    maxWidth: 680,
    width: "100%",
    textAlign: "center",
    paddingTop: 80,
    paddingBottom: 100,
    transform: "scale(0.92)",
    transformOrigin: "top center",
  },

  eyebrow: {
    display: "inline-flex",
    alignItems: "center",
    gap: 7,
    fontSize: 12,
    fontFamily: "'IBM Plex Mono', monospace",
    fontWeight: 600,
    letterSpacing: "0.1em",
    textTransform: "uppercase",
    padding: "6px 16px",
    background: "rgba(139,92,246,0.1)",
    border: "1px solid rgba(139,92,246,0.3)",
    borderRadius: 9999,
    color: "#c4b5fd",
  },

  headline: {
    fontFamily: "'Syne', sans-serif",
    fontSize: "clamp(48px, 7vw, 76px)",
    fontWeight: 800,
    lineHeight: 1.05,
    letterSpacing: "-0.04em",
    color: "#eef2ff",
  },
  headlineAccent: {
    background: "linear-gradient(135deg, #8b5cf6 0%, #22d3ee 60%, #34d399 100%)",
    WebkitBackgroundClip: "text",
    WebkitTextFillColor: "transparent",
    backgroundClip: "text",
  },

  sub: {
    fontSize: 17,
    color: "#7c87a0",
    lineHeight: 1.75,
    maxWidth: 480,
    fontWeight: 400,
  },

  chips: {
    display: "flex",
    flexWrap: "wrap",
    gap: 10,
    justifyContent: "center",
  },
  chip: {
    display: "flex", alignItems: "center", gap: 7,
    fontSize: 12,
    padding: "7px 14px",
    background: "rgba(255,255,255,0.04)",
    border: "1px solid rgba(255,255,255,0.09)",
    borderRadius: 9999,
    color: "#94a3b8",
    backdropFilter: "blur(10px)",
    fontFamily: "'IBM Plex Mono', monospace",
    letterSpacing: "0.02em",
  },
  chipLabel: { fontWeight: 500 },

  /* Upload */
  uploadZone: {
    width: "100%",
    padding: "36px 32px",
    borderRadius: 24,
    border: "1.5px dashed rgba(139,92,246,0.35)",
    background: "rgba(139,92,246,0.04)",
    backdropFilter: "blur(20px)",
    cursor: "pointer",
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    gap: 14,
    position: "relative",
    overflow: "hidden",
  },
  uploadZoneActive: {
    border: "1.5px dashed #8b5cf6",
    background: "rgba(139,92,246,0.1)",
    boxShadow: "0 0 60px rgba(139,92,246,0.2), inset 0 0 40px rgba(139,92,246,0.06)",
    transform: "scale(1.015)",
  },

  uploadIconRing: {
    width: 72, height: 72,
    borderRadius: "50%",
    background: "rgba(139,92,246,0.1)",
    border: "1.5px solid rgba(139,92,246,0.3)",
    display: "flex", alignItems: "center", justifyContent: "center",
    transition: "all 0.22s cubic-bezier(0.23,1,0.32,1)",
  },
  uploadIconRingActive: {
    background: "rgba(139,92,246,0.25)",
    border: "1.5px solid #8b5cf6",
    boxShadow: "0 0 24px rgba(139,92,246,0.5)",
    transform: "scale(1.08)",
  },

  spinner: {
    width: 28, height: 28,
    border: "2.5px solid rgba(139,92,246,0.2)",
    borderTop: "2.5px solid #8b5cf6",
    borderRadius: "50%",
    animation: "spin 0.8s linear infinite",
  },

  uploadMain: { display: "flex", flexDirection: "column", gap: 5 },
  uploadTitle: {
    fontFamily: "'Syne', sans-serif",
    fontSize: 20,
    fontWeight: 700,
    color: "#eef2ff",
    letterSpacing: "-0.02em",
  },
  uploadSub: {
    fontFamily: "'IBM Plex Mono', monospace",
    fontSize: 11,
    color: "#3b4560",
    letterSpacing: "0.04em",
  },

  shimmer: {
    position: "absolute",
    inset: 0,
    background: "linear-gradient(135deg, transparent 0%, rgba(139,92,246,0.06) 50%, transparent 100%)",
    animation: "shimmer 1.5s ease-in-out infinite",
    pointerEvents: "none",
  },

  ellipsis: { display: "inline-block" },

  /* CTA */
  ctaRow: { display: "flex", flexDirection: "column", alignItems: "center", gap: 10 },
  ctaBtn: {
    display: "inline-flex",
    alignItems: "center",
    gap: 10,
    padding: "14px 32px",
    borderRadius: 9999,
    border: "none",
    background: "linear-gradient(135deg, #8b5cf6 0%, #22d3ee 100%)",
    color: "#fff",
    fontFamily: "'Syne', sans-serif",
    fontSize: 16,
    fontWeight: 700,
    letterSpacing: "-0.01em",
    cursor: "pointer",
    boxShadow: "0 8px 30px rgba(139,92,246,0.45)",
    transition: "all 0.22s cubic-bezier(0.23,1,0.32,1)",
  },
  ctaHint: {
    fontFamily: "'IBM Plex Mono', monospace",
    fontSize: 10,
    color: "#3b4560",
    letterSpacing: "0.06em",
  },

  /* Stats */
  statsRow: {
    display: "flex",
    gap: 40,
    justifyContent: "center",
    padding: "16px 0 0",
    borderTop: "1px solid rgba(255,255,255,0.05)",
    width: "100%",
  },
  statItem: { display: "flex", flexDirection: "column", alignItems: "center", gap: 3 },
  statNum: {
    fontFamily: "'Syne', sans-serif",
    fontSize: 22,
    fontWeight: 800,
    background: "linear-gradient(135deg, #8b5cf6, #22d3ee)",
    WebkitBackgroundClip: "text",
    WebkitTextFillColor: "transparent",
    letterSpacing: "-0.03em",
  },
  statLabel: {
    fontFamily: "'IBM Plex Mono', monospace",
    fontSize: 9,
    color: "#3b4560",
    letterSpacing: "0.1em",
    textTransform: "uppercase",
  },

  /* Footer */
  footer: {
    marginTop: 40,
    marginBottom: 20,
    display: "flex",
    alignItems: "center",
    gap: 8,
    zIndex: 2,
    paddingTop: 70,
    paddingBottom: 60,
  },
  footerTag: {
    fontFamily: "'IBM Plex Mono', monospace",
    fontSize: 10,
    color: "#3b4560",
    letterSpacing: "0.06em",
  },
  footerPill: {
    fontFamily: "'IBM Plex Mono', monospace",
    fontSize: 9.5,
    fontWeight: 600,
    padding: "3px 9px",
    borderRadius: 9999,
    background: "rgba(255,255,255,0.04)",
    border: "1px solid rgba(255,255,255,0.07)",
    color: "#475569",
    letterSpacing: "0.04em",
  },
};
