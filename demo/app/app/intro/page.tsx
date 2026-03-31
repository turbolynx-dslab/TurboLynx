"use client";
import { useState, useEffect, useCallback, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import Link from "next/link";

const EASE = [0.16, 1, 0.3, 1] as const;

const tagPill = (color: string, label: string) => (
  <span key={label} style={{
    fontSize: 12, fontWeight: 600, padding: "2px 10px",
    borderRadius: 6, background: color + "12", color,
  }}>{label}</span>
);

// ─── Slide 0: Title Banner ────────────────────────────────────────────
function SlideBanner() {
  return (
    <div style={{
      height: "100%", display: "flex", flexDirection: "column",
      alignItems: "center", justifyContent: "center",
    }}>
      <motion.img
        src="/logo.png"
        alt="TurboLynx"
        initial={{ opacity: 0, scale: 0.85 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.7, ease: EASE }}
        style={{ width: 220, height: "auto", marginBottom: 12 }}
      />
      <motion.h1
        initial={{ opacity: 0, scale: 0.92 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.8, delay: 0.15, ease: EASE }}
        style={{
          fontSize: 96, fontWeight: 800, color: "#1a1a1a",
          letterSpacing: "-0.04em", margin: 0, lineHeight: 1,
        }}
      >
        Turbo<span style={{ color: "#e84545" }}>Lynx</span>
      </motion.h1>
      <motion.p
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6, delay: 0.3, ease: EASE }}
        style={{
          fontSize: 24, color: "#86868b", marginTop: 20,
          fontWeight: 500, letterSpacing: "-0.01em",
          textAlign: "center", lineHeight: 1.4,
        }}
      >
        A High-Performance Graph Analytics Engine<br />
        for Schemaless Property Graphs
      </motion.p>
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.5, delay: 0.7 }}
        style={{
          marginTop: 48, display: "flex", gap: 24,
          fontSize: 15, color: "#86868b", fontWeight: 500,
        }}
      >
        <span>VLDB 2026</span>
        <span style={{ color: "#d2d2d7" }}>|</span>
        <span>Demo Paper</span>
      </motion.div>
    </div>
  );
}

// ─── Slide 1: Use Cases ───────────────────────────────────────────────
const USE_CASES = [
  {
    icon: (
      <svg width="40" height="40" viewBox="0 0 40 40" fill="none">
        <rect x="4" y="8" width="32" height="24" rx="4" stroke="#DC2626" strokeWidth="2.2" />
        <path d="M12 20 L18 26 L28 14" stroke="#DC2626" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
        <circle cx="20" cy="6" r="3" fill="#DC262640" stroke="#DC2626" strokeWidth="1.5" />
      </svg>
    ),
    title: "Fraud Detection",
    desc: "Trace complex transaction patterns across heterogeneous entity networks in real time",
    color: "#DC2626",
  },
  {
    icon: (
      <svg width="40" height="40" viewBox="0 0 40 40" fill="none">
        <circle cx="14" cy="14" r="5" stroke="#6366F1" strokeWidth="2" />
        <circle cx="28" cy="14" r="5" stroke="#6366F1" strokeWidth="2" />
        <circle cx="21" cy="28" r="5" stroke="#6366F1" strokeWidth="2" />
        <line x1="18" y1="16" x2="23" y2="24" stroke="#6366F1" strokeWidth="1.5" strokeDasharray="3,2" />
        <line x1="24" y1="16" x2="22" y2="24" stroke="#6366F1" strokeWidth="1.5" strokeDasharray="3,2" />
        <line x1="19" y1="14" x2="23" y2="14" stroke="#6366F1" strokeWidth="1.5" strokeDasharray="3,2" />
      </svg>
    ),
    title: "Recommendation",
    desc: "Navigate user\u2013item\u2013context relationships with diverse schemas at interactive speed",
    color: "#6366F1",
  },
  {
    icon: (
      <svg width="40" height="40" viewBox="0 0 40 40" fill="none">
        <circle cx="20" cy="20" r="14" stroke="#10B981" strokeWidth="2" />
        <ellipse cx="20" cy="20" rx="14" ry="6" stroke="#10B981" strokeWidth="1.2" />
        <ellipse cx="20" cy="20" rx="6" ry="14" stroke="#10B981" strokeWidth="1.2" />
        <circle cx="20" cy="20" r="2" fill="#10B981" />
      </svg>
    ),
    title: "Knowledge Bases",
    desc: "Query massive graphs like DBpedia \u2014 77M nodes, 282K unique attribute sets",
    color: "#10B981",
  },
];

function SlideUseCases() {
  return (
    <div style={{
      height: "100%", display: "flex", flexDirection: "column",
      alignItems: "center", justifyContent: "center", gap: 40,
    }}>
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6, ease: EASE }}
        style={{ textAlign: "center" }}
      >
        <h2 style={{
          fontSize: 44, fontWeight: 800, color: "#1a1a1a",
          letterSpacing: "-0.03em", margin: 0, lineHeight: 1.2,
        }}>
          Real-World Data Defies Structure
        </h2>
        <p style={{
          fontSize: 20, color: "#86868b", marginTop: 14, fontWeight: 500, lineHeight: 1.5,
        }}>
          As business needs evolve, data models must adapt instantly<br />
          without costly downtime or complex migrations
        </p>
      </motion.div>

      <div style={{ display: "flex", gap: 24, maxWidth: 960 }}>
        {USE_CASES.map((uc, i) => (
          <motion.div
            key={uc.title}
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.15 + i * 0.12, ease: EASE }}
            style={{
              flex: 1, padding: "32px 28px",
              background: "#fff",
              borderRadius: 16,
              border: `1px solid ${uc.color}20`,
              boxShadow: "0 2px 16px rgba(0,0,0,0.04)",
              display: "flex", flexDirection: "column", gap: 14,
            }}
          >
            <div>{uc.icon}</div>
            <h3 style={{
              fontSize: 20, fontWeight: 700, color: uc.color,
              margin: 0, letterSpacing: "-0.01em",
            }}>
              {uc.title}
            </h3>
            <p style={{
              fontSize: 15, color: "#71717a", lineHeight: 1.55, margin: 0,
            }}>
              {uc.desc}
            </p>
          </motion.div>
        ))}
      </div>

      <motion.p
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.5, delay: 0.65 }}
        style={{
          fontSize: 18, color: "#86868b", fontWeight: 500,
          fontStyle: "italic",
        }}
      >
        The demand for ultimate agility drives modern enterprises to schemaless property graphs.
      </motion.p>
    </div>
  );
}

// ─── Slide 2: The Trade-Off ─────────────────────────────────────────
function SlideTradeoff() {
  return (
    <div style={{
      height: "100%", display: "flex", flexDirection: "column",
      alignItems: "center", justifyContent: "center", gap: 44,
    }}>
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6, ease: EASE }}
        style={{ textAlign: "center" }}
      >
        <p style={{
          fontSize: 15, fontWeight: 700, color: "#e84545",
          letterSpacing: "0.1em", textTransform: "uppercase",
          margin: "0 0 16px",
        }}>
          The Accepted Trade-Off
        </p>
        <h2 style={{
          fontSize: 42, fontWeight: 800, color: "#1a1a1a",
          letterSpacing: "-0.03em", margin: 0, lineHeight: 1.3,
        }}>
          &ldquo;Flexibility comes at the<br />
          expense of analytical speed.&rdquo;
        </h2>
      </motion.div>

      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.6, delay: 0.25, ease: EASE }}
        style={{ display: "flex", alignItems: "center", gap: 36 }}
      >
        <div style={{
          width: 260, padding: "32px 24px", textAlign: "center",
          background: "linear-gradient(135deg, #ECFDF5 0%, #D1FAE5 100%)",
          borderRadius: 18, border: "1px solid #10B98125",
        }}>
          <svg width="44" height="44" viewBox="0 0 44 44" fill="none" style={{ marginBottom: 10 }}>
            <path d="M22 6 C28 6 34 12 34 18 C34 24 28 30 22 38 C16 30 10 24 10 18 C10 12 16 6 22 6Z" stroke="#10B981" strokeWidth="2" fill="#10B98115" />
            <circle cx="22" cy="18" r="4" stroke="#10B981" strokeWidth="2" />
          </svg>
          <div style={{ fontSize: 21, fontWeight: 700, color: "#10B981" }}>
            Schema Flexibility
          </div>
          <div style={{ fontSize: 14, color: "#71717a", marginTop: 8, lineHeight: 1.5 }}>
            Dynamic, evolving schemas<br />any property on any node
          </div>
        </div>

        <motion.div
          initial={{ opacity: 0, scale: 0.5 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.4, delay: 0.5, ease: EASE }}
          style={{
            fontSize: 30, fontWeight: 900, color: "#e84545",
            textShadow: "0 0 24px rgba(232,69,69,0.15)",
          }}
        >
          VS
        </motion.div>

        <div style={{
          width: 260, padding: "32px 24px", textAlign: "center",
          background: "linear-gradient(135deg, #FFFBEB 0%, #FEF3C7 100%)",
          borderRadius: 18, border: "1px solid #F59E0B25",
        }}>
          <svg width="44" height="44" viewBox="0 0 44 44" fill="none" style={{ marginBottom: 10 }}>
            <polygon points="22,4 26,16 38,16 28,24 32,36 22,28 12,36 16,24 6,16 18,16" stroke="#F59E0B" strokeWidth="2" fill="#F59E0B15" strokeLinejoin="round" />
          </svg>
          <div style={{ fontSize: 21, fontWeight: 700, color: "#F59E0B" }}>
            Analytical Speed
          </div>
          <div style={{ fontSize: 14, color: "#71717a", marginTop: 8, lineHeight: 1.5 }}>
            Columnar scans, vectorized<br />processing, optimized joins
          </div>
        </div>
      </motion.div>

      <motion.p
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.5, delay: 0.7 }}
        style={{
          fontSize: 18, color: "#71717a", fontWeight: 500,
          textAlign: "center", lineHeight: 1.55,
        }}
      >
        Traditional systems simply struggle to process<br />
        unpredictable schemas efficiently.
      </motion.p>
    </div>
  );
}

// ─── Slide 3: Breakthrough + 183× (combined) ────────────────────────
function AnimatedCounter({ target, duration = 1.2 }: { target: number; duration?: number }) {
  const [count, setCount] = useState(0);
  const frameRef = useRef(0);

  useEffect(() => {
    const start = performance.now();
    const tick = (now: number) => {
      const progress = Math.min((now - start) / (duration * 1000), 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      setCount(Math.round(eased * target));
      if (progress < 1) frameRef.current = requestAnimationFrame(tick);
    };
    frameRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frameRef.current);
  }, [target, duration]);

  return <>{count}</>;
}

function SlideBreakthrough183() {
  return (
    <div style={{
      height: "100%", display: "flex", flexDirection: "column",
      alignItems: "center", justifyContent: "center", gap: 8,
    }}>
      <motion.h1
        initial={{ opacity: 0, scale: 0.9 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.6, ease: EASE }}
        style={{
          fontSize: 64, fontWeight: 800, color: "#1a1a1a",
          letterSpacing: "-0.04em", margin: 0, lineHeight: 1,
        }}
      >
        Turbo<span style={{ color: "#e84545" }}>Lynx</span>
      </motion.h1>

      <motion.p
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, delay: 0.2, ease: EASE }}
        style={{
          fontSize: 26, fontWeight: 700, color: "#1a1a1a",
          margin: "12px 0 0", letterSpacing: "-0.02em",
        }}
      >
        We eliminate that compromise.
      </motion.p>

      <motion.p
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.4, delay: 0.4 }}
        style={{
          fontSize: 18, color: "#86868b", fontWeight: 500,
          margin: "4px 0 0", lineHeight: 1.5,
        }}
      >
        Schemaless performance built into its core.
      </motion.p>

      <motion.div
        initial={{ scaleX: 0 }}
        animate={{ scaleX: 1 }}
        transition={{ duration: 0.6, delay: 0.5, ease: EASE }}
        style={{
          width: 180, height: 2, marginTop: 20,
          background: "linear-gradient(90deg, #10B981, #e84545, #F59E0B)",
          borderRadius: 2, transformOrigin: "center",
        }}
      />

      <motion.div
        initial={{ opacity: 0, scale: 0.6 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.8, delay: 0.6, ease: EASE }}
        style={{
          fontSize: 150, fontWeight: 900, color: "#e84545",
          letterSpacing: "-0.06em", lineHeight: 1, marginTop: 12,
          textShadow: "0 0 80px rgba(232,69,69,0.12)",
        }}
      >
        <AnimatedCounter target={183} />&times;
      </motion.div>

      <motion.p
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, delay: 1.0 }}
        style={{
          fontSize: 22, color: "#52525b", fontWeight: 600,
          margin: 0, letterSpacing: "-0.01em",
        }}
      >
        faster than state-of-the-art graph databases
      </motion.p>

      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.5, delay: 1.2 }}
        style={{
          display: "flex", gap: 16, marginTop: 16,
          fontSize: 14, color: "#aeaeb2", fontWeight: 500,
        }}
      >
        {["Neo4j", "AgensGraph", "Memgraph", "Kuzu", "DuckDB", "Umbra"].map(name => (
          <span key={name} style={{
            padding: "4px 14px", borderRadius: 6,
            background: "#f4f4f5", border: "1px solid #e4e4e7",
          }}>{name}</span>
        ))}
      </motion.div>
    </div>
  );
}

// ─── Slide 4: Demo Overview + Architecture ──────────────────────────
// Graph node data for architecture visualization
const GN: { x: number; y: number; r: number; c: string }[] = [
  { x: 5, y: 8, r: 2.8, c: "#DC2626" },  { x: 12, y: 5, r: 2.2, c: "#6366F1" },
  { x: 20, y: 10, r: 3, c: "#10B981" },   { x: 28, y: 6, r: 2.5, c: "#F59E0B" },
  { x: 36, y: 9, r: 2.8, c: "#DC2626" },  { x: 44, y: 5, r: 2.2, c: "#6366F1" },
  { x: 52, y: 8, r: 3, c: "#10B981" },    { x: 60, y: 6, r: 2.5, c: "#F59E0B" },
  { x: 68, y: 10, r: 2.8, c: "#DC2626" }, { x: 76, y: 5, r: 2.2, c: "#6366F1" },
  { x: 84, y: 8, r: 3, c: "#10B981" },    { x: 92, y: 6, r: 2.5, c: "#F59E0B" },
  { x: 8, y: 22, r: 3, c: "#6366F1" },    { x: 16, y: 18, r: 2.5, c: "#DC2626" },
  { x: 24, y: 24, r: 2.2, c: "#F59E0B" }, { x: 32, y: 19, r: 3, c: "#10B981" },
  { x: 40, y: 22, r: 2.8, c: "#DC2626" }, { x: 48, y: 17, r: 2.5, c: "#6366F1" },
  { x: 56, y: 24, r: 3, c: "#F59E0B" },   { x: 64, y: 19, r: 2.2, c: "#10B981" },
  { x: 72, y: 22, r: 2.8, c: "#DC2626" }, { x: 80, y: 18, r: 3, c: "#6366F1" },
  { x: 88, y: 23, r: 2.5, c: "#F59E0B" }, { x: 96, y: 18, r: 2.2, c: "#10B981" },
  { x: 4, y: 36, r: 2.5, c: "#10B981" },  { x: 14, y: 32, r: 2.8, c: "#F59E0B" },
  { x: 22, y: 38, r: 3, c: "#6366F1" },   { x: 30, y: 34, r: 2.2, c: "#DC2626" },
  { x: 38, y: 37, r: 2.5, c: "#10B981" }, { x: 46, y: 33, r: 3, c: "#F59E0B" },
  { x: 54, y: 38, r: 2.8, c: "#6366F1" }, { x: 62, y: 34, r: 2.5, c: "#DC2626" },
  { x: 70, y: 37, r: 3, c: "#10B981" },   { x: 78, y: 33, r: 2.2, c: "#F59E0B" },
  { x: 86, y: 38, r: 2.8, c: "#6366F1" }, { x: 94, y: 34, r: 2.5, c: "#DC2626" },
];
const GE: [number, number][] = [];
for (let i = 0; i < GN.length; i++) {
  for (let j = i + 1; j < GN.length; j++) {
    const dx = GN[i].x - GN[j].x, dy = GN[i].y - GN[j].y;
    if (Math.sqrt(dx * dx + dy * dy) < 16) GE.push([i, j]);
  }
}
const GL_COLORS = ["#DC2626", "#6366F1", "#10B981", "#F59E0B", "#8B5CF6"];

const ARCH_LAYERS = [
  {
    title: "Graph Query Optimizer",
    desc: "Orca-based optimizer with graphlet-aware rules",
    color: "#F59E0B",
    bg: "linear-gradient(135deg, #FFFBEB 0%, #FEF3C7 100%)",
    tags: ["Orca", "GEM"],
  },
  {
    title: "Vectorized Query Processor",
    desc: "SIMD-friendly operators for heterogeneous schemas",
    color: "#10B981",
    bg: "linear-gradient(135deg, #ECFDF5 0%, #D1FAE5 100%)",
    tags: ["SSRF", "AdjIdxJoin", "IdSeek"],
  },
  {
    title: "Storage Manager",
    desc: "Cost-based graphlet chunking with schema index",
    color: "#8B5CF6",
    bg: "linear-gradient(135deg, #F5F3FF 0%, #EDE9FE 100%)",
    tags: ["CGC", "Schema Index", "CSR"],
  },
];

// ─── Slide 4: System Architecture ─────────────────────────────────
function SlideArchitecture() {
  return (
    <div style={{
      height: "100%", display: "flex", flexDirection: "column",
      alignItems: "center", justifyContent: "center", gap: 32,
    }}>
      <motion.div initial={{ opacity: 0, y: -16 }} animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, ease: EASE }} style={{ textAlign: "center" }}>
        <h2 style={{ fontSize: 40, fontWeight: 800, color: "#1a1a1a", letterSpacing: "-0.03em", margin: 0 }}>
          System Architecture
        </h2>
        <p style={{ fontSize: 18, color: "#86868b", marginTop: 8, fontWeight: 500 }}>
          Schemaless processing integrated end-to-end
        </p>
      </motion.div>

      {/* Architecture stack */}
      <motion.div initial={{ opacity: 0, scale: 0.97 }} animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.5, delay: 0.15, ease: EASE }}
        style={{
          width: "100%", maxWidth: 700, border: "2px solid #e84545", borderRadius: 18,
          padding: "20px 24px", background: "#fff", position: "relative",
        }}>
        <div style={{
          position: "absolute", top: -14, left: 24, background: "#fff", padding: "0 12px",
          fontSize: 18, fontWeight: 800, color: "#1a1a1a",
        }}>
          Turbo<span style={{ color: "#e84545" }}>Lynx</span>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 8 }}>
          {/* Cypher input */}
          <div style={{
            padding: "8px 20px", background: "#1a1a1a", borderRadius: 10,
            fontFamily: "monospace", fontSize: 14, color: "#e5e7eb", textAlign: "center",
          }}>
            <span style={{ color: "#86868b" }}>MATCH </span>
            <span style={{ color: "#fff" }}>(p)-[:<span style={{ color: "#10B981" }}>birthPlace</span>]-&gt;(c)</span>
            <span style={{ color: "#86868b" }}> WHERE </span>
            <span style={{ color: "#fff" }}>p.<span style={{ color: "#F59E0B" }}>birthDate</span> IS NOT NULL</span>
          </div>

          {/* Layers */}
          {ARCH_LAYERS.map((layer, i) => (
            <motion.div key={layer.title}
              initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.35, delay: 0.25 + i * 0.1, ease: EASE }}
              style={{
                padding: "12px 20px", background: layer.bg, borderRadius: 12,
                border: `1px solid ${layer.color}18`,
                display: "flex", alignItems: "center", justifyContent: "space-between",
              }}>
              <div>
                <div style={{ fontSize: 16, fontWeight: 700, color: layer.color }}>{layer.title}</div>
                <div style={{ fontSize: 13, color: "#71717a", marginTop: 2 }}>{layer.desc}</div>
              </div>
              <div style={{ display: "flex", gap: 4 }}>
                {layer.tags.map(t => tagPill(layer.color, t))}
              </div>
            </motion.div>
          ))}

          {/* Graphlet storage */}
          <div style={{
            padding: "10px 16px", background: "linear-gradient(135deg, #f4f4f5, #e4e4e7)",
            borderRadius: 10, border: "1px solid #71717a18",
            display: "flex", alignItems: "center", justifyContent: "space-between",
          }}>
            <span style={{ fontSize: 15, fontWeight: 700, color: "#71717a" }}>Graph-Native Storage</span>
            <div style={{ display: "flex", gap: 5 }}>
              {GL_COLORS.slice(0, 5).map((c, i) => (
                <motion.div key={i} initial={{ opacity: 0, scale: 0.8 }} animate={{ opacity: 1, scale: 1 }}
                  transition={{ delay: 0.5 + i * 0.04, ease: EASE }}
                  style={{
                    width: 52, height: 28, borderRadius: 5, border: `1.5px solid ${c}30`, background: "#fff",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: 8, fontWeight: 700, color: c,
                  }}>GL-{i + 1}</motion.div>
              ))}
            </div>
          </div>
        </div>
      </motion.div>

      {/* Schemaless Property Graph */}
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, delay: 0.6, ease: EASE }}
        style={{
          width: "100%", maxWidth: 700, height: 80, position: "relative",
          borderRadius: 12, overflow: "hidden",
          background: "linear-gradient(180deg, #f8f8fa 0%, #ededf0 100%)",
          border: "1px solid #e4e4e7",
        }}>
        <div style={{
          position: "absolute", top: 6, left: 0, right: 0,
          textAlign: "center", fontSize: 11, fontWeight: 700,
          color: "#52525b", letterSpacing: "0.08em", zIndex: 1,
          textShadow: "0 1px 3px rgba(255,255,255,0.9)",
        }}>SCHEMALESS PROPERTY GRAPH</div>
        <svg viewBox="0 0 100 44" preserveAspectRatio="xMidYMid slice"
          style={{ width: "100%", height: "100%", position: "absolute", inset: 0 }}>
          {GE.map(([a, b], i) => (
            <line key={`e${i}`} x1={GN[a].x} y1={GN[a].y} x2={GN[b].x} y2={GN[b].y}
              stroke="#c8c8cd" strokeWidth={0.35} opacity={0.5} />
          ))}
          {GN.map((n, i) => (
            <circle key={`n${i}`} cx={n.x} cy={n.y} r={n.r}
              fill={n.c + "30"} stroke={n.c} strokeWidth={0.5} />
          ))}
        </svg>
      </motion.div>
    </div>
  );
}

// ─── Slide 5: Scenario Overview ──────────────────────────────────
function SlideScenarios() {
  return (
    <div style={{
      height: "100%", display: "flex", flexDirection: "column",
      alignItems: "center", justifyContent: "center", gap: 36,
    }}>
      <motion.div initial={{ opacity: 0, y: -16 }} animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, ease: EASE }} style={{ textAlign: "center" }}>
        <h2 style={{ fontSize: 40, fontWeight: 800, color: "#1a1a1a", letterSpacing: "-0.03em", margin: 0 }}>
          Two Live Scenarios
        </h2>
        <p style={{ fontSize: 18, color: "#86868b", marginTop: 8, fontWeight: 500 }}>
          on a full DBpedia instance &mdash; 77M nodes, 1,304 graphlets
        </p>
      </motion.div>

      <div style={{ display: "flex", gap: 28, maxWidth: 900 }}>
        {/* Scenario A */}
        <motion.div initial={{ opacity: 0, y: 30 }} animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.15, ease: EASE }}
          style={{
            flex: 1, padding: "28px 28px 24px", background: "#fff", borderRadius: 18,
            border: "1px solid #3b82f620", boxShadow: "0 2px 20px rgba(0,0,0,0.04)",
          }}>
          <div style={{
            fontSize: 12, fontWeight: 700, color: "#3b82f6",
            letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 8,
          }}>Scenario A</div>
          <h3 style={{ fontSize: 22, fontWeight: 700, color: "#1a1a1a", margin: "0 0 12px", lineHeight: 1.3 }}>
            Early Pruning for a<br />Selective Property Filter
          </h3>
          <div style={{
            padding: "8px 12px", background: "#1a1a1a", borderRadius: 8,
            fontFamily: "monospace", fontSize: 12, color: "#e5e7eb", marginBottom: 14,
          }}>
            WHERE p.birthDate IS NOT NULL
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {[
              { label: "Pruning", value: "72% graphlets eliminated", color: "#3b82f6" },
              { label: "Speedup", value: "28\u00D7 faster", color: "#10B981" },
              { label: "Latency", value: "420ms \u2192 15ms", color: "#e84545" },
            ].map(m => (
              <div key={m.label} style={{ display: "flex", justifyContent: "space-between", fontSize: 14 }}>
                <span style={{ color: "#71717a" }}>{m.label}</span>
                <span style={{ fontWeight: 700, color: m.color, fontFamily: "monospace" }}>{m.value}</span>
              </div>
            ))}
          </div>
        </motion.div>

        {/* Scenario B */}
        <motion.div initial={{ opacity: 0, y: 30 }} animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.25, ease: EASE }}
          style={{
            flex: 1, padding: "28px 28px 24px", background: "#fff", borderRadius: 18,
            border: "1px solid #F59E0B20", boxShadow: "0 2px 20px rgba(0,0,0,0.04)",
          }}>
          <div style={{
            fontSize: 12, fontWeight: 700, color: "#F59E0B",
            letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 8,
          }}>Scenario B</div>
          <h3 style={{ fontSize: 22, fontWeight: 700, color: "#1a1a1a", margin: "0 0 12px", lineHeight: 1.3 }}>
            Multi-Hop Query with<br />GEM + SSRF
          </h3>
          <div style={{
            padding: "8px 12px", background: "#1a1a1a", borderRadius: 8,
            fontFamily: "monospace", fontSize: 12, color: "#e5e7eb", marginBottom: 14,
          }}>
            (p)-[:birthPlace]-&gt;(c)-[:country]-&gt;(co)
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {[
              { label: "GEM", value: "6.6B \u2192 6 sub-trees", color: "#F59E0B" },
              { label: "SSRF", value: "57% NULLs eliminated", color: "#10B981" },
              { label: "Latency", value: "1,535ms \u2192 480ms", color: "#e84545" },
            ].map(m => (
              <div key={m.label} style={{ display: "flex", justifyContent: "space-between", fontSize: 14 }}>
                <span style={{ color: "#71717a" }}>{m.label}</span>
                <span style={{ fontWeight: 700, color: m.color, fontFamily: "monospace" }}>{m.value}</span>
              </div>
            ))}
          </div>
        </motion.div>
      </div>

      <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }}
        transition={{ delay: 0.5, duration: 0.4 }} style={{ alignSelf: "center" }}>
        <Link href="/" style={{ textDecoration: "none" }}>
          <motion.button whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}
            style={{
              padding: "14px 44px", fontSize: 17, fontWeight: 600,
              background: "#1a1a1a", color: "#fff", border: "none",
              borderRadius: 980, cursor: "pointer",
            }}>
            Start Demo &rarr;
          </motion.button>
        </Link>
      </motion.div>
    </div>
  );
}

// ─── Main Controller ────────────────────────────────────────────────
type SlideComponent = (props: { phase: number }) => React.ReactNode;

const SLIDES: SlideComponent[] = [
  () => <SlideBanner />,
  () => <SlideUseCases />,
  () => <SlideTradeoff />,
  () => <SlideBreakthrough183 />,
  () => <SlideArchitecture />,
  () => <SlideScenarios />,
];
const SLIDE_STEPS = [1, 1, 1, 1, 1, 1];
const TOTAL_STEPS = SLIDE_STEPS.reduce((a, b) => a + b, 0);

function stepToSlidePhase(step: number): { slide: number; phase: number } {
  let remaining = step;
  for (let s = 0; s < SLIDE_STEPS.length; s++) {
    if (remaining < SLIDE_STEPS[s]) return { slide: s, phase: remaining };
    remaining -= SLIDE_STEPS[s];
  }
  return { slide: SLIDE_STEPS.length - 1, phase: SLIDE_STEPS[SLIDE_STEPS.length - 1] - 1 };
}

export default function IntroPage() {
  const [step, setStep] = useState(0);
  const [dir, setDir] = useState(1);
  const { slide, phase } = stepToSlidePhase(step);

  const go = useCallback((next: number) => {
    if (next < 0 || next >= TOTAL_STEPS) return;
    setDir(next > step ? 1 : -1);
    setStep(next);
  }, [step]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "ArrowRight" || e.key === "ArrowDown" || e.key === " ") {
        e.preventDefault();
        go(step + 1);
      } else if (e.key === "ArrowLeft" || e.key === "ArrowUp") {
        e.preventDefault();
        go(step - 1);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [step, go]);

  const SlideComponent = SLIDES[slide];

  return (
    <div style={{
      height: "100dvh", background: "#fafafa",
      display: "flex", flexDirection: "column", overflow: "hidden",
      fontFamily: '-apple-system, BlinkMacSystemFont, "SF Pro Display", "Segoe UI", sans-serif',
    }}>
      <div style={{ flex: 1, position: "relative", overflow: "hidden" }}>
        <AnimatePresence mode="wait" custom={dir}>
          <motion.div
            key={slide}
            custom={dir}
            initial={{ opacity: 0, x: dir * 60 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: dir * -60 }}
            transition={{ duration: 0.4, ease: EASE }}
            style={{ position: "absolute", inset: 0, padding: "0 48px" }}
          >
            <SlideComponent phase={phase} />
          </motion.div>
        </AnimatePresence>
      </div>

      {/* Bottom navigation dots */}
      <div style={{
        height: 48, flexShrink: 0,
        display: "flex", alignItems: "center", justifyContent: "center", gap: 24,
      }}>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {SLIDE_STEPS.map((steps, si) => {
            let base = 0;
            for (let k = 0; k < si; k++) base += SLIDE_STEPS[k];
            const isActive = si === slide;
            return (
              <div key={si} style={{ display: "flex", gap: 3, alignItems: "center" }}>
                {steps === 1 ? (
                  <button onClick={() => go(base)} style={{
                    width: isActive ? 24 : 8, height: 8,
                    borderRadius: 4, border: "none", cursor: "pointer",
                    background: isActive ? "#1a1a1a" : base < step ? "#86868b" : "#d2d2d7",
                    transition: "all 0.3s ease",
                  }} />
                ) : (
                  Array.from({ length: steps }).map((_, pi) => (
                    <button key={pi} onClick={() => go(base + pi)} style={{
                      width: isActive && pi === phase ? 16 : 6, height: 6,
                      borderRadius: 3, border: "none", cursor: "pointer",
                      background: isActive
                        ? pi === phase ? "#1a1a1a" : pi < phase ? "#86868b" : "#d2d2d7"
                        : base + pi < step ? "#86868b" : "#d2d2d7",
                      transition: "all 0.3s ease",
                    }} />
                  ))
                )}
                {si < SLIDE_STEPS.length - 1 && (
                  <div style={{ width: 1, height: 12, background: "#e4e4e7", margin: "0 4px" }} />
                )}
              </div>
            );
          })}
        </div>
        <span style={{ fontSize: 13, color: "#aeaeb2" }}>&larr; &rarr; to navigate</span>
      </div>
    </div>
  );
}
