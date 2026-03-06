"use client";
import { motion, AnimatePresence } from "framer-motion";

interface Props { step: number; onStep: (n: number) => void; }

// Graphlet groups
const GL_P = [
  { id: "gl_p1", rows: 44200, x: 120, y: 100 },
  { id: "gl_p2", rows: 38100, x: 120, y: 190 },
  { id: "gl_p3", rows: 12000, x: 120, y: 280 },
];
const GL_C = [
  { id: "gl_c1", rows:  5700, x: 560, y: 130 },
  { id: "gl_c2", rows:  1800, x: 560, y: 240 },
];

// All 6 naive join pairs
const NAIVE_PAIRS = GL_P.flatMap(p => GL_C.map(c => ({ from: p, to: c })));

// VG groupings
const VGA_MEMBERS = ["gl_p1", "gl_p2", "gl_c1"];
const VGB_MEMBERS = ["gl_p3", "gl_c2"];

function NodeBox({ id, rows, x, y, color, highlight }: { id: string; rows: number; x: number; y: number; color: string; highlight?: boolean }) {
  return (
    <g>
      <rect x={x - 52} y={y - 22} width={104} height={44} rx={8}
        fill={color + (highlight ? "25" : "10")} stroke={color + (highlight ? "cc" : "50")} strokeWidth={highlight ? 2 : 1.5} />
      <text x={x} y={y - 4} textAnchor="middle" fontSize={11} fill="#f4f4f5" fontFamily="monospace" fontWeight={600}>{id}</text>
      <text x={x} y={y + 11} textAnchor="middle" fontSize={9} fill={color + "cc"} fontFamily="monospace">
        {rows.toLocaleString()} rows
      </text>
    </g>
  );
}

function Step0() {
  // All 6 pairs connected
  return (
    <svg viewBox="0 0 680 380" style={{ width: "100%", height: 320 }}>
      {/* Lines for all 6 combinations */}
      {NAIVE_PAIRS.map((pair, i) => (
        <line key={i}
          x1={pair.from.x + 52} y1={pair.from.y}
          x2={pair.to.x - 52} y2={pair.to.y}
          stroke="#e8454540" strokeWidth={1.5} strokeDasharray="4 3" />
      ))}
      {GL_P.map(n => <NodeBox key={n.id} {...n} color="#8B5CF6" />)}
      {GL_C.map(n => <NodeBox key={n.id} {...n} color="#F59E0B" />)}

      <text x={340} y={350} textAnchor="middle" fontSize={11} fill="#e84545" fontFamily="monospace">
        ⚠️  Naïve: 3 × 2 = 6 graphlet combinations → 6 execution plans
      </text>
    </svg>
  );
}

function Step1() {
  // Show join direction arrows
  const arrows = [
    { from: GL_P[0], to: GL_C[0], dir: "p→c", color: "#3B82F6" },
    { from: GL_P[1], to: GL_C[0], dir: "p→c", color: "#3B82F6" },
    { from: GL_C[0], to: GL_P[2], dir: "c→p", color: "#F59E0B" },
  ];
  return (
    <svg viewBox="0 0 680 380" style={{ width: "100%", height: 320 }}>
      {arrows.map((a, i) => {
        const x1 = a.from.x + (a.dir === "p→c" ? 52 : -52);
        const x2 = a.to.x + (a.dir === "p→c" ? -52 : 52);
        const y1 = a.from.y;
        const y2 = a.to.y;
        const mx = (x1 + x2) / 2;
        const my = (y1 + y2) / 2;
        return (
          <g key={i}>
            <line x1={x1} y1={y1} x2={x2} y2={y2} stroke={a.color} strokeWidth={2} markerEnd={`url(#arr-${a.color.replace("#","")})`} />
            <text x={mx} y={my - 6} textAnchor="middle" fontSize={9} fill={a.color} fontFamily="monospace">{a.dir}</text>
          </g>
        );
      })}
      <defs>
        {["3B82F6","F59E0B"].map(c => (
          <marker key={c} id={`arr-${c}`} markerWidth={8} markerHeight={8} refX={6} refY={3} orient="auto">
            <path d="M0,0 L0,6 L8,3 z" fill={`#${c}`} />
          </marker>
        ))}
      </defs>
      {GL_P.map(n => <NodeBox key={n.id} {...n} color="#8B5CF6" />)}
      {GL_C.map(n => <NodeBox key={n.id} {...n} color="#F59E0B" />)}
      <text x={340} y={350} textAnchor="middle" fontSize={10} fill="#71717a" fontFamily="monospace">
        Blue = p→c direction · Orange = c→p direction
      </text>
    </svg>
  );
}

function Step2() {
  // VG α box
  return (
    <svg viewBox="0 0 680 380" style={{ width: "100%", height: 320 }}>
      {/* VG α bounding box */}
      <rect x={40} y={60} width={560} height={200} rx={14}
        fill="#3B82F615" stroke="#3B82F660" strokeWidth={2} strokeDasharray="6 4" />
      <text x={60} y={52} fontSize={13} fill="#3B82F6" fontFamily="monospace" fontWeight={700}>Virtual Graphlet α</text>
      <text x={60} y={270} fontSize={10} fill="#3B82F6aa" fontFamily="monospace">
        Join order: p→c · Estimated output: 88,100 rows
      </text>

      {[GL_P[0], GL_P[1]].map(n => <NodeBox key={n.id} {...n} color="#3B82F6" highlight />)}
      <NodeBox {...GL_C[0]} color="#3B82F6" highlight />

      {/* Remaining ungrouped */}
      <NodeBox {...GL_P[2]} color="#52525b" />
      <NodeBox {...GL_C[1]} color="#52525b" />

      <text x={340} y={350} textAnchor="middle" fontSize={10} fill="#71717a" fontFamily="monospace">
        gl_p1, gl_p2, gl_c1 share the same join-order preference → grouped into α
      </text>
    </svg>
  );
}

function Step3() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <svg viewBox="0 0 680 320" style={{ width: "100%", height: 260 }}>
        {/* VG α */}
        <rect x={30} y={20} width={280} height={190} rx={14}
          fill="#3B82F615" stroke="#3B82F660" strokeWidth={2} strokeDasharray="6 4" />
        <text x={50} y={14} fontSize={12} fill="#3B82F6" fontFamily="monospace" fontWeight={700}>Virtual Graphlet α</text>

        <rect x={44} y={38} width={98} height={40} rx={7} fill="#3B82F625" stroke="#3B82F6" strokeWidth={1.5} />
        <text x={93} y={56} textAnchor="middle" fontSize={10} fill="#f4f4f5" fontFamily="monospace">gl_p1</text>
        <text x={93} y={70} textAnchor="middle" fontSize={8} fill="#93c5fd" fontFamily="monospace">44,200</text>

        <rect x={160} y={38} width={98} height={40} rx={7} fill="#3B82F625" stroke="#3B82F6" strokeWidth={1.5} />
        <text x={209} y={56} textAnchor="middle" fontSize={10} fill="#f4f4f5" fontFamily="monospace">gl_p2</text>
        <text x={209} y={70} textAnchor="middle" fontSize={8} fill="#93c5fd" fontFamily="monospace">38,100</text>

        <rect x={102} y={120} width={98} height={40} rx={7} fill="#3B82F625" stroke="#3B82F6" strokeWidth={1.5} />
        <text x={151} y={138} textAnchor="middle" fontSize={10} fill="#f4f4f5" fontFamily="monospace">gl_c1</text>
        <text x={151} y={152} textAnchor="middle" fontSize={8} fill="#93c5fd" fontFamily="monospace">5,700</text>

        <text x={160} y={225} textAnchor="middle" fontSize={9} fill="#3B82F6aa" fontFamily="monospace">p→c · out: 88,100</text>

        {/* VG β */}
        <rect x={380} y={60} width={270} height={160} rx={14}
          fill="#F59E0B15" stroke="#F59E0B60" strokeWidth={2} strokeDasharray="6 4" />
        <text x={400} y={54} fontSize={12} fill="#F59E0B" fontFamily="monospace" fontWeight={700}>Virtual Graphlet β</text>

        <rect x={396} y={78} width={98} height={40} rx={7} fill="#F59E0B25" stroke="#F59E0B" strokeWidth={1.5} />
        <text x={445} y={96} textAnchor="middle" fontSize={10} fill="#f4f4f5" fontFamily="monospace">gl_p3</text>
        <text x={445} y={110} textAnchor="middle" fontSize={8} fill="#fcd34d" fontFamily="monospace">12,000</text>

        <rect x={513} y={78} width={98} height={40} rx={7} fill="#F59E0B25" stroke="#F59E0B" strokeWidth={1.5} />
        <text x={562} y={96} textAnchor="middle" fontSize={10} fill="#f4f4f5" fontFamily="monospace">gl_c2</text>
        <text x={562} y={110} textAnchor="middle" fontSize={8} fill="#fcd34d" fontFamily="monospace">1,800</text>

        <text x={515} y={235} textAnchor="middle" fontSize={9} fill="#F59E0Baa" fontFamily="monospace">c→p · out: 13,800</text>
      </svg>

      <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
        {[
          { label: "Before", value: "6 combinations", sub: "6 execution plans", color: "#e84545" },
          { label: "After",  value: "2 Virtual Graphlets", sub: "2 execution plans", color: "#10B981" },
          { label: "Savings", value: "−67%", sub: "plan search space", color: "#3B82F6" },
        ].map(item => (
          <div key={item.label} style={{ padding: "10px 16px", background: "#131316", borderRadius: 10, border: `1px solid ${item.color}30` }}>
            <div style={{ fontSize: 18, fontWeight: 700, color: item.color, fontFamily: "monospace" }}>{item.value}</div>
            <div style={{ fontSize: 11, color: "#71717a" }}>{item.label} · {item.sub}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

const STEP_TITLES = [
  "The Join Problem",
  "GEM analyzes join benefit",
  "Virtual Graphlet α forms",
  "Virtual Graphlet β + final result",
];

const AHA = "GEM collapses 6 graphlet combinations into 2 Virtual Graphlets by grouping by join-order preference. This is especially powerful with heterogeneous schemas — more graphlets = more combinations = larger savings.";

export default function S3_GEM({ step }: Props) {
  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", padding: "32px 48px", gap: 20, overflow: "hidden" }}>
      <AnimatePresence mode="wait">
        <motion.div key={step} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
          transition={{ duration: 0.25 }}>
          <div style={{ fontSize: 11, color: "#10B981", fontFamily: "monospace", marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.08em" }}>
            GEM — Graphlet Execution Model
          </div>
          <h2 style={{ fontSize: 22, fontWeight: 700, color: "#f4f4f5", margin: 0 }}>{STEP_TITLES[step]}</h2>
        </motion.div>
      </AnimatePresence>

      <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 16, overflow: "auto" }}>
        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            transition={{ duration: 0.25 }}>
            {step === 0 && <Step0 />}
            {step === 1 && <Step1 />}
            {step === 2 && <Step2 />}
            {step === 3 && <Step3 />}
          </motion.div>
        </AnimatePresence>

        <AnimatePresence>
          {step === 3 && (
            <motion.div initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
              transition={{ delay: 0.3 }}
              style={{ border: "1px solid #10B98130", background: "#10B98110", borderRadius: 10, padding: "12px 16px", fontSize: 13, color: "#6ee7b7", lineHeight: 1.6, flexShrink: 0 }}>
              <span style={{ color: "#34d399", fontWeight: 600 }}>✓ Key Insight: </span>{AHA}
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
