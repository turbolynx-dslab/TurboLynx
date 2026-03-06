"use client";
import { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface Props { step: number; onStep: (n: number) => void; }

const WIDE_COLS = ["name", "born", "city", "pop", "type", "draftRound", "capacity", "flag", "sponsor", "league", "country", "venue", "height"];
const WIDE_DATA = [
  { name: "Buffon",  born: "1978-01-28", city: "Turin",  pop: "870K",   type: "Soccer" },
  { name: "Kahn",    born: "1969-06-15", city: "Munich", pop: "1.47M",  type: "Soccer" },
  { name: "Čech",    born: "1982-05-20", city: "Plzeň",  pop: "170K",   type: "Soccer" },
];

function WideTable({ highlight }: { highlight: boolean }) {
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ fontSize: 11, fontFamily: "monospace", borderCollapse: "collapse", minWidth: 700 }}>
        <thead>
          <tr>
            {WIDE_COLS.map(c => (
              <th key={c} style={{ padding: "4px 8px", color: "#71717a", textAlign: "left", borderBottom: "1px solid #27272a", whiteSpace: "nowrap" }}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {WIDE_DATA.map((row, ri) => (
            <tr key={ri} style={{ borderBottom: "1px solid #18181b" }}>
              {WIDE_COLS.map((c, ci) => {
                const val = (row as Record<string, string>)[c];
                const isNull = val === undefined;
                return (
                  <td key={c} style={{
                    padding: "4px 8px",
                    color: isNull ? "#e8454555" : "#f4f4f5",
                    background: isNull && highlight ? "#e8454508" : "transparent",
                    whiteSpace: "nowrap",
                  }}>
                    {isNull ? "NULL" : val}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ValidityTable() {
  const SHOW_COLS = ["name", "born", "city", "pop", "type"];
  const validity = ["11001", "11001", "11001"];
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <div style={{ overflowX: "auto" }}>
        <table style={{ fontSize: 11, fontFamily: "monospace", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              {SHOW_COLS.map(c => (
                <th key={c} style={{ padding: "4px 10px", color: "#71717a", textAlign: "left", borderBottom: "1px solid #27272a" }}>{c}</th>
              ))}
              <th style={{ padding: "4px 10px", color: "#10B981", textAlign: "left", borderBottom: "1px solid #27272a" }}>validity</th>
            </tr>
          </thead>
          <tbody>
            {WIDE_DATA.map((row, ri) => (
              <tr key={ri} style={{ borderBottom: "1px solid #18181b" }}>
                {SHOW_COLS.map(c => (
                  <td key={c} style={{ padding: "4px 10px", color: "#f4f4f5" }}>
                    {(row as Record<string, string>)[c] ?? "—"}
                  </td>
                ))}
                <td style={{ padding: "4px 10px" }}>
                  <span style={{
                    fontSize: 12, fontFamily: "monospace", letterSpacing: "0.12em",
                    color: "#10B981", background: "#10B98115", padding: "1px 6px", borderRadius: 4,
                  }}>
                    {validity[ri]}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div style={{ fontSize: 12, color: "#71717a" }}>
        NULL values <span style={{ color: "#f4f4f5" }}>not stored</span> — present values only, positions encoded in validity bit-vector.
      </div>
    </div>
  );
}

function MemoryBar({ animate: doAnimate }: { animate: boolean }) {
  const [width, setWidth] = useState(100);
  useEffect(() => {
    if (doAnimate) {
      const t = setTimeout(() => setWidth(28), 100);
      return () => clearTimeout(t);
    }
  }, [doAnimate]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16, maxWidth: 500 }}>
      {[
        { label: "Standard Schema (wide table)", pct: 100, color: "#e84545", width: 100, animated: false },
        { label: "SSRF (compact + validity)", pct: 28, color: "#10B981", width: doAnimate ? width : 100, animated: true },
      ].map(bar => (
        <div key={bar.label}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
            <span style={{ fontSize: 12, color: "#a1a1aa" }}>{bar.label}</span>
            <span style={{ fontSize: 13, fontWeight: 700, color: bar.color, fontFamily: "monospace" }}>{bar.pct}%</span>
          </div>
          <div style={{ height: 24, background: "#1a1a1e", borderRadius: 6, overflow: "hidden" }}>
            <div style={{
              height: "100%",
              width: `${bar.width}%`,
              background: bar.color,
              borderRadius: 6,
              transition: bar.animated ? "width 0.9s cubic-bezier(0.4,0,0.2,1)" : "none",
            }} />
          </div>
        </div>
      ))}
      <div style={{ fontSize: 13, color: "#6ee7b7", marginTop: 4 }}>
        −72% memory usage · SSRF rows pack into contiguous memory → SIMD ready
      </div>
    </div>
  );
}

function SimdView() {
  const lanes = 8;
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20, maxWidth: 540 }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        <div style={{ fontSize: 12, color: "#a1a1aa" }}>SIMD Register (8 lanes)</div>
        <div style={{ display: "flex", gap: 3 }}>
          {Array.from({ length: lanes }).map((_, i) => (
            <motion.div key={i}
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.06 }}
              style={{
                flex: 1, height: 44, background: "#3B82F630", border: "1px solid #3B82F660",
                borderRadius: 6, display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 9, color: "#93c5fd", fontFamily: "monospace",
              }}>
              v[{i}]
            </motion.div>
          ))}
        </div>
        <div style={{ fontSize: 11, color: "#71717a" }}>→ 8 comparisons processed in 1 SIMD instruction</div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        {[
          { label: "Before SSRF", value: "10M rows × 15 cols", sub: "branch per NULL → cache miss", color: "#e84545" },
          { label: "After SSRF",  value: "2.8M compact rows", sub: "5 common attrs → SIMD batch", color: "#10B981" },
        ].map(item => (
          <div key={item.label} style={{ padding: "12px 14px", background: "#131316", borderRadius: 10, border: `1px solid ${item.color}30` }}>
            <div style={{ fontSize: 14, fontWeight: 700, color: item.color, fontFamily: "monospace" }}>{item.value}</div>
            <div style={{ fontSize: 11, color: "#71717a", marginTop: 4 }}>{item.label}</div>
            <div style={{ fontSize: 11, color: "#52525b", marginTop: 2 }}>{item.sub}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

const STEP_TITLES = [
  "Without SSRF: wide table, many NULLs",
  "SSRF: Validity Vector",
  "Memory comparison",
  "SIMD execution",
];

const AHA = "SSRF removes NULL storage and enables CPU vectorization. On the Hero Query (Goalkeeper + City join), memory drops from 100% to 28% and enables full SIMD throughput.";

export default function S4_SSRF({ step }: Props) {
  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", padding: "32px 48px", gap: 20, overflow: "hidden" }}>
      <AnimatePresence mode="wait">
        <motion.div key={step} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
          transition={{ duration: 0.25 }}>
          <div style={{ fontSize: 11, color: "#F59E0B", fontFamily: "monospace", marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.08em" }}>
            SSRF — Semistructured Relations on Flat Records
          </div>
          <h2 style={{ fontSize: 22, fontWeight: 700, color: "#f4f4f5", margin: 0 }}>{STEP_TITLES[step]}</h2>
        </motion.div>
      </AnimatePresence>

      <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 16, overflow: "auto" }}>
        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            transition={{ duration: 0.25 }}>
            {step === 0 && (
              <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                <WideTable highlight={true} />
                <div style={{ fontSize: 13, color: "#e84545" }}>
                  72% of cells = NULL · CPU branches on each one → cache miss → slow
                </div>
              </div>
            )}
            {step === 1 && <ValidityTable />}
            {step === 2 && <MemoryBar animate={true} />}
            {step === 3 && <SimdView />}
          </motion.div>
        </AnimatePresence>

        <AnimatePresence>
          {step === 3 && (
            <motion.div initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
              transition={{ delay: 0.3 }}
              style={{ border: "1px solid #F59E0B30", background: "#F59E0B10", borderRadius: 10, padding: "12px 16px", fontSize: 13, color: "#fcd34d", lineHeight: 1.6, flexShrink: 0 }}>
              <span style={{ color: "#fbbf24", fontWeight: 600 }}>✓ Key Insight: </span>{AHA}
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
