"use client";
import { useState, useRef } from "react";
import { motion, useInView, AnimatePresence } from "framer-motion";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from "recharts";
import SectionLabel from "@/components/ui/SectionLabel";
import { SSRF_HOP_DATA, SSRF_COL_DATA } from "@/lib/data";

const PERSON_SCHEMAS = [
  { id: "P₁", attrs: ["name"] },
  { id: "P₂", attrs: ["name", "birthDate"] },
  { id: "P₃", attrs: ["name", "birthDate", "birthPlace"] },
  { id: "P₄", attrs: ["name", "birthDate", "birthPlace", "nationality"] },
];
const PLACE_SCHEMAS = [
  { id: "C₁", attrs: ["name"] },
  { id: "C₂", attrs: ["name", "country"] },
  { id: "C₃", attrs: ["name", "country", "population"] },
];

const ALL_COLS = ["name", "birthDate", "birthPlace", "nationality", "name2", "country", "population"];

function SchemaGrid({ ssrfOn }: { ssrfOn: boolean }) {
  if (!ssrfOn) {
    // 4×3 = 12 schema combinations
    return (
      <div className="grid grid-cols-3 gap-2">
        {PERSON_SCHEMAS.flatMap(p =>
          PLACE_SCHEMAS.map(c => {
            const joinAttrs = [...new Set([...p.attrs, ...c.attrs])];
            const allPossible = ALL_COLS;
            return (
              <motion.div
                key={`${p.id}-${c.id}`}
                layout
                className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-2 text-[10px]"
              >
                <div className="text-[var(--text-secondary)] mb-1">{p.id}⋈{c.id}</div>
                <div className="space-y-0.5">
                  {allPossible.map(col => (
                    <div
                      key={col}
                      className="h-2 rounded-sm"
                      style={{ background: joinAttrs.includes(col) ? "#3b82f680" : "#27272a" }}
                      title={joinAttrs.includes(col) ? col : "NULL"}
                    />
                  ))}
                </div>
              </motion.div>
            );
          })
        )}
      </div>
    );
  }

  // SSRF: single unified layout
  return (
    <motion.div
      layout
      initial={{ opacity: 0, scale: 0.9 }}
      animate={{ opacity: 1, scale: 1 }}
      className="bg-[var(--bg-surface)] border border-[var(--accent-green)] rounded-xl p-5"
    >
      <div className="text-xs text-[var(--accent-green)] font-semibold mb-3">Unified SSRF Layout</div>

      {/* Fixed columns */}
      <div className="mb-3">
        <div className="text-[10px] text-[var(--text-secondary)] mb-1.5 uppercase tracking-wider">Dense columns (always present)</div>
        <div className="flex gap-2">
          {["wikiPageID", "join_key"].map(c => (
            <div key={c} className="px-2 py-1.5 rounded-md bg-[#3b82f620] border border-[#3b82f640] text-xs text-[#3b82f6] font-mono">{c}</div>
          ))}
        </div>
      </div>

      {/* Sparse columns + validity vector */}
      <div>
        <div className="text-[10px] text-[var(--text-secondary)] mb-1.5 uppercase tracking-wider">Sparse payload + validity vector</div>
        <div className="bg-[var(--bg-base)] rounded-lg p-3 font-mono text-[10px]">
          {[
            { row: "P₁⋈C₁", bits: [1,0,0,0, 1,0,0] },
            { row: "P₂⋈C₂", bits: [1,1,0,0, 1,1,0] },
            { row: "P₃⋈C₂", bits: [1,1,1,0, 1,1,0] },
            { row: "P₄⋈C₃", bits: [1,1,1,1, 1,1,1] },
          ].map(r => (
            <div key={r.row} className="flex items-center gap-3 mb-1.5">
              <span className="text-[var(--text-secondary)] w-12">{r.row}</span>
              <div className="flex gap-1">
                {r.bits.map((b, i) => (
                  <span
                    key={i}
                    className="w-4 h-4 rounded-sm flex items-center justify-center text-[9px] font-bold"
                    style={{ background: b ? "#22c55e30" : "#27272a", color: b ? "#22c55e" : "#52525b" }}
                  >
                    {b}
                  </span>
                ))}
              </div>
              <span className="text-[var(--text-secondary)] opacity-40">← validity vector</span>
            </div>
          ))}
        </div>
      </div>

      <div className="mt-3 text-xs text-[var(--accent-green)]">
        ✓ One expression tree. Bit-test per column (single instruction). No null scanning.
      </div>
    </motion.div>
  );
}

export default function SSRFExecution() {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true, margin: "-80px" });
  const [ssrfOn, setSsrfOn] = useState(false);
  const [colCount, setColCount] = useState(3);

  return (
    <section ref={ref} className="py-24 px-6 max-w-7xl mx-auto">
      <SectionLabel number="05" label="SSRF: Zero Waste Execution" />

      <div className="grid lg:grid-cols-2 gap-16">
        <div>
          <motion.h2
            initial={{ opacity: 0, y: 16 }}
            animate={inView ? { opacity: 1, y: 0 } : {}}
            className="text-3xl md:text-4xl font-bold mb-3"
          >
            Shared Schema Row Format
          </motion.h2>
          <motion.p
            initial={{ opacity: 0 }}
            animate={inView ? { opacity: 1 } : {}}
            transition={{ delay: 0.2 }}
            className="text-[var(--text-secondary)] mb-6 text-lg"
          >
            Binary join of n×m graphlet schemas = n×m intermediate schema combinations. SSRF collapses this to one layout with a validity vector.
          </motion.p>

          {/* Schema explosion explanation */}
          <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl p-4 mb-6">
            <div className="flex items-center gap-2 mb-3">
              <span className="text-sm font-medium">Person schemas ({PERSON_SCHEMAS.length})</span>
              <span className="text-[var(--text-secondary)]">×</span>
              <span className="text-sm font-medium">Place schemas ({PLACE_SCHEMAS.length})</span>
              <span className="text-[var(--text-secondary)]">=</span>
              <span className={`text-sm font-bold ${ssrfOn ? "text-[var(--accent-green)]" : "text-[var(--accent)]"}`}>
                {ssrfOn ? "1 unified" : `${PERSON_SCHEMAS.length * PLACE_SCHEMAS.length} schemas`}
              </span>
            </div>
            {!ssrfOn && (
              <div className="space-y-1.5 text-xs text-[var(--text-secondary)]">
                <div className="flex items-center gap-2">
                  <span className="text-[var(--accent)]">✗</span>
                  <span>{PERSON_SCHEMAS.length * PLACE_SCHEMAS.length} expression trees compiled</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-[var(--accent)]">✗</span>
                  <span>Avg 47% NULL columns per intermediate row</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-[var(--accent)]">✗</span>
                  <span>Mixed-width rows block SIMD vectorization</span>
                </div>
              </div>
            )}
            {ssrfOn && (
              <div className="space-y-1.5 text-xs">
                <div className="flex items-center gap-2">
                  <span className="text-[var(--accent-green)]">✓</span>
                  <span className="text-[var(--text-secondary)]">1 expression tree</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-[var(--accent-green)]">✓</span>
                  <span className="text-[var(--text-secondary)]">Bit-test validity per column (1 instruction)</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-[var(--accent-green)]">✓</span>
                  <span className="text-[var(--text-secondary)]">Dense columns stay columnar</span>
                </div>
              </div>
            )}
          </div>

          {/* Toggle */}
          <div className="flex items-center gap-4 mb-8">
            <span className={`text-sm ${!ssrfOn ? "text-[var(--accent)] font-semibold" : "text-[var(--text-secondary)]"}`}>SSRF OFF</span>
            <button
              onClick={() => setSsrfOn(v => !v)}
              className={`relative w-14 h-7 rounded-full transition-colors ${ssrfOn ? "bg-[var(--accent-green)]" : "bg-[var(--accent)]"}`}
            >
              <motion.span
                animate={{ x: ssrfOn ? 28 : 4 }}
                transition={{ type: "spring", stiffness: 400, damping: 30 }}
                className="absolute top-1.5 w-4 h-4 rounded-full bg-white shadow block"
              />
            </button>
            <span className={`text-sm ${ssrfOn ? "text-[var(--accent-green)] font-semibold" : "text-[var(--text-secondary)]"}`}>SSRF ON</span>
          </div>

          {/* Benchmark charts */}
          <div className="space-y-6">
            <div>
              <div className="text-sm font-medium mb-3">Hop count effect (Fig 8a)</div>
              <div className="h-40">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={SSRF_HOP_DATA} margin={{ left: -20, right: 10 }}>
                    <XAxis dataKey="hops" tick={{ fill: "#a1a1aa", fontSize: 10 }} label={{ value: "hops", position: "insideBottom", fill: "#71717a", fontSize: 10 }} />
                    <YAxis tick={{ fill: "#a1a1aa", fontSize: 10 }} />
                    <Tooltip contentStyle={{ background: "var(--bg-surface)", border: "1px solid var(--border)", borderRadius: 8, fontSize: 11 }} />
                    <Legend wrapperStyle={{ fontSize: 11 }} />
                    <Line type="monotone" dataKey="SS" stroke="#e84545" strokeWidth={2} dot={false} name="Standard (SS)" />
                    <Line type="monotone" dataKey="SSRF" stroke="#22c55e" strokeWidth={2} dot={false} name="SSRF" />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div>
              <div className="flex items-center justify-between mb-3">
                <div className="text-sm font-medium">Returned columns effect (Fig 8b)</div>
                <div className="flex items-center gap-3">
                  <span className="text-xs text-[var(--text-secondary)]">Columns: {colCount}</span>
                  <input type="range" min="1" max="5" value={colCount} onChange={e => setColCount(+e.target.value)} className="w-20 accent-[var(--accent)]" />
                </div>
              </div>
              <div className="flex gap-4 items-end">
                {["SS", "SSRF"].map((label, li) => {
                  const data = SSRF_COL_DATA[colCount - 1];
                  const val = li === 0 ? data.SS : data.SSRF;
                  return (
                    <div key={label} className="flex flex-col items-center gap-1.5">
                      <span className="text-xs font-semibold" style={{ color: li === 0 ? "#e84545" : "#22c55e" }}>{val.toFixed(1)}s</span>
                      <motion.div
                        animate={{ height: val * 30 }}
                        className="w-12 rounded-t-md"
                        style={{ background: li === 0 ? "#e84545" : "#22c55e", opacity: 0.8 }}
                      />
                      <span className="text-xs text-[var(--text-secondary)]">{label}</span>
                    </div>
                  );
                })}
                <div className="text-xs text-[var(--accent-green)] font-semibold ml-2">
                  {(SSRF_COL_DATA[colCount - 1].SS / SSRF_COL_DATA[colCount - 1].SSRF).toFixed(1)}× speedup
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Right: Schema grid visualization */}
        <div>
          <div className="flex items-center gap-3 mb-4">
            <span className="text-sm text-[var(--text-secondary)]">
              {ssrfOn ? "After SSRF — 1 unified layout" : `Before SSRF — ${PERSON_SCHEMAS.length * PLACE_SCHEMAS.length} schema combinations`}
            </span>
          </div>
          <SchemaGrid ssrfOn={ssrfOn} />

          {!ssrfOn && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="mt-4 p-3 bg-[var(--accent)]/10 border border-[var(--accent)]/30 rounded-xl text-xs text-[var(--text-secondary)]"
            >
              <span className="text-[var(--accent)] font-semibold">Grey = NULL.</span> Each schema needs its own handler. 3.1× processing overhead.
            </motion.div>
          )}

          {ssrfOn && (
            <motion.div
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              className="mt-4 p-3 bg-[var(--accent-green)]/10 border border-[var(--accent-green)]/30 rounded-xl text-xs text-[var(--text-secondary)]"
            >
              <span className="text-[var(--accent-green)] font-semibold">schema_infos</span> table maps schema IDs → offset_infos.{" "}
              <span className="text-[var(--accent-green)] font-semibold">offset_infos[i] = −1</span> means absent.{" "}
              One validity bit per sparse column. <span className="text-[var(--accent-green)] font-semibold">2.1× lower traversal cost.</span>
            </motion.div>
          )}
        </div>
      </div>
    </section>
  );
}
