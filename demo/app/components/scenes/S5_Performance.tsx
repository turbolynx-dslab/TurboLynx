"use client";
import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { BENCHMARKS } from "@/lib/demo-data";

interface Props { step: number; onStep: (n: number) => void; }

type BenchKey = keyof typeof BENCHMARKS;

const MAX_BAR_WIDTH = 520; // px

export default function S5_Performance({ step }: Props) {
  const [active, setActive] = useState<BenchKey>("dbpedia");
  const bench = BENCHMARKS[active];

  // Find max ratio for bar scaling
  const maxRatio = Math.max(...bench.data.map(d => d.ratio));

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", padding: "32px 48px", gap: 20, overflow: "hidden" }}>
      <div>
        <div style={{ fontSize: 11, color: "#e84545", fontFamily: "monospace", marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.08em" }}>
          Performance
        </div>
        <h2 style={{ fontSize: 22, fontWeight: 700, color: "#f4f4f5", margin: 0 }}>Real benchmark results</h2>
      </div>

      {/* Benchmark selector */}
      <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
        {(Object.keys(BENCHMARKS) as BenchKey[]).map(key => (
          <button key={key}
            onClick={() => setActive(key)}
            style={{
              padding: "6px 16px", borderRadius: 8, border: "none", cursor: "pointer",
              fontSize: 13, fontWeight: 500, transition: "all 0.2s",
              background: active === key ? "#e84545" : "#27272a",
              color: active === key ? "#fff" : "#a1a1aa",
            }}>
            {BENCHMARKS[key].label}
          </button>
        ))}
      </div>

      {/* Bar chart */}
      <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 8, overflow: "auto" }}>
        <AnimatePresence mode="wait">
          <motion.div key={active}
            initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            transition={{ duration: 0.25 }}
            style={{ display: "flex", flexDirection: "column", gap: 10 }}>

            <div style={{ fontSize: 12, color: "#71717a", marginBottom: 4 }}>{bench.note}</div>

            {bench.data.map((item, i) => {
              const barW = item.ratio === 1 ? 8 : Math.max(8, (item.ratio / maxRatio) * MAX_BAR_WIDTH);
              const color = item.highlight ? "#e84545" : "#3f3f46";
              const textColor = item.highlight ? "#f4f4f5" : "#a1a1aa";
              return (
                <div key={item.system} style={{ display: "flex", alignItems: "center", gap: 12 }}>
                  <div style={{ width: 90, fontSize: 12, fontFamily: "monospace", color: textColor, textAlign: "right", flexShrink: 0 }}>
                    {item.system}
                  </div>
                  <div style={{ flex: 1, position: "relative", height: 28, display: "flex", alignItems: "center" }}>
                    <motion.div
                      initial={{ width: 0 }}
                      animate={{ width: barW }}
                      transition={{ duration: 0.6, delay: i * 0.05, ease: "easeOut" }}
                      style={{ height: "100%", background: color, borderRadius: 4, minWidth: 8 }}
                    />
                    <span style={{ marginLeft: 10, fontSize: 12, fontFamily: "monospace", color: textColor, whiteSpace: "nowrap" }}>
                      {item.ratio === 1 ? "1× (baseline)" : `${item.ratio}×`}
                    </span>
                  </div>
                </div>
              );
            })}
          </motion.div>
        </AnimatePresence>

        {/* Stats cards */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12, marginTop: 12, flexShrink: 0 }}>
          {active === "dbpedia" && [
            { label: "Faster than Neo4j", value: "86×", color: "#e84545" },
            { label: "NULL ops eliminated", value: "212B", color: "#3B82F6" },
            { label: "Partitions scanned", value: "2/34", color: "#10B981" },
          ].map(c => (
            <div key={c.label} style={{ padding: "14px 16px", background: "#131316", borderRadius: 10, border: `1px solid ${c.color}30` }}>
              <div style={{ fontSize: 24, fontWeight: 700, color: c.color, fontFamily: "monospace" }}>{c.value}</div>
              <div style={{ fontSize: 11, color: "#71717a", marginTop: 4 }}>{c.label}</div>
            </div>
          ))}
          {active === "ldbc" && [
            { label: "Faster than Kuzu", value: "107×", color: "#e84545" },
            { label: "Faster than Memgraph", value: "12×", color: "#F59E0B" },
            { label: "LDBC SF10 scale factor", value: "SF10", color: "#8B5CF6" },
          ].map(c => (
            <div key={c.label} style={{ padding: "14px 16px", background: "#131316", borderRadius: 10, border: `1px solid ${c.color}30` }}>
              <div style={{ fontSize: 24, fontWeight: 700, color: c.color, fontFamily: "monospace" }}>{c.value}</div>
              <div style={{ fontSize: 11, color: "#71717a", marginTop: 4 }}>{c.label}</div>
            </div>
          ))}
          {active === "tpch" && [
            { label: "Faster than DuckDB", value: "1.5×", color: "#e84545" },
            { label: "Faster than Kuzu", value: "14×", color: "#F59E0B" },
            { label: "Relational workload", value: "TPC-H", color: "#3B82F6" },
          ].map(c => (
            <div key={c.label} style={{ padding: "14px 16px", background: "#131316", borderRadius: 10, border: `1px solid ${c.color}30` }}>
              <div style={{ fontSize: 24, fontWeight: 700, color: c.color, fontFamily: "monospace" }}>{c.value}</div>
              <div style={{ fontSize: 11, color: "#71717a", marginTop: 4 }}>{c.label}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
