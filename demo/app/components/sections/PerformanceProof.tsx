"use client";
import { useRef } from "react";
import { motion, useInView } from "framer-motion";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";
import SectionLabel from "@/components/ui/SectionLabel";
import AnimatedCounter from "@/components/ui/AnimatedCounter";
import { BENCHMARK_DATA } from "@/lib/data";

const COMPETITOR_COLORS: Record<string, string> = {
  "Neo4j": "#6b7280",
  "Umbra": "#6b7280",
  "DuckDB": "#6b7280",
  "GraphScope": "#6b7280",
};

export default function PerformanceProof() {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true, margin: "-80px" });

  // Prepare chart data (Q13 — best showcase)
  const chartData = BENCHMARK_DATA.competitors.map(c => ({
    name: c,
    slowdown: BENCHMARK_DATA.slowdowns[c as keyof typeof BENCHMARK_DATA.slowdowns][2],
  })).sort((a, b) => b.slowdown - a.slowdown);

  return (
    <section ref={ref} className="py-24 px-6 max-w-7xl mx-auto">
      <SectionLabel number="06" label="The Proof" />

      {/* Hero stat */}
      <div className="text-center mb-16">
        <motion.p
          initial={{ opacity: 0 }}
          animate={inView ? { opacity: 1 } : {}}
          className="text-[var(--text-secondary)] text-lg mb-2"
        >
          Up to
        </motion.p>
        <motion.div
          initial={{ opacity: 0, scale: 0.8 }}
          animate={inView ? { opacity: 1, scale: 1 } : {}}
          transition={{ type: "spring", stiffness: 120, delay: 0.1 }}
          className="text-[120px] md:text-[160px] font-bold leading-none text-[var(--accent)] tabular-nums"
        >
          {inView && <AnimatedCounter target={86.14} duration={2000} decimals={1} suffix="×" separator={false} />}
        </motion.div>
        <motion.p
          initial={{ opacity: 0 }}
          animate={inView ? { opacity: 1 } : {}}
          transition={{ delay: 0.3 }}
          className="text-[var(--text-secondary)] text-xl"
        >
          faster than the best competitor on DBpedia
        </motion.p>
      </div>

      <div className="grid lg:grid-cols-2 gap-16">
        {/* Bar chart */}
        <div>
          <h3 className="text-xl font-bold mb-2">Q13 — Director ↔ City (DBpedia)</h3>
          <p className="text-[var(--text-secondary)] text-sm mb-6">Relative slowdown vs TurboLynx. Lower = worse for competitor.</p>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData} layout="vertical" margin={{ left: 20, right: 40 }}>
                <XAxis type="number" tick={{ fill: "#a1a1aa", fontSize: 11 }} tickFormatter={v => `${v}×`} />
                <YAxis type="category" dataKey="name" tick={{ fill: "#a1a1aa", fontSize: 12 }} width={90} />
                <Tooltip
                  contentStyle={{ background: "var(--bg-surface)", border: "1px solid var(--border)", borderRadius: 8, fontSize: 12 }}
                  formatter={(v: number | undefined) => [`${(v ?? 0).toFixed(1)}× slower than TurboLynx`, ""]}
                />
                <Bar dataKey="slowdown" radius={[0, 4, 4, 0]}>
                  {chartData.map((entry) => (
                    <Cell key={entry.name} fill={COMPETITOR_COLORS[entry.name]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
          <p className="text-xs text-[var(--text-secondary)] mt-2">TurboLynx = 1× (baseline). All competitors are slower.</p>
        </div>

        {/* Highlight cards */}
        <div className="space-y-4">
          {[
            { name: "Neo4j", stat: "135.8×", desc: "slower on attribute accesses (C3 query)", color: "#6b7280" },
            { name: "Umbra v25.07", stat: "7.74×", desc: "slower — best competitor overall, still far behind", color: "#6b7280" },
          ].map((item, i) => (
            <motion.div
              key={item.name}
              initial={{ opacity: 0, x: 20 }}
              animate={inView ? { opacity: 1, x: 0 } : {}}
              transition={{ delay: 0.1 + i * 0.1 }}
              className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl p-4 flex items-start gap-4"
            >
              <div className="shrink-0">
                <div className="text-2xl font-bold text-[var(--text-primary)]">{item.stat}</div>
                <div className="text-xs text-[var(--text-secondary)]">slower</div>
              </div>
              <div>
                <div className="font-semibold text-sm text-[var(--text-primary)]">{item.name}</div>
                <div className="text-xs text-[var(--text-secondary)] mt-0.5">{item.desc}</div>
              </div>
            </motion.div>
          ))}

          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={inView ? { opacity: 1, y: 0 } : {}}
            transition={{ delay: 0.5 }}
            className="bg-[var(--bg-surface)] border border-[var(--accent)]/40 rounded-xl p-4"
          >
            <div className="text-sm font-semibold text-[var(--text-primary)] mb-1">LDBC SNB (fixed schema)</div>
            <div className="text-xs text-[var(--text-secondary)]">
              TurboLynx also handles fixed schemas.{" "}
              <span className="text-[var(--accent)] font-bold">183.9× faster than Neo4j</span> on LDBC SF1.
            </div>
          </motion.div>
        </div>
      </div>

      {/* Footer CTA */}
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={inView ? { opacity: 1, y: 0 } : {}}
        transition={{ delay: 0.6 }}
        className="mt-16 text-center"
      >
        <div className="flex items-center justify-center gap-6 flex-wrap">
          <a
            href="https://github.com/turbolynx-dslab/TurboLynx"
            target="_blank"
            rel="noreferrer"
            className="flex items-center gap-2 border border-[var(--border)] text-[var(--text-secondary)] hover:border-[var(--text-primary)] hover:text-[var(--text-primary)] font-medium px-6 py-2.5 rounded-full transition-colors"
          >
            View on GitHub →
          </a>
        </div>
        <p className="mt-6 text-xs text-[var(--text-secondary)] opacity-50">
          TurboLynx · VLDB 2026 · POSTECH Database Lab
        </p>
      </motion.div>
    </section>
  );
}
