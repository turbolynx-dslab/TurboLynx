"use client";
import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import type { PhaseTrace, VirtualGraphlet } from "@/lib/pipeline-data";
import { PHASE_DELAYS } from "@/lib/pipeline-data";

type PhaseKey = "cgc" | "plan" | "gem" | "ssrf";

interface PipelinePanelProps {
  trace: PhaseTrace | null;
  active: boolean;
}

const PHASES: { key: PhaseKey; label: string; icon: string; shortLabel: string }[] = [
  { key: "cgc",  label: "CGC",  icon: "①", shortLabel: "Schema Index" },
  { key: "plan", label: "Plan", icon: "②", shortLabel: "UnionAll Tree" },
  { key: "gem",  label: "GEM",  icon: "③", shortLabel: "Graphlet Merge" },
  { key: "ssrf", label: "SSRF", icon: "④", shortLabel: "NULL Compress" },
];

const VG_COLORS = { blue: "#3b82f6", orange: "#f97316" } as const;

function VGCard({ vg }: { vg: VirtualGraphlet }) {
  const color = VG_COLORS[vg.color];
  return (
    <div
      className="rounded-lg p-2 border text-xs"
      style={{ borderColor: color + "50", background: color + "0d" }}
    >
      <div className="flex items-center justify-between mb-1">
        <span className="font-bold font-mono" style={{ color }}>VG {vg.id}</span>
        <span className="text-[var(--text-secondary)] font-mono text-[10px]">{vg.joinOrder}</span>
      </div>
      {vg.graphlets.map(g => (
        <div key={g} className="text-[10px] font-mono text-[var(--text-secondary)]">· {g}</div>
      ))}
    </div>
  );
}

export default function PipelinePanel({ trace, active }: PipelinePanelProps) {
  const [expandedPhase, setExpandedPhase] = useState<PhaseKey | null>(null);

  return (
    <div className="h-full flex flex-col p-3 gap-2 overflow-hidden">
      <span className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider flex-shrink-0">
        Pipeline Trace
      </span>

      {/* Phase steps */}
      <div className="space-y-1 flex-1 min-h-0 overflow-y-auto">
        {PHASES.map((p) => {
          const isActive = active;
          const isExpanded = expandedPhase === p.key;
          const delay = PHASE_DELAYS[p.key];

          return (
            <motion.div
              key={p.key}
              initial={{ opacity: 0, y: 6 }}
              animate={isActive ? { opacity: 1, y: 0 } : { opacity: 0.3, y: 0 }}
              transition={{ delay: isActive ? delay : 0, duration: 0.3 }}
            >
              <button
                onClick={() => setExpandedPhase(isExpanded ? null : p.key)}
                disabled={!isActive}
                className={`w-full flex items-center justify-between px-3 py-2 rounded-lg text-xs border transition-all ${
                  isActive
                    ? isExpanded
                      ? "border-[var(--accent)] bg-[var(--accent)]10 text-[var(--text-primary)]"
                      : "border-[var(--border)] hover:border-[var(--text-secondary)] text-[var(--text-secondary)]"
                    : "border-[var(--border)] text-[var(--border)] cursor-not-allowed"
                }`}
              >
                <div className="flex items-center gap-2">
                  <span className="font-mono">{p.icon}</span>
                  <span className="font-semibold">{p.label}</span>
                  {isActive && (
                    <span className="text-[var(--accent-green)] text-[10px]">✓</span>
                  )}
                </div>
                <span className={`text-[10px] font-mono ${isActive ? "text-[var(--text-secondary)]" : ""}`}>
                  {isActive ? p.shortLabel : "—"}
                </span>
              </button>

              {/* Expanded detail */}
              <AnimatePresence>
                {isExpanded && isActive && trace && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.2 }}
                    className="overflow-hidden"
                  >
                    <div className="mt-1 px-3 pb-2 space-y-1.5">
                      {p.key === "cgc" && (
                        <>
                          <div className="text-[10px] font-mono text-[var(--text-secondary)]">
                            Scanned: <span className="text-[var(--accent-blue)]">{trace.cgc.scannedPartitions}</span> / {trace.cgc.totalPartitions} partitions
                          </div>
                          <div className="text-[10px] font-mono text-[var(--text-secondary)]">
                            Null-ops avoided: <span className="text-[var(--accent-green)]">212B</span>
                          </div>
                        </>
                      )}
                      {p.key === "plan" && (
                        <div className="space-y-0.5">
                          {trace.plan.unionAllGroups.map((g, i) => (
                            <div key={i} className="text-[10px] font-mono text-[var(--text-secondary)]">
                              {i === 0 || i === 2 || i === 4 ? "" : "  "}{g.label}
                            </div>
                          ))}
                        </div>
                      )}
                      {p.key === "gem" && (
                        <div className="space-y-1.5">
                          <div className="text-[10px] font-mono text-[var(--text-secondary)]">
                            {trace.gem.rawCombinations} combinations → {trace.gem.virtualGraphlets.length} VGs (-{trace.gem.reductionPct}%)
                          </div>
                          {trace.gem.virtualGraphlets.map(vg => (
                            <VGCard key={vg.id} vg={vg} />
                          ))}
                        </div>
                      )}
                      {p.key === "ssrf" && (
                        <div className="space-y-2">
                          <div className="text-[10px] font-mono text-[var(--text-secondary)]">
                            NULL reduction: <span className="text-[var(--accent-green)]">-{trace.ssrf.nullPct}%</span>
                          </div>
                          {/* Memory bar comparison */}
                          <div className="space-y-1">
                            <div className="flex items-center gap-2">
                              <span className="text-[10px] font-mono text-[var(--text-secondary)] w-8">SS</span>
                              <div className="flex-1 bg-[var(--bg-elevated)] rounded-full h-2">
                                <div className="bg-red-500/60 h-2 rounded-full" style={{ width: "100%" }} />
                              </div>
                              <span className="text-[10px] font-mono text-[var(--text-secondary)]">100%</span>
                            </div>
                            <div className="flex items-center gap-2">
                              <span className="text-[10px] font-mono text-[var(--text-secondary)] w-8">SSRF</span>
                              <div className="flex-1 bg-[var(--bg-elevated)] rounded-full h-2">
                                <motion.div
                                  initial={{ width: "100%" }}
                                  animate={{ width: `${trace.ssrf.ssrfMemoryRatio * 100}%` }}
                                  transition={{ delay: PHASE_DELAYS.ssrf + 0.2, duration: 0.5 }}
                                  className="bg-[var(--accent-green)] h-2 rounded-full"
                                />
                              </div>
                              <span className="text-[10px] font-mono text-[var(--accent-green)]">
                                {Math.round(trace.ssrf.ssrfMemoryRatio * 100)}%
                              </span>
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
          );
        })}
      </div>

      {!active && (
        <div className="flex-1 flex items-center justify-center">
          <span className="text-xs text-[var(--border)] font-mono">run a query</span>
        </div>
      )}
    </div>
  );
}
