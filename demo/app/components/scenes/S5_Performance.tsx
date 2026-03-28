"use client";
import { useState, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";
import type { QState } from "@/lib/query-state";
import { generateCypher } from "@/lib/query-state";

interface Props { step: number; onStep: (n: number) => void; queryState?: QState; }

// ─── Mock results ────────────────────────────────────────────────────────────
interface MockResult {
  latencyMs: number;
  rows: number;
  plan: string; // short plan description
  scanRows: string;
}

function getMockResult(opts: { pruning: boolean; gem: boolean; ssrf: boolean }): MockResult {
  if (opts.pruning && opts.gem && opts.ssrf) {
    return { latencyMs: 12 + Math.round(Math.random() * 5), rows: 20, plan: "NodeScan(358 GLs) → AdjIdxJoin → IdSeek → Top", scanRows: "1.0M" };
  }
  if (opts.pruning && opts.gem && !opts.ssrf) {
    return { latencyMs: 18 + Math.round(Math.random() * 8), rows: 20, plan: "NodeScan(358 GLs) → AdjIdxJoin → IdSeek → Top (wide-table)", scanRows: "1.0M" };
  }
  if (opts.pruning && !opts.gem && opts.ssrf) {
    return { latencyMs: 45 + Math.round(Math.random() * 15), rows: 20, plan: "UnionAll(358) → AdjIdxJoin → IdSeek → Top", scanRows: "1.0M" };
  }
  if (opts.pruning && !opts.gem && !opts.ssrf) {
    return { latencyMs: 65 + Math.round(Math.random() * 20), rows: 20, plan: "UnionAll(358) → AdjIdxJoin → IdSeek → Top (wide)", scanRows: "1.0M" };
  }
  if (!opts.pruning && opts.gem && opts.ssrf) {
    return { latencyMs: 850 + Math.round(Math.random() * 200), rows: 20, plan: "NodeScan(1304 GLs) → AdjIdxJoin → IdSeek → Top", scanRows: "77.0M" };
  }
  if (!opts.pruning && !opts.gem && !opts.ssrf) {
    return { latencyMs: 4200 + Math.round(Math.random() * 800), rows: 20, plan: "UnionAll(1304) → HashJoin → NodeScan → Top (wide)", scanRows: "77.0M" };
  }
  // Default: no pruning, some optimizations
  return { latencyMs: 1200 + Math.round(Math.random() * 400), rows: 20, plan: "NodeScan(1304 GLs) → AdjIdxJoin → Top", scanRows: "77.0M" };
}

// ─── Panel ───────────────────────────────────────────────────────────────────
interface PanelState {
  id: number;
  pruning: boolean;
  gem: boolean;
  ssrf: boolean;
  running: boolean;
  result: MockResult | null;
}

let _panelId = 0;

function mkPanel(pruning = true, gem = true, ssrf = true): PanelState {
  return { id: ++_panelId, pruning, gem, ssrf, running: false, result: null };
}

export default function S5_Performance({ step, queryState }: Props) {
  const [panels, setPanels] = useState<PanelState[]>([mkPanel()]);
  const cypherText = useMemo(() => queryState ? generateCypher(queryState).replace(/\n\s*/g, " ") : "No query selected", [queryState]);

  const updatePanel = (id: number, update: Partial<PanelState>) => {
    setPanels(prev => prev.map(p => p.id === id ? { ...p, ...update } : p));
  };

  const runPanel = (id: number) => {
    const panel = panels.find(p => p.id === id);
    if (!panel) return;
    updatePanel(id, { running: true, result: null });
    // Simulate execution delay
    const delay = panel.pruning ? 300 + Math.random() * 200 : 1000 + Math.random() * 500;
    setTimeout(() => {
      const result = getMockResult({ pruning: panel.pruning, gem: panel.gem, ssrf: panel.ssrf });
      updatePanel(id, { running: false, result });
    }, delay);
  };

  const addPanel = () => {
    setPanels(prev => [...prev, mkPanel(false, false, false)]); // new panel with all OFF
  };

  const removePanel = (id: number) => {
    setPanels(prev => prev.filter(p => p.id !== id));
  };

  // Find max latency for bar scaling
  const maxLatency = Math.max(1, ...panels.map(p => p.result?.latencyMs ?? 0));

  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{ maxWidth: 1440, margin: "0 auto", padding: "16px 40px", height: "100%", display: "flex", flexDirection: "column", boxSizing: "border-box", gap: 10 }}>

        {/* Query bar */}
        <div style={{ flexShrink: 0, padding: "8px 16px", background: "#18181b", borderRadius: 8, fontFamily: "monospace", fontSize: 13, color: "#e5e7eb", lineHeight: 1.5 }}>
          {cypherText}
        </div>

        {/* Panels */}
        <div style={{ flex: 1, overflowX: "auto", overflowY: "hidden", display: "flex", gap: 12, alignItems: "stretch" }} className="thin-scrollbar">
          <AnimatePresence>
            {panels.map((panel, pi) => (
              <motion.div key={panel.id} initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.95 }} transition={{ duration: 0.2 }}
                style={{
                  flex: "1 0 0", minWidth: 300, display: "flex", flexDirection: "column", gap: 10,
                  padding: "16px", background: "#fff", borderRadius: 12,
                  border: "1px solid #e5e7eb", position: "relative",
                }}>
                {/* Close button (if > 1 panel) */}
                {panels.length > 1 && (
                  <button onClick={() => removePanel(panel.id)}
                    style={{ position: "absolute", top: 8, right: 8, width: 24, height: 24, borderRadius: 5, border: "none", background: "#f0f1f3", color: "#71717a", cursor: "pointer", fontSize: 14, display: "flex", alignItems: "center", justifyContent: "center" }}>
                    &times;
                  </button>
                )}

                {/* Panel header */}
                <div style={{ fontSize: 14, fontWeight: 700, color: "#18181b" }}>
                  Run {pi + 1}
                </div>

                {/* Options toggles */}
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {[
                    { key: "pruning" as const, label: "Graphlet Pruning", color: "#3b82f6" },
                    { key: "gem" as const, label: "GEM", color: "#10B981" },
                    { key: "ssrf" as const, label: "SSRF", color: "#F59E0B" },
                  ].map(opt => (
                    <label key={opt.key} style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer", fontSize: 14 }}>
                      <input type="checkbox" checked={panel[opt.key]}
                        onChange={() => updatePanel(panel.id, { [opt.key]: !panel[opt.key], result: null })}
                        style={{ width: 16, height: 16, cursor: "pointer", accentColor: opt.color }} />
                      <span style={{ color: panel[opt.key] ? "#18181b" : "#9ca3af", fontWeight: panel[opt.key] ? 600 : 400 }}>
                        {opt.label}
                      </span>
                      <span style={{ marginLeft: "auto", fontSize: 12, fontFamily: "monospace", color: panel[opt.key] ? opt.color : "#d4d4d8" }}>
                        {panel[opt.key] ? "ON" : "OFF"}
                      </span>
                    </label>
                  ))}
                </div>

                {/* Run button */}
                <button onClick={() => runPanel(panel.id)} disabled={panel.running}
                  style={{
                    padding: "10px 0", borderRadius: 8, border: "none",
                    background: panel.running ? "#9ca3af" : "#e84545", color: "#fff",
                    fontSize: 14, fontWeight: 700, cursor: panel.running ? "default" : "pointer",
                  }}>
                  {panel.running ? "Running..." : "\u25b6 Run Query"}
                </button>

                {/* Results */}
                {panel.result && (
                  <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}
                    style={{ display: "flex", flexDirection: "column", gap: 8 }}>

                    {/* Latency bar */}
                    <div>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                        <span style={{ fontSize: 12, color: "#71717a" }}>Latency</span>
                        <span style={{ fontSize: 20, fontWeight: 800, fontFamily: "monospace",
                          color: panel.result.latencyMs < 50 ? "#10B981" : panel.result.latencyMs < 500 ? "#F59E0B" : "#e84545" }}>
                          {panel.result.latencyMs} ms
                        </span>
                      </div>
                      <div style={{ height: 8, background: "#f0f1f3", borderRadius: 4, overflow: "hidden" }}>
                        <motion.div initial={{ width: 0 }} animate={{ width: `${Math.max(2, (panel.result.latencyMs / maxLatency) * 100)}%` }}
                          transition={{ duration: 0.5 }}
                          style={{ height: "100%", borderRadius: 4,
                            background: panel.result.latencyMs < 50 ? "#10B981" : panel.result.latencyMs < 500 ? "#F59E0B" : "#e84545" }} />
                      </div>
                    </div>

                    {/* Stats */}
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
                      <div style={{ padding: "6px 10px", background: "#f8f9fa", borderRadius: 6 }}>
                        <div style={{ fontSize: 16, fontWeight: 700, fontFamily: "monospace" }}>{panel.result.rows}</div>
                        <div style={{ fontSize: 11, color: "#71717a" }}>rows returned</div>
                      </div>
                      <div style={{ padding: "6px 10px", background: "#f8f9fa", borderRadius: 6 }}>
                        <div style={{ fontSize: 16, fontWeight: 700, fontFamily: "monospace" }}>{panel.result.scanRows}</div>
                        <div style={{ fontSize: 11, color: "#71717a" }}>rows scanned</div>
                      </div>
                    </div>

                    {/* Plan summary */}
                    <div style={{ padding: "8px 10px", background: "#f8f9fa", borderRadius: 6 }}>
                      <div style={{ fontSize: 11, color: "#71717a", marginBottom: 3 }}>Plan</div>
                      <div style={{ fontSize: 12, fontFamily: "monospace", color: "#374151", lineHeight: 1.5 }}>
                        {panel.result.plan}
                      </div>
                    </div>
                  </motion.div>
                )}
              </motion.div>
            ))}
          </AnimatePresence>

          {/* Add panel button */}
          <motion.button onClick={addPanel} whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}
            style={{
              flex: "0 0 80px", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
              gap: 6, borderRadius: 12, border: "2px dashed #d4d4d8", background: "transparent",
              color: "#9ca3af", cursor: "pointer", fontSize: 28, fontWeight: 300,
            }}>
            +
            <span style={{ fontSize: 12, fontWeight: 600 }}>Add</span>
          </motion.button>
        </div>

        {/* Latency comparison bar (when multiple panels have results) */}
        {panels.filter(p => p.result).length > 1 && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }}
            style={{ flexShrink: 0, padding: "12px 16px", background: "#f8f9fa", borderRadius: 10, border: "1px solid #e5e7eb" }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: "#18181b", marginBottom: 8 }}>Latency Comparison</div>
            <div style={{ display: "flex", gap: 12, alignItems: "flex-end" }}>
              {panels.filter(p => p.result).map((p, i) => {
                const r = p.result!;
                const barH = Math.max(8, (r.latencyMs / maxLatency) * 100);
                const color = r.latencyMs < 50 ? "#10B981" : r.latencyMs < 500 ? "#F59E0B" : "#e84545";
                return (
                  <div key={p.id} style={{ flex: 1, textAlign: "center" }}>
                    <div style={{ fontSize: 16, fontWeight: 800, fontFamily: "monospace", color, marginBottom: 4 }}>
                      {r.latencyMs}ms
                    </div>
                    <div style={{ height: barH, background: color, borderRadius: 4, marginBottom: 4, transition: "height 0.3s" }} />
                    <div style={{ fontSize: 12, color: "#71717a" }}>
                      Run {panels.indexOf(p) + 1}
                      {p.pruning && p.gem && p.ssrf ? " (all ON)" : !p.pruning && !p.gem && !p.ssrf ? " (all OFF)" : ""}
                    </div>
                  </div>
                );
              })}
            </div>
            {/* Speedup */}
            {(() => {
              const results = panels.filter(p => p.result).map(p => p.result!);
              const fastest = Math.min(...results.map(r => r.latencyMs));
              const slowest = Math.max(...results.map(r => r.latencyMs));
              if (slowest > fastest * 1.5) {
                return (
                  <div style={{ marginTop: 8, fontSize: 15, fontWeight: 700, color: "#10B981", textAlign: "center" }}>
                    {(slowest / fastest).toFixed(1)}&times; speedup
                  </div>
                );
              }
              return null;
            })()}
          </motion.div>
        )}
      </div>
    </div>
  );
}
