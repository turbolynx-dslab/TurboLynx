"use client";
import { useState, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";
import type { QState } from "@/lib/query-state";
import { generateCypher } from "@/lib/query-state";
import type { CompletedRun } from "@/app/page";

interface Props {
  step: number; onStep: (n: number) => void; queryState?: QState;
  completedRuns: CompletedRun[];
  onRunComplete: (run: CompletedRun) => void;
  onRunRemove: (id: number) => void;
}

// ─── Plan tree SVG ──────────────────────────────────────────────────────────
interface PlanNode { op: string; color: string; detail?: string; time?: string; children?: PlanNode[]; }
interface LNode { op: string; color: string; detail?: string; time?: string; x: number; y: number; parentIdx: number; }
const CW = 160, CH_ = 52, HG_ = 12, VG_ = 28;

function layoutTree(root: PlanNode): LNode[] {
  const r: LNode[] = []; const wc = new Map<PlanNode, number>();
  const gw = (n: PlanNode): number => { if (wc.has(n)) return wc.get(n)!; const w = !n.children?.length ? CW : Math.max(CW, n.children.map(gw).reduce((a, b) => a + b, 0) + (n.children.length - 1) * HG_); wc.set(n, w); return w; };
  const walk = (n: PlanNode, l: number, t: number, pi: number) => { const sw = gw(n); const idx = r.length; r.push({ op: n.op, color: n.color, detail: n.detail, time: n.time, x: l + sw / 2, y: t, parentIdx: pi }); if (n.children?.length) { const cws = n.children.map(gw); const total = cws.reduce((a, b) => a + b, 0) + (cws.length - 1) * HG_; let cx = l + (sw - total) / 2; n.children.forEach((c, i) => { walk(c, cx, t + CH_ + VG_, idx); cx += cws[i] + HG_; }); } };
  walk(root, 0, 0, -1); return r;
}

const OC: Record<string, string> = {
  NodeScan: "#3b82f6", AdjIdxJoin: "#8B5CF6", IdSeek: "#0891B2", IndexScan: "#0891B2",
  HashJoin: "#e84545", Projection: "#71717a", Top: "#71717a", UnionAll: "#ec4899", Filter: "#F59E0B",
};

function MiniPlanSVG({ root }: { root: PlanNode }) {
  const nodes = useMemo(() => layoutTree(root), [root]);
  if (!nodes.length) return null;
  const w = Math.max(...nodes.map(n => n.x)) + CW / 2;
  const h = Math.max(...nodes.map(n => n.y)) + CH_;
  const pad = 12;
  return (
    <svg viewBox={`${-pad} ${-pad} ${w + pad * 2} ${h + pad * 2}`} preserveAspectRatio="xMidYMid meet" style={{ width: "100%", height: "100%", display: "block" }}>
      {nodes.map((n, i) => { if (n.parentIdx < 0) return null; const p = nodes[n.parentIdx]; const my = (p.y + CH_ + n.y) / 2; return <path key={`e${i}`} d={`M${p.x},${p.y + CH_} C${p.x},${my} ${n.x},${my} ${n.x},${n.y}`} fill="none" stroke="#d4d4d8" strokeWidth={1.5} />; })}
      {nodes.map((n, i) => { const lx = n.x - CW / 2; const c = OC[n.op] ?? "#71717a"; return (
        <g key={i}>
          <rect x={lx + 1} y={n.y + 1} width={CW} height={CH_} rx={6} fill="#00000005" />
          <rect x={lx} y={n.y} width={CW} height={CH_} rx={6} fill="white" stroke={c + "30"} strokeWidth={1} />
          <rect x={lx} y={n.y + 5} width={3} height={CH_ - 10} rx={1.5} fill={c} />
          <text x={lx + 10} y={n.y + 20} fontSize={13} fontWeight={700} fontFamily="monospace" fill={c}>{n.op}</text>
          {n.time && <text x={lx + CW - 6} y={n.y + 20} fontSize={11} fontFamily="monospace" fill="#9ca3af" textAnchor="end">{n.time}</text>}
          {n.detail && <text x={lx + 10} y={n.y + 38} fontSize={11} fontFamily="monospace" fill="#9ca3af">{n.detail.length > 18 ? n.detail.slice(0, 16) + "\u2026" : n.detail}</text>}
        </g>
      ); })}
    </svg>
  );
}

// ─── Mock result generation ─────────────────────────────────────────────────
interface MockResult {
  latencyMs: number; scanRows: string; plan: PlanNode;
  rows: string[][]; cols: string[];
}

const MOCK_ROWS = [
  ["G. Byrne", "1928-06-12", "Dublin"], ["A. Hopkins", "1937-12-31", "Margam"], ["Mel Gibson", "1956-01-03", "Peekskill"],
  ["W. Shatner", "1931-03-22", "Montreal"], ["C. Holst", "1969-04-11", "Copenhagen"], ["D. Kenton", "1978-08-22", "London"],
  ["Suat Usta", "1981-03-15", "Istanbul"], ["J. Ara\u00fajo", "1985-11-07", "Porto"],
];
const MOCK_COLS = ["p.name", "p.birthDate", "c.name"];

// Latency model based on VLDB'26 TurboLynx paper.
// Each optimization has a measured slowdown factor when disabled:
//   - Pruning: ~28× for scan-heavy queries (Figure 7a), ~3.5× for multi-hop
//   - GEM: 1.23× overall (Table 3) — modest, compilation tradeoff
//   - SSRF: 2.1× at 2-hops (Figure 8a), up to 2.6× with 5 cols (Figure 8b)
// "All ON" baseline: ~15ms for 1-hop selective, ~480ms for 2-hop aggregation (Table 4)
function getMockResult(
  opts: { pruning: boolean; gem: boolean; ssrf: boolean },
  qs?: QState,
): MockResult {
  const hops = qs?.matches.filter(m => m.sourceVar && m.edgeType && m.targetVar).length ?? 1;
  const tgtCols = qs?.returns.filter(r => {
    const lastMatch = qs?.matches[qs.matches.length - 1];
    return lastMatch && r.variable === lastMatch.targetVar && r.property;
  }).length ?? 1;
  const isMultiHop = hops >= 2;

  // Baseline: all optimizations ON
  const baseLat = isMultiHop ? 480 : 15;

  // Per-optimization slowdown factors (from paper)
  const pruneSlowdown = isMultiHop ? 3.5 : 28;         // Pruning: dominant for 1-hop
  const gemSlowdown = isMultiHop ? 1.23 : 1.05;         // GEM: 1.23× (Table 3), negligible for 1-hop
  const ssrfSlowdown = isMultiHop                        // SSRF: depends on hops & cols (Figure 8)
    ? (tgtCols >= 4 ? 2.6 : tgtCols >= 2 ? 2.1 : 1.2)
    : (tgtCols >= 3 ? 1.5 : 1.05);                      // 1-hop with few cols: barely matters

  // Multiply slowdown for each disabled optimization
  let lat = baseLat;
  if (!opts.pruning) lat *= pruneSlowdown;
  if (!opts.gem)     lat *= gemSlowdown;
  if (!opts.ssrf)    lat *= ssrfSlowdown;

  // Add ±5% jitter
  lat = Math.round(lat * (0.95 + Math.random() * 0.1));

  const scanR = opts.pruning ? "1.0M" : "77.0M";
  const glCount = opts.pruning ? 358 : 1304;

  // Build plan tree
  let plan: PlanNode;
  const hasOptimizedPlan = opts.pruning || opts.gem;

  if (hasOptimizedPlan) {
    // AdjIdxJoin + IdSeek plan (per hop)
    let inner: PlanNode = { op: "NodeScan", color: OC.NodeScan, detail: `${glCount} GLs`, time: `${(lat * 0.2).toFixed(1)}ms` };
    const matches = qs?.matches.filter(m => m.sourceVar && m.edgeType && m.targetVar) ?? [];
    for (let i = 0; i < matches.length; i++) {
      const m = matches[i];
      inner = { op: "IdSeek", color: OC.IdSeek, detail: m.targetVar, time: `${(lat * 0.05).toFixed(1)}ms`, children: [
        { op: "AdjIdxJoin", color: OC.AdjIdxJoin, detail: `:${m.edgeType}`, time: `${(lat * (0.4 / matches.length)).toFixed(1)}ms`, children: [
          inner,
          { op: "IndexScan", color: OC.IndexScan, detail: "adj_fwd" },
        ]},
        { op: "IndexScan", color: OC.IndexScan, detail: "node_id" },
      ]};
    }
    plan = { op: "Projection", color: OC.Projection, detail: "...", children: [inner] };
  } else {
    // HashJoin + UnionAll plan (no optimization)
    plan = { op: "Projection", color: OC.Projection, children: [
      { op: "HashJoin", color: OC.HashJoin, detail: "join", time: `${(lat * 0.7).toFixed(1)}ms`, children: [
        { op: "UnionAll", color: OC.UnionAll, detail: `${glCount} scans`, children: [
          { op: "NodeScan", color: OC.NodeScan, detail: `(${glCount})`, time: `${(lat * 0.2).toFixed(1)}ms` },
        ]},
        { op: "NodeScan", color: OC.NodeScan, detail: "(1304)", time: `${(lat * 0.05).toFixed(1)}ms` },
      ]},
    ]};
  }

  // Result columns from query RETURN
  const cols = qs?.returns.filter(r => r.variable).map(r => {
    if (r.aggregate) return `${r.aggregate}(${r.variable})`;
    return r.property ? `${r.variable}.${r.property}` : r.variable;
  }) ?? MOCK_COLS;

  return { latencyMs: lat, scanRows: scanR, plan, rows: MOCK_ROWS.slice(0, 8), cols };
}

// ─── Panel state ─────────────────────────────────────────────────────────────
interface PanelState {
  id: number; pruning: boolean; gem: boolean; ssrf: boolean;
  locked: boolean; // first panel = locked (inherited from Plan)
  running: boolean; result: MockResult | null;
}

let _pid = 0;
function mkPanel(locked: boolean, pr = true, ge = true, ss = true): PanelState {
  return { id: ++_pid, pruning: pr, gem: ge, ssrf: ss, locked, running: false, result: null };
}

// ─── Main ────────────────────────────────────────────────────────────────────
export default function S5_Performance({ step, queryState, completedRuns, onRunComplete, onRunRemove }: Props) {
  const [panels, setPanels] = useState<PanelState[]>([mkPanel(true)]);
  const cypher = useMemo(() => queryState ? generateCypher(queryState).replace(/\n\s*/g, " ") : "No query selected", [queryState]);

  const upd = (id: number, u: Partial<PanelState>) => setPanels(p => p.map(x => x.id === id ? { ...x, ...u } : x));

  const run = (id: number) => {
    const p = panels.find(x => x.id === id); if (!p) return;
    upd(id, { running: true, result: null });
    const delay = p.pruning ? 300 + Math.random() * 200 : 800 + Math.random() * 400;
    setTimeout(() => {
      const result = getMockResult({ pruning: p.pruning, gem: p.gem, ssrf: p.ssrf }, queryState);
      upd(id, { running: false, result });
      // Report completed run upward
      const panelIndex = panels.findIndex(x => x.id === id);
      onRunComplete({
        id: p.id,
        label: `Run ${panelIndex + 1}`,
        pruning: p.pruning,
        gem: p.gem,
        ssrf: p.ssrf,
        latencyMs: result.latencyMs,
        locked: p.locked,
      });
    }, delay);
  };

  const removePanel = (id: number) => {
    setPanels(p => p.filter(x => x.id !== id));
    onRunRemove(id);
  };

  const maxLat = Math.max(1, ...panels.map(p => p.result?.latencyMs ?? 0));
  const hasQuery = queryState?.matches.some(m => m.sourceVar && m.edgeType && m.targetVar) ?? false;

  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{ maxWidth: 1440, margin: "0 auto", padding: "14px 40px", height: "100%", display: "flex", flexDirection: "column", boxSizing: "border-box", gap: 10 }}>
        {/* Query */}
        <div style={{ flexShrink: 0, padding: "8px 16px", background: "#18181b", borderRadius: 8, fontFamily: "monospace", fontSize: 13, color: "#e5e7eb" }}>
          {cypher}
        </div>

        {/* Panels */}
        <div style={{ flex: 1, overflowX: "auto", overflowY: "hidden", display: "flex", gap: 12, alignItems: "stretch" }} className="thin-scrollbar">
          <AnimatePresence>
            {panels.map((panel, pi) => (
              <motion.div key={panel.id}
                initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.95 }}
                style={{
                  flex: "1 0 0", minWidth: 320, display: "flex", flexDirection: "column", gap: 8,
                  padding: "14px", background: "#fff", borderRadius: 12,
                  border: panel.locked ? "2px solid #e84545" : "1px solid #e5e7eb",
                  position: "relative", overflow: "hidden",
                }}>
                {/* Close button (not for first locked panel) */}
                {!panel.locked && panels.length > 1 && (
                  <button onClick={() => removePanel(panel.id)}
                    style={{ position: "absolute", top: 8, right: 8, width: 22, height: 22, borderRadius: 4,
                      border: "none", background: "#f0f1f3", color: "#71717a", cursor: "pointer",
                      fontSize: 13, display: "flex", alignItems: "center", justifyContent: "center" }}>
                    &times;
                  </button>
                )}

                {/* Header */}
                <div style={{ flexShrink: 0 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
                    <span style={{ fontSize: 15, fontWeight: 700, color: "#18181b" }}>Run {pi + 1}</span>
                    {panel.locked && (
                      <span style={{
                        fontSize: 10, fontWeight: 700, padding: "2px 6px", borderRadius: 3,
                        background: "#e8454515", color: "#e84545",
                      }}>FROM PLAN</span>
                    )}
                  </div>

                  {/* Toggles — hidden for locked (FROM PLAN) panel */}
                  {!panel.locked && (
                    <div style={{ display: "flex", gap: 10, marginBottom: 8 }}>
                      {([
                        { key: "pruning" as const, label: "Pruning", color: "#3b82f6" },
                        { key: "gem" as const, label: "GEM", color: "#10B981" },
                        { key: "ssrf" as const, label: "SSRF", color: "#F59E0B" },
                      ]).map(o => (
                        <label key={o.key} style={{
                          display: "flex", alignItems: "center", gap: 4,
                          cursor: "pointer", fontSize: 13,
                        }}>
                          <input type="checkbox" checked={panel[o.key]}
                            onChange={() => upd(panel.id, { [o.key]: !panel[o.key], result: null })}
                            style={{ width: 14, height: 14, accentColor: o.color }} />
                          <span style={{ color: panel[o.key] ? o.color : "#9ca3af", fontWeight: 600 }}>{o.label}</span>
                        </label>
                      ))}
                    </div>
                  )}

                  {/* Run button */}
                  <button onClick={() => run(panel.id)} disabled={panel.running || !hasQuery}
                    style={{
                      padding: "8px 0", borderRadius: 7, border: "none", width: "100%",
                      background: panel.running ? "#9ca3af" : !hasQuery ? "#d4d4d8" : "#e84545",
                      color: !hasQuery ? "#9ca3af" : "#fff", fontSize: 14, fontWeight: 700,
                      cursor: panel.running || !hasQuery ? "default" : "pointer",
                    }}>
                    {!hasQuery ? "Build a query first" : panel.running ? "Running..." : "\u25b6 Run"}
                  </button>
                </div>

                {/* Results */}
                {panel.result && (
                  <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 8, overflowY: "auto", minHeight: 0 }} className="thin-scrollbar">
                    {/* Latency */}
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <span style={{ fontSize: 12, color: "#71717a" }}>Latency</span>
                      <span style={{ fontSize: 22, fontWeight: 800, fontFamily: "monospace",
                        color: panel.result.latencyMs < 50 ? "#10B981" : panel.result.latencyMs < 500 ? "#F59E0B" : "#e84545" }}>
                        {panel.result.latencyMs} ms
                      </span>
                    </div>
                    <div style={{ height: 6, background: "#f0f1f3", borderRadius: 3, overflow: "hidden", flexShrink: 0 }}>
                      <motion.div initial={{ width: 0 }}
                        animate={{ width: `${Math.max(2, (panel.result.latencyMs / maxLat) * 100)}%` }}
                        transition={{ duration: 0.4 }}
                        style={{ height: "100%", borderRadius: 3,
                          background: panel.result.latencyMs < 50 ? "#10B981" : panel.result.latencyMs < 500 ? "#F59E0B" : "#e84545" }} />
                    </div>
                    <div style={{ display: "flex", gap: 6, flexShrink: 0 }}>
                      <div style={{ flex: 1, padding: "5px 8px", background: "#f8f9fa", borderRadius: 5, fontSize: 13, fontFamily: "monospace" }}>
                        <span style={{ fontWeight: 700 }}>{panel.result.rows.length}</span> <span style={{ color: "#9ca3af" }}>rows</span>
                      </div>
                      <div style={{ flex: 1, padding: "5px 8px", background: "#f8f9fa", borderRadius: 5, fontSize: 13, fontFamily: "monospace" }}>
                        <span style={{ fontWeight: 700 }}>{panel.result.scanRows}</span> <span style={{ color: "#9ca3af" }}>scanned</span>
                      </div>
                    </div>

                    {/* Result table */}
                    <div style={{ flexShrink: 0, overflowX: "auto" }}>
                      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: "monospace" }}>
                        <thead>
                          <tr>{panel.result.cols.map(c => <th key={c} style={{ padding: "5px 8px", textAlign: "left", borderBottom: "2px solid #e5e7eb", color: "#71717a", fontWeight: 700 }}>{c}</th>)}</tr>
                        </thead>
                        <tbody>
                          {panel.result.rows.map((row, ri) => (
                            <tr key={ri}>{row.map((cell, ci) => <td key={ci} style={{ padding: "4px 8px", borderBottom: "1px solid #f0f1f3", color: "#374151" }}>{cell}</td>)}</tr>
                          ))}
                        </tbody>
                      </table>
                    </div>

                    {/* Plan tree */}
                    <div style={{ flexShrink: 0 }}>
                      <div style={{ fontSize: 12, color: "#71717a", fontWeight: 600, marginBottom: 4 }}>PLAN</div>
                      <div style={{ height: 200, background: "#fafbfc", borderRadius: 8, border: "1px solid #f0f1f3", overflow: "hidden" }}>
                        <MiniPlanSVG root={panel.result.plan} />
                      </div>
                    </div>
                  </div>
                )}
              </motion.div>
            ))}
          </AnimatePresence>

          {/* Add panel button */}
          <motion.button onClick={() => {
            // Smart defaults based on scenario:
            // Scenario A (1-hop): compare pruning → pruning OFF, GEM/SSRF ON
            // Scenario B (multi-hop): compare GEM+SSRF → pruning ON, GEM OFF, SSRF OFF
            const hops = queryState?.matches.filter(m => m.sourceVar && m.edgeType && m.targetVar).length ?? 1;
            const isMultiHop = hops >= 2;
            setPanels(p => [...p, mkPanel(false,
              /* pruning */ isMultiHop ? true : false,
              /* gem */     isMultiHop ? false : true,
              /* ssrf */    isMultiHop ? false : true,
            )]);
          }} whileHover={{ scale: 1.02 }}
            style={{
              flex: "0 0 70px", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
              gap: 4, borderRadius: 12, border: "2px dashed #d4d4d8", background: "transparent",
              color: "#9ca3af", cursor: "pointer", fontSize: 28, fontWeight: 300,
            }}>
            +
            <span style={{ fontSize: 11, fontWeight: 600 }}>Add Run</span>
          </motion.button>
        </div>

        {/* Comparison bar */}
        {panels.filter(p => p.result).length > 1 && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }}
            style={{ flexShrink: 0, padding: "10px 16px", background: "#f8f9fa", borderRadius: 10, border: "1px solid #e5e7eb", display: "flex", alignItems: "center", gap: 16 }}>
            {panels.filter(p => p.result).map((p) => {
              const r = p.result!;
              const c = r.latencyMs < 50 ? "#10B981" : r.latencyMs < 500 ? "#F59E0B" : "#e84545";
              return (
                <div key={p.id} style={{ flex: 1, textAlign: "center" }}>
                  <div style={{ fontSize: 20, fontWeight: 800, fontFamily: "monospace", color: c }}>{r.latencyMs}ms</div>
                  <div style={{ fontSize: 11, color: "#71717a" }}>
                    Run {panels.indexOf(p) + 1}
                    {p.locked && <span style={{ color: "#e84545", marginLeft: 4 }}>(Plan)</span>}
                  </div>
                </div>
              );
            })}
            {(() => {
              const lats = panels.filter(p => p.result).map(p => p.result!.latencyMs);
              const fast = Math.min(...lats), slow = Math.max(...lats);
              return slow > fast * 1.5 ? <div style={{ fontSize: 18, fontWeight: 800, color: "#10B981", fontFamily: "monospace" }}>{(slow / fast).toFixed(0)}&times; speedup</div> : null;
            })()}
          </motion.div>
        )}
      </div>
    </div>
  );
}
