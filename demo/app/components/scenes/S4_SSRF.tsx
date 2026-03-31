"use client";
import React, { useState, useEffect, useMemo, useRef, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { QState, generateCypher } from "@/lib/query-state";
import type { CompletedRun } from "@/app/page";

interface Props {
  step: number; onStep: (n: number) => void;
  queryState?: QState;
  completedRuns: CompletedRun[];
}

// ─── Catalog ─────────────────────────────────────────────────────────────────
interface CatGL { id: number; rows: number; cols: number; schema: string[]; }
interface Catalog {
  vertexPartitions: { graphlets: CatGL[]; totalRows: number }[];
  edgePartitions: { type: string; short: string; totalRows: number }[];
}

function fmt(n: number): string {
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(n);
}

// ─── Derive join hops + output schema from query ─────────────────────────────
interface JoinHop {
  sourceVar: string; edgeType: string; targetVar: string;
  sourceCols: string[]; targetCols: string[];
}

function extractHops(qs: QState): JoinHop[] {
  return qs.matches
    .filter(m => m.sourceVar && m.edgeType && m.targetVar)
    .map(m => {
      const sc = qs.returns.filter(r => r.variable === m.sourceVar && r.property).map(r => r.property);
      const tc = qs.returns.filter(r => r.variable === m.targetVar && r.property).map(r => r.property);
      if (sc.length === 0) sc.push("name");
      if (tc.length === 0) tc.push("name");
      return { sourceVar: m.sourceVar, edgeType: m.edgeType, targetVar: m.targetVar, sourceCols: sc, targetCols: tc };
    });
}

// ─── Batch simulation — based on query's target columns ──────────────────────
// Per-tuple record: which graphlet this tuple came from
interface TupleRecord { gl: CatGL; tupleIdx: number; }

interface BatchData {
  index: number;
  tupleCount: number;
  // Individual tuples in arrival order (interleaved GLs)
  tuples: TupleRecord[];
  hits: { gl: CatGL; count: number }[];
  colNulls: Map<string, number>;
  totalNulls: number;
  totalCells: number;
  nullPct: number;
}

interface ReplayData {
  batches: BatchData[];
  totalBatches: number;
  cumNulls: number[];
  cumCells: number[];
}

function buildReplay(graphlets: CatGL[], targetCols: string[], totalEdges: number, batchSize: number): ReplayData {
  if (graphlets.length === 0 || targetCols.length === 0) {
    return { batches: [], totalBatches: 0, cumNulls: [], cumCells: [] };
  }

  const maxTuples = batchSize * 20;
  const totalTuples = Math.min(totalEdges, maxTuples);
  const totalBatches = Math.ceil(totalTuples / batchSize);

  // CDF for proportional distribution
  const cdf: { idx: number; cum: number }[] = [];
  let cum = 0;
  for (let i = 0; i < graphlets.length; i++) { cum += graphlets[i].rows; cdf.push({ idx: i, cum }); }
  const totalRows = cum || 1;
  const lookup = (pos: number): number => {
    let lo = 0, hi = cdf.length - 1;
    while (lo < hi) { const mid = (lo + hi) >>> 1; if (cdf[mid].cum <= pos) lo = mid + 1; else hi = mid; }
    return cdf[lo].idx;
  };

  const batches: BatchData[] = [];
  const cumNulls: number[] = [];
  const cumCells: number[] = [];
  let runN = 0, runC = 0;

  // Simple LCG for deterministic pseudo-random interleaving
  const lcg = (seed: number) => ((seed * 1664525 + 1013904223) >>> 0) % totalRows;

  for (let b = 0; b < totalBatches; b++) {
    const tupleCount = Math.min(batchSize, totalTuples - b * batchSize);

    // Generate per-tuple GL assignments with pseudo-random positions (simulates real edge traversal)
    const tuples: TupleRecord[] = [];
    const hitMap = new Map<number, number>();
    const glCounters = new Map<number, number>();

    for (let t = 0; t < tupleCount; t++) {
      const pos = lcg(b * batchSize + t + 42);
      const gi = lookup(pos);
      hitMap.set(gi, (hitMap.get(gi) ?? 0) + 1);
      const cnt = glCounters.get(gi) ?? 0;
      glCounters.set(gi, cnt + 1);
      tuples.push({ gl: graphlets[gi], tupleIdx: cnt });
    }

    const hits: { gl: CatGL; count: number }[] = [];
    for (const [gi, count] of hitMap) hits.push({ gl: graphlets[gi], count });
    hits.sort((a, b) => a.gl.id - b.gl.id);

    const colNulls = new Map<string, number>();
    let totalNulls = 0;
    for (const col of targetCols) {
      let n = 0;
      for (const h of hits) {
        if (!h.gl.schema.includes(col)) n += h.count;
      }
      colNulls.set(col, n);
      totalNulls += n;
    }
    const totalCells = tupleCount * targetCols.length;
    const nullPct = totalCells > 0 ? Math.round(totalNulls / totalCells * 100) : 0;

    batches.push({ index: b, tupleCount, tuples, hits, colNulls, totalNulls, totalCells, nullPct });
    runN += totalNulls; runC += totalCells;
    cumNulls.push(runN); cumCells.push(runC);
  }
  return { batches, totalBatches, cumNulls, cumCells };
}

// ─── Sample values ───────────────────────────────────────────────────────────
function sampleVal(col: string, gid: number, ri: number): string {
  const s = gid * 1000 + ri;
  const N = ["Einstein","Curie","Tesla","Darwin","Hawking","Turing","Euler","Gauss","Planck","Bohr","Newton","Fermi"];
  const C = ["Berlin","Paris","Tokyo","London","Seoul","Rome","Vienna","Madrid","Prague","Oslo"];
  switch (col) {
    case "name": case "label": return N[s % N.length];
    case "birthDate": return `${1920 + s % 80}-${String(1 + s % 12).padStart(2,"0")}`;
    case "deathDate": return `${1970 + s % 50}-${String(1 + s % 12).padStart(2,"0")}`;
    case "birthPlace": return C[s % C.length];
    case "abstract": return `Abstract #${s % 500}...`;
    case "comment": return `Comment_${s % 100}`;
    case "populationTotal": return fmt(5000 + (s * 7919) % 9995000);
    case "areaTotal": return `${10 + (s * 31) % 9990}km2`;
    case "elevation": return `${s % 3000}m`;
    case "nationality": return ["German","French","British","Korean","Italian","Japanese"][s % 6];
    case "occupation": return ["Scientist","Engineer","Artist","Writer","Musician","Actor"][s % 6];
    default: return `val_${s % 100}`;
  }
}

const GL_COLORS = ["#0891B2","#DC2626","#B45309","#7C3AED","#059669","#D97706","#2563EB","#DB2777","#4F46E5","#0D9488"];

// ─── Main ────────────────────────────────────────────────────────────────────
export default function S4_SSRF({ step, onStep, queryState, completedRuns }: Props) {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [selectedHop, setSelectedHop] = useState(0);
  const [currentBatch, setCurrentBatch] = useState(0);
  const [playing, setPlaying] = useState(false);
  const playRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    fetch("/dbpedia_catalog.json").then(r => r.json()).then(d => setCatalog(d)).catch(() => {});
  }, []);

  const hasRuns = completedRuns.length > 0;
  const ssrfOnRun = completedRuns.find(r => r.ssrf);
  const ssrfOffRun = completedRuns.find(r => !r.ssrf);

  const cypher = useMemo(() => queryState ? generateCypher(queryState).replace(/\n\s*/g, " ") : "", [queryState]);
  const hops = useMemo(() => queryState ? extractHops(queryState) : [], [queryState]);
  const hop = hops[selectedHop] ?? null;

  const allGraphlets = catalog?.vertexPartitions?.[0]?.graphlets ?? [];
  const totalEdges = useMemo(() => {
    if (!catalog?.edgePartitions || !hop) return 1_600_000;
    const ep = catalog.edgePartitions.find(e => e.short?.includes(hop.edgeType) || e.type?.includes(hop.edgeType));
    return ep?.totalRows ?? 1_600_000;
  }, [catalog, hop]);

  // Filter: only graphlets that have at least one of the requested target columns
  // (e.g., GL-0 with just {id, uri} wouldn't be a meaningful IdSeek target for birthPlace)
  const relevantGraphlets = useMemo(() => {
    if (!hop) return allGraphlets;
    const tgtSet = new Set(hop.targetCols);
    return allGraphlets.filter(g => g.schema.some(c => tgtSet.has(c)));
  }, [allGraphlets, hop]);

  // Build replay based on query's TARGET columns
  const replay = useMemo(
    () => buildReplay(relevantGraphlets, hop?.targetCols ?? [], totalEdges, 1024),
    [relevantGraphlets, hop, totalEdges],
  );
  const batch = replay.batches[currentBatch] ?? null;

  // Full output schema: source cols + target cols
  const outputCols = useMemo(() => {
    if (!hop) return [];
    return [
      ...hop.sourceCols.map(c => ({ var: hop.sourceVar, prop: c, side: "src" as const })),
      ...hop.targetCols.map(c => ({ var: hop.targetVar, prop: c, side: "tgt" as const })),
    ];
  }, [hop]);

  // Display rows from interleaved tuples (show first 20, represent all 1024)
  const DISPLAY_LIMIT = 20;
  const displayRows = useMemo(() => {
    if (!batch || !hop) return [];
    const rows: { gl: CatGL; tupleIdx: number; cells: { value: string | null; isNull: boolean }[] }[] = [];
    const limit = Math.min(DISPLAY_LIMIT, batch.tuples.length);
    for (let t = 0; t < limit; t++) {
      const rec = batch.tuples[t];
      const schemaSet = new Set(rec.gl.schema);
      const cells = outputCols.map(col => {
        if (col.side === "src") {
          return { value: sampleVal(col.prop, rec.gl.id + 9999, rec.tupleIdx), isNull: false };
        }
        if (schemaSet.has(col.prop)) {
          return { value: sampleVal(col.prop, rec.gl.id, rec.tupleIdx), isNull: false };
        }
        return { value: null, isNull: true };
      });
      rows.push({ gl: rec.gl, tupleIdx: rec.tupleIdx, cells });
    }
    return rows;
  }, [batch, hop, outputCols]);

  // Play/pause
  useEffect(() => {
    if (playing) {
      playRef.current = setInterval(() => {
        setCurrentBatch(prev => {
          if (prev >= replay.totalBatches - 1) { setPlaying(false); return prev; }
          return prev + 1;
        });
      }, 600);
    } else {
      if (playRef.current) clearInterval(playRef.current);
    }
    return () => { if (playRef.current) clearInterval(playRef.current); };
  }, [playing, replay.totalBatches]);

  const togglePlay = useCallback(() => {
    if (currentBatch >= replay.totalBatches - 1) { setCurrentBatch(0); setPlaying(true); }
    else setPlaying(p => !p);
  }, [currentBatch, replay.totalBatches]);

  const cumNullPct = (replay.cumCells[currentBatch] ?? 0) > 0
    ? Math.round((replay.cumNulls[currentBatch] ?? 0) / replay.cumCells[currentBatch] * 100) : 0;

  // ── Empty state ──
  if (!hasRuns) {
    return (
      <div style={{ height: "100%", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ textAlign: "center", maxWidth: 420 }}>
          <h2 style={{ fontSize: 22, fontWeight: 700, color: "#18181b", marginBottom: 8 }}>No Runs Yet</h2>
          <p style={{ fontSize: 16, color: "#71717a", lineHeight: 1.6 }}>
            Execute queries in the <strong>Results</strong> tab first.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{
        maxWidth: 1440, margin: "0 auto", padding: "14px 36px",
        height: "100%", display: "flex", flexDirection: "column",
        boxSizing: "border-box", gap: 8,
      }}>
        {/* ── Query ── */}
        <div style={{
          flexShrink: 0, padding: "8px 16px", background: "#18181b", borderRadius: 8,
          fontFamily: "monospace", fontSize: 15, color: "#e5e7eb",
        }}>{cypher || "No query"}</div>

        {/* ── Header ── */}
        <div style={{ flexShrink: 0, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <h2 style={{ fontSize: 20, fontWeight: 700, color: "#18181b", margin: 0 }}>
            Inspect: IdSeek Intermediate Result
          </h2>
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            {ssrfOnRun && (
              <span style={{ padding: "4px 12px", borderRadius: 6, fontFamily: "monospace", fontSize: 14,
                background: "#dcfce7", color: "#166534", fontWeight: 700, border: "1px solid #bbf7d0" }}>
                {ssrfOnRun.label}: SSRF ON — {ssrfOnRun.latencyMs}ms
              </span>
            )}
            {ssrfOnRun && ssrfOffRun && <span style={{ color: "#9ca3af", fontSize: 14 }}>vs</span>}
            {ssrfOffRun && (
              <span style={{ padding: "4px 12px", borderRadius: 6, fontFamily: "monospace", fontSize: 14,
                background: "#fee2e2", color: "#b91c1c", fontWeight: 700, border: "1px solid #fecaca" }}>
                {ssrfOffRun.label}: SSRF OFF — {ssrfOffRun.latencyMs}ms
              </span>
            )}
          </div>
        </div>

        {/* ── Plan tree (IdSeek selector) + batch slider ── */}
        <div style={{ flexShrink: 0, display: "flex", gap: 12, alignItems: "stretch" }}>
          {/* Mini plan tree — horizontal, click IdSeek nodes */}
          <div style={{
            display: "flex", alignItems: "center", gap: 0,
            padding: "6px 12px", background: "#fafbfc", borderRadius: 8, border: "1px solid #e5e7eb",
          }}>
            <div style={{
              padding: "3px 8px", borderRadius: 4, fontSize: 12, fontFamily: "monospace",
              border: "1px solid #e5e7eb", color: "#9ca3af", background: "#fff", opacity: 0.5,
            }}>Proj</div>
            {hops.map((h, i) => (
              <React.Fragment key={i}>
                <div style={{ width: 10, height: 2, background: "#d4d4d8" }} />
                <motion.button
                  onClick={() => { setSelectedHop(i); setCurrentBatch(0); }}
                  whileHover={{ scale: 1.04 }}
                  style={{
                    padding: "5px 10px", borderRadius: 5, fontFamily: "monospace", fontSize: 14,
                    border: selectedHop === i ? "2px solid #0891B2" : "1px solid #d4d4d8",
                    background: selectedHop === i ? "#0891B210" : "#fff",
                    color: selectedHop === i ? "#0891B2" : "#52525b",
                    fontWeight: 700, cursor: "pointer", whiteSpace: "nowrap",
                  }}>
                  IdSeek({h.targetVar})
                </motion.button>
                <div style={{ width: 10, height: 2, background: "#d4d4d8" }} />
                <div style={{
                  padding: "3px 8px", borderRadius: 4, fontSize: 12, fontFamily: "monospace",
                  border: "1px solid #e5e7eb", color: "#9ca3af", background: "#fff", opacity: 0.5,
                }}>AdjIdx</div>
              </React.Fragment>
            ))}
            <div style={{ width: 10, height: 2, background: "#d4d4d8" }} />
            <div style={{
              padding: "3px 8px", borderRadius: 4, fontSize: 12, fontFamily: "monospace",
              border: "1px solid #e5e7eb", color: "#9ca3af", background: "#fff", opacity: 0.5,
            }}>Scan({hops[0]?.sourceVar ?? "?"})</div>
          </div>

          {/* Batch slider */}
          <div style={{
            flex: 1, display: "flex", flexDirection: "column", justifyContent: "center", gap: 6,
            padding: "8px 16px", background: "#fafbfc", borderRadius: 8, border: "1px solid #e5e7eb",
          }}>
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <motion.button onClick={togglePlay} whileHover={{ scale: 1.08 }} whileTap={{ scale: 0.95 }}
                style={{
                  width: 38, height: 38, borderRadius: "50%", border: "none", cursor: "pointer",
                  background: playing ? "#e84545" : "#10B981", color: "#fff", fontSize: 18,
                  fontWeight: 700, display: "flex", alignItems: "center", justifyContent: "center",
                }}>
                {playing ? "\u275A\u275A" : "\u25B6"}
              </motion.button>
              <div style={{ flex: 1 }}>
                <input type="range" min={0} max={Math.max(0, replay.totalBatches - 1)} value={currentBatch}
                  onChange={e => { setCurrentBatch(Number(e.target.value)); setPlaying(false); }}
                  style={{ width: "100%", height: 6, cursor: "pointer", accentColor: "#F59E0B" }} />
                <div style={{ display: "flex", justifyContent: "space-between", fontFamily: "monospace", fontSize: 13, color: "#9ca3af" }}>
                  <span>0</span>
                  <span style={{ color: "#F59E0B", fontWeight: 700 }}>Batch {currentBatch}</span>
                  <span>{Math.max(0, replay.totalBatches - 1)}</span>
                </div>
              </div>
            </div>
            {batch && (
              <div style={{ display: "flex", gap: 16, fontFamily: "monospace", fontSize: 14, color: "#6b7280" }}>
                <span>{batch.tupleCount} tuples</span>
                <span>GLs: <strong style={{ color: "#0891B2" }}>{batch.hits.length}</strong></span>
                <span>NULLs: <strong style={{ color: "#e84545" }}>{fmt(batch.totalNulls)}</strong> ({batch.nullPct}%)</span>
                {/* Per-column NULL breakdown */}
                {hop && hop.targetCols.map(c => {
                  const n = batch.colNulls.get(c) ?? 0;
                  const pct = batch.tupleCount > 0 ? Math.round(n / batch.tupleCount * 100) : 0;
                  return <span key={c} style={{ color: n > 0 ? "#e84545" : "#10B981" }}>
                    {hop.targetVar}.{c}: <strong>{pct}%</strong> NULL
                  </span>;
                })}
              </div>
            )}
          </div>
        </div>

        {/* ── Main: SSRF ON (left) vs SSRF OFF (right) ── */}
        <div style={{ flex: 1, minHeight: 0, display: "flex", gap: 12, overflow: "hidden" }}>

          {/* ═══ LEFT: SSRF ON — same schema, but c cols point to TupleStore via SchPtr ═══ */}
          <div style={{
            flex: 1, display: "flex", flexDirection: "column",
            background: "#f0fdf4", border: "1px solid #bbf7d0", borderRadius: 10,
            padding: "12px 16px", overflow: "hidden", minWidth: 0,
          }}>
            <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 8, flexShrink: 0 }}>
              <span style={{ fontSize: 18, color: "#10B981", fontFamily: "monospace", fontWeight: 700 }}>SSRF ON</span>
              <span style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace" }}>target cols → TupleStore via SchPtr</span>
              {ssrfOnRun && <span style={{ fontSize: 13, color: "#9ca3af", marginLeft: "auto" }}>{ssrfOnRun.label}</span>}
            </div>

            <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 10 }}>
              {/* Relation: source cols (inline) | target cols (grayed, data in TS) | SchPtr */}
              <table style={{ fontSize: 14, fontFamily: "monospace", borderCollapse: "collapse", whiteSpace: "nowrap", width: "100%" }}>
                <thead>
                  <tr>
                    {hop && hop.sourceCols.map(c => (
                      <th key={`s-${c}`} style={{
                        padding: "5px 8px", borderBottom: "2px solid #86efac", textAlign: "left",
                        color: "#166534", fontSize: 13, background: "#f0fdf4",
                      }}>{hop.sourceVar}.{c}</th>
                    ))}
                    {/* Target cols grouped — single header spanning all */}
                    {hop && hop.targetCols.length > 0 && (
                      <th colSpan={hop.targetCols.length} style={{
                        padding: "5px 8px", borderBottom: "2px solid #0891B240", textAlign: "center",
                        color: "#0891B2", fontSize: 12, background: "#0891B206",
                      }}>{hop.targetVar}.* ({hop.targetCols.length} cols) → TupleStore</th>
                    )}
                    <th style={{
                      padding: "5px 8px", borderBottom: "2px solid #86efac", textAlign: "center",
                      color: "#0891B2", fontSize: 13, fontWeight: 700, background: "#f0fdf4",
                    }}>SchPtr</th>
                  </tr>
                </thead>
                <tbody>
                  <AnimatePresence mode="popLayout">
                    {displayRows.map((row, ri) => {
                      const ci = batch!.hits.findIndex(h => h.gl.id === row.gl.id);
                      const color = GL_COLORS[ci % GL_COLORS.length];
                      const srcCells = row.cells.filter((_, i) => outputCols[i]?.side === "src");
                      return (
                        <motion.tr key={`b${currentBatch}-r${ri}`}
                          initial={{ opacity: 0, y: 3 }} animate={{ opacity: 1, y: 0 }}
                          transition={{ delay: ri * 0.02, duration: 0.12 }}>
                          {srcCells.map((cell, i) => (
                            <td key={`s${i}`} style={{
                              padding: "4px 8px", borderBottom: "1px solid #dcfce7",
                              color: "#374151", fontSize: 14,
                            }}>{cell.value}</td>
                          ))}
                          {/* Target cols: one merged cell pointing to TupleStore */}
                          {hop && hop.targetCols.length > 0 && (() => {
                            // Compute byte offset of this tuple in the TupleStore
                            const schemaSet = new Set(row.gl.schema);
                            const storedCount = hop.targetCols.filter(c => schemaSet.has(c)).length;
                            // Approximate offset: sum of all prior tuples' stored cols * 8
                            let tsOffset = 0;
                            for (let p = 0; p < ri; p++) {
                              const prevGL = displayRows[p].gl;
                              const prevStored = hop.targetCols.filter(c => new Set(prevGL.schema).has(c)).length;
                              tsOffset += prevStored * 8;
                            }
                            return (
                              <td colSpan={hop.targetCols.length} style={{
                                padding: "4px 8px", borderBottom: "1px solid #dcfce7",
                                background: color + "06", textAlign: "center",
                              }}>
                                <span style={{ fontSize: 12, color: color + "80" }}>
                                  @{tsOffset} ({storedCount} cols)
                                </span>
                              </td>
                            );
                          })()}
                          <td style={{ padding: "4px 8px", borderBottom: "1px solid #dcfce7", textAlign: "center" }}>
                            <span style={{
                              padding: "2px 8px", borderRadius: 4,
                              background: color + "15", color, fontWeight: 700, fontSize: 12,
                              border: `1px solid ${color}30`,
                            }}>GL-{row.gl.id}</span>
                          </td>
                        </motion.tr>
                      );
                    })}
                  </AnimatePresence>
                </tbody>
              </table>

              {/* Schema Infos — per schema type, column→offset mapping */}
              {batch && batch.hits.length > 0 && hop && (
                <div>
                  <div style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace", marginBottom: 4, fontWeight: 700 }}>
                    Schema Infos
                  </div>
                  <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                    {batch.hits.slice(0, 6).map(h => {
                      const ci = batch.hits.indexOf(h);
                      const c = GL_COLORS[ci % GL_COLORS.length];
                      const schemaSet = new Set(h.gl.schema);
                      // Compute real offsets: only cols this GL has get sequential offsets
                      let off = 0;
                      const offsets = hop.targetCols.map(col => {
                        if (schemaSet.has(col)) { const o = off; off += 8; return o; }
                        return -1;
                      });
                      return (
                        <div key={h.gl.id} style={{
                          background: c + "06", borderRadius: 5, padding: "4px 8px",
                          border: `1px solid ${c}15`, fontSize: 13, fontFamily: "monospace",
                        }}>
                          <span style={{ fontWeight: 700, color: c }}>GL-{h.gl.id}</span>
                          <span style={{ color: "#9ca3af", marginLeft: 4 }}>{h.count}r</span>
                          <div style={{ display: "flex", gap: 3, marginTop: 2 }}>
                            {hop.targetCols.map((col, oi) => {
                              const o = offsets[oi];
                              const has = o >= 0;
                              return (
                                <span key={col} style={{
                                  fontSize: 11, padding: "0 4px", borderRadius: 2,
                                  background: has ? "#10B98112" : "#e8454508",
                                  color: has ? "#10B981" : "#d4d4d8",
                                  fontWeight: has ? 600 : 400,
                                }}>{col}:{o}</span>
                              );
                            })}
                          </div>
                        </div>
                      );
                    })}
                    {batch.hits.length > 6 && <span style={{ fontSize: 13, color: "#9ca3af", alignSelf: "center" }}>+{batch.hits.length - 6}</span>}
                  </div>
                </div>
              )}

              {/* TupleStore — row store, tuples packed left-to-right with boundaries */}
              {batch && displayRows.length > 0 && hop && (
                <div>
                  <div style={{ fontSize: 14, color: "#0891B2", fontFamily: "monospace", marginBottom: 4, fontWeight: 700 }}>
                    TupleStore <span style={{ color: "#10B981", fontWeight: 600 }}>row-packed, 0 NULLs</span>
                    <span style={{ color: "#9ca3af", fontWeight: 400, marginLeft: 8 }}>{batch.tupleCount} tuples</span>
                  </div>
                  <div style={{
                    display: "flex", flexWrap: "wrap", gap: 2,
                    padding: 3, background: "#f8fffe", borderRadius: 4, border: "1px solid #0891B220",
                  }}>
                    {displayRows.map((row, ri) => {
                      const ci = batch!.hits.findIndex(h => h.gl.id === row.gl.id);
                      const color = GL_COLORS[ci % GL_COLORS.length];
                      const schemaSet = new Set(row.gl.schema);
                      const stored = hop!.targetCols.filter(col => schemaSet.has(col));
                      // Each tuple = one contiguous block
                      return (
                        <div key={ri} style={{
                          display: "flex", borderRadius: 3, overflow: "hidden",
                          border: `1.5px solid ${color}40`,
                        }}>
                          {stored.map((col, si) => (
                            <div key={si} style={{
                              fontSize: 12, fontFamily: "monospace",
                              padding: "2px 5px",
                              background: color + "12",
                              color: "#374151",
                              borderRight: si < stored.length - 1 ? `1px solid ${color}20` : "none",
                              whiteSpace: "nowrap",
                            }}>{sampleVal(col, row.gl.id, row.tupleIdx)}</div>
                          ))}
                        </div>
                      );
                    })}
                    {batch.tupleCount > DISPLAY_LIMIT && (
                      <span style={{
                        fontSize: 12, color: "#9ca3af", fontFamily: "monospace",
                        alignSelf: "center", padding: "2px 6px",
                      }}>... +{batch.tupleCount - DISPLAY_LIMIT} tuples</span>
                    )}
                  </div>
                </div>
              )}
            </div>

            <div style={{ marginTop: 8, flexShrink: 0, textAlign: "center", fontFamily: "monospace", fontSize: 18 }}>
              <span style={{ color: "#6b7280" }}>Target NULLs: </span>
              <span style={{ color: "#10B981", fontWeight: 700, fontSize: 22 }}>0</span>
              <span style={{ color: "#6b7280", marginLeft: 12, fontSize: 14 }}>(data in TupleStore)</span>
            </div>
          </div>

          {/* ═══ RIGHT: SSRF OFF — ALL cols in one wide table, NULLs for missing ═══ */}
          <div style={{
            flex: 1, display: "flex", flexDirection: "column",
            background: "#fff8f8", border: "1px solid #fecaca", borderRadius: 10,
            padding: "12px 16px", overflow: "hidden", minWidth: 0,
          }}>
            <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 8, flexShrink: 0 }}>
              <span style={{ fontSize: 18, color: "#e84545", fontFamily: "monospace", fontWeight: 700 }}>SSRF OFF</span>
              <span style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace" }}>UNIONALL wide table</span>
              {ssrfOffRun && <span style={{ fontSize: 13, color: "#9ca3af", marginLeft: "auto" }}>{ssrfOffRun.label}</span>}
            </div>

            <div style={{ flex: 1, minHeight: 0, overflow: "auto" }}>
              <table style={{ fontSize: 14, fontFamily: "monospace", borderCollapse: "collapse", whiteSpace: "nowrap", width: "100%" }}>
                <thead>
                  <tr>
                    {outputCols.map(c => (
                      <th key={`${c.var}.${c.prop}`} style={{
                        padding: "5px 8px", borderBottom: "2px solid #fca5a5", textAlign: "left",
                        color: c.side === "src" ? "#166534" : "#b91c1c", fontSize: 13,
                        position: "sticky", top: 0, background: "#fff8f8",
                      }}>{c.var}.{c.prop}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  <AnimatePresence mode="popLayout">
                    {displayRows.map((row, ri) => (
                      <motion.tr key={`b${currentBatch}-r${ri}`}
                        initial={{ opacity: 0, y: 3 }} animate={{ opacity: 1, y: 0 }}
                        transition={{ delay: ri * 0.02, duration: 0.12 }}>
                        {row.cells.map((cell, i) => (
                          <td key={i} style={{
                            padding: "4px 8px", borderBottom: "1px solid #fee2e2",
                            color: cell.isNull ? "#e84545" : "#374151",
                            background: cell.isNull ? "#fecaca" : "transparent",
                            fontWeight: cell.isNull ? 700 : 400,
                            fontStyle: cell.isNull ? "italic" : "normal",
                            fontSize: 14, maxWidth: 90, overflow: "hidden", textOverflow: "ellipsis",
                          }}>
                            {cell.isNull ? "NULL" : cell.value}
                          </td>
                        ))}
                      </motion.tr>
                    ))}
                  </AnimatePresence>
                </tbody>
              </table>
            </div>

            <div style={{ marginTop: 8, flexShrink: 0, textAlign: "center", fontFamily: "monospace", fontSize: 18 }}>
              <span style={{ color: "#6b7280" }}>Cols: </span>
              <span style={{ color: "#e84545", fontWeight: 700, fontSize: 22 }}>{outputCols.length}</span>
              <span style={{ color: "#6b7280", marginLeft: 16 }}>NULLs: </span>
              <span style={{ color: "#e84545", fontWeight: 700, fontSize: 22 }}>{batch?.nullPct ?? 0}%</span>
            </div>
          </div>
        </div>

        {/* ── Bottom: cumulative ── */}
        <div style={{
          flexShrink: 0, display: "flex", gap: 20, justifyContent: "center", alignItems: "center",
          padding: "8px 20px", background: "#fafbfc", borderRadius: 8, border: "1px solid #e5e7eb",
          fontFamily: "monospace", fontSize: 15,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ color: "#e84545", fontWeight: 700 }}>Cumulative NULLs:</span>
            <div style={{ width: 100, height: 14, background: "#fee2e2", borderRadius: 4, position: "relative", overflow: "hidden" }}>
              <motion.div animate={{ width: `${Math.min(100, cumNullPct)}%` }} transition={{ duration: 0.3 }}
                style={{ height: "100%", background: "#e84545", borderRadius: 4 }} />
            </div>
            <span style={{ color: "#e84545", fontWeight: 700 }}>{fmt(replay.cumNulls[currentBatch] ?? 0)}</span>
            <span style={{ color: "#9ca3af" }}>({cumNullPct}%)</span>
          </div>

          <div style={{ width: 1, height: 24, background: "#e5e7eb" }} />
          <span style={{ color: "#10B981", fontWeight: 700 }}>SSRF: <span style={{ fontSize: 20 }}>0</span> NULLs</span>
          <div style={{ width: 1, height: 24, background: "#e5e7eb" }} />
          <span style={{ color: "#10B981", fontWeight: 800 }}>
            {fmt(replay.cumNulls[currentBatch] ?? 0)} NULLs eliminated
          </span>
          <div style={{ width: 1, height: 24, background: "#e5e7eb" }} />
          <span>Batch <strong style={{ color: "#F59E0B" }}>{currentBatch + 1}</strong>/{replay.totalBatches}</span>

          {ssrfOnRun && ssrfOffRun && <>
            <div style={{ width: 1, height: 24, background: "#e5e7eb" }} />
            <span style={{ color: "#10B981", fontWeight: 800, fontSize: 20 }}>
              {Math.round(ssrfOffRun.latencyMs / ssrfOnRun.latencyMs)}&times; faster
            </span>
          </>}
        </div>
      </div>
    </div>
  );
}
