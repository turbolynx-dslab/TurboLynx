"use client";
import { useEffect, useState, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface Props { step: number; onStep: (n: number) => void; }

interface Graphlet { id: number; rows: number; extents: number; cols: number; schema: string[]; schemaTruncated?: boolean; }
interface EdgeGraphlet { id: number; rows: number; }
interface VertexPartition { label: string; numColumns: number; numGraphlets: number; totalRows: number; graphlets: Graphlet[]; }
interface EdgePartition { type: string; short: string; totalRows: number; numGraphlets: number; graphlets: EdgeGraphlet[]; }
interface Summary { totalNodes: number; totalEdges: number; vertexPartitions: number; edgePartitions: number; totalGraphlets: number; }
interface Catalog { summary: Summary; vertexPartitions: VertexPartition[]; edgePartitions: EdgePartition[]; }

function fmt(n: number): string {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(n);
}

type View = "nodeGL" | "edgeGL";

function sizeClass(rows: number): { w: number; h: number } {
  if (rows >= 10_000_000) return { w: 4, h: 3 };
  if (rows >= 1_000_000)  return { w: 3, h: 2 };
  if (rows >= 100_000)    return { w: 2, h: 2 };
  if (rows >= 10_000)     return { w: 2, h: 1 };
  return { w: 1, h: 1 };
}

const HEAT = ["#dbeafe", "#93c5fd", "#60a5fa", "#3b82f6", "#2563eb", "#1d4ed8", "#1e40af"];
function heatColor(rows: number, max: number): string {
  if (max === 0) return HEAT[0];
  const ratio = Math.log1p(rows) / Math.log1p(max);
  return HEAT[Math.min(HEAT.length - 1, Math.floor(ratio * HEAT.length))];
}

const EHEAT = ["#fce7f3", "#f9a8d4", "#f472b6", "#ec4899", "#db2777", "#be185d", "#9d174d"];
function edgeHeatColor(rows: number, max: number): string {
  if (max === 0) return EHEAT[0];
  const ratio = Math.log1p(rows) / Math.log1p(max);
  return EHEAT[Math.min(EHEAT.length - 1, Math.floor(ratio * EHEAT.length))];
}

// ─── Detail Panel (shared) ───────────────────────────────────────────────────
function DetailLabel({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ fontSize: 14, color: "#71717a", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6, fontWeight: 600 }}>
      {children}
    </div>
  );
}

function StatBox({ v, l }: { v: string; l: string }) {
  return (
    <div style={{ padding: "10px 12px", background: "#fff", borderRadius: 7, border: "1px solid #f0f1f3" }}>
      <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "monospace", color: "#18181b", lineHeight: 1 }}>{v}</div>
      <div style={{ fontSize: 13, color: "#71717a", marginTop: 4 }}>{l}</div>
    </div>
  );
}

function ShareBar({ value, total, color }: { value: number; total: number; color: string }) {
  const pct = total > 0 ? (value / total * 100) : 0;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
      <div style={{ flex: 1, height: 8, background: "#e5e7eb", borderRadius: 4, overflow: "hidden" }}>
        <div style={{ height: "100%", borderRadius: 4, background: color, width: `${Math.max(0.5, pct)}%` }} />
      </div>
      <span style={{ fontSize: 18, fontWeight: 700, fontFamily: "monospace", color, minWidth: 60, textAlign: "right" }}>
        {pct.toFixed(1)}%
      </span>
    </div>
  );
}

function HeatLegend({ colors, labels }: { colors: string[]; labels: string[] }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 3, flexShrink: 0 }}>
      <span style={{ fontSize: 12, color: "#9ca3af", marginRight: 4 }}>{labels[0]}</span>
      {colors.map((c, i) => (
        <div key={i} style={{ width: 18, height: 10, background: c, borderRadius: 2 }} />
      ))}
      <span style={{ fontSize: 12, color: "#9ca3af", marginLeft: 4 }}>{labels[1]}</span>
    </div>
  );
}

const HR = () => <div style={{ height: 1, background: "#e5e7eb40" }} />;

function CloseBtn({ onClick }: { onClick: () => void }) {
  return (
    <button onClick={onClick} style={{
      position: "absolute", top: 10, right: 10,
      width: 28, height: 28, borderRadius: 6, border: "none",
      background: "#f0f1f3", color: "#71717a", cursor: "pointer",
      fontSize: 16, fontWeight: 600, display: "flex", alignItems: "center", justifyContent: "center",
    }}>&times;</button>
  );
}

function cleanEdgeType(t: string): string {
  return t.replace(/@NODE@NODE$/, "");
}
function cleanEdgeShort(t: string): string {
  return t.replace(/@NODE@NODE$/, "");
}

// ─── Main ────────────────────────────────────────────────────────────────────
export default function S1_Storage({ step }: Props) {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [view, setView] = useState<View>("nodeGL");
  const [displayMode, setDisplayMode] = useState<"grid" | "list">("grid");
  const [selectedGL, setSelectedGL] = useState<number | null>(null);
  const [selectedEdgeGL, setSelectedEdgeGL] = useState<string | null>(null);

  useEffect(() => {
    fetch("/dbpedia_catalog.json").then(r => r.json()).then(setCatalog).catch(() => {});
  }, []);

  const sortedNodeGLs = useMemo(() => {
    if (!catalog) return [];
    return [...catalog.vertexPartitions[0].graphlets].sort((a, b) => b.rows - a.rows);
  }, [catalog]);

  const sortedEdges = useMemo(() => {
    if (!catalog) return [];
    return [...catalog.edgePartitions].sort((a, b) => b.totalRows - a.totalRows);
  }, [catalog]);

  if (!catalog) {
    return <div style={{ height: "100%", display: "flex", alignItems: "center", justifyContent: "center", color: "#9ca3af", fontSize: 16 }}>Loading catalog...</div>;
  }

  const s = catalog.summary;
  const vp = catalog.vertexPartitions[0];
  const maxNodeRows = sortedNodeGLs[0]?.rows ?? 1;
  const maxEdgeRows = sortedEdges[0]?.totalRows ?? 1;
  const selGL = selectedGL !== null ? vp.graphlets.find(g => g.id === selectedGL) : null;
  const selEdgeGL = selectedEdgeGL ? catalog.edgePartitions.find(e => e.short === selectedEdgeGL) : null;

  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{
        maxWidth: 1440, margin: "0 auto", padding: "16px 40px", height: "100%",
        display: "flex", flexDirection: "column", boxSizing: "border-box", gap: 12,
      }}>
        {/* Summary */}
        <div style={{ display: "flex", gap: 10, flexShrink: 0, flexWrap: "wrap" }}>
          {[
            { v: fmt(s.totalNodes), l: "Nodes" },
            { v: fmt(s.totalEdges), l: "Edges" },
            { v: String(vp.numColumns), l: "Properties" },
            { v: String(vp.numGraphlets), l: "Node Graphlets", accent: "#3b82f6" },
            { v: String(s.edgePartitions), l: "Edge Graphlets", accent: "#ec4899" },
          ].map((c, i) => (
            <div key={i} style={{
              padding: "10px 16px", background: "#fff", borderRadius: 8,
              border: `1px solid ${"accent" in c && c.accent ? c.accent + "25" : "#e5e7eb"}`,
            }}>
              <div style={{ fontSize: 26, fontWeight: 700, fontFamily: "monospace",
                color: ("accent" in c && c.accent) || "#18181b", lineHeight: 1 }}>{c.v}</div>
              <div style={{ fontSize: 13, color: "#9ca3af", marginTop: 4 }}>{c.l}</div>
            </div>
          ))}
        </div>

        {/* 3-tab toggle */}
        <div style={{ display: "flex", gap: 4, flexShrink: 0 }}>
          {([
            { key: "nodeGL" as View, label: `Node Graphlets (${vp.numGraphlets})` },
            { key: "edgeGL" as View, label: `Edge Graphlets (${s.edgePartitions})` },
          ]).map(t => (
            <button key={t.key} onClick={() => { setView(t.key); setSelectedGL(null); setSelectedEdgeGL(null); }}
              style={{
                padding: "6px 16px", borderRadius: 6, border: "none", cursor: "pointer",
                fontSize: 14, fontWeight: 600,
                background: view === t.key ? "#18181b" : "#f0f1f3",
                color: view === t.key ? "#fff" : "#71717a",
              }}>
              {t.label}
            </button>
          ))}
          <div style={{ marginLeft: "auto", display: "flex", gap: 2, background: "#f0f1f3", borderRadius: 6, padding: 2 }}>
            {(["grid", "list"] as const).map(m => (
              <button key={m} onClick={() => setDisplayMode(m)}
                style={{
                  padding: "4px 10px", borderRadius: 4, border: "none", cursor: "pointer",
                  fontSize: 13, fontWeight: 600,
                  background: displayMode === m ? "#fff" : "transparent",
                  color: displayMode === m ? "#18181b" : "#9ca3af",
                  boxShadow: displayMode === m ? "0 1px 2px rgba(0,0,0,0.06)" : "none",
                }}>
                {m === "grid" ? "Grid" : "List"}
              </button>
            ))}
          </div>
        </div>

        {/* Content */}
        <div style={{ flex: 1, minHeight: 0, display: "flex", gap: 14, overflow: "hidden" }}>
          <AnimatePresence mode="wait">

            {/* ═══ Node Graphlets ═══ */}
            {view === "nodeGL" && (
              <motion.div key="nodeGL" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                style={{ flex: 1, display: "flex", gap: 14, overflow: "hidden" }}>
                <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
                  {displayMode === "grid" && (
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", marginBottom: 6, flexShrink: 0 }}>
                      <HeatLegend colors={HEAT} labels={["fewer rows", "more rows"]} />
                    </div>
                  )}
                  <div style={{ flex: 1, overflowY: "auto", overflowX: "hidden" }} className="thin-scrollbar">
                    {displayMode === "grid" ? (
                      <div style={{ display: "flex", flexWrap: "wrap", gap: 4, alignContent: "flex-start" }}>
                        {sortedNodeGLs.map(g => {
                          const active = selectedGL === g.id;
                          const sz = sizeClass(g.rows);
                          const base = 36;
                          return (
                            <div key={g.id} onClick={() => setSelectedGL(active ? null : g.id)}
                              style={{
                                width: sz.w * base + (sz.w - 1) * 4,
                                height: sz.h * base + (sz.h - 1) * 4,
                                borderRadius: 5, cursor: "pointer",
                                background: heatColor(g.rows, maxNodeRows),
                                border: active ? "2px solid #1d4ed8" : "1px solid transparent",
                                display: "flex", alignItems: "center", justifyContent: "center",
                                fontSize: g.rows >= 1_000_000 ? 13 : 11,
                                color: g.rows >= 100000 ? "#fff" : "#3b82f6",
                                fontFamily: "monospace", fontWeight: 700,
                              }}
                              title={`GL-${g.id}: ${g.rows.toLocaleString()} rows, ${g.cols} cols`}>
                              {sz.w >= 2 ? fmt(g.rows) : ""}
                            </div>
                          );
                        })}
                      </div>
                    ) : (
                      <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                        {sortedNodeGLs.map(g => {
                          const active = selectedGL === g.id;
                          const barW = Math.max(1, (g.rows / maxNodeRows) * 100);
                          return (
                            <div key={g.id} onClick={() => setSelectedGL(active ? null : g.id)}
                              style={{
                                padding: "8px 14px", borderRadius: 6, cursor: "pointer",
                                border: active ? "1px solid #3b82f640" : "1px solid transparent",
                                background: active ? "#eff6ff" : "transparent",
                              }}>
                              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
                                <span style={{ fontSize: 14, fontFamily: "monospace", fontWeight: 600,
                                  color: active ? "#1d4ed8" : "#374151" }}>
                                  GL-{g.id}
                                  <span style={{ fontWeight: 400, color: "#9ca3af", marginLeft: 8 }}>{g.cols} cols</span>
                                </span>
                                <span style={{ fontSize: 14, fontFamily: "monospace", color: "#71717a" }}>
                                  {g.rows.toLocaleString()}
                                </span>
                              </div>
                              <div style={{ height: 4, background: "#e5e7eb", borderRadius: 2 }}>
                                <div style={{
                                  height: "100%", borderRadius: 2, width: `${barW}%`,
                                  background: active ? "#3b82f6" : "#93c5fd",
                                }} />
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                </div>

                {/* Detail */}
                <AnimatePresence>
                  {selGL && (
                    <motion.div key={selGL.id}
                      initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }}
                      exit={{ opacity: 0, x: 20 }} transition={{ duration: 0.12 }}
                      style={{
                        width: 340, flexShrink: 0, background: "#fafbfc", position: "relative",
                        border: "1px solid #e5e7eb", borderRadius: 10, padding: "18px 20px",
                        alignSelf: "flex-start", maxHeight: "100%", overflowY: "auto",
                        display: "flex", flexDirection: "column", gap: 14,
                      }} className="thin-scrollbar">
                      <CloseBtn onClick={() => setSelectedGL(null)} />
                      <div>
                        <DetailLabel>Graphlet</DetailLabel>
                        <div style={{ fontSize: 28, fontWeight: 700, fontFamily: "monospace", color: "#18181b" }}>
                          GL-{selGL.id}
                        </div>
                      </div>
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                        <StatBox v={selGL.rows.toLocaleString()} l="rows" />
                        <StatBox v={String(selGL.cols)} l="columns" />
                        <StatBox v={String(selGL.extents)} l="extents" />
                      </div>
                      <HR />
                      <div>
                        <DetailLabel>Schema ({selGL.cols} attrs{selGL.schemaTruncated ? ", top 10" : ""})</DetailLabel>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                          {selGL.schema.map((attr, i) => (
                            <span key={i} style={{
                              fontSize: 14, fontFamily: "monospace", padding: "5px 10px",
                              background: "#eff6ff", color: "#1e40af", borderRadius: 5, border: "1px solid #bfdbfe",
                            }}>{attr}</span>
                          ))}
                          {selGL.schemaTruncated && (
                            <span style={{ fontSize: 14, color: "#71717a", padding: "5px 6px" }}>+{selGL.cols - 10} more</span>
                          )}
                        </div>
                      </div>
                      <HR />
                      <div>
                        <DetailLabel>Share of Total Nodes</DetailLabel>
                        <ShareBar value={selGL.rows} total={vp.totalRows} color="#3b82f6" />
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>
              </motion.div>
            )}

            {/* ═══ Edge Graphlets ═══ */}
            {view === "edgeGL" && (
              <motion.div key="edgeGL" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                style={{ flex: 1, display: "flex", gap: 14, overflow: "hidden" }}>
                <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
                  {displayMode === "grid" && (
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", marginBottom: 6, flexShrink: 0 }}>
                      <HeatLegend colors={EHEAT} labels={["fewer edges", "more edges"]} />
                    </div>
                  )}
                  <div style={{ flex: 1, overflowY: "auto", overflowX: "hidden" }} className="thin-scrollbar">
                    {displayMode === "grid" ? (
                      <div style={{ display: "flex", flexWrap: "wrap", gap: 4, alignContent: "flex-start" }}>
                        {sortedEdges.map(e => {
                          const active = selectedEdgeGL === e.short;
                          const sz = sizeClass(e.totalRows);
                          const base = 36;
                          return (
                            <div key={e.short} onClick={() => setSelectedEdgeGL(active ? null : e.short)}
                              style={{
                                width: sz.w * base + (sz.w - 1) * 4,
                                height: sz.h * base + (sz.h - 1) * 4,
                                borderRadius: 5, cursor: "pointer",
                                background: edgeHeatColor(e.totalRows, maxEdgeRows),
                                border: active ? "2px solid #9d174d" : "1px solid transparent",
                                display: "flex", alignItems: "center", justifyContent: "center",
                                flexDirection: "column", gap: 1,
                                fontSize: 11, color: e.totalRows >= 100000 ? "#fff" : "#ec4899",
                                fontFamily: "monospace", fontWeight: 700,
                              }}
                              title={`${cleanEdgeShort(e.short)}: ${e.totalRows.toLocaleString()} edges`}>
                              {sz.w >= 2 && <span>{fmt(e.totalRows)}</span>}
                            </div>
                          );
                        })}
                      </div>
                    ) : (
                      <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                        {sortedEdges.map(e => {
                          const active = selectedEdgeGL === e.short;
                          const barW = Math.max(1, (e.totalRows / maxEdgeRows) * 100);
                          return (
                            <div key={e.short} onClick={() => setSelectedEdgeGL(active ? null : e.short)}
                              style={{
                                padding: "8px 14px", borderRadius: 6, cursor: "pointer",
                                border: active ? "1px solid #ec489940" : "1px solid transparent",
                                background: active ? "#fdf2f8" : "transparent",
                              }}>
                              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
                                <span style={{ fontSize: 14, fontFamily: "monospace", fontWeight: 600,
                                  color: active ? "#9d174d" : "#374151" }}>
                                  {cleanEdgeShort(e.short)}
                                </span>
                                <span style={{ fontSize: 14, fontFamily: "monospace", color: "#71717a" }}>
                                  {e.totalRows.toLocaleString()}
                                </span>
                              </div>
                              <div style={{ height: 4, background: "#e5e7eb", borderRadius: 2 }}>
                                <div style={{
                                  height: "100%", borderRadius: 2, width: `${barW}%`,
                                  background: active ? "#ec4899" : "#f9a8d4",
                                }} />
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                </div>

                {/* Detail */}
                <AnimatePresence>
                  {selEdgeGL && (
                    <motion.div key={selEdgeGL.short}
                      initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }}
                      exit={{ opacity: 0, x: 20 }} transition={{ duration: 0.12 }}
                      style={{
                        width: 340, flexShrink: 0, background: "#fafbfc", position: "relative",
                        border: "1px solid #e5e7eb", borderRadius: 10, padding: "18px 20px",
                        alignSelf: "flex-start",
                        display: "flex", flexDirection: "column", gap: 14,
                      }}>
                      <CloseBtn onClick={() => setSelectedEdgeGL(null)} />
                      <div>
                        <DetailLabel>Edge Graphlet</DetailLabel>
                        <div style={{ fontSize: 26, fontWeight: 700, fontFamily: "monospace", color: "#18181b" }}>
                          {cleanEdgeShort(selEdgeGL.short)}
                        </div>
                        <div style={{ fontSize: 13, fontFamily: "monospace", color: "#71717a", marginTop: 3, wordBreak: "break-all" }}>
                          {cleanEdgeType(selEdgeGL.type)}
                        </div>
                      </div>
                      <HR />
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                        <StatBox v={selEdgeGL.totalRows.toLocaleString()} l="edges" />
                        <StatBox v={String(selEdgeGL.numGraphlets)} l="graphlet(s)" />
                      </div>
                      {selEdgeGL.graphlets.length > 1 && (
                        <>
                          <HR />
                          <div>
                            <DetailLabel>Sub-graphlets</DetailLabel>
                            {selEdgeGL.graphlets.map((g, i) => (
                              <div key={i} style={{ fontSize: 14, fontFamily: "monospace", color: "#374151", padding: "3px 0" }}>
                                GL-{g.id}: {g.rows.toLocaleString()} edges
                              </div>
                            ))}
                          </div>
                        </>
                      )}
                      <HR />
                      <div>
                        <DetailLabel>Share of Total Edges</DetailLabel>
                        <ShareBar value={selEdgeGL.totalRows} total={s.totalEdges} color="#ec4899" />
                      </div>
                      <div style={{ fontSize: 14, color: "#52525b", fontFamily: "monospace" }}>
                        NODE → NODE
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>
              </motion.div>
            )}


          </AnimatePresence>
        </div>
      </div>
    </div>
  );
}
