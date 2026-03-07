"use client";
import { useState, useEffect, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface GNode {
  id: string; name: string;
  schema: string[]; schemaSize: number;
}
interface SchemaGroup {
  gid: string; schema: string[]; nodes: GNode[]; color: string;
}
interface MergeRecord {
  aGid: string; bGid: string; aCount: number; bCount: number;
  newNulls: number; score: number; resultGid: string;
}
interface GroupSnap { gid: string; size: number; color: string; }
interface Props { step: number; onStep: (n: number) => void; }

// ─── Palette ──────────────────────────────────────────────────────────────────
const PAL = [
  "#3B82F6","#8B5CF6","#F59E0B","#10B981","#e84545",
  "#EC4899","#14B8A6","#F97316","#A855F7","#06B6D4",
  "#84CC16","#EAB308","#6366F1","#D946EF","#22C55E",
  "#0EA5E9","#7C3AED","#D97706","#059669","#DC2626",
];

// ─── OURS cost model (demo-scale constants) ────────────────────────────────────
// Production values: C_null=0.3, C_vec=10000, κ=1024 (calibrated for 77M nodes)
// Demo-scale:        C_null=4,   C_vec=100,   κ=50   (calibrated for ~15-node groups)
const C_SCH  = 100;
const C_NULL = 4;
const C_VEC  = 100;
const KAPPA  = 50;

function psi(n: number) { return n < KAPPA ? KAPPA / n : 0; }

function casimScore(gi: SchemaGroup, gj: SchemaGroup): { score: number; newNulls: number } {
  const si = new Set(gi.schema), sj = new Set(gj.schema);
  const nullsI = [...sj].filter(a => !si.has(a)).length * gi.nodes.length;
  const nullsJ = [...si].filter(a => !sj.has(a)).length * gj.nodes.length;
  const newNulls = nullsI + nullsJ;
  const score = C_SCH
    - C_NULL * newNulls
    + C_VEC * (psi(gi.nodes.length) + psi(gj.nodes.length) - psi(gi.nodes.length + gj.nodes.length));
  return { score, newNulls };
}

function computeGroups(nodes: GNode[]): SchemaGroup[] {
  const map = new Map<string, SchemaGroup>();
  let ci = 0;
  for (const n of nodes) {
    const k = [...n.schema].sort().join("\0");
    if (!map.has(k))
      map.set(k, { gid: `S${map.size + 1}`, schema: [...n.schema].sort(), nodes: [], color: PAL[ci++ % PAL.length] });
    map.get(k)!.nodes.push(n);
  }
  return [...map.values()].sort((a, b) => b.nodes.length - a.nodes.length);
}

function splitLayers(groups: SchemaGroup[]): { L1: SchemaGroup[]; L2: SchemaGroup[]; L3: SchemaGroup[] } {
  const L1: SchemaGroup[] = [], L2: SchemaGroup[] = [], L3: SchemaGroup[] = [];
  for (const g of groups) {
    if      (g.nodes.length >= 3) L1.push(g);
    else if (g.nodes.length >= 2) L2.push(g);
    else                          L3.push(g);
  }
  return { L1, L2, L3 };
}

function agglomerateWithHistory(
  groups: SchemaGroup[]
): { result: SchemaGroup[]; history: MergeRecord[]; snapshots: GroupSnap[][] } {
  let cur = groups.map(g => ({ ...g, nodes: [...g.nodes], schema: [...g.schema] }));
  const history: MergeRecord[] = [];
  const snap = (): GroupSnap[] => cur.map(g => ({ gid: g.gid, size: g.nodes.length, color: g.color }));
  const snapshots: GroupSnap[][] = [snap()];

  for (;;) {
    let bi = -1, bj = -1, bs = 0;
    for (let i = 0; i < cur.length; i++)
      for (let j = i + 1; j < cur.length; j++) {
        const { score } = casimScore(cur[i], cur[j]);
        if (score > bs) { bs = score; bi = i; bj = j; }
      }
    if (bi < 0) break;

    const { newNulls } = casimScore(cur[bi], cur[bj]);
    history.push({
      aGid: cur[bi].gid, bGid: cur[bj].gid,
      aCount: cur[bi].nodes.length, bCount: cur[bj].nodes.length,
      newNulls, score: bs,
      resultGid: cur[bi].gid,
    });

    const merged: SchemaGroup = {
      gid: cur[bi].gid,
      schema: [...new Set([...cur[bi].schema, ...cur[bj].schema])],
      nodes:  [...cur[bi].nodes, ...cur[bj].nodes],
      color:  cur[bi].nodes.length >= cur[bj].nodes.length ? cur[bi].color : cur[bj].color,
    };
    cur.splice(bj, 1);
    cur[bi] = merged;
    snapshots.push(snap());
  }
  return { result: cur, history, snapshots };
}

// ─── Phase components ─────────────────────────────────────────────────────────
type Phase = 0 | 1 | 2 | 3 | 4;

const PHASE_LABEL: Record<Phase, string> = {
  0: "Sampled Schemaless Property Graph",
  1: "Identify Schema Distribution",
  2: "Split Schemas into Layers",
  3: "Layered Agglomerative Clustering with Cost-Aware Similarity Function",
  4: "Result — Graphlet Storage",
};

function PhaseNav({ phase, onNext, onBack }: { phase: Phase; onNext: () => void; onBack: () => void }) {
  const dots: Phase[] = [0, 1, 2, 3, 4];
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      {dots.map(d => (
        <div key={d} style={{ width: d === phase ? 22 : 8, height: 8, borderRadius: 4,
          transition: "all 0.25s",
          background: d === phase ? "#8B5CF6" : d < phase ? "#8B5CF666" : "#27272a" }} />
      ))}
      {phase > 0 && (
        <button onClick={onBack}
          style={{ padding: "7px 16px", borderRadius: 8, cursor: "pointer",
            border: "1px solid #27272a", background: "transparent",
            color: "#a1a1aa", fontSize: 13, fontWeight: 500, marginLeft: 4 }}>
          ← Back
        </button>
      )}
      {phase < 4 && (
        <button onClick={onNext}
          style={{ padding: "7px 18px", borderRadius: 8, border: "none", cursor: "pointer",
            background: phase === 0 ? "#8B5CF6" : "#27272a",
            color: "#fff", fontSize: 13, fontWeight: 700 }}>
          {phase === 0 ? "▶ CLUSTER" : "Next →"}
        </button>
      )}
    </div>
  );
}

// ─── LineChart ────────────────────────────────────────────────────────────────
function LineChart({ data, current, color, label }: {
  data: number[]; current: number; color: string; label: string;
}) {
  const W = 100, H = 56, P = 2;
  const max = Math.max(...data), min = Math.min(...data);
  const range = max - min || 1;
  const px = (i: number) => P + (i / Math.max(data.length - 1, 1)) * (W - P * 2);
  const py = (v: number) => P + (1 - (v - min) / range) * (H - P * 2);
  const visPath  = data.slice(0, current + 1).map((v, i) => `${px(i)},${py(v)}`).join(" ");
  const fullPath = data.map((v, i) => `${px(i)},${py(v)}`).join(" ");
  const curX = px(current), curY = py(data[current] ?? data[0]);
  return (
    <div>
      <div style={{ fontSize: 11, color: "#71717a", fontFamily: "monospace", marginBottom: 4 }}>
        {label}
        {current >= 0 && data[current] !== undefined && (
          <span style={{ color, marginLeft: 6, fontWeight: 700 }}>{data[current].toFixed(0)}</span>
        )}
      </div>
      <svg width="100%" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none"
        style={{ display: "block", height: 56 }}>
        {data.length > 1 && (
          <polyline points={fullPath} fill="none" stroke={color + "22"} strokeWidth={1.5} strokeDasharray="2,2" />
        )}
        {current >= 1 && (
          <>
            <polyline points={`${px(0)},${H} ${visPath} ${curX},${H}`} fill={color + "18"} stroke="none" />
            <polyline points={visPath} fill="none" stroke={color} strokeWidth={2}
              strokeLinecap="round" strokeLinejoin="round" />
          </>
        )}
        {current >= 0 && (
          <circle cx={curX} cy={curY} r={3} fill={color} />
        )}
      </svg>
      <div style={{ display: "flex", justifyContent: "space-between", marginTop: 2 }}>
        <span style={{ fontSize: 10, color: "#3f3f46", fontFamily: "monospace" }}>{data[0]?.toFixed(0)}</span>
        <span style={{ fontSize: 10, color: "#3f3f46", fontFamily: "monospace" }}>{data[data.length - 1]?.toFixed(0)}</span>
      </div>
    </div>
  );
}

// ─── ClusteringView ───────────────────────────────────────────────────────────
function ClusteringView() {
  const [nodes,        setNodes]        = useState<GNode[]>([]);
  const [shuffledNodes,setShuffledNodes] = useState<GNode[]>([]);
  const [groups,       setGroups]       = useState<SchemaGroup[]>([]);
  const [layers,       setLayers]       = useState<ReturnType<typeof splitLayers> | null>(null);
  const [finalGLs,     setFinalGLs]     = useState<SchemaGroup[]>([]);
  const [history,      setHistory]      = useState<MergeRecord[]>([]);
  const [snapshots,    setSnapshots]    = useState<GroupSnap[][]>([]);
  const [phase,        setPhase]        = useState<Phase>(0);
  // Phase 3 animation
  const [animStep,     setAnimStep]     = useState(0);
  const [isAnimating,  setIsAnimating]  = useState(false);
  const [schemaHist,   setSchemaHist]   = useState<number[]>([]);
  const [costHist,     setCostHist]     = useState<number[]>([]);
  const animRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const logRef  = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetch("/cgc_sample.json").then(r => r.json()).then((d: { nodes: GNode[] }) => {
      setNodes(d.nodes);
      const shuffled = [...d.nodes];
      for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
      }
      setShuffledNodes(shuffled);
      const grps = computeGroups(d.nodes);
      setGroups(grps);
      setLayers(splitLayers(grps));
      const toCluster = grps.filter(g => g.nodes.length >= 2);
      const { result, history: hist, snapshots: snaps } = agglomerateWithHistory(toCluster);
      setFinalGLs(result);
      setHistory(hist);
      setSnapshots(snaps);
      // Pre-compute schema-count and cost histories for charts
      const nG = toCluster.length;
      const initCost = C_SCH * nG + toCluster.reduce((s, g) => s + C_VEC * psi(g.nodes.length), 0);
      const sH = [nG], cH = [initCost];
      let cc = initCost;
      for (let k = 0; k < hist.length; k++) {
        cc -= hist[k].score;
        sH.push(nG - k - 1);
        cH.push(cc);
      }
      setSchemaHist(sH);
      setCostHist(cH);
    });
  }, []);

  // Auto-scroll merge log to latest entry
  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [animStep]);

  // Stop animation when leaving phase 3
  useEffect(() => {
    if (phase !== 3) {
      if (animRef.current) { clearInterval(animRef.current); animRef.current = null; }
      setIsAnimating(false);
    }
    if (phase === 3) { setAnimStep(0); }
  }, [phase]);

  // Cleanup on unmount
  useEffect(() => () => { if (animRef.current) clearInterval(animRef.current); }, []);

  const startAnim = () => {
    if (animRef.current || animStep >= history.length) return;
    setIsAnimating(true);
    animRef.current = setInterval(() => {
      setAnimStep(prev => {
        const next = prev + 1;
        if (next >= history.length) {
          clearInterval(animRef.current!); animRef.current = null;
          setIsAnimating(false);
        }
        return next;
      });
    }, 130);
  };

  const pauseAnim = () => {
    if (animRef.current) { clearInterval(animRef.current); animRef.current = null; }
    setIsAnimating(false);
  };

  const advancePhase = () => {
    if (nodes.length === 0) return;
    setPhase(p => Math.min(4, p + 1) as Phase);
  };
  const backPhase = () => setPhase(p => Math.max(0, p - 1) as Phase);

  // colorOf returns group color for a node (for phase ≥ 1)
  const groupByNodeId = phase >= 1
    ? new Map(groups.flatMap(g => g.nodes.map(n => [n.id, g.color])))
    : new Map<string, string>();
  const colorOf = (n: GNode) => groupByNodeId.get(n.id) ?? "#3f3f46";

  // Phase-3 derived state
  const curSnap = snapshots[animStep] ?? snapshots[0] ?? [];
  const curL1   = curSnap.filter(g => g.size >= 3);
  const curL2   = curSnap.filter(g => g.size === 2);
  const isDone  = animStep >= history.length && history.length > 0;
  const glMap   = new Map(finalGLs.map((g, i) => [g.gid, i + 1]));

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", gap: 14, overflow: "hidden" }}>

      {/* Header */}
      <div style={{ flexShrink: 0, display: "flex", alignItems: "flex-start", gap: 16 }}>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: 12, color: "#8B5CF6", fontFamily: "monospace",
            textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 5 }}>
            CGC — Cost-Aware Graphlet Clustering
          </div>
          <AnimatePresence mode="wait">
            <motion.div key={phase}
              initial={{ opacity: 0, y: 5 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
              transition={{ duration: 0.2 }}>
              <h2 style={{ fontSize: 20, fontWeight: 700, color: "#f4f4f5", margin: "0 0 4px" }}>
                {PHASE_LABEL[phase]}
              </h2>
              {phase === 0 && (
                <p style={{ margin: 0, fontSize: 13, color: "#71717a", lineHeight: 1.55, maxWidth: 780 }}>
                  Minimize{" "}
                  <span style={{ fontFamily: "monospace", color: "#a1a1aa" }}>
                    c(H) = C<sub>sch</sub>·|H| + C<sub>null</sub>·ΣNULL + C<sub>vec</sub>·Σ(κ/|gl|)
                  </span>
                  {" "}— balancing schema proliferation, null overhead, and vectorization throughput.
                  Merge two graphlets when casim = c(H) − c(H′) &gt; 0.
                </p>
              )}
              {phase === 1 && (
                <p style={{ margin: 0, fontSize: 13, color: "#71717a" }}>
                  Each node's attribute set is hashed to a schema ID.
                  Nodes sharing the exact same schema → one provisional graphlet (color = schema).
                </p>
              )}
              {phase === 2 && (
                <p style={{ margin: 0, fontSize: 13, color: "#71717a" }}>
                  Sort graphlets by tuple count into layers L₁ … Lₚ.
                  Larger graphlets are processed first — prevents premature fusion of small groups.
                </p>
              )}
              {phase === 3 && (
                <p style={{ margin: 0, fontSize: 13, color: "#71717a" }}>
                  Repeatedly merge the pair with highest casim while casim &gt; 0.{" "}
                  <span style={{ fontFamily: "monospace", color: "#52525b", fontSize: 11 }}>
                    (demo: C<sub>sch</sub>=100, C<sub>null</sub>=4, C<sub>vec</sub>=100, κ=50)
                  </span>
                </p>
              )}
              {phase === 4 && (
                <p style={{ margin: 0, fontSize: 13, color: "#71717a" }}>
                  <span style={{ color: "#f4f4f5", fontWeight: 700 }}>{finalGLs.length + (layers?.L3.length ?? 0)}</span> graphlets total
                  ({finalGLs.length} merged · {layers?.L3.length ?? 0} singletons) from{" "}
                  <span style={{ color: "#a1a1aa" }}>{nodes.length} nodes</span>.
                  Each graphlet is a dense columnar table — NULLs minimized by design.
                </p>
              )}
            </motion.div>
          </AnimatePresence>
        </div>
        {nodes.length > 0 && (
          <div style={{ flexShrink: 0 }}>
            <PhaseNav phase={phase} onNext={advancePhase} onBack={backPhase} />
          </div>
        )}
      </div>

      {/* Body */}
      <AnimatePresence mode="wait">
        <motion.div key={phase}
          initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -10 }} transition={{ duration: 0.25 }}
          style={{ flex: 1, minHeight: 0, display: "flex", gap: 14 }}>

          {/* Phase 0: raw node grid — compact, wraps to content */}
          {phase === 0 && (
            <div style={{
              alignSelf: "flex-start",
              background: "#0e0e10", borderRadius: 12,
              border: "1px solid #27272a", padding: "14px 16px",
              maxHeight: "calc(100vh - 240px)", overflowY: "auto",
              maxWidth: "100%",
            }}>
              <div style={{ fontSize: 12, color: "#52525b", fontFamily: "monospace", marginBottom: 10 }}>
                {nodes.length} nodes · no schema assigned yet
              </div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 5, alignContent: "flex-start" }}>
                {nodes.slice(0, 300).map((n, i) => {
                  const c = PAL[i % PAL.length];
                  return (
                    <div key={n.id} style={{ padding: "3px 9px", borderRadius: 6,
                      background: c + "18", border: `1px solid ${c}44`,
                      fontSize: 11, color: c, fontFamily: "monospace", whiteSpace: "nowrap" }}>
                      {n.name.length > 16 ? n.name.slice(0, 15) + "…" : n.name}
                    </div>
                  );
                })}
                {nodes.length > 300 && (
                  <div style={{ fontSize: 11, color: "#3f3f46", fontFamily: "monospace",
                    padding: "3px 9px", alignSelf: "center" }}>+{nodes.length - 300} more</div>
                )}
              </div>
            </div>
          )}

          {/* Phase 1: left=colored chips, right=schema inventory */}
          {phase === 1 && (
            <>
              <div style={{ width: 320, flexShrink: 0, background: "#0e0e10", borderRadius: 12,
                border: "1px solid #27272a", padding: "14px 16px", overflow: "hidden",
                display: "flex", flexDirection: "column" }}>
                <div style={{ fontSize: 12, color: "#52525b", fontFamily: "monospace", marginBottom: 10 }}>
                  nodes colored by schema
                </div>
                <div style={{ flex: 1, minHeight: 0, overflowY: "auto",
                  display: "flex", flexWrap: "wrap", gap: 4, alignContent: "flex-start" }}>
                  {shuffledNodes.slice(0, 200).map(n => {
                    const c = colorOf(n);
                    return (
                      <motion.div key={n.id} layout
                        initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.3 }}
                        style={{ padding: "3px 9px", borderRadius: 6,
                          background: c + "20", border: `1px solid ${c}55`,
                          fontSize: 11, color: c, fontFamily: "monospace", whiteSpace: "nowrap" }}>
                        {n.name.length > 14 ? n.name.slice(0, 13) + "…" : n.name}
                      </motion.div>
                    );
                  })}
                  {shuffledNodes.length > 200 && (
                    <div style={{ fontSize: 11, color: "#3f3f46", fontFamily: "monospace",
                      padding: "3px 9px", alignSelf: "center" }}>+{shuffledNodes.length - 200} more</div>
                  )}
                </div>
              </div>
              <div style={{ flex: 1, minHeight: 0, background: "#0e0e10", borderRadius: 12,
                border: "1px solid #27272a", padding: "14px 16px", overflow: "hidden",
                display: "flex", flexDirection: "column", gap: 8 }}>
                <div style={{ fontSize: 13, color: "#a1a1aa", fontFamily: "monospace", flexShrink: 0 }}>
                  {nodes.length} nodes →{" "}
                  <span style={{ color: "#f4f4f5", fontWeight: 700 }}>{groups.length} distinct schemas</span>
                  <span style={{ color: "#52525b" }}> ({groups.filter(g => g.nodes.length === 1).length} singletons)</span>
                </div>
                <div style={{ flex: 1, minHeight: 0, overflowY: "auto", display: "flex", flexDirection: "column", gap: 5 }}>
                  {groups.filter(g => g.nodes.length >= 2).map((g, i) => (
                    <motion.div key={g.gid}
                      initial={{ opacity: 0, x: 14 }} animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: i * 0.015 }}
                      style={{ display: "flex", alignItems: "center", gap: 10,
                        padding: "7px 12px", borderRadius: 8,
                        background: g.color + "14", border: `1px solid ${g.color}40` }}>
                      <span style={{ fontSize: 13, fontWeight: 700, color: g.color,
                        fontFamily: "monospace", minWidth: 36 }}>{g.gid}</span>
                      <span style={{ fontSize: 12, color: "#71717a", fontFamily: "monospace", minWidth: 64 }}>
                        ×{g.nodes.length} nodes
                      </span>
                      <span style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace",
                        overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        [{g.schema.slice(0, 5).join(", ")}{g.schema.length > 5 ? ", …" : ""}]
                      </span>
                    </motion.div>
                  ))}
                </div>
              </div>
            </>
          )}

          {/* Phase 2: layer view */}
          {phase === 2 && layers && (
            <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 10, overflow: "hidden" }}>
              {([
                { key: "L1" as const, label: "L₁ — large  (≥ 3 nodes)",  flex: false },
                { key: "L2" as const, label: "L₂ — medium  (= 2 nodes)", flex: false },
                { key: "L3" as const, label: "L₃ — singletons  (1 node)", flex: true  },
              ] as const).map(({ key, label, flex: isFlex }, li) => {
                const layer = layers[key];
                if (layer.length === 0) return null;
                return (
                  <motion.div key={key}
                    initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: li * 0.1 }}
                    style={{ background: "#0e0e10", borderRadius: 10, border: "1px solid #27272a",
                      padding: "12px 16px", display: "flex", flexDirection: "column",
                      ...(isFlex ? { flex: 1, minHeight: 0 } : { flexShrink: 0 }) }}>
                    <div style={{ fontSize: 13, color: "#71717a", fontFamily: "monospace",
                      marginBottom: 8, flexShrink: 0 }}>
                      {label}
                      <span style={{ color: "#3f3f46", marginLeft: 10 }}>({layer.length} schema{layer.length !== 1 ? "s" : ""})</span>
                    </div>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 5, overflowY: "auto",
                      alignContent: "flex-start" }}>
                      {layer.map(g => (
                        <div key={g.gid} style={{ padding: key === "L3" ? "3px 9px" : "5px 13px", borderRadius: 7,
                          background: g.color + "20", border: `1px solid ${g.color}55`,
                          fontSize: key === "L3" ? 11 : 13, fontFamily: "monospace", color: g.color }}>
                          {key === "L3" ? g.nodes[0].name.length > 14 ? g.nodes[0].name.slice(0, 13) + "…" : g.nodes[0].name
                                       : `${g.gid} ×${g.nodes.length}`}
                        </div>
                      ))}
                    </div>
                  </motion.div>
                );
              })}
            </div>
          )}

          {/* Phase 3: layered animated clustering */}
          {phase === 3 && (
            <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 8 }}>

              {/* Controls */}
              <div style={{ flexShrink: 0, display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ fontSize: 12, color: "#52525b", fontFamily: "monospace" }}>
                  {animStep === 0
                    ? `${history.length} merges queued`
                    : animStep < history.length
                      ? `merge ${animStep} / ${history.length}`
                      : `✓ done — ${finalGLs.length} graphlets`}
                </span>
                <div style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
                  {!isAnimating && animStep < history.length && (
                    <button onClick={startAnim}
                      style={{ padding: "5px 16px", borderRadius: 7, border: "none", cursor: "pointer",
                        background: "#8B5CF6", color: "#fff", fontSize: 12, fontWeight: 700, fontFamily: "monospace" }}>
                      {animStep === 0 ? "▶ RUN" : "▶ RESUME"}
                    </button>
                  )}
                  {isAnimating && (
                    <button onClick={pauseAnim}
                      style={{ padding: "5px 14px", borderRadius: 7, border: "1px solid #27272a",
                        cursor: "pointer", background: "transparent", color: "#a1a1aa", fontSize: 12, fontFamily: "monospace" }}>
                      ⏸ PAUSE
                    </button>
                  )}
                  {animStep > 0 && !isAnimating && (
                    <button onClick={() => setAnimStep(0)}
                      style={{ padding: "5px 10px", borderRadius: 7, border: "1px solid #27272a",
                        cursor: "pointer", background: "transparent", color: "#52525b", fontSize: 12, fontFamily: "monospace" }}>
                      ↺
                    </button>
                  )}
                </div>
              </div>

              {/* Main: layers (left) + charts + log (right) */}
              <div style={{ flex: 1, minHeight: 0, display: "flex", gap: 10 }}>

                {/* Left: L1 / L2 / L3 */}
                <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 8 }}>

                  {/* L1 */}
                  <div style={{ flex: 2, minHeight: 0, background: "#0e0e10", borderRadius: 10,
                    border: "1px solid #27272a", padding: "10px 12px",
                    display: "flex", flexDirection: "column", gap: 6 }}>
                    <div style={{ fontSize: 12, color: "#71717a", fontFamily: "monospace", flexShrink: 0 }}>
                      L₁ — large graphlets
                      <span style={{ color: "#3f3f46", marginLeft: 8 }}>({curL1.length})</span>
                      {isDone && <span style={{ color: "#10B981", marginLeft: 8, fontWeight: 700 }}>→ {finalGLs.length} final</span>}
                    </div>
                    <div style={{ flex: 1, minHeight: 0, overflowY: "auto",
                      display: "flex", flexWrap: "wrap", gap: 5, alignContent: "flex-start" }}>
                      {curL1.map(g => (
                        <motion.div key={`${g.gid}_${g.size}`}
                          initial={{ opacity: 0, scale: 1.15, background: "#8B5CF640" }}
                          animate={{ opacity: 1, scale: 1, background: g.color + "20" }}
                          transition={{ duration: 0.2 }}
                          style={{ padding: "5px 12px", borderRadius: 7, border: `1px solid ${g.color}55`,
                            fontSize: 12, fontFamily: "monospace", color: g.color }}>
                          {isDone && glMap.has(g.gid) ? `GL-${glMap.get(g.gid)}` : g.gid} ×{g.size}
                        </motion.div>
                      ))}
                      {animStep === 0 && curL1.length === 0 && (
                        <div style={{ fontSize: 12, color: "#3f3f46", fontFamily: "monospace", margin: "auto" }}>
                          Press ▶ RUN to start
                        </div>
                      )}
                    </div>
                  </div>

                  {/* L2 */}
                  <div style={{ flexShrink: 0, background: "#0e0e10", borderRadius: 10,
                    border: "1px solid #27272a", padding: "10px 12px" }}>
                    <div style={{ fontSize: 12, color: "#71717a", fontFamily: "monospace", marginBottom: 6 }}>
                      L₂ — pairs
                      <span style={{ color: curL2.length === 0 && isDone ? "#10B981" : "#3f3f46", marginLeft: 8 }}>
                        ({curL2.length}{curL2.length === 0 && isDone ? " — all absorbed" : ""})
                      </span>
                    </div>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 4,
                      maxHeight: 64, overflowY: "auto", alignContent: "flex-start" }}>
                      {curL2.map(g => (
                        <div key={g.gid} style={{ padding: "3px 10px", borderRadius: 6,
                          background: g.color + "18", border: `1px solid ${g.color}44`,
                          fontSize: 11, fontFamily: "monospace", color: g.color }}>
                          {g.gid} ×2
                        </div>
                      ))}
                      {curL2.length === 0 && (
                        <div style={{ fontSize: 11, color: "#3f3f46", fontFamily: "monospace" }}>
                          {animStep === 0 ? "—" : "all absorbed ✓"}
                        </div>
                      )}
                    </div>
                  </div>

                  {/* L3 */}
                  {layers && layers.L3.length > 0 && (
                    <div style={{ flex: 1, minHeight: 0, background: "#0e0e10", borderRadius: 10,
                      border: "1px solid #27272a", padding: "10px 12px",
                      display: "flex", flexDirection: "column" }}>
                      <div style={{ fontSize: 12, color: "#71717a", fontFamily: "monospace", marginBottom: 6, flexShrink: 0 }}>
                        L₃ — singletons
                        <span style={{ color: "#52525b", marginLeft: 8 }}>({layers.L3.length} ·</span>
                        <span style={{ color: "#a1a1aa", fontWeight: 700, marginLeft: 4 }}>unchanged</span>
                        <span style={{ color: "#52525b" }}>)</span>
                      </div>
                      <div style={{ flex: 1, minHeight: 0, overflowY: "auto",
                        display: "flex", flexWrap: "wrap", gap: 3, alignContent: "flex-start" }}>
                        {layers.L3.slice(0, 120).map(g => (
                          <div key={g.gid} style={{ padding: "2px 8px", borderRadius: 5,
                            background: g.color + "14", border: `1px solid ${g.color}38`,
                            fontSize: 11, fontFamily: "monospace", color: g.color + "bb" }}>
                            {g.nodes[0].name.length > 12 ? g.nodes[0].name.slice(0, 11) + "…" : g.nodes[0].name}
                          </div>
                        ))}
                        {layers.L3.length > 120 && (
                          <div style={{ fontSize: 11, color: "#3f3f46", fontFamily: "monospace", alignSelf: "center" }}>
                            +{layers.L3.length - 120}
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </div>

                {/* Right: schema chart + cost chart + merge log */}
                <div style={{ width: 340, flexShrink: 0, display: "flex", flexDirection: "column", gap: 8 }}>

                  <div style={{ background: "#0e0e10", borderRadius: 10, border: "1px solid #27272a",
                    padding: "10px 12px", flexShrink: 0 }}>
                    <LineChart data={schemaHist} current={animStep} color="#8B5CF6" label="graphlets" />
                  </div>

                  <div style={{ background: "#0e0e10", borderRadius: 10, border: "1px solid #27272a",
                    padding: "10px 12px", flexShrink: 0 }}>
                    <LineChart data={costHist} current={animStep} color="#F59E0B" label="cost" />
                  </div>

                  <div style={{ flex: 1, minHeight: 0, background: "#0e0e10", borderRadius: 10,
                    border: "1px solid #27272a", padding: "10px 12px",
                    display: "flex", flexDirection: "column", gap: 4 }}>
                    <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", flexShrink: 0 }}>
                      merge log
                    </div>
                    <div ref={logRef} style={{ flex: 1, minHeight: 0, overflowY: "auto",
                      display: "flex", flexDirection: "column", gap: 3 }}>
                      {history.slice(0, animStep).map((m, i) => (
                        <motion.div key={i}
                          initial={{ opacity: 0, x: -6 }} animate={{ opacity: 1, x: 0 }}
                          transition={{ duration: 0.15 }}
                          style={{ display: "flex", alignItems: "center", gap: 4,
                            padding: "3px 7px", borderRadius: 4,
                            background: "#131316", border: "1px solid #1f1f23",
                            fontSize: 10, fontFamily: "monospace" }}>
                          <span style={{ color: "#8B5CF6", fontWeight: 700 }}>{m.aGid}</span>
                          <span style={{ color: "#3f3f46" }}>×{m.aCount}</span>
                          <span style={{ color: "#3f3f46" }}>⊕</span>
                          <span style={{ color: "#8B5CF6", fontWeight: 700 }}>{m.bGid}</span>
                          <span style={{ color: "#3f3f46" }}>×{m.bCount}</span>
                          <span style={{ color: "#52525b" }}>→</span>
                          <span style={{ color: "#10B981", fontWeight: 700 }}>×{m.aCount + m.bCount}</span>
                          <span style={{ color: "#e84545", fontWeight: 700, marginLeft: "auto" }}>−{m.score.toFixed(0)}</span>
                          {costHist[i + 1] !== undefined && (
                            <span style={{ color: "#52525b", fontSize: 9 }}>({costHist[i + 1].toFixed(0)})</span>
                          )}
                        </motion.div>
                      ))}
                      {animStep === 0 && (
                        <div style={{ fontSize: 11, color: "#3f3f46", fontFamily: "monospace",
                          textAlign: "center", marginTop: 12 }}>—</div>
                      )}
                    </div>
                  </div>
                </div>
              </div>

            </div>
          )}

          {/* Phase 4: graphlet tables */}
          {phase === 4 && (() => {
            const totalGLs   = finalGLs.length + (layers?.L3.length ?? 0);
            const totalNulls = finalGLs.reduce((s, gl) => {
              return s + gl.nodes.reduce((ns, n) => ns + gl.schema.filter(a => !n.schema.includes(a)).length, 0);
            }, 0);
            const avgNulls   = finalGLs.length > 0 ? (totalNulls / finalGLs.length).toFixed(1) : "0";
            return (
              <div style={{ flex: 1, minHeight: 0, display: "grid",
                gridTemplateColumns: "repeat(3, 1fr)", gridTemplateRows: "1fr 1fr",
                gap: 10, overflow: "hidden" }}>
                {finalGLs.slice(0, 5).map((gl, gi) => {
                  const dispSchema  = gl.schema.slice(0, 6);
                  const showRows    = gl.nodes.slice(0, 4);
                  const extra       = gl.nodes.length - showRows.length;
                  const nullCount   = gl.nodes.reduce((s, n) => s + gl.schema.filter(a => !n.schema.includes(a)).length, 0);
                  return (
                    <motion.div key={gl.gid}
                      initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }}
                      transition={{ delay: gi * 0.06 }}
                      style={{ background: gl.color + "0e", border: `1px solid ${gl.color}40`,
                        borderRadius: 10, overflow: "hidden", display: "flex", flexDirection: "column" }}>
                      <div style={{ padding: "6px 12px", borderBottom: `1px solid ${gl.color}25`,
                        background: gl.color + "18", display: "flex", alignItems: "center", gap: 8, flexShrink: 0 }}>
                        <span style={{ fontSize: 13, fontWeight: 700, color: gl.color, fontFamily: "monospace" }}>GL-{gi + 1}</span>
                        <span style={{ fontSize: 11, color: "#71717a" }}>{gl.nodes.length} nodes · {gl.schema.length} attrs</span>
                        <span style={{ fontSize: 11, marginLeft: "auto", fontFamily: "monospace",
                          color: nullCount === 0 ? "#10B981" : "#F59E0B", fontWeight: 600 }}>
                          {nullCount === 0 ? "0 NULLs" : `${nullCount} NULLs`}
                        </span>
                      </div>
                      <div style={{ flex: 1, overflow: "hidden" }}>
                        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: "monospace" }}>
                          <thead>
                            <tr>
                              <th style={{ padding: "3px 8px", textAlign: "left", color: "#71717a",
                                borderBottom: `1px solid ${gl.color}20`, fontWeight: 500, whiteSpace: "nowrap" }}>node</th>
                              {dispSchema.map(a => (
                                <th key={a} style={{ padding: "3px 6px", textAlign: "center",
                                  color: gl.color + "99", borderBottom: `1px solid ${gl.color}20`,
                                  fontWeight: 500, whiteSpace: "nowrap" }}>{a}</th>
                              ))}
                              {gl.schema.length > 6 && (
                                <th style={{ padding: "3px 6px", color: "#3f3f46",
                                  borderBottom: `1px solid ${gl.color}20`, fontWeight: 400, fontSize: 10 }}>
                                  +{gl.schema.length - 6}
                                </th>
                              )}
                            </tr>
                          </thead>
                          <tbody>
                            {showRows.map((n, ni) => (
                              <motion.tr key={n.id}
                                initial={{ opacity: 0 }} animate={{ opacity: 1 }}
                                transition={{ delay: gi * 0.06 + ni * 0.04 + 0.1 }}>
                                <td style={{ padding: "3px 8px", color: "#a1a1aa", whiteSpace: "nowrap",
                                  borderBottom: ni < showRows.length - 1 ? "1px solid #1f1f23" : "none" }}>
                                  {n.name.length > 14 ? n.name.slice(0, 13) + "…" : n.name}
                                </td>
                                {dispSchema.map(a => {
                                  const has = n.schema.includes(a);
                                  return (
                                    <td key={a} style={{ padding: "3px 6px", textAlign: "center",
                                      borderBottom: ni < showRows.length - 1 ? "1px solid #1f1f23" : "none" }}>
                                      {has
                                        ? <span style={{ color: "#10B981", fontWeight: 700, fontSize: 12 }}>✓</span>
                                        : <span style={{ color: "#3f3f46", fontSize: 11 }}>—</span>
                                      }
                                    </td>
                                  );
                                })}
                                {gl.schema.length > 6 && <td />}
                              </motion.tr>
                            ))}
                            {extra > 0 && (
                              <tr><td colSpan={dispSchema.length + 2}
                                style={{ padding: "3px 8px", color: "#3f3f46", fontSize: 10, fontStyle: "italic" }}>
                                +{extra} more nodes…
                              </td></tr>
                            )}
                          </tbody>
                        </table>
                      </div>
                    </motion.div>
                  );
                })}

                {/* Summary tile */}
                <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.32 }}
                  style={{ background: "#0e0e10", border: "1px solid #27272a", borderRadius: 10,
                    display: "flex", flexDirection: "column", justifyContent: "center",
                    padding: "20px 24px", gap: 16 }}>
                  <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace",
                    textTransform: "uppercase", letterSpacing: "0.06em" }}>total result</div>
                  {[
                    { label: "graphlets", val: totalGLs, sub: `${finalGLs.length} merged + ${layers?.L3.length ?? 0} singletons`, color: "#8B5CF6" },
                    { label: "avg NULLs / graphlet", val: avgNulls, sub: "vs. catastrophic flat-table NULLs", color: "#10B981" },
                    { label: "nodes covered", val: nodes.length, sub: "across all graphlets", color: "#3B82F6" },
                  ].map(s => (
                    <div key={s.label}>
                      <div style={{ fontSize: 22, fontWeight: 700, color: s.color, fontFamily: "monospace" }}>{s.val}</div>
                      <div style={{ fontSize: 11, color: "#52525b", marginTop: 2 }}>{s.label}</div>
                      <div style={{ fontSize: 10, color: "#3f3f46", marginTop: 1 }}>{s.sub}</div>
                    </div>
                  ))}
                </motion.div>
              </div>
            );
          })()}

        </motion.div>
      </AnimatePresence>
    </div>
  );
}

// ─── Step 0: Graphlet-level Query Pruning ─────────────────────────────────────
// CGC graphlets predicted from DBPEDIA_NODES (S0_Problem's 20-node sample)
const CGC_GLS = [
  {
    gid: "GL-1", label: "Person", color: "#3B82F6",
    schema: ["abstract","birthDate","occupation","nationality","award","birthPlace"],
    nodes: [
      { name: "Tiger Woods",    attrs: ["abstract","birthDate","occupation","birthPlace"] },
      { name: "Ferdinand I",    attrs: ["abstract","birthDate","nationality","birthPlace"] },
      { name: "Kate Forsyth",   attrs: ["abstract","birthDate","occupation","nationality","award"] },
      { name: "Cato the Elder", attrs: ["abstract","birthDate","occupation","nationality"] },
    ],
  },
  {
    gid: "GL-2", label: "Film", color: "#8B5CF6",
    schema: ["abstract","director","starring","runtime","country","language","imdbId"],
    nodes: [
      { name: "Sholay",           attrs: ["abstract","director","starring","runtime","country","language"] },
      { name: "Henry VIII",       attrs: ["abstract","director","starring","runtime","country","language","imdbId"] },
      { name: "One Night of Love",attrs: ["abstract","director","starring","runtime","country","imdbId"] },
    ],
  },
  {
    gid: "GL-3", label: "City", color: "#F59E0B",
    schema: ["abstract","population","areaTotal","timezone","country","postalCode"],
    nodes: [
      { name: "Lake City FL",   attrs: ["abstract","population","areaTotal","timezone","country","postalCode"] },
      { name: "Priolo Gargallo",attrs: ["abstract","population","areaTotal","timezone","country","postalCode"] },
      { name: "Fisher AR",      attrs: ["abstract","population","areaTotal","timezone","country","postalCode"] },
    ],
  },
  {
    gid: "GL-4", label: "Book", color: "#10B981",
    schema: ["abstract","author","publisher","isbn","genre","language"],
    nodes: [
      { name: "Moon Goddess",    attrs: ["abstract","author","publisher","isbn","genre","language"] },
      { name: "Lycurgus",        attrs: ["abstract"] },
      { name: "English Teacher", attrs: ["abstract","author","publisher","isbn","genre","language"] },
    ],
  },
];

const PRUNE_ATTRS = [
  { key: "birthDate",  label: "birthDate"  },
  { key: "director",   label: "director"   },
  { key: "population", label: "population" },
  { key: "isbn",       label: "isbn"       },
  { key: "abstract",   label: "abstract"   },
];

type ScanState = "idle" | "scanning" | "done";
type GLResult  = "pending" | "scan" | "prune";

function GraphletQueryView() {
  const [attr, setAttr]           = useState("birthDate");
  const [scanState, setScanState] = useState<ScanState>("idle");
  const [results, setResults]     = useState<GLResult[]>(CGC_GLS.map(() => "pending"));

  const reset = () => { setScanState("idle"); setResults(CGC_GLS.map(() => "pending")); };
  const handleAttr = (a: string) => { setAttr(a); reset(); };

  const runQuery = () => {
    if (scanState !== "idle") return;
    setScanState("scanning");
    const r: GLResult[] = CGC_GLS.map(() => "pending");
    setResults([...r]);
    CGC_GLS.forEach((gl, i) => {
      setTimeout(() => {
        r[i] = gl.schema.includes(attr) ? "scan" : "prune";
        setResults([...r]);
        if (i === CGC_GLS.length - 1) setScanState("done");
      }, (i + 1) * 300);
    });
  };

  const scannedNodes = CGC_GLS.filter((_, i) => results[i] === "scan").reduce((s, gl) => s + gl.nodes.length, 0);
  const prunedNodes  = CGC_GLS.filter((_, i) => results[i] === "prune").reduce((s, gl) => s + gl.nodes.length, 0);
  const prunedGLs    = results.filter(r => r === "prune").length;
  const totalNodes   = CGC_GLS.reduce((s, gl) => s + gl.nodes.length, 0);

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", gap: 14, overflow: "hidden" }}>

      {/* Header */}
      <div style={{ flexShrink: 0 }}>
        <div style={{ fontSize: 12, color: "#8B5CF6", fontFamily: "monospace",
          textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 5 }}>
          CGC — Cost-Aware Graphlet Clustering
        </div>
        <h2 style={{ fontSize: 22, fontWeight: 700, color: "#f4f4f5", margin: "0 0 4px" }}>
          NULL-sparse storage with graphlet-level scan pruning
        </h2>
        <p style={{ margin: 0, fontSize: 13, color: "#71717a", lineHeight: 1.5 }}>
          CGC finds the <span style={{ color: "#8B5CF6", fontWeight: 600 }}>sweet spot</span>: neither
          per-node schemas (explosion) nor one flat table (NULL flood).
          Similar-schema nodes are grouped into dense <em style={{ color: "#a1a1aa" }}>graphlets</em>
          — enabling whole-graphlet pruning at query time.
        </p>
      </div>

      {/* Query bar */}
      <div style={{ flexShrink: 0, display: "flex", alignItems: "center", gap: 10,
        padding: "8px 12px 8px 16px", background: "#0e0e10", borderRadius: 9,
        border: "1px solid #27272a", fontFamily: "monospace" }}>
        <span style={{ fontSize: 14, color: "#52525b", whiteSpace: "nowrap" }}>
          SELECT * FROM <span style={{ color: "#f97316", fontWeight: 600 }}>DBPEDIA</span> WHERE
        </span>
        <div style={{ display: "flex", gap: 5 }}>
          {PRUNE_ATTRS.map(a => (
            <button key={a.key} onClick={() => handleAttr(a.key)}
              style={{ padding: "5px 14px", borderRadius: 6, cursor: "pointer",
                fontSize: 13, fontWeight: 600, fontFamily: "monospace",
                border: `1px solid ${attr === a.key ? "#8B5CF6" : "#27272a"}`,
                background: attr === a.key ? "#2e1f5e" : "transparent",
                color: attr === a.key ? "#c4b5fd" : "#71717a", transition: "all 0.15s" }}>
              {a.label}
            </button>
          ))}
        </div>
        <span style={{ fontSize: 14, color: "#52525b", whiteSpace: "nowrap" }}>IS NOT NULL</span>
        <button onClick={scanState === "idle" ? runQuery : reset}
          style={{ marginLeft: "auto", padding: "7px 18px", borderRadius: 6, border: "none", cursor: "pointer",
            background: scanState === "scanning" ? "#1c1c20" : scanState === "done" ? "#27272a" : "#8B5CF6",
            color: scanState === "scanning" ? "#52525b" : scanState === "done" ? "#a1a1aa" : "#fff",
            fontSize: 13, fontWeight: 700, fontFamily: "monospace", transition: "all 0.2s",
            opacity: scanState === "scanning" ? 0.6 : 1 }}>
          {scanState === "scanning" ? "◌ running" : scanState === "done" ? "↺ reset" : "▶ RUN"}
        </button>
      </div>

      {/* 2×2 graphlet grid */}
      <div style={{ flex: 1, minHeight: 0, display: "grid",
        gridTemplateColumns: "1fr 1fr", gridTemplateRows: "1fr 1fr", gap: 10, overflow: "hidden" }}>
        {CGC_GLS.map((gl, i) => {
          const r = results[i];
          const isScan = r === "scan", isPrune = r === "prune";
          const sc = isScan ? "#10B981" : isPrune ? "#e84545" : gl.color;
          const nullCount = gl.nodes.reduce((s, n) => s + gl.schema.filter(a => !n.attrs.includes(a)).length, 0);
          return (
            <motion.div key={gl.gid} animate={{ opacity: isPrune ? 0.28 : 1 }} transition={{ duration: 0.35 }}
              style={{ background: sc + "0e", border: `1px solid ${sc}40`, borderRadius: 10,
                overflow: "hidden", display: "flex", flexDirection: "column" }}>
              {/* Graphlet header */}
              <div style={{ padding: "8px 14px", borderBottom: `1px solid ${sc}25`,
                background: sc + "18", display: "flex", alignItems: "center", gap: 8, flexShrink: 0 }}>
                <span style={{ fontSize: 15, fontWeight: 700, color: sc, fontFamily: "monospace" }}>{gl.gid}</span>
                <AnimatePresence>
                  {r !== "pending" && (
                    <motion.span initial={{ opacity: 0, scale: 0.7 }} animate={{ opacity: 1, scale: 1 }}
                      style={{ fontSize: 12, fontWeight: 700, fontFamily: "monospace",
                        color: isScan ? "#10B981" : "#e84545",
                        padding: "2px 9px", borderRadius: 5,
                        background: isScan ? "#10B98122" : "#e8454522",
                        border: `1px solid ${isScan ? "#10B98155" : "#e8454555"}` }}>
                      {isScan ? "SCAN" : "PRUNED"}
                    </motion.span>
                  )}
                </AnimatePresence>
                <span style={{ fontSize: 12, color: "#52525b", marginLeft: "auto", whiteSpace: "nowrap" }}>
                  {gl.nodes.length} nodes · {gl.schema.length} attrs
                  {nullCount > 0 && <span style={{ color: "#F59E0B", marginLeft: 6 }}>{nullCount} NULLs</span>}
                  {nullCount === 0 && <span style={{ color: "#10B981", marginLeft: 6 }}>0 NULLs</span>}
                </span>
              </div>
              {/* Table */}
              <div style={{ flex: 1, overflow: "hidden" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13, fontFamily: "monospace" }}>
                  <thead>
                    <tr>
                      <th style={{ padding: "5px 12px", textAlign: "left", color: "#71717a",
                        borderBottom: `1px solid ${sc}18`, fontWeight: 500, whiteSpace: "nowrap" }}>node</th>
                      {gl.schema.map(a => (
                        <th key={a} style={{ padding: "5px 8px", textAlign: "center",
                          color: a === attr ? (isScan ? "#10B981" : isPrune ? "#e8454575" : sc + "99") : sc + "55",
                          borderBottom: `1px solid ${sc}18`, fontWeight: a === attr ? 700 : 500,
                          whiteSpace: "nowrap", fontSize: a === attr ? 13 : 12 }}>
                          {a}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {gl.nodes.map((n, ni) => (
                      <tr key={n.name}>
                        <td style={{ padding: "5px 12px", color: "#a1a1aa", whiteSpace: "nowrap",
                          borderBottom: ni < gl.nodes.length - 1 ? "1px solid #1f1f23" : "none", fontSize: 13 }}>
                          {n.name}
                        </td>
                        {gl.schema.map(a => {
                          const has = n.attrs.includes(a);
                          return (
                            <td key={a} style={{ padding: "5px 8px", textAlign: "center",
                              borderBottom: ni < gl.nodes.length - 1 ? "1px solid #1f1f23" : "none" }}>
                              {has
                                ? <span style={{ color: isPrune ? "#e8454540" : "#10B981", fontWeight: 700, fontSize: 14 }}>✓</span>
                                : <span style={{ color: "#3f3f46", fontSize: 12 }}>—</span>
                              }
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </motion.div>
          );
        })}
      </div>

      {/* Stats bar */}
      <AnimatePresence>
        {scanState === "done" && (
          <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}
            style={{ display: "flex", gap: 10, flexShrink: 0 }}>
            {[
              { label: "Schema checks", val: `${CGC_GLS.length}`, sub: "one per graphlet", color: "#8B5CF6" },
              { label: "Nodes scanned", val: `${scannedNodes}`, sub: `of ${totalNodes} · attr present in schema`, color: "#10B981" },
              { label: "Nodes skipped", val: `${prunedNodes}`, sub: `${prunedGLs} graphlet${prunedGLs !== 1 ? "s" : ""} pruned entirely`, color: "#e84545" },
            ].map(s => (
              <div key={s.label} style={{ flex: 1, padding: "12px 16px", borderRadius: 10,
                background: "#0e0e10", border: `1px solid ${s.color}30` }}>
                <div style={{ fontSize: 24, fontWeight: 700, color: s.color, fontFamily: "monospace" }}>{s.val}</div>
                <div style={{ fontSize: 12, color: "#71717a", marginTop: 3 }}>{s.label} · {s.sub}</div>
              </div>
            ))}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ─── Main ─────────────────────────────────────────────────────────────────────
export default function S1_CGC({ step }: Props) {
  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{ maxWidth: 1440, margin: "0 auto", padding: "28px 48px", height: "100%",
        display: "flex", flexDirection: "column", boxSizing: "border-box" }}>
        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }} transition={{ duration: 0.25 }}
            style={{ flex: 1, minHeight: 0 }}>
            {step === 0 && <GraphletQueryView />}
            {step === 1 && <ClusteringView />}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}
