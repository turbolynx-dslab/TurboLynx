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
      map.set(k, { gid: "", schema: [...n.schema].sort(), nodes: [], color: PAL[ci++ % PAL.length] });
    map.get(k)!.nodes.push(n);
  }
  const sorted = [...map.values()].sort((a, b) => b.nodes.length - a.nodes.length);
  // Renumber: multi-node groups get G-1, G-2...; singletons get s-1, s-2...
  let gi = 1, si = 1;
  for (const g of sorted) {
    g.gid = g.nodes.length >= 2 ? `SCH-${gi++}` : `sch-${si++}`;
  }
  return sorted;
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
          background: d === phase ? "#8B5CF6" : d < phase ? "#8B5CF666" : "#d4d4d8" }} />
      ))}
      {phase > 0 && (
        <button onClick={onBack}
          style={{ padding: "7px 16px", borderRadius: 8, cursor: "pointer",
            border: "1px solid #d4d4d8", background: "transparent",
            color: "#52525b", fontSize: 15, fontWeight: 500, marginLeft: 4 }}>
          ← Back
        </button>
      )}
      {phase < 4 && (
        <button onClick={onNext}
          style={{ padding: "7px 18px", borderRadius: 8, border: "none", cursor: "pointer",
            background: "#8B5CF6",
            color: "#fff", fontSize: 15, fontWeight: 700 }}>
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
      <div style={{ fontSize: 14, color: "#71717a", fontFamily: "monospace", marginBottom: 4 }}>
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
        <span style={{ fontSize: 12, color: "#9ca3af", fontFamily: "monospace" }}>{data[0]?.toFixed(0)}</span>
        <span style={{ fontSize: 12, color: "#9ca3af", fontFamily: "monospace" }}>{data[data.length - 1]?.toFixed(0)}</span>
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
    fetch(`${process.env.NEXT_PUBLIC_BASE_PATH || ""}/cgc_sample.json`).then(r => r.json()).then((d: { nodes: GNode[] }) => {
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
  const colorOf = (n: GNode) => groupByNodeId.get(n.id) ?? "#9ca3af";

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
          <AnimatePresence mode="wait">
            <motion.div key={phase}
              initial={{ opacity: 0, y: 5 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
              transition={{ duration: 0.2 }}>
              <h2 style={{ fontSize: 22, fontWeight: 700, color: "#18181b", margin: "0 0 4px" }}>
                {PHASE_LABEL[phase]}
              </h2>
              {phase === 0 && (
                <p style={{ margin: 0, fontSize: 16, color: "#52525b", lineHeight: 1.55, maxWidth: 780 }}>
                  Raw nodes from DBpedia — each with a different attribute set. No grouping yet.
                  CGC will cluster these into dense graphlets by minimizing a cost function.
                </p>
              )}
              {phase === 1 && (
                <p style={{ margin: 0, fontSize: 16, color: "#52525b" }}>
                  Each node's attribute set is hashed to a schema ID.
                  Nodes sharing the exact same schema form one provisional graphlet.
                </p>
              )}
              {phase === 2 && (
                <p style={{ margin: 0, fontSize: 16, color: "#52525b" }}>
                  Sort graphlets by size into layers. Larger graphlets are processed first
                  to prevent premature fusion of small groups.
                </p>
              )}
              {phase === 3 && (
                <p style={{ margin: 0, fontSize: 16, color: "#52525b" }}>
                  Repeatedly merge the pair with the highest cost-aware similarity score.
                  Stop when no beneficial merge remains.
                </p>
              )}
              {phase === 4 && (
                <p style={{ margin: 0, fontSize: 16, color: "#52525b" }}>
                  <span style={{ color: "#18181b", fontWeight: 700 }}>{finalGLs.length + (layers?.L3.length ?? 0)}</span> graphlets total
                  ({finalGLs.length} merged + {layers?.L3.length ?? 0} singletons) from{" "}
                  <span style={{ fontWeight: 600 }}>{nodes.length} nodes</span>.
                  Each graphlet is a dense columnar table with minimal NULLs.
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
              background: "#f0f1f3", borderRadius: 12,
              border: "1px solid #d4d4d8", padding: "14px 16px",
              maxHeight: "calc(100vh - 240px)", overflowY: "auto",
              maxWidth: "100%",
            }}>
              <div style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace", marginBottom: 10 }}>
                {nodes.length} nodes · no schema assigned yet
              </div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 5, alignContent: "flex-start" }}>
                {nodes.slice(0, 300).map((n) => {
                  return (
                    <div key={n.id} style={{ padding: "3px 9px", borderRadius: 6,
                      background: "#e5e7eb", border: "1px solid #d4d4d8",
                      fontSize: 14, color: "#52525b", fontFamily: "monospace", whiteSpace: "nowrap" }}>
                      {n.name.length > 16 ? n.name.slice(0, 15) + "…" : n.name}
                    </div>
                  );
                })}
                {nodes.length > 300 && (
                  <div style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace",
                    padding: "3px 9px", alignSelf: "center" }}>+{nodes.length - 300} more</div>
                )}
              </div>
            </div>
          )}

          {/* Phase 1: left=colored chips, right=schema inventory */}
          {phase === 1 && (
            <>
              <div style={{ width: 320, flexShrink: 0, background: "#f0f1f3", borderRadius: 12,
                border: "1px solid #d4d4d8", padding: "14px 16px", overflow: "hidden",
                display: "flex", flexDirection: "column" }}>
                <div style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace", marginBottom: 10 }}>
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
                          fontSize: 14, color: c, fontFamily: "monospace", whiteSpace: "nowrap" }}>
                        {n.name.length > 14 ? n.name.slice(0, 13) + "…" : n.name}
                      </motion.div>
                    );
                  })}
                  {shuffledNodes.length > 200 && (
                    <div style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace",
                      padding: "3px 9px", alignSelf: "center" }}>+{shuffledNodes.length - 200} more</div>
                  )}
                </div>
              </div>
              <div style={{ flex: 1, minHeight: 0, background: "#f0f1f3", borderRadius: 12,
                border: "1px solid #d4d4d8", padding: "14px 16px", overflow: "hidden",
                display: "flex", flexDirection: "column", gap: 8 }}>
                <div style={{ fontSize: 15, color: "#52525b", fontFamily: "monospace", flexShrink: 0 }}>
                  {nodes.length} nodes →{" "}
                  <span style={{ color: "#18181b", fontWeight: 700 }}>{groups.length} distinct schemas</span>
                  <span style={{ color: "#6b7280" }}> ({groups.filter(g => g.nodes.length === 1).length} singletons)</span>
                </div>
                <div style={{ flex: 1, minHeight: 0, overflowY: "auto", display: "flex", flexDirection: "column", gap: 5 }}>
                  {groups.filter(g => g.nodes.length >= 2).map((g, i) => (
                    <motion.div key={g.gid}
                      initial={{ opacity: 0, x: 14 }} animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: i * 0.015 }}
                      style={{ display: "flex", alignItems: "center", gap: 10,
                        padding: "7px 12px", borderRadius: 8,
                        background: g.color + "14", border: `1px solid ${g.color}40` }}>
                      <span style={{ fontSize: 15, fontWeight: 700, color: g.color,
                        fontFamily: "monospace", minWidth: 36 }}>{g.gid}</span>
                      <span style={{ fontSize: 14, color: "#71717a", fontFamily: "monospace", minWidth: 64 }}>
                        {g.nodes.length} nodes
                      </span>
                      <span style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace",
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
                    style={{ background: "#f0f1f3", borderRadius: 10, border: "1px solid #d4d4d8",
                      padding: "12px 16px", display: "flex", flexDirection: "column",
                      ...(isFlex ? { flex: 1, minHeight: 0 } : { flexShrink: 0 }) }}>
                    <div style={{ fontSize: 15, color: "#71717a", fontFamily: "monospace",
                      marginBottom: 8, flexShrink: 0 }}>
                      {label}
                      <span style={{ color: "#9ca3af", marginLeft: 10 }}>({layer.length} schema{layer.length !== 1 ? "s" : ""})</span>
                    </div>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 5, overflowY: "auto",
                      alignContent: "flex-start" }}>
                      {layer.map(g => (
                        <div key={g.gid} style={{ padding: key === "L3" ? "3px 9px" : "5px 13px", borderRadius: 7,
                          background: g.color + "20", border: `1px solid ${g.color}55`,
                          fontSize: key === "L3" ? 13 : 15, fontFamily: "monospace", color: g.color }}>
                          {key === "L3" ? g.nodes[0].name.length > 14 ? g.nodes[0].name.slice(0, 13) + "…" : g.nodes[0].name
                                       : `${g.gid} · ${g.nodes.length} nodes`}
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
                <span style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace" }}>
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
                        background: "#8B5CF6", color: "#fff", fontSize: 14, fontWeight: 700, fontFamily: "monospace" }}>
                      {animStep === 0 ? "▶ RUN" : "▶ RESUME"}
                    </button>
                  )}
                  {isAnimating && (
                    <button onClick={pauseAnim}
                      style={{ padding: "5px 14px", borderRadius: 7, border: "1px solid #d4d4d8",
                        cursor: "pointer", background: "transparent", color: "#52525b", fontSize: 14, fontFamily: "monospace" }}>
                      ⏸ PAUSE
                    </button>
                  )}
                  {animStep > 0 && !isAnimating && (
                    <button onClick={() => setAnimStep(0)}
                      style={{ padding: "5px 10px", borderRadius: 7, border: "1px solid #d4d4d8",
                        cursor: "pointer", background: "transparent", color: "#6b7280", fontSize: 14, fontFamily: "monospace" }}>
                      ↺
                    </button>
                  )}
                </div>
              </div>

              {/* Main: layers (left) + charts + log (right) */}
              <div style={{ flex: 1, minHeight: 0, display: "flex", gap: 10 }}>

                {/* Left: visual cluster cards */}
                <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 8 }}>

                  {/* Cluster cards */}
                  <div style={{ flex: 1, minHeight: 0, background: "#f0f1f3", borderRadius: 10,
                    border: "1px solid #d4d4d8", padding: "12px 14px",
                    display: "flex", flexDirection: "column", gap: 6 }}>
                    <div style={{ fontSize: 14, color: "#71717a", fontFamily: "monospace", flexShrink: 0 }}>
                      {curSnap.length} schema groups
                      {isDone && <span style={{ color: "#10B981", marginLeft: 8, fontWeight: 700 }}>→ {finalGLs.length} graphlets</span>}
                    </div>
                    <div style={{ flex: 1, minHeight: 0, overflowY: "auto",
                      display: "flex", flexWrap: "wrap", gap: 8, alignContent: "flex-start" }}>
                      {(() => {
                        const lastMerge = animStep > 0 ? history[animStep - 1] : null;
                        // Build merge lineage: gid → list of original SCH ids absorbed
                        const lineage = new Map<string, string[]>();
                        curSnap.forEach(g => lineage.set(g.gid, [g.gid]));
                        const applied = history.slice(0, animStep);
                        // Replay merges to build up lineage for surviving gids
                        const lin2 = new Map<string, string[]>();
                        // Start from initial snapshot
                        (snapshots[0] ?? []).forEach(g => lin2.set(g.gid, [g.gid]));
                        for (const m of applied) {
                          const aList = lin2.get(m.aGid) ?? [m.aGid];
                          const bList = lin2.get(m.bGid) ?? [m.bGid];
                          lin2.delete(m.bGid);
                          lin2.set(m.resultGid, [...aList, ...bList]);
                        }
                        return curSnap.map(g => {
                          const justMerged = lastMerge?.resultGid === g.gid;
                          const members = lin2.get(g.gid) ?? [g.gid];
                          const labelText = isDone && glMap.has(g.gid)
                            ? `GL-${glMap.get(g.gid)}`
                            : members.length <= 3
                              ? members.join(" + ")
                              : `${members.slice(0, 2).join(" + ")} +${members.length - 2}`;
                          return (
                            <motion.div key={g.gid}
                              layout
                              initial={{ opacity: 0, scale: 1.15 }}
                              animate={{ opacity: 1, scale: 1 }}
                              transition={{ duration: 0.25, layout: { duration: 0.3 } }}
                              style={{
                                padding: "8px 12px", borderRadius: 10,
                                border: `2px solid ${g.color}${justMerged ? "cc" : "55"}`,
                                background: g.color + (justMerged ? "22" : "0e"),
                                display: "flex", flexDirection: "column", gap: 5,
                                minWidth: 72,
                                boxShadow: justMerged ? `0 0 12px ${g.color}40` : "none",
                                transition: "box-shadow 0.3s, border-color 0.3s",
                              }}>
                              <span style={{ fontSize: 12, fontWeight: 700, color: g.color, fontFamily: "monospace",
                                lineHeight: 1.3 }}>
                                {labelText}
                              </span>
                              <div style={{ display: "flex", flexWrap: "wrap", gap: 3 }}>
                                {Array.from({ length: g.size }).map((_, di) => (
                                  <motion.div key={di}
                                    initial={justMerged && di >= (g.size - (lastMerge?.bCount ?? 0)) ? { scale: 0, opacity: 0 } : false}
                                    animate={{ scale: 1, opacity: 1 }}
                                    transition={{ delay: justMerged ? di * 0.02 : 0, duration: 0.2 }}
                                    style={{
                                      width: 10, height: 10, borderRadius: "50%",
                                      background: g.color, opacity: 0.75,
                                    }} />
                                ))}
                              </div>
                              <span style={{ fontSize: 12, color: "#6b7280", fontFamily: "monospace" }}>
                                {g.size} nodes
                              </span>
                            </motion.div>
                          );
                        });
                      })()}
                      {animStep === 0 && curSnap.length === 0 && (
                        <div style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace", margin: "auto" }}>
                          Press ▶ RUN to start
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Singletons (unchanged) */}
                  {layers && layers.L3.length > 0 && (
                    <div style={{ flexShrink: 0, background: "#f0f1f3", borderRadius: 10,
                      border: "1px solid #d4d4d8", padding: "8px 12px" }}>
                      <div style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace" }}>
                        {layers.L3.length} singletons — unchanged
                      </div>
                    </div>
                  )}
                </div>

                {/* Right: schema chart + cost chart + merge log */}
                <div style={{ width: 340, flexShrink: 0, display: "flex", flexDirection: "column", gap: 8 }}>

                  <div style={{ background: "#f0f1f3", borderRadius: 10, border: "1px solid #d4d4d8",
                    padding: "10px 12px", flexShrink: 0 }}>
                    <LineChart data={schemaHist} current={animStep} color="#8B5CF6" label="graphlets" />
                  </div>

                  <div style={{ background: "#f0f1f3", borderRadius: 10, border: "1px solid #d4d4d8",
                    padding: "10px 12px", flexShrink: 0 }}>
                    <LineChart data={costHist} current={animStep} color="#F59E0B" label="cost" />
                  </div>

                  <div style={{ flex: 1, minHeight: 0, background: "#f0f1f3", borderRadius: 10,
                    border: "1px solid #d4d4d8", padding: "10px 12px",
                    display: "flex", flexDirection: "column", gap: 4 }}>
                    <div style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace", flexShrink: 0 }}>
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
                            background: "#f8f9fa", border: "1px solid #e5e7eb",
                            fontSize: 12, fontFamily: "monospace", whiteSpace: "nowrap" }}>
                          <span style={{ color: "#8B5CF6", fontWeight: 700 }}>{m.aGid}</span>
                          <span style={{ color: "#9ca3af" }}>({m.aCount})</span>
                          <span style={{ color: "#9ca3af" }}>+</span>
                          <span style={{ color: "#8B5CF6", fontWeight: 700 }}>{m.bGid}</span>
                          <span style={{ color: "#9ca3af" }}>({m.bCount})</span>
                          <span style={{ color: "#6b7280" }}>→</span>
                          <span style={{ color: "#10B981", fontWeight: 700 }}>{m.aCount + m.bCount}</span>
                          <span style={{ color: "#e84545", fontWeight: 700, marginLeft: "auto" }}>−{m.score.toFixed(0)}</span>
                        </motion.div>
                      ))}
                      {animStep === 0 && (
                        <div style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace",
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
              <div style={{ flex: 1, minHeight: 0, display: "flex", gap: 14 }}>

                {/* Left: scrollable graphlet list */}
                <div style={{ flex: 1, minHeight: 0, overflowY: "auto",
                  display: "flex", flexDirection: "column", gap: 10 }}>
                  {finalGLs.map((gl, gi) => {
                    const dispSchema = gl.schema.slice(0, 6);
                    const showRows   = gl.nodes.slice(0, 4);
                    const extra      = gl.nodes.length - showRows.length;
                    const nullCount  = gl.nodes.reduce((s, n) => s + gl.schema.filter(a => !n.schema.includes(a)).length, 0);
                    return (
                      <motion.div key={gl.gid}
                        initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }}
                        transition={{ delay: gi * 0.06 }}
                        style={{ background: gl.color + "0e", border: `2px solid ${gl.color}55`,
                          borderRadius: 10, overflow: "hidden", flexShrink: 0 }}>
                        <div style={{ padding: "6px 12px", borderBottom: `1px solid ${gl.color}25`,
                          background: gl.color + "22", display: "flex", alignItems: "center", gap: 8 }}>
                          <span style={{ fontSize: 16, fontWeight: 800, color: gl.color, fontFamily: "monospace" }}>GL-{gi + 1}</span>
                          <span style={{ fontSize: 14, color: "#52525b" }}>{gl.nodes.length} nodes · {gl.schema.length} attrs</span>
                          <span style={{ fontSize: 14, marginLeft: "auto", fontFamily: "monospace",
                            color: nullCount === 0 ? "#10B981" : "#F59E0B", fontWeight: 600 }}>
                            {nullCount === 0 ? "0 NULLs" : `${nullCount} NULLs`}
                          </span>
                        </div>
                        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 14, fontFamily: "monospace" }}>
                          <thead>
                            <tr>
                              <th style={{ padding: "3px 8px", textAlign: "left", color: "#71717a",
                                borderBottom: `1px solid ${gl.color}20`, fontWeight: 500, whiteSpace: "nowrap" }}>node</th>
                              {dispSchema.map(a => (
                                <th key={a} style={{ padding: "3px 6px", textAlign: "center",
                                  color: gl.color, borderBottom: `1px solid ${gl.color}20`,
                                  fontWeight: 500, whiteSpace: "nowrap" }}>{a}</th>
                              ))}
                              {gl.schema.length > 6 && (
                                <th style={{ padding: "3px 6px", color: "#9ca3af",
                                  borderBottom: `1px solid ${gl.color}20`, fontWeight: 400, fontSize: 12 }}>
                                  +{gl.schema.length - 6}
                                </th>
                              )}
                            </tr>
                          </thead>
                          <tbody>
                            {showRows.map((n, ni) => (
                              <tr key={n.id}>
                                <td style={{ padding: "3px 8px", color: "#52525b", whiteSpace: "nowrap",
                                  borderBottom: ni < showRows.length - 1 ? "1px solid #e5e7eb" : "none" }}>
                                  {n.name.length > 14 ? n.name.slice(0, 13) + "…" : n.name}
                                </td>
                                {dispSchema.map(a => {
                                  const has = n.schema.includes(a);
                                  return (
                                    <td key={a} style={{ padding: "3px 6px", textAlign: "center",
                                      borderBottom: ni < showRows.length - 1 ? "1px solid #e5e7eb" : "none" }}>
                                      {has
                                        ? <span style={{ color: "#10B981", fontWeight: 700, fontSize: 14 }}>✓</span>
                                        : <span style={{ color: "#9ca3af", fontSize: 14 }}>—</span>
                                      }
                                    </td>
                                  );
                                })}
                                {gl.schema.length > 6 && <td />}
                              </tr>
                            ))}
                            {extra > 0 && (
                              <tr><td colSpan={dispSchema.length + 2}
                                style={{ padding: "3px 8px", color: "#9ca3af", fontSize: 12, fontStyle: "italic" }}>
                                +{extra} more nodes…
                              </td></tr>
                            )}
                          </tbody>
                        </table>
                      </motion.div>
                    );
                  })}
                </div>

                {/* Right: summary stats */}
                <div style={{ width: 280, flexShrink: 0, display: "flex", flexDirection: "column", gap: 12 }}>
                  <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.2 }}
                    style={{ background: "#f0f1f3", border: "1px solid #d4d4d8", borderRadius: 10,
                      padding: "20px 24px", display: "flex", flexDirection: "column", gap: 20 }}>
                    <div style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace",
                      textTransform: "uppercase", letterSpacing: "0.06em" }}>summary</div>
                    {[
                      { label: "graphlets", val: totalGLs, sub: `${finalGLs.length} merged + ${layers?.L3.length ?? 0} singletons`, color: "#8B5CF6" },
                      { label: "avg NULLs / graphlet", val: avgNulls, sub: "vs. catastrophic flat-table NULLs", color: "#10B981" },
                      { label: "nodes covered", val: nodes.length, sub: "across all graphlets", color: "#3B82F6" },
                    ].map(s => (
                      <div key={s.label}>
                        <div style={{ fontSize: 28, fontWeight: 700, color: s.color, fontFamily: "monospace" }}>{s.val}</div>
                        <div style={{ fontSize: 14, color: "#52525b", marginTop: 2 }}>{s.label}</div>
                        <div style={{ fontSize: 12, color: "#9ca3af", marginTop: 1 }}>{s.sub}</div>
                      </div>
                    ))}
                  </motion.div>
                </div>
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
    gid: "GL-1", label: "City", color: "#D97706",
    schema: ["popTotal","area","elevation","utcOffset"],
    nodes: [
      { name: "St. Paul, MO",    attrs: ["popTotal","area","elevation","utcOffset"] },
      { name: "Mt. Olive, NJ",   attrs: ["popTotal","area","elevation","utcOffset"] },
      { name: "Girard, OH",      attrs: ["popTotal","area","elevation"] },
      { name: "Nashville, KS",   attrs: ["popTotal","area","elevation","utcOffset"] },
    ],
  },
  {
    gid: "GL-2", label: "Person", color: "#2563EB",
    schema: ["height","birthDate","caps","clubs"],
    nodes: [
      { name: "Suat Usta",       attrs: ["height","birthDate","caps","clubs"] },
      { name: "C. Holst",        attrs: ["height","birthDate","caps"] },
      { name: "D. Kenton",       attrs: ["height","birthDate","caps","clubs"] },
      { name: "J. Araújo",       attrs: ["height","birthDate","caps","clubs"] },
    ],
  },
  {
    gid: "GL-3", label: "Person", color: "#7C3AED",
    schema: ["birthDate","occupation","birthName","spouse"],
    nodes: [
      { name: "G. Byrne",        attrs: ["birthDate","occupation","birthName","spouse"] },
      { name: "A. Hopkins",      attrs: ["birthDate","occupation","birthName"] },
      { name: "Mel Gibson",      attrs: ["birthDate","occupation","birthName"] },
      { name: "W. Shatner",      attrs: ["birthDate","occupation","birthName"] },
    ],
  },
  {
    gid: "GL-4", label: "Org", color: "#059669",
    schema: ["capacity","clubname","fullname","nickname"],
    nodes: [
      { name: "Konyaspor",       attrs: ["capacity","clubname","fullname","nickname"] },
      { name: "Silkeborg IF",    attrs: ["capacity","clubname","fullname"] },
      { name: "MVV Maastricht",  attrs: ["capacity","clubname","fullname","nickname"] },
    ],
  },
  {
    gid: "GL-5", label: "Book", color: "#DC2626",
    schema: ["isbn","pages","releaseDate","oclc"],
    nodes: [
      { name: "Kiss of Shadows", attrs: ["isbn","pages","releaseDate","oclc"] },
      { name: "Shohola Falls",   attrs: ["isbn","pages","oclc"] },
      { name: "Hunting f.H.G.",  attrs: ["isbn","releaseDate","oclc"] },
    ],
  },
  {
    gid: "GL-6", label: "Film", color: "#0891B2",
    schema: ["runtime","budget","gross","starring"],
    nodes: [
      { name: "The Bounty",      attrs: ["runtime","budget","gross","starring"] },
      { name: "Brylcreem Boys",  attrs: ["runtime","budget"] },
      { name: "Star Trek V",     attrs: ["runtime","budget","gross"] },
    ],
  },
];

const PRUNE_ATTRS = [
  { key: "birthDate",  label: "birthDate"  },
  { key: "capacity",   label: "capacity"   },
  { key: "isbn",       label: "isbn"       },
  { key: "runtime",    label: "runtime"    },
  { key: "elevation",  label: "elevation"  },
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
        <h2 style={{ fontSize: 22, fontWeight: 700, color: "#18181b", margin: "0 0 4px" }}>
          Graphlet-Level Query Pruning
        </h2>
        <p style={{ margin: 0, fontSize: 16, color: "#52525b", lineHeight: 1.5 }}>
          Similar-schema nodes are grouped into dense graphlets. At query time, graphlets whose schema
          lacks the target attribute are pruned entirely — no per-node scan needed.
        </p>
      </div>

      {/* Query bar */}
      <div style={{ flexShrink: 0, display: "flex", alignItems: "center", gap: 12,
        padding: "10px 16px", background: "#f0f1f3", borderRadius: 10,
        border: "1px solid #d4d4d8", fontFamily: "monospace" }}>
        <span style={{ fontSize: 17, color: "#52525b", whiteSpace: "nowrap" }}>
          SELECT * WHERE
        </span>
        <div style={{ display: "flex", gap: 6 }}>
          {PRUNE_ATTRS.map(a => (
            <button key={a.key} onClick={() => handleAttr(a.key)}
              style={{ padding: "6px 16px", borderRadius: 7, cursor: "pointer",
                fontSize: 16, fontWeight: 600, fontFamily: "monospace",
                border: `1px solid ${attr === a.key ? "#8B5CF6" : "#d4d4d8"}`,
                background: attr === a.key ? "#ede9fe" : "transparent",
                color: attr === a.key ? "#7c3aed" : "#71717a", transition: "all 0.15s" }}>
              {a.label}
            </button>
          ))}
        </div>
        <span style={{ fontSize: 17, color: "#52525b", whiteSpace: "nowrap" }}>IS NOT NULL</span>
        <button onClick={scanState === "idle" ? runQuery : reset}
          style={{ marginLeft: "auto", padding: "8px 22px", borderRadius: 7, border: "none", cursor: "pointer",
            background: scanState === "scanning" ? "#f0f1f3" : scanState === "done" ? "#d4d4d8" : "#8B5CF6",
            color: scanState === "scanning" ? "#6b7280" : scanState === "done" ? "#52525b" : "#fff",
            fontSize: 16, fontWeight: 700, fontFamily: "monospace", transition: "all 0.2s",
            opacity: scanState === "scanning" ? 0.6 : 1 }}>
          {scanState === "scanning" ? "◌ running" : scanState === "done" ? "↺ reset" : "▶ RUN"}
        </button>
      </div>

      {/* 2×2 graphlet grid */}
      <div style={{ flex: 1, minHeight: 0, display: "grid",
        gridTemplateColumns: "1fr 1fr 1fr", gridTemplateRows: "1fr 1fr", gap: 10, overflow: "hidden" }}>
        {CGC_GLS.map((gl, i) => {
          const r = results[i];
          const isScan = r === "scan", isPrune = r === "prune";
          const sc = isScan ? "#10B981" : isPrune ? "#e84545" : gl.color;
          const nullCount = gl.nodes.reduce((s, n) => s + gl.schema.filter(a => !n.attrs.includes(a)).length, 0);
          return (
            <motion.div key={gl.gid} animate={{ opacity: isPrune ? 0.28 : 1 }} transition={{ duration: 0.35 }}
              style={{ background: sc + "18", border: `2px solid ${sc}66`, borderRadius: 10,
                overflow: "hidden", display: "flex", flexDirection: "column" }}>
              {/* Graphlet header */}
              <div style={{ padding: "8px 14px", borderBottom: `1px solid ${sc}30`,
                background: sc + "30", display: "flex", alignItems: "center", gap: 8, flexShrink: 0 }}>
                <span style={{ fontSize: 18, fontWeight: 800, color: sc, fontFamily: "monospace" }}>{gl.gid}</span>
                <AnimatePresence>
                  {r !== "pending" && (
                    <motion.span initial={{ opacity: 0, scale: 0.7 }} animate={{ opacity: 1, scale: 1 }}
                      style={{ fontSize: 14, fontWeight: 700, fontFamily: "monospace",
                        color: isScan ? "#10B981" : "#e84545",
                        padding: "2px 9px", borderRadius: 5,
                        background: isScan ? "#10B98122" : "#e8454522",
                        border: `1px solid ${isScan ? "#10B98155" : "#e8454555"}` }}>
                      {isScan ? "SCAN" : "PRUNED"}
                    </motion.span>
                  )}
                </AnimatePresence>
                <span style={{ fontSize: 14, color: "#6b7280", marginLeft: "auto", whiteSpace: "nowrap" }}>
                  {gl.nodes.length} nodes · {gl.schema.length} attrs
                  {nullCount > 0 && <span style={{ color: "#F59E0B", marginLeft: 6 }}>{nullCount} NULLs</span>}
                  {nullCount === 0 && <span style={{ color: "#10B981", marginLeft: 6 }}>0 NULLs</span>}
                </span>
              </div>
              {/* Table */}
              <div style={{ flex: 1, overflowX: "auto", overflowY: "hidden" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 14, fontFamily: "monospace" }}>
                  <thead>
                    <tr>
                      <th style={{ padding: "4px 8px", textAlign: "left", color: "#71717a",
                        borderBottom: `1px solid ${sc}18`, fontWeight: 500, whiteSpace: "nowrap" }}>node</th>
                      {gl.schema.map(a => (
                        <th key={a} style={{ padding: "4px 6px", textAlign: "center",
                          color: a === attr ? (isScan ? "#10B981" : isPrune ? "#e8454575" : sc) : sc + "aa",
                          borderBottom: `1px solid ${sc}18`, fontWeight: a === attr ? 700 : 500,
                          whiteSpace: "nowrap", fontSize: a === attr ? 14 : 13 }}>
                          {a}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {gl.nodes.map((n, ni) => (
                      <tr key={n.name}>
                        <td style={{ padding: "4px 8px", color: "#52525b", whiteSpace: "nowrap",
                          borderBottom: ni < gl.nodes.length - 1 ? "1px solid #e5e7eb" : "none", fontSize: 14 }}>
                          {n.name}
                        </td>
                        {gl.schema.map(a => {
                          const has = n.attrs.includes(a);
                          return (
                            <td key={a} style={{ padding: "4px 6px", textAlign: "center",
                              borderBottom: ni < gl.nodes.length - 1 ? "1px solid #e5e7eb" : "none" }}>
                              {has
                                ? <span style={{ color: isPrune ? "#e8454540" : "#10B981", fontWeight: 700, fontSize: 14 }}>✓</span>
                                : <span style={{ color: "#9ca3af", fontSize: 14 }}>—</span>
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
                background: "#f0f1f3", border: `1px solid ${s.color}30` }}>
                <div style={{ fontSize: 28, fontWeight: 700, color: s.color, fontFamily: "monospace" }}>{s.val}</div>
                <div style={{ fontSize: 14, color: "#71717a", marginTop: 3 }}>{s.label} · {s.sub}</div>
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
