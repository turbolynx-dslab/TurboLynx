"use client";
import React, { useEffect, useState, useMemo, useRef, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { QState, generateCypher } from "@/lib/query-state";

interface Props { step: number; onStep: (n: number) => void; queryState?: QState; }
interface Graphlet { id: number; rows: number; extents: number; cols: number; schema: string[]; schemaTruncated?: boolean; }
interface Catalog { vertexPartitions: { graphlets: Graphlet[]; numGraphlets: number; totalRows: number }[]; edgePartitions: any[]; summary: any; }

function fmt(n: number): string {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(n);
}

// ─── Plan node for SVG ───────────────────────────────────────────────────────
interface PlanNode { op: string; color: string; detail?: string; cost?: string; rows?: string; children?: PlanNode[]; }
interface LNode { op: string; color: string; detail?: string; cost?: string; rows?: string; x: number; y: number; parentIdx: number; }
const CW = 200, CH = 64, HG = 20, VG = 36;

function layoutTree(root: PlanNode): LNode[] {
  const r: LNode[] = []; const wc = new Map<PlanNode, number>();
  const gw = (n: PlanNode): number => { if (wc.has(n)) return wc.get(n)!; const w = !n.children?.length ? CW : Math.max(CW, n.children.map(gw).reduce((a, b) => a + b, 0) + (n.children.length - 1) * HG); wc.set(n, w); return w; };
  const walk = (n: PlanNode, l: number, t: number, pi: number) => { const sw = gw(n); const idx = r.length; r.push({ op: n.op, color: n.color, detail: n.detail, cost: n.cost, rows: n.rows, x: l + sw / 2, y: t, parentIdx: pi }); if (n.children?.length) { const cws = n.children.map(gw); const total = cws.reduce((a, b) => a + b, 0) + (cws.length - 1) * HG; let cx = l + (sw - total) / 2; n.children.forEach((c, i) => { walk(c, cx, t + CH + VG, idx); cx += cws[i] + HG; }); } };
  walk(root, 0, 0, -1); return r;
}

// ─── ZoomPan SVG ─────────────────────────────────────────────────────────────
function ZoomPanSVG({ children, width, height }: { children: React.ReactNode; width: number; height: number }) {
  const [pan, setPan] = useState({ x: 0, y: 0 }); const [zoom, setZoom] = useState(1); const [drag, setDrag] = useState(false); const last = useRef({ x: 0, y: 0 });
  useEffect(() => { setPan({ x: 0, y: 0 }); setZoom(1); }, [width, height]);
  const pad = 40; const vbW = width + pad * 2; const vbH = height + pad * 2;
  return (
    <div style={{ width: "100%", height: "100%", overflow: "hidden", cursor: drag ? "grabbing" : "grab" }}
      onWheel={e => { e.preventDefault(); setZoom(z => Math.max(0.3, Math.min(3, z * (e.deltaY > 0 ? 0.9 : 1.1)))); }}
      onPointerDown={e => { setDrag(true); last.current = { x: e.clientX, y: e.clientY }; (e.target as HTMLElement).setPointerCapture(e.pointerId); }}
      onPointerMove={e => { if (!drag) return; setPan(p => ({ x: p.x + e.clientX - last.current.x, y: p.y + e.clientY - last.current.y })); last.current = { x: e.clientX, y: e.clientY }; }}
      onPointerUp={() => setDrag(false)} onPointerLeave={() => setDrag(false)}>
      <svg style={{ width: "100%", height: "100%", display: "block" }} viewBox={`${-pad} ${-pad} ${vbW} ${vbH}`} preserveAspectRatio="xMidYMid meet">
        <g transform={`translate(${pan.x / zoom}, ${pan.y / zoom}) scale(${zoom})`}>{children}</g>
      </svg>
    </div>
  );
}

// ─── SVG Plan Cards ──────────────────────────────────────────────────────────
function PlanCards({ nodes, onNodeClick }: { nodes: LNode[]; onNodeClick?: (op: string, detail?: string) => void }) {
  return (<>
    {nodes.map((n, i) => { if (n.parentIdx < 0) return null; const p = nodes[n.parentIdx]; const my = (p.y + CH + n.y) / 2; return <path key={`e-${i}`} d={`M${p.x},${p.y + CH} C${p.x},${my} ${n.x},${my} ${n.x},${n.y}`} fill="none" stroke="#d4d4d8" strokeWidth={2} />; })}
    {nodes.map((n, i) => { const lx = n.x - CW / 2; const click = (n.op === "NodeScan" || n.op === "Get") && onNodeClick;
      return (<g key={i} onClick={click ? (e) => { e.stopPropagation(); onNodeClick!(n.op, n.detail); } : undefined} style={{ cursor: click ? "pointer" : "default" }}>
        <rect x={lx + 2} y={n.y + 2} width={CW} height={CH} rx={8} fill="#00000006" />
        <rect x={lx} y={n.y} width={CW} height={CH} rx={8} fill="white" stroke={n.color + "35"} strokeWidth={1.2} />
        <rect x={lx} y={n.y + 6} width={4} height={CH - 12} rx={2} fill={n.color} />
        <text x={lx + 14} y={n.y + 24} fontSize={15} fontWeight={700} fontFamily="monospace" fill={n.color}>{n.op}</text>
        {n.cost && <text x={lx + CW - 8} y={n.y + 24} fontSize={12} fontWeight={600} fontFamily="monospace" fill="#9ca3af" textAnchor="end">{n.cost}</text>}
        {n.detail && <text x={lx + 14} y={n.y + 46} fontSize={13} fontFamily="monospace" fill="#6b7280">{n.detail.length > 22 ? n.detail.slice(0, 20) + "\u2026" : n.detail}</text>}
        {n.rows && <text x={lx + CW - 8} y={n.y + 46} fontSize={12} fontFamily="monospace" fill="#9ca3af" textAnchor="end">{n.rows}</text>}
        {click && <text x={lx + CW - 8} y={n.y + 13} fontSize={9} fill="#b4b4b8" textAnchor="end">click</text>}
      </g>); })}
  </>);
}

// ─── Colors ──────────────────────────────────────────────────────────────────
const OP_COLOR: Record<string, string> = {
  Get: "#3b82f6", GetEdge: "#60a5fa", NAryJoin: "#F59E0B", Select: "#F59E0B", Project: "#71717a", Limit: "#71717a",
  ProduceResults: "#71717a", Top: "#71717a", Projection: "#71717a", Filter: "#F59E0B",
  NodeScan: "#3b82f6", AdjIdxJoin: "#8B5CF6", IdSeek: "#0891B2", IndexScan: "#0891B2",
  HashJoin: "#e84545", NLJoin: "#F59E0B", UnionAll: "#ec4899",
};
const oc = (op: string) => OP_COLOR[op] ?? "#71717a";

type Phase = "bind" | "logical" | "joinorder" | "physical" | "gem";

// ─── Join ordering ───────────────────────────────────────────────────────────
interface JoinOrder { id: string; label: string; }

// ─── Main ────────────────────────────────────────────────────────────────────
export default function S3_Plan({ step, queryState }: Props) {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [phase, setPhase] = useState<Phase>("bind");
  const [activeNode, setActiveNode] = useState<string | null>(null);
  const [boundNodes, setBoundNodes] = useState<Map<string, number[]>>(new Map());
  const [bindAnimating, setBindAnimating] = useState(false);
  const [clickedScan, setClickedScan] = useState<string | null>(null);
  const [pushdownTarget, setPushdownTarget] = useState("");
  const [selectedJO, setSelectedJO] = useState<string | null>(null);
  const [pushdownApplied, setPushdownApplied] = useState(false);
  const [subtreeOrders, setSubtreeOrders] = useState<Map<number, boolean>>(new Map()); // which subtrees have "find orders" expanded
  const [simulateOverlay, setSimulateOverlay] = useState<null | "running" | "done">(null);
  const [simulatedCount, setSimulatedCount] = useState(0);
  const bindContainerRef = useRef<HTMLDivElement>(null);
  const [cellSize, setCellSize] = useState(8);

  useEffect(() => { fetch("/dbpedia_catalog.json").then(r => r.json()).then(setCatalog).catch(() => {}); }, []);

  const vp = catalog?.vertexPartitions[0];
  const allGLs = useMemo(() => vp ? [...vp.graphlets].sort((a, b) => b.rows - a.rows) : [], [vp]);

  // Query structure
  const qNodes = useMemo(() => {
    if (!queryState) return [];
    const vars = new Map<string, string | undefined>();
    for (const m of queryState.matches) { if (m.sourceVar && !vars.has(m.sourceVar)) vars.set(m.sourceVar, undefined); if (m.targetVar && !vars.has(m.targetVar)) vars.set(m.targetVar, undefined); }
    for (const w of queryState.wheres) { if (w.variable && w.property && w.operator === "IS NOT NULL") vars.set(w.variable, w.property); }
    return [...vars.entries()].map(([v, fp]) => ({ variable: v, filterProp: fp }));
  }, [queryState]);
  const qEdges = useMemo(() => queryState ? queryState.matches.filter(m => m.sourceVar && m.edgeType && m.targetVar) : [], [queryState]);
  const cypherText = useMemo(() => queryState ? generateCypher(queryState).replace(/\n\s*/g, " ") : "", [queryState]);
  const allBound = qNodes.length > 0 && qNodes.every(n => boundNodes.has(n.variable));

  // Cell size for binding
  useEffect(() => {
    if (boundNodes.size === 0 || !bindContainerRef.current) return;
    requestAnimationFrame(() => {
      const c = bindContainerRef.current; if (!c) return;
      const cardW = (c.clientWidth - 14) / Math.max(1, qNodes.length) - 32; const cardH = c.clientHeight - 140;
      if (cardW <= 0 || cardH <= 0) return; let best = 4;
      for (let s = 4; s <= 20; s++) { const cols = Math.floor(cardW / (s + 2)); if (cols <= 0) break; if (Math.ceil(allGLs.length / cols) * (s + 2) <= cardH) best = s; else break; }
      setCellSize(best);
    });
  }, [boundNodes.size, qNodes.length, allGLs.length]);

  const bindingsFor = (v: string): Graphlet[] => { if (!vp) return []; const n = qNodes.find(x => x.variable === v); return n?.filterProp ? vp.graphlets.filter(g => g.schema.includes(n.filterProp!)) : vp.graphlets; };

  // Return detail
  const retDetail = useMemo(() => {
    if (!queryState) return "...";
    const items = queryState.returns.filter(r => r.variable).map(r => { let e = r.property ? `${r.variable}.${r.property}` : r.variable; if (r.aggregate) e = `${r.aggregate}(${e})`; return e; });
    const s = items.join(", "); return s.length > 28 ? s.slice(0, 26) + "..." : s;
  }, [queryState]);

  // Base join orderings
  const baseJoinOrders = useMemo((): JoinOrder[] => {
    if (qEdges.length === 0) return [];
    const e = qEdges[0]; const s = e.sourceVar, t = e.targetVar, et = e.edgeType;
    return [
      { id: "jo1", label: `(${s} \u22c8 :${et}) \u22c8 ${t}` },
      { id: "jo2", label: `(${t} \u22c8 :${et}) \u22c8 ${s}` },
      { id: "jo3", label: `(:${et} \u22c8 ${s}) \u22c8 ${t}` },
      { id: "jo4", label: `(:${et} \u22c8 ${t}) \u22c8 ${s}` },
      { id: "jo5", label: `${s} \u22c8 (:${et} \u22c8 ${t})` },
      { id: "jo6", label: `${t} \u22c8 (:${et} \u22c8 ${s})` },
    ];
  }, [qEdges]);

  // Logical plan
  const logicalPlan = useMemo((): PlanNode | null => {
    if (!allBound || qEdges.length === 0) return null;
    const e = qEdges[0]; const srcIds = boundNodes.get(e.sourceVar) ?? []; const tgtIds = boundNodes.get(e.targetVar) ?? [];
    const srcNode = qNodes.find(n => n.variable === e.sourceVar);
    const getSrc: PlanNode = { op: "Get", color: oc("Get"), detail: `${e.sourceVar} (${srcIds.length} GLs)`, rows: fmt(srcIds.reduce((s, id) => s + (vp!.graphlets.find(g => g.id === id)?.rows ?? 0), 0)) };
    const getEdge: PlanNode = { op: "Get", color: oc("GetEdge"), detail: `:${e.edgeType}` };
    const getTgt: PlanNode = { op: "Get", color: oc("Get"), detail: `${e.targetVar} (${tgtIds.length} GLs)`, rows: fmt(tgtIds.reduce((s, id) => s + (vp!.graphlets.find(g => g.id === id)?.rows ?? 0), 0)) };
    const nj: PlanNode = { op: "NAryJoin", color: oc("NAryJoin"), detail: `${e.sourceVar}._id=_sid, _tid=${e.targetVar}._id`, children: [getSrc, getEdge, getTgt] };
    let filtered: PlanNode = nj;
    if (srcNode?.filterProp) filtered = { op: "Select", color: oc("Filter"), detail: `${e.sourceVar}.${srcNode.filterProp} IS NOT NULL`, children: [nj] };
    const proj: PlanNode = { op: "Project", color: oc("Projection"), detail: retDetail, children: [filtered] };
    return queryState?.limit ? { op: "Limit", color: oc("Top"), detail: `${queryState.limit}`, children: [proj] } : proj;
  }, [allBound, qEdges, qNodes, boundNodes, vp, retDetail, queryState]);

  // Sample graphlets for pushdown display
  const pushdownVar = pushdownTarget || qNodes[0]?.variable || "";
  const pushdownGLs = useMemo(() => {
    const ids = boundNodes.get(pushdownVar) ?? [];
    return vp ? vp.graphlets.filter(g => ids.includes(g.id)).sort((a, b) => b.rows - a.rows) : [];
  }, [vp, boundNodes, pushdownVar]);

  // Number of subtrees with "find orders" expanded
  const expandedSubtreeCount = [...subtreeOrders.values()].filter(Boolean).length;

  // Total plan count calculation
  const totalPlans = useMemo(() => {
    if (!pushdownApplied) return baseJoinOrders.length;
    const glCount = pushdownGLs.length;
    // base orders × graphlets, and each subtree with expanded orders adds × baseOrders factor
    let total = baseJoinOrders.length * glCount;
    // Each expanded subtree multiplies by number of orders
    total *= Math.pow(baseJoinOrders.length, expandedSubtreeCount);
    return total;
  }, [pushdownApplied, pushdownGLs.length, baseJoinOrders.length, expandedSubtreeCount]);

  // Actions
  const runBinding = (v: string) => { if (bindAnimating) return; setActiveNode(v); setBindAnimating(true); setTimeout(() => { setBoundNodes(prev => new Map(prev).set(v, bindingsFor(v).map(g => g.id))); setBindAnimating(false); }, 500); };

  const reset = useCallback(() => {
    setPhase("bind"); setActiveNode(null); setBoundNodes(new Map()); setBindAnimating(false);
    setClickedScan(null); setPushdownTarget(""); setSelectedJO(null); setPushdownApplied(false);
    setSubtreeOrders(new Map()); setSimulateOverlay(null); setSimulatedCount(0);
  }, []);
  useEffect(() => { reset(); }, [queryState]);

  if (!catalog || !vp) return <div style={{ height: "100%", display: "flex", alignItems: "center", justifyContent: "center", color: "#9ca3af" }}>Loading...</div>;
  if (qNodes.length === 0) return <div style={{ height: "100%", display: "flex", alignItems: "center", justifyContent: "center" }}><div style={{ textAlign: "center" }}><div style={{ fontSize: 20, fontWeight: 700, color: "#18181b", marginBottom: 8 }}>No query selected</div><div style={{ fontSize: 15, color: "#9ca3af" }}>Go to the Query tab first.</div></div></div>;

  const activePlan = logicalPlan;
  const layout = activePlan ? layoutTree(activePlan) : [];
  const svgW = layout.length > 0 ? Math.max(...layout.map(n => n.x)) + CW / 2 : 400;
  const svgH = layout.length > 0 ? Math.max(...layout.map(n => n.y)) + CH : 200;

  return (
    <div style={{ height: "100%", overflow: "hidden", position: "relative" }}>
      <div style={{ maxWidth: 1440, margin: "0 auto", padding: "14px 40px", height: "100%", display: "flex", flexDirection: "column", boxSizing: "border-box", gap: 10 }}>

        {/* Query bar */}
        <div style={{ flexShrink: 0, padding: "10px 20px", background: "#18181b", borderRadius: 10, fontFamily: "monospace", fontSize: 14, color: "#e5e7eb", display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ flex: 1, lineHeight: 1.6 }}>
            {cypherText.split(/\b(MATCH|OPTIONAL MATCH|WHERE|RETURN|ORDER BY|LIMIT|AND|AS|IS NOT NULL|IS NULL|DESC|COUNT|SUM|AVG|MIN|MAX)\b/g).map((part, j) => {
              if (/^(MATCH|OPTIONAL MATCH|WHERE|RETURN|ORDER BY|LIMIT|AND|AS|IS NOT NULL|IS NULL|DESC|COUNT|SUM|AVG|MIN|MAX)$/.test(part))
                return <span key={j} style={{ color: "#e84545", fontWeight: 700 }}>{part}</span>;
              if (phase === "bind") {
                const pieces: React.ReactNode[] = []; let last = 0; let m; const vr = /\((\w+)\)/g;
                while ((m = vr.exec(part)) !== null) { if (m.index > last) pieces.push(part.slice(last, m.index)); const v = m[1]; const isBound = boundNodes.has(v);
                  pieces.push(<span key={`${j}-${m.index}`} onClick={() => qNodes.some(n => n.variable === v) && runBinding(v)} style={{ cursor: qNodes.some(n => n.variable === v) ? "pointer" : "default", background: isBound ? "#10B981" : "#3f3f46", color: isBound ? "#fff" : "#e5e7eb", padding: "1px 6px", borderRadius: 4, fontWeight: 700 }}>({v})</span>);
                  last = m.index + m[0].length; }
                if (last < part.length) pieces.push(part.slice(last)); return <span key={j}>{pieces}</span>;
              }
              return <span key={j}>{part}</span>;
            })}
          </div>
          {phase !== "bind" && <button onClick={reset} style={{ padding: "4px 12px", borderRadius: 5, border: "1px solid #3f3f46", background: "transparent", color: "#71717a", fontSize: 12, cursor: "pointer", flexShrink: 0 }}>Reset</button>}
        </div>

        {/* Content */}
        <div style={{ flex: 1, minHeight: 0, overflow: "hidden" }}>
          <AnimatePresence mode="wait">

            {/* BIND */}
            {phase === "bind" && (
              <motion.div key="bind" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} style={{ height: "100%", display: "flex", flexDirection: "column", gap: 10 }}>
                <div style={{ fontSize: 15, color: "#52525b" }}>Click each <span style={{ fontFamily: "monospace", fontWeight: 700 }}>(variable)</span> in the query to bind graphlets.</div>
                <div ref={bindContainerRef} style={{ display: "flex", gap: 14, flex: 1, minHeight: 0, overflow: "hidden" }}>
                  {qNodes.map(n => <BindCard key={n.variable} variable={n.variable} filterProp={n.filterProp} total={vp.numGraphlets} bound={boundNodes.get(n.variable)} allGLs={allGLs} animating={bindAnimating && activeNode === n.variable} cellSize={cellSize} />)}
                </div>
                {allBound && <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}><button onClick={() => setPhase("logical")} style={{ padding: "12px 0", borderRadius: 8, border: "none", background: "#e84545", color: "#fff", fontSize: 16, fontWeight: 700, cursor: "pointer", width: "100%" }}>Build Logical Plan &rarr;</button></motion.div>}
              </motion.div>
            )}

            {/* LOGICAL */}
            {phase === "logical" && (
              <motion.div key="logical" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} style={{ height: "100%", display: "flex", flexDirection: "column", gap: 10 }}>
                <div style={{ flex: 1, background: "#fafbfc", borderRadius: 10, border: "1px solid #e5e7eb", overflow: "hidden", position: "relative" }}>
                  {activePlan && <ZoomPanSVG width={svgW} height={svgH}><PlanCards nodes={layout} onNodeClick={(op, detail) => { if (op === "Get") { const v = detail?.match(/^(\w+)/)?.[1]; if (v) setClickedScan(prev => prev === v ? null : v); } }} /></ZoomPanSVG>}
                  <div style={{ position: "absolute", bottom: 8, right: 12, fontSize: 11, color: "#b4b4b8" }}>scroll to zoom, drag to pan</div>
                </div>
                <button onClick={() => { setSelectedJO(baseJoinOrders[0]?.id ?? null); setPushdownTarget(qNodes[0]?.variable ?? ""); setPhase("joinorder"); }}
                  style={{ padding: "11px 0", borderRadius: 8, border: "none", background: "#18181b", color: "#fff", fontSize: 15, fontWeight: 700, cursor: "pointer", width: "100%", flexShrink: 0 }}>
                  Optimize using Orca &rarr;
                </button>
              </motion.div>
            )}

            {/* JOIN ORDER: SVG tree-centric */}
            {phase === "joinorder" && (() => {
              const pVar = qNodes[0]?.variable ?? "p";
              const otherVar = qNodes[1]?.variable ?? "c";
              const edgeType = qEdges[0]?.edgeType ?? "?";
              const glCount = pushdownGLs.length;

              // Build dynamic plan tree
              let planTree: PlanNode;
              if (!pushdownApplied) {
                planTree = logicalPlan!;
              } else {
                const sampleGLs = pushdownGLs.slice(0, 3);
                const subTrees: PlanNode[] = sampleGLs.map((gl, i) => {
                  const isExp = subtreeOrders.get(i) ?? false;
                  if (!isExp) {
                    return { op: "Join", color: oc("NAryJoin"), detail: `GL-${gl.id} (${fmt(gl.rows)})`,
                      children: [
                        { op: "Get", color: oc("Get"), detail: `GL-${gl.id}`, rows: fmt(gl.rows) },
                        { op: "Get", color: oc("GetEdge"), detail: `:${edgeType}` },
                        { op: "Get", color: oc("Get"), detail: otherVar },
                      ] };
                  }
                  // Expanded: Alternatives node showing different orderings
                  return { op: "Alternatives", color: "#e84545", detail: `${baseJoinOrders.length} orders`,
                    children: baseJoinOrders.slice(0, 3).map(jo => ({
                      op: "Join", color: oc("NAryJoin"),
                      detail: jo.label.replace(new RegExp(`\\b${pVar}\\b`), `GL-${gl.id}`).slice(0, 22),
                    })) };
                });
                if (glCount > 3) subTrees.push({ op: "...", color: "#9ca3af", detail: `+${glCount - 3} more` });
                const ua: PlanNode = { op: "UnionAll", color: oc("UnionAll"), detail: `${glCount} sub-trees`, children: subTrees };
                const proj: PlanNode = { op: "Project", color: oc("Projection"), detail: retDetail, children: [ua] };
                planTree = queryState?.limit ? { op: "Limit", color: oc("Top"), detail: `${queryState.limit}`, children: [proj] } : proj;
              }

              const tl = layoutTree(planTree);
              const tw = tl.length > 0 ? Math.max(...tl.map(n => n.x)) + CW / 2 : 400;
              const th = tl.length > 0 ? Math.max(...tl.map(n => n.y)) + CH : 200;

              return (
              <motion.div key="joinorder" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                style={{ height: "100%", display: "flex", flexDirection: "column", gap: 8 }}>

                {/* Header */}
                <div style={{ display: "flex", alignItems: "center", gap: 12, flexShrink: 0 }}>
                  <div style={{ fontSize: 15, fontWeight: 600, color: "#18181b" }}>Join Order Exploration</div>
                  <div style={{ marginLeft: "auto", padding: "5px 14px", background: totalPlans > 6 ? "#fef2f2" : "#f8f9fa",
                    borderRadius: 8, border: `1px solid ${totalPlans > 6 ? "#fecaca" : "#e5e7eb"}` }}>
                    <span style={{ fontSize: 13, color: "#71717a" }}>Plans: </span>
                    <span style={{ fontSize: 24, fontWeight: 800, fontFamily: "monospace", color: totalPlans > 6 ? "#e84545" : "#18181b" }}>
                      {totalPlans.toLocaleString()}
                    </span>
                  </div>
                </div>

                {/* SVG plan tree — the main visualization */}
                <div style={{ flex: 1, background: "#fafbfc", borderRadius: 10, border: "1px solid #e5e7eb", overflow: "hidden", position: "relative" }}>
                  <ZoomPanSVG width={tw} height={th}>
                    <PlanCards nodes={tl} onNodeClick={(op, detail) => {
                      if (op === "Get") { const v = detail?.match(/^(\w+)/)?.[1]; if (v) setClickedScan(prev => prev === v ? null : v); }
                    }} />
                    {/* Dashed borders around sub-trees under UnionAll */}
                    {pushdownApplied && tl.map((n, i) => {
                      if (n.parentIdx >= 0 && tl[n.parentIdx]?.op === "UnionAll" && n.op !== "...") {
                        return <rect key={`d${i}`} x={n.x - CW / 2 - 6} y={n.y - 6} width={CW + 12} height={CH + 12} rx={10}
                          fill="none" stroke="#e84545" strokeWidth={1.5} strokeDasharray="6 4" opacity={0.35} />;
                      }
                      return null;
                    })}
                  </ZoomPanSVG>
                  <div style={{ position: "absolute", bottom: 8, right: 12, fontSize: 11, color: "#b4b4b8" }}>scroll to zoom, drag to pan</div>
                </div>

                {/* Action buttons */}
                <div style={{ display: "flex", gap: 8, flexShrink: 0, flexWrap: "wrap" }}>
                  {!pushdownApplied && (
                    <button onClick={() => { setPushdownApplied(true); setSubtreeOrders(new Map()); }}
                      style={{ padding: "10px 20px", borderRadius: 8, border: "none", background: "#e84545", color: "#fff", fontSize: 14, fontWeight: 700, cursor: "pointer" }}>
                      Apply PushJoinBelowUnionAll
                    </button>
                  )}
                  {pushdownApplied && pushdownGLs.slice(0, 3).map((gl, i) => {
                    const isExp = subtreeOrders.get(i) ?? false;
                    return (
                      <button key={i} onClick={() => setSubtreeOrders(prev => { const n = new Map(prev); n.set(i, !isExp); return n; })}
                        style={{ padding: "8px 14px", borderRadius: 6,
                          border: isExp ? "1px solid #e84545" : "1px dashed #d4d4d8",
                          background: isExp ? "#fef2f2" : "transparent",
                          color: isExp ? "#e84545" : "#71717a", fontSize: 12, fontWeight: 600, cursor: "pointer" }}>
                        {isExp ? "Collapse" : "Find Orders"} GL-{gl.id}
                      </button>
                    );
                  })}
                  {pushdownApplied && (
                    <button onClick={() => {
                      setSimulateOverlay("running");
                      const fullCount = baseJoinOrders.length * glCount * Math.pow(baseJoinOrders.length, qNodes.length);
                      let s = 0; const steps = 40;
                      const tick = () => { s++; setSimulatedCount(Math.round((1 - Math.pow(1 - s / steps, 3)) * fullCount)); if (s < steps) setTimeout(tick, 50); else setSimulateOverlay("done"); };
                      setTimeout(tick, 300);
                    }} style={{ padding: "8px 16px", borderRadius: 8, border: "none", background: "#18181b", color: "#fff", fontSize: 13, fontWeight: 700, cursor: "pointer", marginLeft: "auto" }}>
                      Simulate Full Pushdown
                    </button>
                  )}
                </div>
              </motion.div>
              );
            })()}

            {/* PHYSICAL / GEM — keep existing */}
            {(phase === "physical" || phase === "gem") && (
              <motion.div key="physical" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                style={{ height: "100%", display: "flex", alignItems: "center", justifyContent: "center" }}>
                <div style={{ textAlign: "center", color: "#9ca3af" }}>
                  <div style={{ fontSize: 18, marginBottom: 8 }}>Physical Plan Selection</div>
                  <div style={{ fontSize: 14 }}>Coming soon...</div>
                </div>
              </motion.div>
            )}

          </AnimatePresence>
        </div>
      </div>

      {/* ═══ Simulate Full Pushdown Overlay ═══ */}
      <AnimatePresence>
        {simulateOverlay && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
            style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.6)", zIndex: 100,
              display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 16 }}>

            {simulateOverlay === "running" && (
              <>
                <motion.div animate={{ rotate: 360 }} transition={{ repeat: Infinity, duration: 1, ease: "linear" }}
                  style={{ width: 48, height: 48, border: "4px solid #e8454540", borderTopColor: "#e84545", borderRadius: "50%" }} />
                <div style={{ fontSize: 18, color: "#fff", fontWeight: 600 }}>Enumerating all possible plans...</div>
                <div style={{ fontSize: 48, fontWeight: 800, fontFamily: "monospace", color: "#e84545" }}>
                  {simulatedCount.toLocaleString()}
                </div>
              </>
            )}

            {simulateOverlay === "done" && (
              <>
                <div style={{ fontSize: 16, color: "#9ca3af" }}>Full plan space after UNION ALL pushdown:</div>
                <div style={{ fontSize: 72, fontWeight: 900, fontFamily: "monospace", color: "#e84545", lineHeight: 1 }}>
                  {simulatedCount.toLocaleString()}
                </div>
                <div style={{ fontSize: 15, color: "#9ca3af", fontFamily: "monospace" }}>
                  {baseJoinOrders.length} orderings &times; {pushdownGLs.length} graphlets &times; {baseJoinOrders.length}<sup>{qNodes.length}</sup> sub-orders
                </div>
                <div style={{ display: "flex", gap: 10, marginTop: 12 }}>
                  <button onClick={() => { setSimulateOverlay(null); setPhase("gem"); }}
                    style={{ padding: "12px 28px", borderRadius: 8, border: "none", background: "#10B981", color: "#fff", fontSize: 16, fontWeight: 700, cursor: "pointer" }}>
                    Reduce using GEM
                  </button>
                  <button onClick={() => setSimulateOverlay(null)}
                    style={{ padding: "12px 28px", borderRadius: 8, border: "1px solid #52525b", background: "transparent", color: "#fff", fontSize: 14, cursor: "pointer" }}>
                    Close
                  </button>
                </div>
              </>
            )}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ─── Binding Card ────────────────────────────────────────────────────────────
function BindCard({ variable, filterProp, total, bound, allGLs, animating, cellSize }: {
  variable: string; filterProp?: string; total: number; bound: number[] | undefined; allGLs: Graphlet[]; animating: boolean; cellSize: number;
}) {
  const boundSet = bound ? new Set(bound) : null;
  const boundRows = bound ? allGLs.filter(g => boundSet!.has(g.id)).reduce((s, g) => s + g.rows, 0) : 0;
  const prunedCount = total - (bound?.length ?? 0);
  const pruneRatio = total > 0 ? ((prunedCount / total) * 100).toFixed(1) : "0";
  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 8, overflow: "hidden", padding: "14px 16px", background: "#fafbfc", borderRadius: 10, border: "1px solid #e5e7eb" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ fontSize: 18, fontWeight: 700, fontFamily: "monospace" }}>({variable})</div>
        {bound && <span style={{ padding: "3px 10px", borderRadius: 5, fontSize: 13, fontWeight: 700, background: "#d1fae5", color: "#059669" }}>BOUND</span>}
        {!bound && !animating && <span style={{ fontSize: 13, color: "#9ca3af" }}>Click in query</span>}
        {animating && <span style={{ fontSize: 13, color: "#fbbf24", fontWeight: 600 }}>Resolving...</span>}
      </div>
      <div style={{ fontSize: 14, fontFamily: "monospace", color: "#71717a" }}>
        {filterProp ? <>Filter: <span style={{ color: "#fbbf24", fontWeight: 600 }}>{filterProp}</span> IS NOT NULL</> : <>No filter — all {total} graphlets</>}
      </div>
      {bound ? (
        <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} style={{ display: "flex", gap: 6 }}>
          <div style={{ padding: "8px 12px", background: "#fff", borderRadius: 6, border: "1px solid #d1fae5", flex: 1 }}>
            <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "monospace", color: "#10B981" }}>{bound.length}</div>
            <div style={{ fontSize: 11, color: "#71717a" }}>bound</div>
          </div>
          <div style={{ padding: "8px 12px", background: "#fff", borderRadius: 6, border: "1px solid #e5e7eb", flex: 1 }}>
            <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "monospace", color: "#10B981" }}>{fmt(boundRows)}</div>
            <div style={{ fontSize: 11, color: "#71717a" }}>rows</div>
          </div>
          <div style={{ padding: "8px 12px", background: "#fff", borderRadius: 6, border: `1px solid ${filterProp ? "#fecaca" : "#e5e7eb"}`, flex: 1 }}>
            <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "monospace", color: filterProp ? "#e84545" : "#71717a" }}>{filterProp ? `${pruneRatio}%` : `${total}`}</div>
            <div style={{ fontSize: 11, color: "#71717a" }}>{filterProp ? "pruned" : "total"}</div>
          </div>
        </motion.div>
      ) : <div style={{ height: 54 }} />}
      {bound && (
        <div style={{ flex: 1, overflow: "hidden" }}>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 2 }}>
            {allGLs.map(g => <div key={g.id} style={{ width: cellSize, height: cellSize, borderRadius: Math.max(2, cellSize / 4), background: boundSet!.has(g.id) ? "#10B981" : "#fecaca", opacity: boundSet!.has(g.id) ? 1 : 0.35 }} title={`GL-${g.id}: ${g.rows.toLocaleString()} rows`} />)}
          </div>
        </div>
      )}
    </div>
  );
}
