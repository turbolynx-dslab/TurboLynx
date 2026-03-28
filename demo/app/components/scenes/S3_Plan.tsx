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
  const result: LNode[] = [];
  const wc = new Map<PlanNode, number>();
  const getW = (n: PlanNode): number => {
    if (wc.has(n)) return wc.get(n)!;
    const w = !n.children?.length ? CW : Math.max(CW, n.children.map(getW).reduce((a, b) => a + b, 0) + (n.children.length - 1) * HG);
    wc.set(n, w); return w;
  };
  const walk = (n: PlanNode, l: number, t: number, pi: number) => {
    const sw = getW(n); const idx = result.length;
    result.push({ op: n.op, color: n.color, detail: n.detail, cost: n.cost, rows: n.rows, x: l + sw / 2, y: t, parentIdx: pi });
    if (n.children?.length) {
      const cws = n.children.map(getW);
      const total = cws.reduce((a, b) => a + b, 0) + (cws.length - 1) * HG;
      let cx = l + (sw - total) / 2;
      n.children.forEach((c, i) => { walk(c, cx, t + CH + VG, idx); cx += cws[i] + HG; });
    }
  };
  walk(root, 0, 0, -1); return result;
}

// ─── Zoom/Pan SVG ────────────────────────────────────────────────────────────
function ZoomPanSVG({ children, width, height }: { children: React.ReactNode; width: number; height: number }) {
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [zoom, setZoom] = useState(1);
  const [drag, setDrag] = useState(false);
  const last = useRef({ x: 0, y: 0 });
  useEffect(() => { setPan({ x: 0, y: 0 }); setZoom(1); }, [width, height]);
  const pad = 40; const vbW = width + pad * 2; const vbH = height + pad * 2;
  return (
    <div style={{ width: "100%", height: "100%", overflow: "hidden", cursor: drag ? "grabbing" : "grab" }}
      onWheel={e => { e.preventDefault(); setZoom(z => Math.max(0.3, Math.min(3, z * (e.deltaY > 0 ? 0.9 : 1.1)))); }}
      onPointerDown={e => { setDrag(true); last.current = { x: e.clientX, y: e.clientY }; (e.target as HTMLElement).setPointerCapture(e.pointerId); }}
      onPointerMove={e => { if (!drag) return; setPan(p => ({ x: p.x + e.clientX - last.current.x, y: p.y + e.clientY - last.current.y })); last.current = { x: e.clientX, y: e.clientY }; }}
      onPointerUp={() => setDrag(false)} onPointerLeave={() => setDrag(false)}>
      <svg style={{ width: "100%", height: "100%", display: "block" }}
        viewBox={`${-pad} ${-pad} ${vbW} ${vbH}`} preserveAspectRatio="xMidYMid meet">
        <g transform={`translate(${pan.x / zoom}, ${pan.y / zoom}) scale(${zoom})`}>
          {children}
        </g>
      </svg>
    </div>
  );
}

// ─── SVG Plan Cards ──────────────────────────────────────────────────────────
function PlanCards({ nodes, onNodeClick }: { nodes: LNode[]; onNodeClick?: (op: string, detail?: string) => void }) {
  return (
    <>
      {nodes.map((n, i) => {
        if (n.parentIdx < 0) return null;
        const p = nodes[n.parentIdx];
        const my = (p.y + CH + n.y) / 2;
        return <path key={`e-${i}`} d={`M${p.x},${p.y + CH} C${p.x},${my} ${n.x},${my} ${n.x},${n.y}`} fill="none" stroke="#d4d4d8" strokeWidth={2} />;
      })}
      {nodes.map((n, i) => {
        const lx = n.x - CW / 2;
        const click = (n.op === "NodeScan" || n.op === "UnionAll" || n.op === "Get") && onNodeClick;
        return (
          <g key={i} onClick={click ? (e) => { e.stopPropagation(); onNodeClick!(n.op, n.detail); } : undefined}
            style={{ cursor: click ? "pointer" : "default" }}>
            <rect x={lx + 2} y={n.y + 2} width={CW} height={CH} rx={8} fill="#00000006" />
            <rect x={lx} y={n.y} width={CW} height={CH} rx={8} fill="white" stroke={n.color + "35"} strokeWidth={1.2} />
            <rect x={lx} y={n.y + 6} width={4} height={CH - 12} rx={2} fill={n.color} />
            <text x={lx + 14} y={n.y + 24} fontSize={15} fontWeight={700} fontFamily="monospace" fill={n.color}>{n.op}</text>
            {n.cost && <text x={lx + CW - 8} y={n.y + 24} fontSize={12} fontWeight={600} fontFamily="monospace" fill="#9ca3af" textAnchor="end">{n.cost}</text>}
            {n.detail && <text x={lx + 14} y={n.y + 46} fontSize={13} fontFamily="monospace" fill="#6b7280">{n.detail.length > 22 ? n.detail.slice(0, 20) + "\u2026" : n.detail}</text>}
            {n.rows && <text x={lx + CW - 8} y={n.y + 46} fontSize={12} fontFamily="monospace" fill="#9ca3af" textAnchor="end">{n.rows}</text>}
            {click && <text x={lx + CW - 8} y={n.y + 13} fontSize={9} fill="#b4b4b8" textAnchor="end">click</text>}
          </g>
        );
      })}
    </>
  );
}

// ─── Colors ──────────────────────────────────────────────────────────────────
const OP_COLOR: Record<string, string> = {
  // Logical operators
  Get: "#3b82f6", GetEdge: "#60a5fa", NAryJoin: "#F59E0B", Select: "#F59E0B",
  Project: "#71717a", Limit: "#71717a",
  // Physical operators
  ProduceResults: "#71717a", Top: "#71717a", Projection: "#71717a", Filter: "#F59E0B",
  NodeScan: "#3b82f6", AdjIdxJoin: "#8B5CF6", IdSeek: "#0891B2", IndexScan: "#0891B2",
  HashJoin: "#e84545", NLJoin: "#F59E0B", UnionAll: "#ec4899",
};
const oc = (op: string) => OP_COLOR[op] ?? "#71717a";

type Phase = "bind" | "logical" | "joinorder" | "physical" | "gem";

// Join ordering = pure logical expression like "(p ⋈ edge) ⋈ c"
interface JoinOrder {
  id: string;
  label: string;
  desc: string;
  gen: number;      // generation: 0=original, 1=1st pushdown, ...
  parentId?: string; // which JO this was derived from
}

// Physical plan = a join order + physical operator choices
interface PhysPlan {
  id: string;
  joinOrderId: string;
  label: string;
  cost: number;
  tree: PlanNode;
}

// ─── Plan alternative ────────────────────────────────────────────────────────
interface PlanAlt { id: string; label: string; cost: number; tree: PlanNode; }

// ─── Main ────────────────────────────────────────────────────────────────────
export default function S3_Plan({ step, queryState }: Props) {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [phase, setPhase] = useState<Phase>("bind");
  const [activeNode, setActiveNode] = useState<string | null>(null);
  const [boundNodes, setBoundNodes] = useState<Map<string, number[]>>(new Map());
  const [bindAnimating, setBindAnimating] = useState(false);
  const [clickedScan, setClickedScan] = useState<string | null>(null);
  // Join order phase
  const [joinOrders, setJoinOrders] = useState<JoinOrder[]>([]);
  const [checkedJOs, setCheckedJOs] = useState<Set<string>>(new Set());
  const [joSpaceCount, setJoSpaceCount] = useState(0);
  // Physical phase
  const [physPlans, setPhysPlans] = useState<PhysPlan[]>([]);
  const [selectedPhys, setSelectedPhys] = useState<string | null>(null);
  const bindContainerRef = useRef<HTMLDivElement>(null);
  const [cellSize, setCellSize] = useState(8);

  useEffect(() => { fetch("/dbpedia_catalog.json").then(r => r.json()).then(setCatalog).catch(() => {}); }, []);

  const vp = catalog?.vertexPartitions[0];
  const allGLs = useMemo(() => vp ? [...vp.graphlets].sort((a, b) => b.rows - a.rows) : [], [vp]);

  // Query structure
  const qNodes = useMemo(() => {
    if (!queryState) return [];
    const vars = new Map<string, string | undefined>();
    for (const m of queryState.matches) {
      if (m.sourceVar && !vars.has(m.sourceVar)) vars.set(m.sourceVar, undefined);
      if (m.targetVar && !vars.has(m.targetVar)) vars.set(m.targetVar, undefined);
    }
    for (const w of queryState.wheres) {
      if (w.variable && w.property && w.operator === "IS NOT NULL") vars.set(w.variable, w.property);
    }
    return [...vars.entries()].map(([v, fp]) => ({ variable: v, filterProp: fp }));
  }, [queryState]);

  const qEdges = useMemo(() => queryState ? queryState.matches.filter(m => m.sourceVar && m.edgeType && m.targetVar) : [], [queryState]);
  const cypherText = useMemo(() => queryState ? generateCypher(queryState).replace(/\n\s*/g, " ") : "", [queryState]);

  // Cell size
  useEffect(() => {
    if (boundNodes.size === 0 || !bindContainerRef.current) return;
    requestAnimationFrame(() => {
      const c = bindContainerRef.current; if (!c) return;
      const cardW = (c.clientWidth - 14) / Math.max(1, qNodes.length) - 32;
      const cardH = c.clientHeight - 140;
      if (cardW <= 0 || cardH <= 0) return;
      let best = 4;
      for (let s = 4; s <= 20; s++) {
        const cols = Math.floor(cardW / (s + 2));
        if (cols <= 0) break;
        if (Math.ceil(allGLs.length / cols) * (s + 2) <= cardH) best = s; else break;
      }
      setCellSize(best);
    });
  }, [boundNodes.size, qNodes.length, allGLs.length]);

  const bindingsFor = (v: string): Graphlet[] => {
    if (!vp) return [];
    const n = qNodes.find(x => x.variable === v);
    return n?.filterProp ? vp.graphlets.filter(g => g.schema.includes(n.filterProp!)) : vp.graphlets;
  };

  const allBound = qNodes.length > 0 && qNodes.every(n => boundNodes.has(n.variable));
  const naivePlanSpace = qNodes.reduce((p, n) => p * (boundNodes.get(n.variable)?.length ?? 1), 1);

  // ─── Build plan trees ─────────────────────────────────────────────────────
  const retDetail = useMemo(() => {
    if (!queryState) return "...";
    const items = queryState.returns.filter(r => r.variable).map(r => {
      let e = r.property ? `${r.variable}.${r.property}` : r.variable;
      if (r.aggregate) e = `${r.aggregate}(${e})`;
      return e;
    });
    const s = items.join(", ");
    return s.length > 28 ? s.slice(0, 26) + "..." : s;
  }, [queryState]);

  // Logical plan: the "correct" TurboLynx plan structure
  // Logical Plan: pure logical operators (Get, Join, Project) — no physical ops
  const logicalPlan = useMemo((): PlanNode | null => {
    if (!allBound || qEdges.length === 0) return null;
    const e = qEdges[0];
    const srcIds = boundNodes.get(e.sourceVar) ?? [];
    const tgtIds = boundNodes.get(e.targetVar) ?? [];
    const srcNode = qNodes.find(n => n.variable === e.sourceVar);
    const hasFilter = srcNode?.filterProp;

    // CLogicalGet for source node
    const getSrc: PlanNode = { op: "Get", color: oc("Get"), detail: `${e.sourceVar} (${srcIds.length} GLs)`,
      rows: fmt(srcIds.reduce((s, id) => s + (vp!.graphlets.find(g => g.id === id)?.rows ?? 0), 0)) };
    // CLogicalGet for edge
    const getEdge: PlanNode = { op: "Get", color: oc("GetEdge"), detail: `:${e.edgeType}` };
    // CLogicalGet for target node
    const getTgt: PlanNode = { op: "Get", color: oc("Get"), detail: `${e.targetVar} (${tgtIds.length} GLs)`,
      rows: fmt(tgtIds.reduce((s, id) => s + (vp!.graphlets.find(g => g.id === id)?.rows ?? 0), 0)) };

    // CLogicalNAryJoin: join all three with equality predicates
    const naryJoin: PlanNode = { op: "NAryJoin", color: oc("NAryJoin"),
      detail: `${e.sourceVar}._id = _sid, _tid = ${e.targetVar}._id`,
      children: [getSrc, getEdge, getTgt] };

    // Filter (if WHERE clause)
    let filtered: PlanNode = naryJoin;
    if (hasFilter) {
      filtered = { op: "Select", color: oc("Filter"), detail: `${e.sourceVar}.${srcNode!.filterProp} IS NOT NULL`, children: [naryJoin] };
    }

    // Projection
    const proj: PlanNode = { op: "Project", color: oc("Projection"), detail: retDetail, children: [filtered] };
    const top: PlanNode = queryState?.limit ? { op: "Limit", color: oc("Top"), detail: `${queryState.limit}`, children: [proj] } : proj;
    return top;
  }, [allBound, qEdges, qNodes, boundNodes, vp, retDetail, queryState]);

  // ─── Base join orderings (generated when entering joinorder phase) ──────
  const baseJoinOrders = useMemo((): JoinOrder[] => {
    if (qEdges.length === 0) return [];
    const e = qEdges[0];
    const s = e.sourceVar, t = e.targetVar, et = e.edgeType;
    return [
      { id: "jo1", label: `(${s} \u22c8 :${et}) \u22c8 ${t}`, desc: "source-first", gen: 0 },
      { id: "jo2", label: `(${t} \u22c8 :${et}) \u22c8 ${s}`, desc: "target-first (bwd)", gen: 0 },
      { id: "jo3", label: `(:${et} \u22c8 ${s}) \u22c8 ${t}`, desc: "edge-scan, then source", gen: 0 },
      { id: "jo4", label: `(:${et} \u22c8 ${t}) \u22c8 ${s}`, desc: "edge-scan, then target", gen: 0 },
      { id: "jo5", label: `${s} \u22c8 (:${et} \u22c8 ${t})`, desc: "right-deep, target inner", gen: 0 },
      { id: "jo6", label: `${t} \u22c8 (:${et} \u22c8 ${s})`, desc: "right-deep, source inner", gen: 0 },
    ];
  }, [qEdges]);

  // Generate physical plans from join orderings
  const generatePhysPlans = useCallback((orders: JoinOrder[]): PhysPlan[] => {
    if (!vp || qEdges.length === 0) return [];
    const e = qEdges[0];
    const srcIds = boundNodes.get(e.sourceVar) ?? [];
    const tgtIds = boundNodes.get(e.targetVar) ?? [];
    const srcRows = srcIds.reduce((s, id) => s + (vp.graphlets.find(g => g.id === id)?.rows ?? 0), 0);
    const tgtRows = tgtIds.reduce((s, id) => s + (vp.graphlets.find(g => g.id === id)?.rows ?? 0), 0);
    const edgeRows = 1587102;
    const lim = queryState?.limit ?? 20;
    const wrap = (inner: PlanNode): PlanNode => ({
      op: "Top", color: oc("Top"), detail: `LIMIT ${lim}`, children: [
        { op: "Projection", color: oc("Projection"), detail: retDetail, children: [inner] }],
    });

    const plans: PhysPlan[] = [];
    for (const jo of orders) {
      const isSrcFirst = jo.id.includes("1") || jo.id === "jo1";
      const isTgtFirst = jo.id.includes("2") || jo.id === "jo2";

      // AdjIdxJoin variant
      if (isSrcFirst || jo.desc.includes("source-first")) {
        plans.push({ id: `${jo.id}_adj`, joinOrderId: jo.id, label: `${jo.label} — AdjIdx+IdSeek`, cost: Math.round((srcRows * 0.001 + edgeRows * 0.0001) * 10) / 10,
          tree: wrap({ op: "IdSeek", color: oc("IdSeek"), detail: e.targetVar, children: [
            { op: "AdjIdxJoin", color: oc("AdjIdxJoin"), detail: `:${e.edgeType} (fwd)`, children: [
              { op: "NodeScan", color: oc("NodeScan"), detail: `${e.sourceVar} (${srcIds.length} GLs)`, rows: fmt(srcRows) },
              { op: "IndexScan", color: oc("IndexScan"), detail: `${e.edgeType}_fwd` }]},
            { op: "IndexScan", color: oc("IndexScan"), detail: `NODE_id` }]})});
      }
      if (isTgtFirst || jo.desc.includes("target-first")) {
        plans.push({ id: `${jo.id}_adj`, joinOrderId: jo.id, label: `${jo.label} — AdjIdx(bwd)+IdSeek`, cost: Math.round((tgtRows * 0.001 + edgeRows * 0.0001) * 10) / 10,
          tree: wrap({ op: "IdSeek", color: oc("IdSeek"), detail: e.sourceVar, children: [
            { op: "AdjIdxJoin", color: oc("AdjIdxJoin"), detail: `:${e.edgeType} (bwd)`, children: [
              { op: "NodeScan", color: oc("NodeScan"), detail: `${e.targetVar} (${tgtIds.length} GLs)`, rows: fmt(tgtRows) },
              { op: "IndexScan", color: oc("IndexScan"), detail: `${e.edgeType}_bwd` }]},
            { op: "IndexScan", color: oc("IndexScan"), detail: `NODE_id` }]})});
      }
      // HashJoin variant for all orderings
      const hcost = Math.round((srcRows * 0.002 + tgtRows * 0.002 + edgeRows * 0.001) * 10) / 10;
      plans.push({ id: `${jo.id}_hash`, joinOrderId: jo.id, label: `${jo.label} — HashJoin`, cost: hcost,
        tree: wrap({ op: "HashJoin", color: oc("HashJoin"), detail: `:${e.edgeType}`, children: [
          { op: "HashJoin", color: oc("HashJoin"), detail: `_sid = ${e.sourceVar}._id`, children: [
            { op: "NodeScan", color: oc("NodeScan"), detail: `:${e.edgeType}`, rows: fmt(edgeRows) },
            { op: "NodeScan", color: oc("NodeScan"), detail: `${e.sourceVar} (${srcIds.length} GLs)`, rows: fmt(srcRows) }]},
          { op: "NodeScan", color: oc("NodeScan"), detail: `${e.targetVar} (${tgtIds.length} GLs)`, rows: fmt(tgtRows) }]})});
    }
    return plans.sort((a, b) => a.cost - b.cost);
  }, [vp, qEdges, boundNodes, retDetail, queryState]);

  // GEM
  const gemVGs = useMemo(() => !allBound ? [] : qNodes.map(n => ({ v: n.variable, vgs: Math.max(1, Math.ceil((boundNodes.get(n.variable)?.length ?? 0) / 50)) })), [allBound, boundNodes, qNodes]);
  const gemSpace = gemVGs.reduce((p, v) => p * v.vgs, 1) * Math.max(1, physPlans.length);

  // Actions
  const runBinding = (v: string) => {
    if (bindAnimating) return;
    setActiveNode(v); setBindAnimating(true);
    setTimeout(() => { setBoundNodes(prev => new Map(prev).set(v, bindingsFor(v).map(g => g.id))); setBindAnimating(false); }, 500);
  };


  const reset = useCallback(() => {
    setPhase("bind"); setActiveNode(null); setBoundNodes(new Map());
    setBindAnimating(false); setClickedScan(null);
    setJoinOrders([]); setCheckedJOs(new Set()); setJoSpaceCount(0);
    setPhysPlans([]); setSelectedPhys(null);
  }, []);

  useEffect(() => { reset(); }, [queryState]);

  if (!catalog || !vp) return <div style={{ height: "100%", display: "flex", alignItems: "center", justifyContent: "center", color: "#9ca3af" }}>Loading...</div>;
  if (qNodes.length === 0) return (
    <div style={{ height: "100%", display: "flex", alignItems: "center", justifyContent: "center" }}>
      <div style={{ textAlign: "center" }}>
        <div style={{ fontSize: 20, fontWeight: 700, color: "#18181b", marginBottom: 8 }}>No query selected</div>
        <div style={{ fontSize: 15, color: "#9ca3af" }}>Go to the Query tab first.</div>
      </div>
    </div>
  );

  const selPhys = selectedPhys ? physPlans.find(p => p.id === selectedPhys) : null;
  const activePlan = selPhys?.tree ?? logicalPlan;
  const layout = activePlan ? layoutTree(activePlan) : [];
  const svgW = layout.length > 0 ? Math.max(...layout.map(n => n.x)) + CW / 2 : 400;
  const svgH = layout.length > 0 ? Math.max(...layout.map(n => n.y)) + CH : 200;

  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{ maxWidth: 1440, margin: "0 auto", padding: "14px 40px", height: "100%", display: "flex", flexDirection: "column", boxSizing: "border-box", gap: 10 }}>

        {/* Query bar */}
        <div style={{ flexShrink: 0, padding: "10px 20px", background: "#18181b", borderRadius: 10, fontFamily: "monospace", fontSize: 14, color: "#e5e7eb", display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ flex: 1, lineHeight: 1.6 }}>
            {(() => {
              const kw = /\b(MATCH|OPTIONAL MATCH|WHERE|RETURN|ORDER BY|LIMIT|AND|AS|IS NOT NULL|IS NULL|DESC|COUNT|SUM|AVG|MIN|MAX)\b/g;
              return cypherText.split(kw).map((part, j) => {
                if (kw.test(part)) return <span key={j} style={{ color: "#e84545", fontWeight: 700 }}>{part}</span>;
                if (phase === "bind") {
                  const pieces: React.ReactNode[] = []; let last = 0; let m;
                  const vr = /\((\w+)\)/g;
                  while ((m = vr.exec(part)) !== null) {
                    if (m.index > last) pieces.push(part.slice(last, m.index));
                    const v = m[1]; const isBound = boundNodes.has(v);
                    pieces.push(<span key={`${j}-${m.index}`} onClick={() => qNodes.some(n => n.variable === v) && runBinding(v)}
                      style={{ cursor: qNodes.some(n => n.variable === v) ? "pointer" : "default", background: isBound ? "#10B981" : "#3f3f46", color: isBound ? "#fff" : "#e5e7eb", padding: "1px 6px", borderRadius: 4, fontWeight: 700 }}>({v})</span>);
                    last = m.index + m[0].length;
                  }
                  if (last < part.length) pieces.push(part.slice(last));
                  return <span key={j}>{pieces}</span>;
                }
                return <span key={j}>{part}</span>;
              });
            })()}
          </div>
          {phase !== "bind" && <button onClick={reset} style={{ padding: "4px 12px", borderRadius: 5, border: "1px solid #3f3f46", background: "transparent", color: "#71717a", fontSize: 12, cursor: "pointer", flexShrink: 0 }}>Reset</button>}
        </div>

        {/* Content */}
        <div style={{ flex: 1, minHeight: 0, overflow: "hidden" }}>
          <AnimatePresence mode="wait">

            {/* BIND */}
            {phase === "bind" && (
              <motion.div key="bind" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                style={{ height: "100%", display: "flex", flexDirection: "column", gap: 10 }}>
                <div style={{ fontSize: 15, color: "#52525b" }}>Click each <span style={{ fontFamily: "monospace", fontWeight: 700 }}>(variable)</span> in the query to resolve graphlet bindings.</div>
                <div ref={bindContainerRef} style={{ display: "flex", gap: 14, flex: 1, minHeight: 0, overflow: "hidden" }}>
                  {qNodes.map(n => (
                    <BindCard key={n.variable} variable={n.variable} filterProp={n.filterProp}
                      total={vp.numGraphlets} bound={boundNodes.get(n.variable)}
                      allGLs={allGLs} animating={bindAnimating && activeNode === n.variable} cellSize={cellSize} />
                  ))}
                </div>
                {allBound && (
                  <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}>
                    <button onClick={() => setPhase("logical")} style={{ padding: "12px 0", borderRadius: 8, border: "none", background: "#e84545", color: "#fff", fontSize: 16, fontWeight: 700, cursor: "pointer", width: "100%" }}>
                      Build Logical Plan &rarr;
                    </button>
                  </motion.div>
                )}
              </motion.div>
            )}

            {/* LOGICAL: plan tree only */}
            {phase === "logical" && (
              <motion.div key="logical" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                style={{ height: "100%", display: "flex", flexDirection: "column", gap: 10 }}>
                <div style={{ flex: 1, display: "flex", gap: 14, overflow: "hidden" }}>
                  {/* SVG */}
                  <div style={{ flex: 1, background: "#fafbfc", borderRadius: 10, border: "1px solid #e5e7eb", overflow: "hidden", position: "relative" }}>
                    {logicalPlan && (
                      <ZoomPanSVG width={svgW} height={svgH}>
                        <PlanCards nodes={layout} onNodeClick={(op, detail) => {
                          if (op === "Get" || op === "NodeScan") { const v = detail?.match(/^(\w+)/)?.[1]; if (v) setClickedScan(prev => prev === v ? null : v); }
                        }} />
                      </ZoomPanSVG>
                    )}
                    <div style={{ position: "absolute", bottom: 8, right: 12, fontSize: 11, color: "#b4b4b8" }}>scroll to zoom, drag to pan</div>
                  </div>
                  {/* NodeScan detail */}
                  <AnimatePresence>
                    {clickedScan && (() => {
                      const ids = boundNodes.get(clickedScan) ?? [];
                      const gls = vp.graphlets.filter(g => ids.includes(g.id)).sort((a, b) => b.rows - a.rows);
                      return (
                        <motion.div key={clickedScan} initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }}
                          exit={{ opacity: 0, x: 20 }} style={{ width: 240, flexShrink: 0, background: "#fff", position: "relative", border: "1px solid #e5e7eb", borderRadius: 10, padding: "12px 14px", display: "flex", flexDirection: "column", gap: 8, overflowY: "auto" }} className="thin-scrollbar">
                          <button onClick={() => setClickedScan(null)} style={{ position: "absolute", top: 6, right: 6, width: 22, height: 22, borderRadius: 4, border: "none", background: "#f0f1f3", color: "#71717a", cursor: "pointer", fontSize: 13, display: "flex", alignItems: "center", justifyContent: "center" }}>&times;</button>
                          <div style={{ fontSize: 11, color: "#9ca3af", textTransform: "uppercase" }}>Get → UnionAll</div>
                          <div style={{ fontSize: 20, fontWeight: 700, fontFamily: "monospace" }}>({clickedScan}) — {gls.length} GLs</div>
                          {gls.slice(0, 12).map(g => (
                            <div key={g.id} style={{ display: "flex", justifyContent: "space-between", padding: "3px 6px", background: "#fafbfc", borderRadius: 4, fontSize: 12, fontFamily: "monospace" }}>
                              <span style={{ fontWeight: 600 }}>GL-{g.id}</span>
                              <span style={{ color: "#9ca3af" }}>{g.rows.toLocaleString()}</span>
                            </div>
                          ))}
                          {gls.length > 12 && <div style={{ fontSize: 11, color: "#9ca3af" }}>+{gls.length - 12} more</div>}
                        </motion.div>
                      );
                    })()}
                  </AnimatePresence>
                </div>
                <button onClick={() => { setJoinOrders([...baseJoinOrders]); setJoSpaceCount(baseJoinOrders.length); setPhase("joinorder"); }}
                  style={{ padding: "11px 0", borderRadius: 8, border: "none", background: "#18181b", color: "#fff", fontSize: 15, fontWeight: 700, cursor: "pointer", width: "100%", flexShrink: 0 }}>
                  Optimize using Orca
                </button>
              </motion.div>
            )}

            {/* JOIN ORDER: horizontal column provenance view */}
            {phase === "joinorder" && (
              <motion.div key="joinorder" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                style={{ height: "100%", display: "flex", flexDirection: "column", gap: 10 }}>
                {/* Header */}
                <div style={{ display: "flex", alignItems: "center", gap: 12, flexShrink: 0 }}>
                  <div style={{ fontSize: 14, color: "#71717a" }}>Step 1: Join Order Exploration</div>
                  <div style={{ marginLeft: "auto", padding: "6px 14px",
                    background: joSpaceCount > 50 ? "#fef2f2" : "#f8f9fa",
                    borderRadius: 8, border: `1px solid ${joSpaceCount > 50 ? "#fecaca" : "#e5e7eb"}` }}>
                    <span style={{ fontSize: 12, color: "#71717a" }}>Total orderings: </span>
                    <span style={{ fontSize: 22, fontWeight: 800, fontFamily: "monospace",
                      color: joSpaceCount > 50 ? "#e84545" : "#18181b" }}>
                      {(joSpaceCount || joinOrders.length).toLocaleString()}
                    </span>
                  </div>
                </div>

                {/* Column-based provenance view */}
                <div style={{ flex: 1, overflowX: "auto", overflowY: "hidden" }} className="thin-scrollbar">
                  <div style={{ display: "flex", gap: 0, height: "100%", minWidth: "fit-content" }}>
                    {(() => {
                      // Group JOs by generation
                      const maxGen = Math.max(0, ...joinOrders.map(j => j.gen));
                      const columns: { gen: number; groups: { parentId: string | undefined; items: JoinOrder[] }[] }[] = [];

                      for (let g = 0; g <= maxGen; g++) {
                        const genJOs = joinOrders.filter(j => j.gen === g);
                        // Group by parentId
                        const groupMap = new Map<string, JoinOrder[]>();
                        for (const jo of genJOs) {
                          const key = jo.parentId ?? "__root__";
                          if (!groupMap.has(key)) groupMap.set(key, []);
                          groupMap.get(key)!.push(jo);
                        }
                        columns.push({ gen: g, groups: [...groupMap.entries()].map(([pid, items]) => ({ parentId: pid === "__root__" ? undefined : pid, items })) });
                      }

                      return columns.map((col, ci) => (
                        <div key={ci} style={{ display: "flex", flexDirection: "row", alignItems: "stretch" }}>
                          {/* Connection lines */}
                          {ci > 0 && (
                            <div style={{ width: 24, flexShrink: 0, position: "relative" }}>
                              {col.groups.map((grp, gi) => (
                                <div key={gi} style={{
                                  position: "absolute", left: 0, right: 0,
                                  top: `${(gi / Math.max(1, col.groups.length)) * 100}%`,
                                  height: 2, background: "#d4d4d8",
                                }} />
                              ))}
                            </div>
                          )}
                          {/* Column */}
                          <div style={{
                            width: ci === 0 ? 260 : 220, flexShrink: 0,
                            display: "flex", flexDirection: "column", gap: 6,
                            overflowY: "auto", padding: "0 4px",
                          }} className="thin-scrollbar">
                            {/* Column header */}
                            <div style={{ fontSize: 11, fontWeight: 700, color: "#9ca3af", textTransform: "uppercase",
                              letterSpacing: "0.06em", padding: "4px 0", flexShrink: 0, position: "sticky", top: 0, background: "#fff", zIndex: 1 }}>
                              {ci === 0 ? "Base Orderings" : `Pushdown #${ci}`}
                            </div>
                            {col.groups.map((grp, gi) => {
                              const showMax = 4;
                              const hidden = grp.items.length - showMax;
                              return (
                                <div key={gi} style={{
                                  background: ci === 0 ? "#f8f9fa" : "#fef2f208",
                                  borderRadius: 8, border: `1px solid ${ci === 0 ? "#e5e7eb" : "#fecaca40"}`,
                                  padding: "6px", marginBottom: 2,
                                }}>
                                  {grp.items.slice(0, showMax).map(jo => {
                                    const isChecked = checkedJOs.has(jo.id);
                                    return (
                                      <div key={jo.id} style={{
                                        display: "flex", alignItems: "center", gap: 6,
                                        padding: "5px 8px", marginBottom: 2, borderRadius: 5,
                                        background: isChecked ? "#fef2f2" : "transparent",
                                        border: isChecked ? "1px solid #fecaca" : "1px solid transparent",
                                      }}>
                                        <input type="checkbox" checked={isChecked}
                                          onChange={() => setCheckedJOs(prev => {
                                            const n = new Set(prev);
                                            if (n.has(jo.id)) n.delete(jo.id); else n.add(jo.id);
                                            return n;
                                          })}
                                          style={{ width: 14, height: 14, cursor: "pointer", flexShrink: 0 }} />
                                        <div style={{ flex: 1, minWidth: 0 }}>
                                          <div style={{ fontSize: 12, fontFamily: "monospace", fontWeight: 600, color: "#18181b",
                                            overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                            {jo.label}
                                          </div>
                                        </div>
                                      </div>
                                    );
                                  })}
                                  {hidden > 0 && (
                                    <div style={{ fontSize: 11, color: "#e84545", fontFamily: "monospace", fontWeight: 700,
                                      padding: "3px 8px", textAlign: "center" }}>
                                      +{hidden.toLocaleString()} more
                                    </div>
                                  )}
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      ));
                    })()}
                  </div>
                </div>

                {/* Buttons */}
                <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
                  {checkedJOs.size > 0 && (
                    <button onClick={() => {
                      const nextGen = Math.max(0, ...joinOrders.map(j => j.gen)) + 1;
                      const maxGLCount = Math.max(...qNodes.map(n => boundNodes.get(n.variable)?.length ?? 1));
                      const sampleGLs = allGLs.slice(0, 5);
                      const newJOs: JoinOrder[] = [];
                      for (const joId of checkedJOs) {
                        const parent = joinOrders.find(j => j.id === joId);
                        if (!parent) continue;
                        for (const gl of sampleGLs) {
                          newJOs.push({
                            id: `${joId}_GL${gl.id}`,
                            label: `${parent.label.replace(/ \[GL.*$/, "")} [GL-${gl.id}]`,
                            desc: `graphlet ${gl.id} (${fmt(gl.rows)})`,
                            gen: nextGen, parentId: joId,
                          });
                        }
                      }
                      setJoinOrders(prev => [...prev, ...newJOs]);
                      setJoSpaceCount(prev => (prev || joinOrders.length) * maxGLCount);
                      setCheckedJOs(new Set());
                    }} style={{ flex: 1, padding: "11px 0", borderRadius: 8, border: "none",
                      background: "#e84545", color: "#fff", fontSize: 14, fontWeight: 700, cursor: "pointer" }}>
                      Apply UNION ALL Pushdown ({checkedJOs.size} selected)
                    </button>
                  )}
                  <button onClick={() => {
                    setPhysPlans(generatePhysPlans(joinOrders));
                    setPhase("physical");
                  }} style={{ flex: 1, padding: "11px 0", borderRadius: 8, border: "none",
                    background: "#18181b", color: "#fff", fontSize: 14, fontWeight: 700, cursor: "pointer" }}>
                    Generate Physical Plans &rarr;
                  </button>
                </div>
              </motion.div>
            )}

            {/* PHYSICAL / GEM: left = physical plan list, right = selected plan tree */}
            {(phase === "physical" || phase === "gem") && (
              <motion.div key="physical" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                style={{ height: "100%", display: "flex", gap: 14, overflow: "hidden" }}>

                {/* Left: plan list */}
                <div style={{ flex: "0 0 400px", display: "flex", flexDirection: "column", gap: 8, overflow: "hidden" }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 12, flexShrink: 0 }}>
                    <div style={{ fontSize: 14, color: "#71717a" }}>Step 2: Physical Plan Selection</div>
                    <div style={{ marginLeft: "auto", padding: "6px 14px", background: phase === "gem" ? "#f0fdf4" : "#f8f9fa",
                      borderRadius: 8, border: `1px solid ${phase === "gem" ? "#bbf7d0" : "#e5e7eb"}` }}>
                      <span style={{ fontSize: 12, color: "#71717a" }}>Physical plans: </span>
                      <span style={{ fontSize: 22, fontWeight: 800, fontFamily: "monospace", color: phase === "gem" ? "#10B981" : "#18181b" }}>
                        {phase === "gem" ? gemSpace.toLocaleString() : physPlans.length}
                      </span>
                    </div>
                  </div>

                  {phase === "gem" && physPlans.length > 0 && (
                    <div style={{ padding: "8px 14px", background: "#f0fdf4", borderRadius: 8, border: "1px solid #bbf7d0", fontSize: 14, color: "#10B981", fontWeight: 700, textAlign: "center", flexShrink: 0 }}>
                      GEM reduced plan space by {physPlans.length > 0 ? ((1 - gemSpace / (joSpaceCount * physPlans.length / joinOrders.length)) * 100).toFixed(0) : 0}%
                    </div>
                  )}

                  {/* Physical plan list */}
                  <div style={{ flex: 1, overflowY: "auto" }} className="thin-scrollbar">
                    {physPlans.map((p, i) => {
                      const isSel = selectedPhys === p.id;
                      const isBest = i === 0 && phase === "gem";
                      return (
                        <div key={p.id} onClick={() => setSelectedPhys(isSel ? null : p.id)}
                          style={{
                            padding: "10px 14px", marginBottom: 4, borderRadius: 8, cursor: "pointer",
                            border: isSel ? "2px solid #18181b" : isBest ? "2px solid #10B981" : "1px solid #e5e7eb",
                            background: isSel ? "#f0f1f3" : isBest ? "#f0fdf4" : "#fff",
                          }}>
                          <div style={{ fontSize: 13, fontFamily: "monospace", fontWeight: 700, color: "#374151",
                            overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.label}</div>
                          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 3 }}>
                            <span style={{ fontSize: 12, color: "#9ca3af" }}>estimated cost</span>
                            <span style={{ fontSize: 16, fontWeight: 700, fontFamily: "monospace", color: isBest ? "#10B981" : "#18181b" }}>{p.cost}</span>
                          </div>
                          {isBest && <span style={{ fontSize: 11, fontWeight: 700, color: "#10B981" }}>OPTIMAL</span>}
                        </div>
                      );
                    })}
                  </div>

                  {/* GEM button */}
                  {phase === "physical" && (
                    <button onClick={() => setPhase("gem")} style={{ padding: "11px 0", borderRadius: 8, border: "none", background: "#10B981", color: "#fff", fontSize: 14, fontWeight: 700, cursor: "pointer", width: "100%", flexShrink: 0 }}>
                      Apply GEM (Graphlet Early Merge)
                    </button>
                  )}

                  {/* Legend */}
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 5, fontSize: 11, color: "#71717a", flexShrink: 0 }}>
                    {["NodeScan", "AdjIdxJoin", "IdSeek", "IndexScan", "HashJoin"].map(op => (
                      <div key={op} style={{ display: "flex", alignItems: "center", gap: 3 }}>
                        <div style={{ width: 8, height: 8, borderRadius: 2, background: oc(op) }} />{op}
                      </div>
                    ))}
                  </div>
                </div>

                {/* Right: selected plan tree */}
                <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
                  {selPhys ? (
                    <>
                      <div style={{ fontSize: 13, fontFamily: "monospace", fontWeight: 700, color: "#18181b", marginBottom: 6, flexShrink: 0 }}>
                        {selPhys.label} — cost {selPhys.cost}
                      </div>
                      <div style={{ flex: 1, background: "#fafbfc", borderRadius: 10, border: "1px solid #e5e7eb", overflow: "hidden", position: "relative" }}>
                        <ZoomPanSVG width={svgW} height={svgH}>
                          <PlanCards nodes={layout} />
                        </ZoomPanSVG>
                        <div style={{ position: "absolute", bottom: 8, right: 12, fontSize: 11, color: "#b4b4b8" }}>scroll to zoom, drag to pan</div>
                      </div>
                    </>
                  ) : (
                    <div style={{ flex: 1, background: "#fafbfc", borderRadius: 10, border: "1px solid #e5e7eb", display: "flex", alignItems: "center", justifyContent: "center" }}>
                      <div style={{ textAlign: "center", color: "#9ca3af" }}>
                        <div style={{ fontSize: 16, marginBottom: 4 }}>Select a plan to view its tree</div>
                        <div style={{ fontSize: 13 }}>Click any plan on the left</div>
                      </div>
                    </div>
                  )}
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </div>
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
            {allGLs.map(g => (
              <div key={g.id} style={{ width: cellSize, height: cellSize, borderRadius: Math.max(2, cellSize / 4), background: boundSet!.has(g.id) ? "#10B981" : "#fecaca", opacity: boundSet!.has(g.id) ? 1 : 0.35 }}
                title={`GL-${g.id}: ${g.rows.toLocaleString()} rows`} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
