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

type Phase = "bind" | "logical" | "orca" | "pushdown" | "gem";
// Track which variables have had UnionAll pushdown applied
// Each pushdown multiplies plan space by that variable's graphlet count

// ─── Plan alternative ────────────────────────────────────────────────────────
interface PlanAlt { id: string; label: string; cost: number; tree: PlanNode; }

// ─── Main ────────────────────────────────────────────────────────────────────
export default function S3_Plan({ step, queryState }: Props) {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [phase, setPhase] = useState<Phase>("bind");
  const [activeNode, setActiveNode] = useState<string | null>(null);
  const [boundNodes, setBoundNodes] = useState<Map<string, number[]>>(new Map());
  const [bindAnimating, setBindAnimating] = useState(false);
  const [selectedPlan, setSelectedPlan] = useState<string | null>(null);
  const [clickedScan, setClickedScan] = useState<string | null>(null);
  const [pushedDown, setPushedDown] = useState<Set<string>>(new Set());
  const [pushdownAnimCount, setPushdownAnimCount] = useState(0);
  const [pushdownAnimating, setPushdownAnimating] = useState(false);
  const [checkedPlans, setCheckedPlans] = useState<Set<string>>(new Set());
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

  const [extraPlans, setExtraPlans] = useState<PlanAlt[]>([]);

  // Base Orca plan alternatives
  const basePlanAlts = useMemo((): PlanAlt[] => {
    if (!allBound || !vp || qEdges.length === 0) return [];
    const e = qEdges[0];
    const srcIds = boundNodes.get(e.sourceVar) ?? [];
    const tgtIds = boundNodes.get(e.targetVar) ?? [];
    const srcRows = srcIds.reduce((s, id) => s + (vp.graphlets.find(g => g.id === id)?.rows ?? 0), 0);
    const tgtRows = tgtIds.reduce((s, id) => s + (vp.graphlets.find(g => g.id === id)?.rows ?? 0), 0);
    const edgeRows = 1587102; // birthPlace edges
    const lim = queryState?.limit ?? 20;

    const mk = (id: string, label: string, cost: number, tree: PlanNode): PlanAlt => ({ id, label, cost: Math.round(cost * 10) / 10, tree });

    // Helper: wrap with Top + ProduceResults
    const wrap = (inner: PlanNode): PlanNode => ({
      op: "Top", color: oc("Top"), detail: `LIMIT ${lim}`, children: [
        { op: "Projection", color: oc("Projection"), detail: retDetail, children: [inner] },
      ],
    });

    const alts: PlanAlt[] = [];

    // ─── Join Order 1: (p ⋈ edge) ⋈ c — source-first ───
    // 1a: AdjIdxJoin(fwd) + IdSeek — BEST
    const c1a = srcRows * 0.001 + edgeRows * 0.0001;
    alts.push(mk("1a", `(${e.sourceVar} ⋈ edge) ⋈ ${e.targetVar} — AdjIdx+IdSeek`, c1a, wrap({
      op: "IdSeek", color: oc("IdSeek"), detail: e.targetVar, children: [
        { op: "AdjIdxJoin", color: oc("AdjIdxJoin"), detail: `:${e.edgeType} (fwd)`, children: [
          { op: "NodeScan", color: oc("NodeScan"), detail: `${e.sourceVar} (${srcIds.length} GLs)`, rows: fmt(srcRows) },
          { op: "IndexScan", color: oc("IdSeek"), detail: `${e.edgeType}_fwd` },
        ]},
        { op: "IndexScan", color: oc("IdSeek"), detail: `NODE_id` },
      ],
    })));

    // 1b: HashJoin(p, edge) + IdSeek(c)
    const c1b = srcRows * 0.002 + edgeRows * 0.001;
    alts.push(mk("1b", `(${e.sourceVar} ⋈ edge) ⋈ ${e.targetVar} — Hash+IdSeek`, c1b, wrap({
      op: "IdSeek", color: oc("IdSeek"), detail: e.targetVar, children: [
        { op: "HashJoin", color: oc("HashJoin"), detail: `${e.sourceVar}._id = _sid`, children: [
          { op: "NodeScan", color: oc("NodeScan"), detail: `${e.sourceVar} (${srcIds.length} GLs)`, rows: fmt(srcRows) },
          { op: "NodeScan", color: oc("NodeScan"), detail: `:${e.edgeType}`, rows: fmt(edgeRows) },
        ]},
        { op: "IndexScan", color: oc("IdSeek"), detail: `NODE_id` },
      ],
    })));

    // ─── Join Order 2: (c ⋈ edge) ⋈ p — target-first (backward) ───
    // 2a: AdjIdxJoin(bwd) + IdSeek
    const c2a = tgtRows * 0.001 + edgeRows * 0.0001;
    alts.push(mk("2a", `(${e.targetVar} ⋈ edge) ⋈ ${e.sourceVar} — AdjIdx(bwd)+IdSeek`, c2a, wrap({
      op: "IdSeek", color: oc("IdSeek"), detail: e.sourceVar, children: [
        { op: "AdjIdxJoin", color: oc("AdjIdxJoin"), detail: `:${e.edgeType} (bwd)`, children: [
          { op: "NodeScan", color: oc("NodeScan"), detail: `${e.targetVar} (${tgtIds.length} GLs)`, rows: fmt(tgtRows) },
          { op: "IndexScan", color: oc("IdSeek"), detail: `${e.edgeType}_bwd` },
        ]},
        { op: "IndexScan", color: oc("IdSeek"), detail: `NODE_id` },
      ],
    })));

    // 2b: HashJoin(c, edge) + IdSeek(p)
    const c2b = tgtRows * 0.002 + edgeRows * 0.001;
    alts.push(mk("2b", `(${e.targetVar} ⋈ edge) ⋈ ${e.sourceVar} — Hash(bwd)+IdSeek`, c2b, wrap({
      op: "IdSeek", color: oc("IdSeek"), detail: e.sourceVar, children: [
        { op: "HashJoin", color: oc("HashJoin"), detail: `_tid = ${e.targetVar}._id`, children: [
          { op: "NodeScan", color: oc("NodeScan"), detail: `${e.targetVar} (${tgtIds.length} GLs)`, rows: fmt(tgtRows) },
          { op: "NodeScan", color: oc("NodeScan"), detail: `:${e.edgeType}`, rows: fmt(edgeRows) },
        ]},
        { op: "IndexScan", color: oc("IdSeek"), detail: `NODE_id` },
      ],
    })));

    // ─── Join Order 3: (edge ⋈ p) ⋈ c — edge-scan first ───
    const c3 = edgeRows * 0.002 + srcRows * 0.001 + tgtRows * 0.001;
    alts.push(mk("3", `(edge ⋈ ${e.sourceVar}) ⋈ ${e.targetVar} — Hash+Hash`, c3, wrap({
      op: "HashJoin", color: oc("HashJoin"), detail: `_tid = ${e.targetVar}._id`, children: [
        { op: "HashJoin", color: oc("HashJoin"), detail: `_sid = ${e.sourceVar}._id`, children: [
          { op: "NodeScan", color: oc("NodeScan"), detail: `:${e.edgeType}`, rows: fmt(edgeRows) },
          { op: "NodeScan", color: oc("NodeScan"), detail: `${e.sourceVar} (${srcIds.length} GLs)`, rows: fmt(srcRows) },
        ]},
        { op: "NodeScan", color: oc("NodeScan"), detail: `${e.targetVar} (${tgtIds.length} GLs)`, rows: fmt(tgtRows) },
      ],
    })));

    // ─── Join Order 4: (edge ⋈ c) ⋈ p — edge-scan, target first ───
    const c4 = edgeRows * 0.002 + tgtRows * 0.001 + srcRows * 0.001;
    alts.push(mk("4", `(edge ⋈ ${e.targetVar}) ⋈ ${e.sourceVar} — Hash+Hash`, c4, wrap({
      op: "HashJoin", color: oc("HashJoin"), detail: `_sid = ${e.sourceVar}._id`, children: [
        { op: "HashJoin", color: oc("HashJoin"), detail: `_tid = ${e.targetVar}._id`, children: [
          { op: "NodeScan", color: oc("NodeScan"), detail: `:${e.edgeType}`, rows: fmt(edgeRows) },
          { op: "NodeScan", color: oc("NodeScan"), detail: `${e.targetVar} (${tgtIds.length} GLs)`, rows: fmt(tgtRows) },
        ]},
        { op: "NodeScan", color: oc("NodeScan"), detail: `${e.sourceVar} (${srcIds.length} GLs)`, rows: fmt(srcRows) },
      ],
    })));

    return alts.sort((a, b) => a.cost - b.cost);
  }, [allBound, vp, qEdges, boundNodes, retDetail, queryState]);

  // All plans = base + expanded
  const planAlts = useMemo(() => [...basePlanAlts, ...extraPlans].sort((a, b) => a.cost - b.cost), [basePlanAlts, extraPlans]);

  // Current plan space = base alts × product of pushed-down graphlet counts
  const currentPlanSpace = useMemo(() => {
    let space = planAlts.length;
    for (const v of pushedDown) {
      space *= (boundNodes.get(v)?.length ?? 1);
    }
    return space;
  }, [planAlts.length, pushedDown, boundNodes]);

  // Next variable to push down (not yet pushed)
  const nextPushdownVar = useMemo(() => {
    return qNodes.find(n => !pushedDown.has(n.variable));
  }, [qNodes, pushedDown]);

  const allPushedDown = qNodes.length > 0 && qNodes.every(n => pushedDown.has(n.variable));

  // GEM
  const gemVGs = useMemo(() => !allBound ? [] : qNodes.map(n => ({ v: n.variable, vgs: Math.max(1, Math.ceil((boundNodes.get(n.variable)?.length ?? 0) / 50)) })), [allBound, boundNodes, qNodes]);
  const gemSpace = gemVGs.reduce((p, v) => p * v.vgs, 1) * planAlts.length;
  const fullPushdownSpace = useMemo(() => {
    let s = planAlts.length;
    for (const n of qNodes) s *= (boundNodes.get(n.variable)?.length ?? 1);
    return s;
  }, [planAlts.length, qNodes, boundNodes]);

  // Actions
  const runBinding = (v: string) => {
    if (bindAnimating) return;
    setActiveNode(v); setBindAnimating(true);
    setTimeout(() => { setBoundNodes(prev => new Map(prev).set(v, bindingsFor(v).map(g => g.id))); setBindAnimating(false); }, 500);
  };

  const applyPushdown = (variable: string) => {
    setPushdownAnimating(true);
    const glCount = boundNodes.get(variable)?.length ?? 1;
    const from = currentPlanSpace;
    const target = from * glCount;
    setPushdownAnimCount(from);
    let s = 0; const steps = 25;
    const tick = () => {
      s++;
      setPushdownAnimCount(Math.round(from + (1 - Math.pow(1 - s / steps, 3)) * (target - from)));
      if (s < steps) setTimeout(tick, 40);
      else {
        setPushedDown(prev => new Set(prev).add(variable));
        setPushdownAnimating(false);
        setPushdownAnimCount(target);
      }
    };
    setTimeout(tick, 100);
  };

  const reset = useCallback(() => {
    setPhase("bind"); setActiveNode(null); setBoundNodes(new Map());
    setBindAnimating(false); setSelectedPlan(null); setClickedScan(null);
    setPushedDown(new Set()); setPushdownAnimCount(0); setPushdownAnimating(false); setCheckedPlans(new Set()); setExtraPlans([]);
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

  const selAlt = selectedPlan ? planAlts.find(p => p.id === selectedPlan) : null;
  const activePlan = selAlt?.tree ?? logicalPlan;
  const layout = activePlan ? layoutTree(activePlan) : [];
  const svgW = layout.length > 0 ? Math.max(...layout.map(n => n.x)) + CW / 2 : 400;
  const svgH = layout.length > 0 ? Math.max(...layout.map(n => n.y)) + CH : 200;
  const totalPushdownSpace = naivePlanSpace * planAlts.length;

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
                <button onClick={() => setPhase("orca")} style={{ padding: "11px 0", borderRadius: 8, border: "none", background: "#18181b", color: "#fff", fontSize: 15, fontWeight: 700, cursor: "pointer", width: "100%", flexShrink: 0 }}>
                  Optimize using Orca
                </button>
              </motion.div>
            )}

            {/* ORCA / PUSHDOWN / GEM: left = plan grid + actions, right = selected plan tree */}
            {(phase === "orca" || phase === "pushdown" || phase === "gem") && (
              <motion.div key="orca" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                style={{ height: "100%", display: "flex", gap: 14, overflow: "hidden" }}>

                {/* Left: plan list + plan space + actions */}
                <div style={{ flex: "0 0 380px", display: "flex", flexDirection: "column", gap: 8, overflow: "hidden" }}>
                  {/* Plan space counter */}
                  {(() => {
                    const isGem = phase === "gem";
                    const count = isGem ? gemSpace : (pushdownAnimating ? pushdownAnimCount : currentPlanSpace);
                    const color = isGem ? "#10B981" : pushedDown.size > 0 ? "#e84545" : "#18181b";
                    const bg = isGem ? "#f0fdf4" : pushedDown.size > 0 ? "#fef2f2" : "#f8f9fa";
                    return (
                      <div style={{ padding: "12px 14px", background: bg, borderRadius: 10, border: `1px solid ${isGem ? "#bbf7d0" : pushedDown.size > 0 ? "#fecaca" : "#e5e7eb"}`, textAlign: "center", flexShrink: 0 }}>
                        <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 8 }}>
                          <span style={{ fontSize: 12, color: "#71717a" }}>Plan Space:</span>
                          <span style={{ fontSize: count > 99999 ? 28 : 36, fontWeight: 800, fontFamily: "monospace", color, lineHeight: 1 }}>
                            {count.toLocaleString()}
                          </span>
                        </div>
                        {isGem && fullPushdownSpace > 0 && (
                          <div style={{ fontSize: 14, fontWeight: 700, color: "#10B981", marginTop: 4 }}>
                            {((1 - gemSpace / fullPushdownSpace) * 100).toFixed(1)}% reduction from GEM
                          </div>
                        )}
                      </div>
                    );
                  })()}

                  {/* Plan list with checkboxes */}
                  <div style={{ flex: 1, overflowY: "auto" }} className="thin-scrollbar">
                    {planAlts.map((p, i) => {
                      const isSel = selectedPlan === p.id;
                      const isChecked = checkedPlans.has(p.id);
                      const isBest = i === 0 && phase === "gem";
                      return (
                        <div key={p.id} style={{
                          display: "flex", alignItems: "center", gap: 8, padding: "8px 10px", marginBottom: 3,
                          borderRadius: 7, cursor: "pointer",
                          border: isSel ? "2px solid #18181b" : isBest ? "2px solid #10B981" : "1px solid #e5e7eb",
                          background: isSel ? "#f0f1f3" : isBest ? "#f0fdf4" : "#fff",
                        }}>
                          {phase !== "gem" && (
                            <input type="checkbox" checked={isChecked}
                              onChange={() => setCheckedPlans(prev => { const n = new Set(prev); if (n.has(p.id)) n.delete(p.id); else n.add(p.id); return n; })}
                              style={{ width: 16, height: 16, cursor: "pointer", flexShrink: 0 }} />
                          )}
                          <div style={{ flex: 1, minWidth: 0 }} onClick={() => setSelectedPlan(isSel ? null : p.id)}>
                            <div style={{ fontSize: 13, fontFamily: "monospace", fontWeight: 700, color: "#374151", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                              {p.label}
                            </div>
                            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 2 }}>
                              <span style={{ fontSize: 12, color: "#9ca3af" }}>cost</span>
                              <span style={{ fontSize: 15, fontWeight: 700, fontFamily: "monospace", color: isBest ? "#10B981" : "#18181b" }}>{p.cost}</span>
                            </div>
                          </div>
                          {isBest && <span style={{ fontSize: 10, fontWeight: 700, color: "#10B981", flexShrink: 0 }}>BEST</span>}
                        </div>
                      );
                    })}
                    {/* Overflow indicator for large plan spaces */}
                    {currentPlanSpace > planAlts.length && (
                      <div style={{ padding: "10px", textAlign: "center", fontSize: 13, color: "#9ca3af", fontFamily: "monospace" }}>
                        ... and {(currentPlanSpace - planAlts.length).toLocaleString()} more plans
                      </div>
                    )}
                  </div>

                  {/* Action buttons */}
                  {phase !== "gem" && checkedPlans.size > 0 && !pushdownAnimating && (
                    <button onClick={() => {
                      setPhase("pushdown");
                      // Generate expanded plans from checked ones
                      const expanded: PlanAlt[] = [];
                      const sampleGLs = allGLs.slice(0, 5); // show 5 sample graphlet expansions
                      for (const pid of checkedPlans) {
                        const parent = planAlts.find(p => p.id === pid);
                        if (!parent) continue;
                        for (const gl of sampleGLs) {
                          const costVar = 1 + (gl.rows / 1e6) * 0.1;
                          expanded.push({
                            id: `${pid}_GL${gl.id}_${Date.now()}`,
                            label: `${parent.label.split("—")[0].trim()} [GL-${gl.id}]`,
                            cost: Math.round(parent.cost * costVar * 10) / 10,
                            tree: parent.tree,
                          });
                        }
                      }
                      setExtraPlans(prev => [...prev, ...expanded]);

                      // Animate plan space growth
                      const prevSpace = currentPlanSpace;
                      const multiplier = Math.max(...qNodes.map(n => boundNodes.get(n.variable)?.length ?? 1));
                      const newSpace = prevSpace * multiplier;
                      setPushdownAnimating(true);
                      let s = 0; const steps = 25;
                      const tick = () => {
                        s++; setPushdownAnimCount(Math.round(prevSpace + (1 - Math.pow(1 - s / steps, 3)) * (newSpace - prevSpace)));
                        if (s < steps) setTimeout(tick, 40);
                        else {
                          setPushedDown(prev => { const n = new Set(prev); qNodes.forEach(x => n.add(x.variable)); return n; });
                          setPushdownAnimating(false);
                          setCheckedPlans(new Set());
                        }
                      };
                      setTimeout(tick, 100);
                    }} style={{ padding: "11px 0", borderRadius: 8, border: "none", background: "#e84545", color: "#fff", fontSize: 14, fontWeight: 700, cursor: "pointer", width: "100%", flexShrink: 0 }}>
                      Apply UNION ALL Pushdown ({checkedPlans.size} selected)
                    </button>
                  )}
                  {phase !== "gem" && !pushdownAnimating && pushedDown.size > 0 && (
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
                  {selAlt ? (
                    <>
                      <div style={{ fontSize: 13, fontFamily: "monospace", fontWeight: 700, color: "#18181b", marginBottom: 6, flexShrink: 0 }}>
                        {selAlt.label} — cost {selAlt.cost}
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
