"use client";
import React, { useEffect, useState, useMemo, useRef, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { QState, generateCypher } from "@/lib/query-state";
import {
  PlanNode as RulePlanNode, buildNAryJoin, expandNAryJoinDP,
  pushJoinBelowUnionAll, joinAssociativity, joinCommutativity,
  expandGEM, computeCost,
  type QueryNode, type QueryEdge, type Catalog as RuleCatalog,
} from "@/lib/plan-rules";

interface Props { step: number; onStep: (n: number) => void; queryState?: QState; }
interface Graphlet { id: number; rows: number; extents: number; cols: number; schema: string[]; schemaTruncated?: boolean; }
interface Catalog { vertexPartitions: { label?: string; numColumns?: number; graphlets: Graphlet[]; numGraphlets: number; totalRows: number }[]; edgePartitions: any[]; summary: any; }

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
function ZoomPanSVG({ children, width, height, focusX, focusY }: { children: React.ReactNode; width: number; height: number; focusX?: number; focusY?: number }) {
  const panRef = useRef({ x: 0, y: 0 });
  const zoomRef = useRef(1);
  const [, forceRender] = useState(0);
  const dragRef = useRef(false);
  const lastRef = useRef({ x: 0, y: 0 });
  const divRef = useRef<HTMLDivElement>(null);

  useEffect(() => { panRef.current = { x: 0, y: 0 }; zoomRef.current = 1; forceRender(n => n + 1); }, [width, height]);

  // Auto-focus on a node when focusX/focusY change
  useEffect(() => {
    if (focusX == null || focusY == null || !divRef.current) return;
    const div = divRef.current;
    const rect = div.getBoundingClientRect();
    const centerX = rect.width / 2;
    const centerY = rect.height / 2;
    // Zoom in a bit and center on the target
    zoomRef.current = 1.5;
    panRef.current = { x: centerX - focusX * 1.5, y: centerY - focusY * 1.5 };
    forceRender(n => n + 1);
  }, [focusX, focusY]);

  useEffect(() => {
    const div = divRef.current;
    if (!div) return;

    const onDown = (e: MouseEvent) => {
      // Only left button
      if (e.button !== 0) return;
      e.preventDefault();
      e.stopPropagation();
      dragRef.current = true;
      movedRef.current = false;
      lastRef.current = { x: e.clientX, y: e.clientY };
    };
    let rafId = 0;
    const onMove = (e: MouseEvent) => {
      if (!dragRef.current) return;
      const dx = e.clientX - lastRef.current.x;
      const dy = e.clientY - lastRef.current.y;
      if (Math.abs(dx) > 1 || Math.abs(dy) > 1) movedRef.current = true;
      panRef.current = { x: panRef.current.x + dx, y: panRef.current.y + dy };
      lastRef.current = { x: e.clientX, y: e.clientY };
      if (!rafId) rafId = requestAnimationFrame(() => { rafId = 0; forceRender(n => n + 1); });
    };
    const onUp = () => { dragRef.current = false; };
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      zoomRef.current = Math.max(0.3, Math.min(3, zoomRef.current * (e.deltaY > 0 ? 0.95 : 1.05)));
      forceRender(n => n + 1);
    };
    // Double-click: zoom in toward click position
    const onDblClick = (e: MouseEvent) => {
      const rect = div.getBoundingClientRect();
      const clickX = e.clientX - rect.left;
      const clickY = e.clientY - rect.top;
      const newZoom = Math.min(3, zoomRef.current * 1.3);
      const scale = newZoom / zoomRef.current;
      // Adjust pan so the click point stays in place
      panRef.current = {
        x: clickX - (clickX - panRef.current.x) * scale,
        y: clickY - (clickY - panRef.current.y) * scale,
      };
      zoomRef.current = newZoom;
      forceRender(n => n + 1);
    };

    div.addEventListener("mousedown", onDown, true); // capture phase
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
    div.addEventListener("wheel", onWheel, { passive: false });
    div.addEventListener("dblclick", onDblClick);

    return () => {
      div.removeEventListener("mousedown", onDown, true);
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
      div.removeEventListener("wheel", onWheel);
      div.removeEventListener("dblclick", onDblClick);
    };
  }, []);

  const movedRef = useRef(false);
  const pad = 40; const vbW = width + pad * 2; const vbH = height + pad * 2;
  const z = zoomRef.current; const p = panRef.current;

  return (
    <div ref={divRef} style={{ width: "100%", height: "100%", overflow: "hidden", cursor: "grab", userSelect: "none", WebkitUserSelect: "none" }}>
      <svg style={{ width: "100%", height: "100%", display: "block", pointerEvents: "none" }}
        viewBox={`${-pad} ${-pad} ${vbW} ${vbH}`} preserveAspectRatio="xMidYMid meet">
        <g transform={`translate(${p.x / z}, ${p.y / z}) scale(${z})`} style={{ pointerEvents: "auto" }}>{children}</g>
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
        {n.detail && <text x={lx + 14} y={n.y + 46} fontSize={13} fontFamily="monospace" fill="#6b7280">{n.detail.length > 26 ? n.detail.slice(0, 24) + "\u2026" : n.detail}</text>}
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
  HashJoin: "#e84545", NLJoin: "#F59E0B", UnionAll: "#ec4899", Alternatives: "#e84545",
};
const oc = (op: string) => OP_COLOR[op] ?? "#71717a";

type Phase = "bind" | "logical" | "joinorder" | "physical";

// ─── Join ordering with per-JO state ─────────────────────────────────────────
type JOState = "initial" | "pushed" | "gem" | "done";
interface JoinOrder {
  id: string;
  label: string;
  state: JOState;
  childCount: number;  // enumerated plans so far
  tree: RulePlanNode;  // actual plan tree from plan-rules engine
  totalSubTrees?: number; // total sub-trees after pushdown
  exploredSubTrees?: number; // how many sub-trees have been explored
}

interface Trial {
  id: number;
  orders: JoinOrder[];
  finished: boolean;
}

// ─── Main ────────────────────────────────────────────────────────────────────
export default function S3_Plan({ step, queryState }: Props) {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [phase, setPhase] = useState<Phase>("bind");
  const [activeNode, setActiveNode] = useState<string | null>(null);
  const [boundNodes, setBoundNodes] = useState<Map<string, number[]>>(new Map());
  const [bindAnimating, setBindAnimating] = useState(false);
  const [clickedScan, setClickedScan] = useState<string | null>(null);
  const [selectedJO, setSelectedJO] = useState<string | null>(null);
  // Trial system
  const [trials, setTrials] = useState<Trial[]>([]);
  const [activeTrialId, setActiveTrialId] = useState<number | null>(null);
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
    return proj;
  }, [allBound, qEdges, qNodes, boundNodes, vp, retDetail, queryState]);

  // Sample graphlets for pushdown display
  const pushdownVar = qNodes[0]?.variable || "";
  const pushdownGLs = useMemo(() => {
    const ids = boundNodes.get(pushdownVar) ?? [];
    return vp ? vp.graphlets.filter(g => ids.includes(g.id)).sort((a, b) => b.rows - a.rows) : [];
  }, [vp, boundNodes, pushdownVar]);


  // Plan-rules integration
  const ruleQueryNodes = useMemo((): QueryNode[] =>
    qNodes.map(n => ({ alias: n.variable, graphletIds: boundNodes.get(n.variable) ?? [] })),
    [qNodes, boundNodes]);

  const ruleQueryEdges = useMemo((): QueryEdge[] =>
    qEdges.map(e => ({ type: e.edgeType, srcAlias: e.sourceVar, dstAlias: e.targetVar })),
    [qEdges]);

  const ruleCatalog = useMemo((): RuleCatalog | null => {
    if (!catalog) return null;
    return {
      summary: catalog.summary,
      vertexPartitions: catalog.vertexPartitions.map(vp => ({
        label: vp.label ?? "NODE",
        numColumns: vp.numColumns ?? 0, numGraphlets: vp.numGraphlets, totalRows: vp.totalRows,
        graphlets: vp.graphlets.map(g => ({ id: g.id, rows: g.rows, cols: g.cols, schema: g.schema })),
      })),
      edgePartitions: catalog.edgePartitions.map(ep => ({ type: ep.type ?? ep.short, totalRows: ep.totalRows })),
    };
  }, [catalog]);

  // Build initial NAryJoin plan via plan-rules
  const naryJoinPlan = useMemo(() => {
    if (!ruleCatalog || ruleQueryNodes.length === 0 || ruleQueryEdges.length === 0) return null;
    if (!ruleQueryNodes.every(n => n.graphletIds.length > 0)) return null;
    return buildNAryJoin(ruleQueryNodes, ruleQueryEdges, ruleCatalog);
  }, [ruleCatalog, ruleQueryNodes, ruleQueryEdges]);

  // Actions
  const runBinding = (v: string) => { if (bindAnimating) return; setActiveNode(v); setBindAnimating(true); setTimeout(() => { setBoundNodes(prev => new Map(prev).set(v, bindingsFor(v).map(g => g.id))); setBindAnimating(false); }, 500); };

  const reset = useCallback(() => {
    setPhase("bind"); setActiveNode(null); setBoundNodes(new Map()); setBindAnimating(false);
    setClickedScan(null); setSelectedJO(null);
    setTrials([]); setActiveTrialId(null); setSimulateOverlay(null); setSimulatedCount(0);
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
                <button onClick={() => {
                  const t: Trial = { id: 1, orders: [], finished: false };
                  setTrials([t]); setActiveTrialId(1); setSelectedJO(null); setPhase("joinorder");
                }}
                  style={{ padding: "11px 0", borderRadius: 8, border: "none", background: "#18181b", color: "#fff", fontSize: 15, fontWeight: 700, cursor: "pointer", width: "100%", flexShrink: 0 }}>
                  Optimize using Orca &rarr;
                </button>
              </motion.div>
            )}

            {/* JOIN ORDER: Interactive Rule Application */}
            {/* JOIN ORDER: Interactive Rule Application via plan-rules engine */}
            {phase === "joinorder" && (() => {
              const trial = trials.find(t => t.id === activeTrialId);
              if (!trial) return null;
              const totalPlans = trial.orders.reduce((s, jo) => s + jo.childCount, 0);
              const selJO = trial.orders.find(j => j.id === selectedJO);
              const graphletData = vp.graphlets.map(g => ({ id: g.id, rows: g.rows, cols: g.cols, schema: g.schema }));

              const updateJO = (joId: string, upd: Partial<JoinOrder>) => {
                setTrials(prev => prev.map(t => t.id !== activeTrialId ? t :
                  { ...t, orders: t.orders.map(jo => jo.id === joId ? { ...jo, ...upd } : jo) }));
              };
              const addTrial = () => {
                const newId = Math.max(0, ...trials.map(t => t.id)) + 1;
                setTrials(prev => [...prev, { id: newId, orders: [], finished: false }]);
                setActiveTrialId(newId); setSelectedJO(null);
              };
              const finishTrial = () => {
                setTrials(prev => prev.map(t => t.id !== activeTrialId ? t : { ...t, finished: true }));
              };

              const hasExpanded = trial.orders.length > 0;

              // Render a RulePlanNode as our SVG PlanNode type
              const toSVG = (rn: RulePlanNode): PlanNode => ({
                op: rn.op, detail: rn.detail, color: rn.color ?? "#71717a",
                // cost not shown at logical level
                rows: rn.rows ? fmt(rn.rows) : undefined,
                children: rn.children?.map(toSVG),
              });

              // SVG tree for selected JO
              const svgTree = selJO ? toSVG(selJO.tree) : (naryJoinPlan ? toSVG(naryJoinPlan) : logicalPlan ?? { op: "Project", color: "#71717a" });
              const tl = svgTree ? layoutTree(svgTree) : [];
              const tw = tl.length > 0 ? Math.max(...tl.map(n => n.x)) + CW / 2 : 400;
              const th = tl.length > 0 ? Math.max(...tl.map(n => n.y)) + CH : 200;

              // Describe a plan tree as a short label
              const treeLabel = (t: RulePlanNode): string => {
                if (t.op === "UnionAll") return `UnionAll(${t.children?.length ?? 0})`;
                if (t.op === "Join" && t.children?.length === 2) {
                  return `(${treeLabel(t.children[0])} \u22c8 ${treeLabel(t.children[1])})`;
                }
                if (t.op === "Get") return t.tableId ?? t.detail?.split(" ")[0] ?? "?";
                return t.op;
              };

              return (
                <motion.div key="joinorder" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                  style={{ height: "100%", display: "flex", gap: 14, overflow: "hidden" }}>

                  {/* Left panel */}
                  <div style={{ width: 380, flexShrink: 0, display: "flex", flexDirection: "column", gap: 6, overflow: "hidden" }}>

                    {/* Finished trial cards */}
                    {trials.filter(t => t.finished).length > 0 && (
                      <div style={{ display: "flex", gap: 6, flexWrap: "wrap", flexShrink: 0 }}>
                        {trials.filter(t => t.finished).map(t => (
                          <div key={t.id} style={{ padding: "6px 12px", background: "#f8f9fa", borderRadius: 7, border: "1px solid #e5e7eb", textAlign: "center" }}>
                            <div style={{ fontSize: 11, color: "#71717a" }}>Trial {t.id}</div>
                            <div style={{ fontSize: 18, fontWeight: 800, fontFamily: "monospace" }}>{t.orders.reduce((s, j) => s + j.childCount, 0).toLocaleString()}</div>
                          </div>
                        ))}
                      </div>
                    )}

                    {/* Active trial header */}
                    <div style={{ display: "flex", alignItems: "center", gap: 8, flexShrink: 0 }}>
                      <div style={{ fontSize: 14, fontWeight: 700, color: "#18181b" }}>Trial {trial.id}</div>
                      <div style={{ marginLeft: "auto", padding: "4px 12px", background: totalPlans > 6 ? "#fef2f2" : "#f8f9fa",
                        borderRadius: 6, border: `1px solid ${totalPlans > 6 ? "#fecaca" : "#e5e7eb"}` }}>
                        <span style={{ fontSize: 12, color: "#71717a" }}>Enumerated: </span>
                        <span style={{ fontSize: 22, fontWeight: 800, fontFamily: "monospace", color: totalPlans > 6 ? "#e84545" : "#18181b" }}>
                          {totalPlans === Infinity ? "∞" : totalPlans.toLocaleString()}
                        </span>
                      </div>
                    </div>

                    {/* Step 1: Rule selection (DP / GEM) */}
                    {!hasExpanded && !trial.finished && naryJoinPlan && ruleCatalog && (
                      <div style={{ padding: "12px", background: "#f8f9fa", borderRadius: 8, border: "1px solid #e5e7eb" }}>
                        <div style={{ fontSize: 13, fontWeight: 700, color: "#18181b", marginBottom: 8 }}>
                          Apply NAryJoin Expansion Rule:
                        </div>
                        <div style={{ display: "flex", gap: 6 }}>
                          <button onClick={() => {
                            const results = expandNAryJoinDP(naryJoinPlan, 2);
                            const orders: JoinOrder[] = results.map((tree, i) => ({
                              id: `dp-${i}`, label: treeLabel(tree), state: "initial" as JOState,
                              childCount: 1, tree,
                            }));
                            setTrials(prev => prev.map(t => t.id !== activeTrialId ? t : { ...t, orders }));
                            setSelectedJO(orders[0]?.id ?? null);
                          }} style={{ flex: 1, padding: "10px 0", borderRadius: 7, border: "none", background: "#18181b", color: "#fff", fontSize: 13, fontWeight: 700, cursor: "pointer" }}>
                            ExpandNAryJoinDP
                          </button>
                          <button onClick={() => {
                            const results = expandGEM(naryJoinPlan, graphletData, 2);
                            const orders: JoinOrder[] = results.map((tree, i) => ({
                              id: `gem-${i}`, label: `GEM: ${treeLabel(tree)}`, state: "gem" as JOState,
                              childCount: tree.children?.length ?? 1, tree,
                            }));
                            setTrials(prev => prev.map(t => t.id !== activeTrialId ? t : { ...t, orders }));
                            setSelectedJO(orders[0]?.id ?? null);
                          }} style={{ flex: 1, padding: "10px 0", borderRadius: 7, border: "none", background: "#10B981", color: "#fff", fontSize: 13, fontWeight: 700, cursor: "pointer" }}>
                            ExpandGEM
                          </button>
                        </div>
                      </div>
                    )}

                    {/* Step 2: JO list with per-JO rules */}
                    {hasExpanded && !trial.finished && (
                      <div style={{ flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", gap: 3 }} className="thin-scrollbar">
                        {trial.orders.map(jo => {
                          const isSel = selectedJO === jo.id;
                          const stColor = jo.state === "initial" ? "#e5e7eb" : jo.state === "gem" ? "#bbf7d0" : jo.state === "pushed" ? "#fecaca" : "#d4d4d8";
                          return (
                            <div key={jo.id}>
                              <div onClick={() => setSelectedJO(jo.id)}
                                style={{ padding: "8px 10px", borderRadius: 7, cursor: "pointer",
                                  border: isSel ? "2px solid #18181b" : `1px solid ${stColor}`,
                                  background: isSel ? "#f0f1f3" : jo.state === "done" ? "#fafbfc" : "#fff",
                                  opacity: jo.state === "done" ? 0.6 : 1 }}>
                                <div style={{ fontSize: 13, fontFamily: "monospace", fontWeight: 600, color: "#18181b", lineHeight: 1.5, wordBreak: "break-word" }}>
                                  {jo.label}
                                </div>
                                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 2 }}>
                                  <span style={{ fontSize: 11, color: "#9ca3af" }}>{jo.state}</span>
                                  <span style={{ fontSize: 13, fontWeight: 700, fontFamily: "monospace" }}>&times;{jo.childCount}</span>
                                </div>
                              </div>

                              {/* Per-JO rules — when selected */}
                              {isSel && jo.state !== "done" && (
                                <div style={{ margin: "4px 0 4px 12px", display: "flex", flexDirection: "column", gap: 3 }}>
                                  {/* PushJoinBelowUnionAll */}
                                  {jo.state === "initial" && (
                                    <button onClick={() => {
                                      const pushed = pushJoinBelowUnionAll(jo.tree, graphletData);
                                      const total = parseInt(pushed.detail?.replace(/[^0-9]/g, "") ?? "1") || 1;
                                      updateJO(jo.id, { state: "pushed", childCount: 0, tree: pushed,
                                        label: `Pushed: ${treeLabel(pushed)}`, totalSubTrees: total, exploredSubTrees: 0 });
                                    }} style={{ padding: "7px 10px", borderRadius: 6, border: "none", background: "#e84545", color: "#fff", fontSize: 12, fontWeight: 700, cursor: "pointer", textAlign: "left" }}>
                                      PushJoinBelowUnionAll
                                    </button>
                                  )}
                                  {/* Explore sub-trees one by one or all at once */}
                                  {jo.state === "pushed" && (() => {
                                    const explored = jo.exploredSubTrees ?? 0;
                                    const total = jo.totalSubTrees ?? 1;
                                    const altsPerSubTree = 3;
                                    const remaining = total - explored;
                                    return (
                                      <>
                                        <div style={{ fontSize: 12, color: "#71717a", fontFamily: "monospace", padding: "2px 0" }}>
                                          Explored: {explored.toLocaleString()} / {total.toLocaleString()} sub-trees
                                        </div>
                                        {remaining > 0 && (
                                          <button onClick={() => {
                                            const newExplored = Math.min(explored + 1, total);
                                            updateJO(jo.id, {
                                              exploredSubTrees: newExplored,
                                              childCount: newExplored * altsPerSubTree,
                                              state: newExplored >= total ? "done" : "pushed",
                                            });
                                          }} style={{ padding: "7px 10px", borderRadius: 6, border: "1px dashed #F59E0B", background: "transparent", color: "#F59E0B", fontSize: 12, fontWeight: 700, cursor: "pointer", textAlign: "left" }}>
                                            Find Best for Sub-tree {explored + 1} → +{altsPerSubTree} plans
                                          </button>
                                        )}
                                        {remaining > 1 && (
                                          <button onClick={() => {
                                            updateJO(jo.id, {
                                              exploredSubTrees: total,
                                              childCount: total * altsPerSubTree,
                                              state: "done",
                                            });
                                          }} style={{ padding: "7px 10px", borderRadius: 6, border: "none", background: "#F59E0B", color: "#fff", fontSize: 12, fontWeight: 700, cursor: "pointer", textAlign: "left" }}>
                                            Find All ({remaining.toLocaleString()} remaining) → {(total * altsPerSubTree).toLocaleString()} plans
                                          </button>
                                        )}
                                      </>
                                    );
                                  })()}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    )}

                    {/* Bottom buttons */}
                    {hasExpanded && !trial.finished && (
                      <div style={{ display: "flex", gap: 4, flexShrink: 0 }}>
                        <button onClick={() => { finishTrial(); addTrial(); }}
                          style={{ flex: 1, padding: "8px 0", borderRadius: 7, border: "1px solid #e5e7eb", background: "transparent", color: "#71717a", fontSize: 12, fontWeight: 600, cursor: "pointer" }}>
                          Finish &amp; New Trial
                        </button>
                        <button onClick={finishTrial}
                          style={{ padding: "8px 12px", borderRadius: 7, border: "1px solid #e5e7eb", background: "transparent", color: "#9ca3af", fontSize: 12, cursor: "pointer" }}>
                          Finish
                        </button>
                      </div>
                    )}

                    <button onClick={() => setPhase("physical")}
                      style={{ padding: "9px 0", borderRadius: 8, border: "1px solid #e5e7eb", background: "transparent", color: "#18181b", fontSize: 13, fontWeight: 600, cursor: "pointer", width: "100%", flexShrink: 0 }}>
                      Proceed to Implementation &rarr;
                    </button>
                  </div>

                  {/* Right: SVG plan tree */}
                  <div style={{ flex: 1, background: "#fafbfc", borderRadius: 10, border: "1px solid #e5e7eb", overflow: "hidden", position: "relative" }}>
                    <ZoomPanSVG width={tw} height={th}>
                      <PlanCards nodes={tl} />
                    </ZoomPanSVG>
                    {selJO && <div style={{ position: "absolute", top: 8, left: 12, fontSize: 12, fontFamily: "monospace", fontWeight: 700, color: "#18181b",
                      background: "#fff", padding: "4px 10px", borderRadius: 5, border: "1px solid #e5e7eb", maxWidth: "80%", wordBreak: "break-word" }}>
                      {selJO.label} | {selJO.state}
                    </div>}
                    <div style={{ position: "absolute", bottom: 8, right: 12, fontSize: 11, color: "#b4b4b8" }}>scroll to zoom, drag to pan</div>
                  </div>
                </motion.div>
              );
            })()}

            {/* PHYSICAL — placeholder */}
            {phase === "physical" && (
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
                  6 orderings &times; {pushdownGLs.length} graphlets &times; 6<sup>{qNodes.length}</sup> sub-orders
                </div>
                <div style={{ display: "flex", gap: 10, marginTop: 12 }}>
                  <button onClick={() => setSimulateOverlay(null)}
                    style={{ padding: "12px 28px", borderRadius: 8, border: "none", background: "#10B981", color: "#fff", fontSize: 16, fontWeight: 700, cursor: "pointer" }}>
                    OK
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
