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
  HashJoin: "#e84545", NLJoin: "#F59E0B", UnionAll: "#ec4899", Alternatives: "#e84545",
};
const oc = (op: string) => OP_COLOR[op] ?? "#71717a";

type Phase = "bind" | "logical" | "joinorder" | "physical";

// ─── Join ordering with per-JO state ─────────────────────────────────────────
type JOState = "initial" | "pushed" | "gem" | "gem+pushed" | "done";
interface JoinOrder {
  id: string;
  label: string;      // display label
  state: JOState;
  childCount: number;  // how many plans this JO contributes (1 initially)
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
  const baseJoinOrders = useMemo((): JoinOrder[] => {
    if (qEdges.length === 0) return [];
    const e = qEdges[0]; const s = e.sourceVar, t = e.targetVar, et = e.edgeType;
    return [
      { id: "jo1", label: `(${s} \u22c8 :${et}) \u22c8 ${t}`, state: "initial", childCount: 1 },
      { id: "jo2", label: `(${t} \u22c8 :${et}) \u22c8 ${s}`, state: "initial", childCount: 1 },
      { id: "jo3", label: `(:${et} \u22c8 ${s}) \u22c8 ${t}`, state: "initial", childCount: 1 },
      { id: "jo4", label: `(:${et} \u22c8 ${t}) \u22c8 ${s}`, state: "initial", childCount: 1 },
      { id: "jo5", label: `${s} \u22c8 (:${et} \u22c8 ${t})`, state: "initial", childCount: 1 },
      { id: "jo6", label: `${t} \u22c8 (:${et} \u22c8 ${s})`, state: "initial", childCount: 1 },
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
    return proj;
  }, [allBound, qEdges, qNodes, boundNodes, vp, retDetail, queryState]);

  // Sample graphlets for pushdown display
  const pushdownVar = qNodes[0]?.variable || "";
  const pushdownGLs = useMemo(() => {
    const ids = boundNodes.get(pushdownVar) ?? [];
    return vp ? vp.graphlets.filter(g => ids.includes(g.id)).sort((a, b) => b.rows - a.rows) : [];
  }, [vp, boundNodes, pushdownVar]);


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
                  const t: Trial = { id: 1, orders: baseJoinOrders.map(jo => ({ ...jo, state: "initial" as JOState, childCount: 1 })), finished: false };
                  setTrials([t]); setActiveTrialId(1); setSelectedJO(baseJoinOrders[0]?.id ?? null); setPhase("joinorder");
                }}
                  style={{ padding: "11px 0", borderRadius: 8, border: "none", background: "#18181b", color: "#fff", fontSize: 15, fontWeight: 700, cursor: "pointer", width: "100%", flexShrink: 0 }}>
                  Optimize using Orca &rarr;
                </button>
              </motion.div>
            )}

            {/* JOIN ORDER: Trial-based */}
            {/* JOIN ORDER: Trial-based */}
            {phase === "joinorder" && (() => {
              const pVar = qNodes[0]?.variable ?? "p";
              const cVar = qNodes[1]?.variable ?? "c";
              const edgeType = qEdges[0]?.edgeType ?? "?";
              const pGLCount = pushdownGLs.length;
              const cGLCount = boundNodes.get(cVar)?.length ?? 1;
              const vgs = 2;

              const trial = trials.find(t => t.id === activeTrialId);
              if (!trial) return null;
              const totalPlans = trial.orders.reduce((s, jo) => s + jo.childCount, 0);

              const updateJO = (joId: string, upd: Partial<JoinOrder>) => {
                setTrials(prev => prev.map(t => t.id !== activeTrialId ? t :
                  { ...t, orders: t.orders.map(jo => jo.id === joId ? { ...jo, ...upd } : jo) }));
              };
              const addOrders = (news: JoinOrder[]) => {
                setTrials(prev => prev.map(t => t.id !== activeTrialId ? t :
                  { ...t, orders: [...t.orders, ...news] }));
              };
              const addTrial = () => {
                const newId = (trials.length > 0 ? Math.max(...trials.map(t => t.id)) : 0) + 1;
                const t: Trial = { id: newId, orders: baseJoinOrders.map(jo => ({ ...jo, state: "initial" as JOState, childCount: 1 })), finished: false };
                setTrials(prev => [...prev, t]); setActiveTrialId(newId);
              };
              const finishTrial = () => {
                setTrials(prev => prev.map(t => t.id !== activeTrialId ? t : { ...t, finished: true }));
              };

              // Selected JO details
              const selJO = trial.orders.find(j => j.id === selectedJO);

              return (
                <motion.div key="joinorder" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                  style={{ height: "100%", display: "flex", gap: 14, overflow: "hidden" }}>

                  {/* Left: Trial tabs + JO list */}
                  <div style={{ width: 340, flexShrink: 0, display: "flex", flexDirection: "column", gap: 6, overflow: "hidden" }}>
                    {/* Trial tabs */}
                    <div style={{ display: "flex", gap: 4, flexWrap: "wrap", flexShrink: 0 }}>
                      {trials.map(t => (
                        <button key={t.id} onClick={() => setActiveTrialId(t.id)}
                          style={{ padding: "4px 12px", borderRadius: 5, border: "none", cursor: "pointer", fontSize: 13, fontWeight: 600,
                            background: t.id === activeTrialId ? "#18181b" : "#f0f1f3",
                            color: t.id === activeTrialId ? "#fff" : "#71717a" }}>
                          Trial {t.id} {t.finished ? `(${t.orders.reduce((s, j) => s + j.childCount, 0)})` : ""}
                        </button>
                      ))}
                      <button onClick={addTrial}
                        style={{ padding: "4px 10px", borderRadius: 5, border: "1px dashed #d4d4d8", background: "transparent", color: "#9ca3af", fontSize: 13, cursor: "pointer" }}>
                        + Trial
                      </button>
                    </div>

                    {/* Plan counter */}
                    <div style={{ padding: "6px 12px", background: "#f8f9fa", borderRadius: 7, border: "1px solid #e5e7eb", flexShrink: 0 }}>
                      <span style={{ fontSize: 13, color: "#71717a" }}>Plans: </span>
                      <span style={{ fontSize: 24, fontWeight: 800, fontFamily: "monospace", color: totalPlans > 6 ? "#e84545" : "#18181b" }}>
                        {totalPlans}
                      </span>
                    </div>

                    {/* JO list */}
                    {!trial.finished ? (
                      <div style={{ flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", gap: 3 }} className="thin-scrollbar">
                        {trial.orders.map(jo => {
                          const isSel = selectedJO === jo.id;
                          const stColor = jo.state === "initial" ? "#e5e7eb" : jo.state.includes("gem") ? "#bbf7d0" : "#fecaca";
                          return (
                            <div key={jo.id} onClick={() => setSelectedJO(jo.id)}
                              style={{ padding: "8px 10px", borderRadius: 7, cursor: "pointer",
                                border: isSel ? `2px solid #18181b` : `1px solid ${stColor}`,
                                background: isSel ? "#f0f1f3" : jo.state === "done" ? "#fafbfc" : "#fff",
                                opacity: jo.state === "done" ? 0.6 : 1 }}>
                              <div style={{ fontSize: 13, fontFamily: "monospace", fontWeight: 600, color: "#18181b", lineHeight: 1.5, wordBreak: "break-word" }}>
                                {jo.label}
                              </div>
                              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 3 }}>
                                <span style={{ fontSize: 11, color: "#9ca3af" }}>{jo.state}</span>
                                <span style={{ fontSize: 13, fontWeight: 700, fontFamily: "monospace", color: "#18181b" }}>×{jo.childCount}</span>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    ) : (
                      <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", color: "#9ca3af", fontSize: 14 }}>
                        Trial finished — {totalPlans} plans
                      </div>
                    )}

                    {/* Actions for selected JO */}
                    {!trial.finished && selJO && (
                      <div style={{ display: "flex", flexDirection: "column", gap: 4, flexShrink: 0 }}>
                        {/* Pushdown — available on initial or gem state */}
                        {(selJO.state === "initial" || selJO.state === "gem") && (
                          <button onClick={() => {
                            const mult = selJO.state === "gem" ? vgs * vgs : pGLCount * cGLCount;
                            // Generate child labels
                            const children: JoinOrder[] = [];
                            if (selJO.state === "gem") {
                              for (let pi = 0; pi < vgs; pi++) for (let ci = 0; ci < vgs; ci++) {
                                children.push({ id: `${selJO.id}_vg${pi}${ci}`, label: `${selJO.label.replace(/\(.*?\)/, `(VG-${pVar}${pi+1})`)} [VG-${cVar}${ci+1}]`,
                                  state: "done", childCount: 1 });
                              }
                            }
                            updateJO(selJO.id, { state: selJO.state === "gem" ? "gem+pushed" : "pushed", childCount: mult });
                            if (children.length) addOrders(children);
                          }} style={{ padding: "9px 0", borderRadius: 7, border: "none", background: "#e84545", color: "#fff", fontSize: 13, fontWeight: 700, cursor: "pointer" }}>
                            PushJoinBelowUnionAll ({selJO.state === "gem" ? `${vgs}×${vgs}=${vgs*vgs}` : `${pGLCount}×${cGLCount}`} sub-trees)
                          </button>
                        )}
                        {/* GEM — available on initial state */}
                        {selJO.state === "initial" && (
                          <button onClick={() => {
                            updateJO(selJO.id, {
                              state: "gem",
                              label: `UnionAll(VG-${pVar}1..${vgs}) ⋈ :${edgeType} ⋈ ${cVar}`,
                              childCount: 1,
                            });
                          }} style={{ padding: "9px 0", borderRadius: 7, border: "none", background: "#10B981", color: "#fff", fontSize: 13, fontWeight: 700, cursor: "pointer" }}>
                            Apply GEM (→ {vgs} VGs)
                          </button>
                        )}
                        {/* Find Equiv — available on pushed state */}
                        {selJO.state === "pushed" && (
                          <button onClick={() => {
                            const altCount = baseJoinOrders.length - 1;
                            updateJO(selJO.id, { state: "done", childCount: selJO.childCount * baseJoinOrders.length });
                          }} style={{ padding: "9px 0", borderRadius: 7, border: "1px dashed #d4d4d8", background: "transparent", color: "#71717a", fontSize: 13, fontWeight: 600, cursor: "pointer" }}>
                            Find Equivalent Plans (×{baseJoinOrders.length})
                          </button>
                        )}
                      </div>
                    )}

                    {/* Simulate + Finish */}
                    {!trial.finished && (
                      <div style={{ display: "flex", gap: 4, flexShrink: 0 }}>
                        <button onClick={() => {
                          // Simulate: set all initial JOs to pushed+equiv
                          const fullCount = baseJoinOrders.length * pGLCount * cGLCount * baseJoinOrders.length;
                          setSimulateOverlay("running"); setSimulatedCount(0);
                          let s = 0; const steps = 40;
                          const tick = () => { s++; setSimulatedCount(Math.round((1 - Math.pow(1 - s / steps, 3)) * fullCount)); if (s < steps) setTimeout(tick, 50); else setSimulateOverlay("done"); };
                          setTimeout(tick, 200);
                        }} style={{ flex: 1, padding: "8px 0", borderRadius: 7, border: "none", background: "#18181b", color: "#fff", fontSize: 12, fontWeight: 700, cursor: "pointer" }}>
                          Simulate All
                        </button>
                        <button onClick={finishTrial}
                          style={{ flex: 1, padding: "8px 0", borderRadius: 7, border: "1px solid #e5e7eb", background: "transparent", color: "#71717a", fontSize: 12, fontWeight: 600, cursor: "pointer" }}>
                          Finish Trial
                        </button>
                      </div>
                    )}

                    <button onClick={() => setPhase("physical")}
                      style={{ padding: "9px 0", borderRadius: 8, border: "1px solid #e5e7eb", background: "transparent", color: "#18181b", fontSize: 13, fontWeight: 600, cursor: "pointer", width: "100%", flexShrink: 0 }}>
                      Proceed to Implementation &rarr;
                    </button>
                  </div>

                  {/* Right: SVG plan tree — dynamic based on selected JO state */}
                  {(() => {
                    // Build plan tree based on selected JO's state
                    const mkLD = (a: PlanNode, b: PlanNode, c: PlanNode, jd1: string, jd2: string): PlanNode => ({
                      op: "Join", color: oc("NAryJoin"), detail: jd2, children: [
                        { op: "Join", color: oc("NAryJoin"), detail: jd1, children: [a, b] }, c ] });

                    let tree: PlanNode;
                    if (!selJO || selJO.state === "initial") {
                      // Default logical plan
                      tree = logicalPlan ?? { op: "Project", color: oc("Projection"), detail: retDetail };
                    } else if (selJO.state === "gem") {
                      // GEM applied: VG-based plan
                      const gVgs = 2;
                      tree = { op: "Project", color: oc("Projection"), detail: retDetail, children: [
                        { op: "UnionAll", color: oc("UnionAll"), detail: `${gVgs} virtual graphlets`, children:
                          Array.from({ length: gVgs }, (_, i) =>
                            mkLD(
                              { op: "Get", color: "#10B981", detail: `VG-${pVar}${i + 1}` },
                              { op: "Get", color: oc("GetEdge"), detail: `:${edgeType}` },
                              { op: "Get", color: oc("Get"), detail: cVar },
                              `\u22c8 :${edgeType}`, `\u22c8 ${cVar}`))
                        }] };
                    } else if (selJO.state === "pushed" || selJO.state === "gem+pushed") {
                      // Pushdown: UnionAll with sub-tree samples
                      const isGem = selJO.state === "gem+pushed";
                      const sampleCount = isGem ? 2 * 2 : 4; // show 4 samples
                      const totalST = isGem ? 2 * 2 : pGLCount * cGLCount;
                      const sampleP = isGem ? [{ label: `VG-${pVar}1` }, { label: `VG-${pVar}2` }] : pushdownGLs.slice(0, 2).map(g => ({ label: `GL-${g.id}` }));
                      const sampleC = isGem ? [{ label: `VG-${cVar}1` }, { label: `VG-${cVar}2` }] : (vp.graphlets.filter(g => (boundNodes.get(cVar) ?? []).includes(g.id)).sort((a, b) => b.rows - a.rows).slice(0, 2).map(g => ({ label: `GL-${g.id}` })));
                      const subTrees: PlanNode[] = [];
                      for (const sp of sampleP) {
                        for (const sc of sampleC) {
                          subTrees.push(mkLD(
                            { op: "Get", color: isGem ? "#10B981" : oc("Get"), detail: sp.label },
                            { op: "Get", color: oc("GetEdge"), detail: `:${edgeType}` },
                            { op: "Get", color: isGem ? "#10B981" : oc("Get"), detail: sc.label },
                            `\u22c8 :${edgeType}`, `\u22c8 ${sc.label}`));
                        }
                      }
                      if (totalST > subTrees.length) subTrees.push({ op: "...", color: "#9ca3af", detail: `+${(totalST - subTrees.length).toLocaleString()} more` });
                      tree = { op: "Project", color: oc("Projection"), detail: retDetail, children: [
                        { op: "UnionAll", color: oc("UnionAll"), detail: `${totalST.toLocaleString()} sub-trees`, children: subTrees }] };
                    } else {
                      tree = logicalPlan ?? { op: "Project", color: oc("Projection"), detail: retDetail };
                    }

                    const tl2 = layoutTree(tree);
                    const tw2 = tl2.length > 0 ? Math.max(...tl2.map(n => n.x)) + CW / 2 : 400;
                    const th2 = tl2.length > 0 ? Math.max(...tl2.map(n => n.y)) + CH : 200;

                    return (
                      <div style={{ flex: 1, background: "#fafbfc", borderRadius: 10, border: "1px solid #e5e7eb", overflow: "hidden", position: "relative" }}>
                        <ZoomPanSVG width={tw2} height={th2}>
                          <PlanCards nodes={tl2} />
                        </ZoomPanSVG>
                        {selJO && <div style={{ position: "absolute", top: 8, left: 12, fontSize: 13, fontFamily: "monospace", fontWeight: 700, color: "#18181b",
                          background: "#fff", padding: "4px 10px", borderRadius: 5, border: "1px solid #e5e7eb", maxWidth: "80%", wordBreak: "break-word" }}>
                          {selJO.label} ({selJO.state})
                        </div>}
                        <div style={{ position: "absolute", bottom: 8, right: 12, fontSize: 11, color: "#b4b4b8" }}>scroll to zoom, drag to pan</div>
                      </div>
                    );
                  })()}
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
                  {baseJoinOrders.length} orderings &times; {pushdownGLs.length} graphlets &times; {baseJoinOrders.length}<sup>{qNodes.length}</sup> sub-orders
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
