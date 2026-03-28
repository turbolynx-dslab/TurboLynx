"use client";
import { useEffect, useRef, useState, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { DBPEDIA_NODES, DEMO_PERSONS, PERSON_TABLE_COLS, QUERY_ATTRS } from "@/lib/demo-data";

interface Props { step: number; onStep: (n: number) => void; }

// ─── Graph data types ─────────────────────────────────────────────────────────
interface GNode {
  id: string; name: string; group: string; color: string;
  schemaSize: number; schema: string[]; isSeed: boolean;
}
interface GEdge { s: number; d: number; l: string; }
interface GraphData { nodes: GNode[]; edges: GEdge[]; stats: Record<string,number>; }

// ─── Physics node ─────────────────────────────────────────────────────────────
interface SimNode extends GNode {
  x: number; y: number; vx: number; vy: number;
  fx: number | null; fy: number | null;
}

// ─── All columns for flat table ───────────────────────────────────────────────
const ALL_COLS = [
  "abstract","height","weight","birthDate","birthPlace","team","position","graduationYear",
  "orderInOffice","deathDate","spouse","genre","nationality","award","occupation",
  "runtime","budget","gross","imdbId","releaseDate","director","starring","musicComposer",
  "areaTotal","areaCode","elevation","population","utcOffset","timezone","postalCode","country",
  "isbn","lcc","numberOfPages","oclc","publisher","author","dcc","mediaType","language",
  "wikiPageID","wikiPageRevisionID",
];

const STEP_TITLES = [
  "",
  "Split approach: each node stores its own schema.",
  "Merge approach: one flat table. Catastrophic NULLs.",
];


// ─── Load graph data ──────────────────────────────────────────────────────────
function useGraphData() {
  const [data, setData] = useState<GraphData | null>(null);
  useEffect(() => {
    fetch("/dbpedia_graph.json")
      .then(r => r.json())
      .then(setData)
      .catch(() => setData(null));
  }, []);
  return data;
}

// ─── Force simulation (D3-style alpha cooling) ────────────────────────────────
function useForce(data: GraphData | null, w: number, h: number) {
  const nodesRef = useRef<SimNode[]>([]);
  const [tick, setTick] = useState(0);
  const animRef = useRef<number>(0);
  const alphaRef = useRef(1.0);
  const dragging = useRef<{ idx: number; ox: number; oy: number } | null>(null);

  useEffect(() => {
    cancelAnimationFrame(animRef.current);
    alphaRef.current = 1.0;
    if (!data || w === 0 || h === 0) return;
    // Place nodes in a grid-like spread so none start on top of each other
    const n = data.nodes.length;
    const cols = Math.ceil(Math.sqrt(n * (w / h)));
    const rows = Math.ceil(n / cols);
    nodesRef.current = data.nodes.map((nd, i) => ({
      ...nd,
      x: (0.5 + (i % cols)) / cols * w + (Math.random() - 0.5) * 20,
      y: (0.5 + Math.floor(i / cols)) / rows * h + (Math.random() - 0.5) * 20,
      vx: 0, vy: 0, fx: null, fy: null,
    }));
  }, [data, w, h]);

  useEffect(() => {
    if (!data || w === 0 || h === 0) return;
    const R = 18;
    let fr = 0;

    const sim = () => {
      const ns = nodesRef.current;
      if (!ns.length) { animRef.current = requestAnimationFrame(sim); return; }

      const alpha = alphaRef.current;
      // velocity decay (D3 default: multiply by 1 - 0.4 = 0.6 each tick)
      const vDecay = 0.55;

      for (let i = 0; i < ns.length; i++) {
        const n = ns[i];
        if (n.fx !== null) { n.x = n.fx; n.y = n.fy!; n.vx = 0; n.vy = 0; continue; }
        let fx = 0, fy = 0;

        // Many-body repulsion — clamp min distance to prevent explosion
        for (const m of ns) {
          if (m === n) continue;
          const dx = n.x - m.x, dy = n.y - m.y;
          const d = Math.sqrt(dx * dx + dy * dy);
          if (d === 0 || d > 180) continue;
          const eff = Math.max(d, 25);
          const str = alpha * 2200 / (eff * eff);
          fx += (dx / d) * str;
          fy += (dy / d) * str;
        }

        // Link spring (pull neighbors to target distance 100px)
        for (const e of data.edges) {
          const j = e.s === i ? e.d : e.d === i ? e.s : -1;
          if (j < 0) continue;
          const m = ns[j];
          const dx = m.x - n.x, dy = m.y - n.y;
          const d = Math.sqrt(dx * dx + dy * dy) + 0.01;
          const str = (d - 100) * 0.18 * alpha;
          fx += (dx / d) * str;
          fy += (dy / d) * str;
        }

        // Very weak center gravity — keeps graph visible but allows full spread
        fx += (w * 0.5 - n.x) * 0.002 * alpha;
        fy += (h * 0.5 - n.y) * 0.002 * alpha;

        n.vx = (n.vx + fx) * vDecay;
        n.vy = (n.vy + fy) * vDecay;
        n.x += n.vx; n.y += n.vy;
        if (n.x < R)     { n.x = R;     n.vx =  Math.abs(n.vx) * 0.2; }
        if (n.x > w - R) { n.x = w - R; n.vx = -Math.abs(n.vx) * 0.2; }
        if (n.y < R)     { n.y = R;     n.vy =  Math.abs(n.vy) * 0.2; }
        if (n.y > h - R) { n.y = h - R; n.vy = -Math.abs(n.vy) * 0.2; }
      }

      // Cool alpha — reaches ~0.001 in ~250 frames
      if (alpha > 0.001) alphaRef.current = alpha * 0.975;

      fr++;
      if (fr % 2 === 0) setTick(t => t + 1);
      animRef.current = requestAnimationFrame(sim);
    };
    animRef.current = requestAnimationFrame(sim);
    return () => cancelAnimationFrame(animRef.current);
  }, [data, w, h]);

  const startDrag = useCallback((idx: number, ex: number, ey: number) => {
    const n = nodesRef.current[idx];
    if (!n) return;
    dragging.current = { idx, ox: ex - n.x, oy: ey - n.y };
    n.fx = n.x; n.fy = n.y;
    alphaRef.current = 0.4; // reheat slightly on drag
  }, []);
  const moveDrag = useCallback((ex: number, ey: number) => {
    if (!dragging.current) return;
    const n = nodesRef.current[dragging.current.idx];
    if (n) { n.fx = ex - dragging.current.ox; n.fy = ey - dragging.current.oy; }
  }, []);
  const endDrag = useCallback(() => {
    if (!dragging.current) return;
    const n = nodesRef.current[dragging.current.idx];
    if (n) { n.fx = null; n.fy = null; n.vx = 0; n.vy = 0; }
    dragging.current = null;
  }, []);

  return { nodes: nodesRef.current, tick, startDrag, moveDrag, endDrag };
}

// ─── Cluster ellipse ─────────────────────────────────────────────────────────
function clusterBounds(nodes: SimNode[], group: string) {
  const ns = nodes.filter(n => n.group === group);
  if (!ns.length) return null;
  const xs = ns.map(n => n.x), ys = ns.map(n => n.y);
  const cx = xs.reduce((a, b) => a + b, 0) / ns.length;
  const cy = ys.reduce((a, b) => a + b, 0) / ns.length;
  return {
    cx, cy,
    rx: Math.max(45, Math.max(...xs.map(x => Math.abs(x - cx))) + 30),
    ry: Math.max(35, Math.max(...ys.map(y => Math.abs(y - cy))) + 30),
  };
}

// ─── Background "scale" dots (static, seeded) ────────────────────────────────
const BG_DOTS = Array.from({ length: 420 }, (_, i) => {
  // Deterministic pseudo-random using LCG
  const s1 = (i * 1664525 + 1013904223) & 0xffffffff;
  const s2 = (s1 * 1664525 + 1013904223) & 0xffffffff;
  return {
    x: ((s1 >>> 0) % 10000) / 100,   // 0-100 (percentage)
    y: ((s2 >>> 0) % 10000) / 100,
    r: 1 + (i % 3) * 0.5,
  };
});

// ─── Schema panel helpers ─────────────────────────────────────────────────────
const ATTR_BUCKETS = [
  { label: "1–5",   min: 1,  max: 5  },
  { label: "6–10",  min: 6,  max: 10 },
  { label: "11–15", min: 11, max: 15 },
  { label: "16–20", min: 16, max: 20 },
  { label: "21–25", min: 21, max: 25 },
  { label: "26+",   min: 26, max: Infinity },
];

// ─── Mini bar chart ───────────────────────────────────────────────────────────
function MiniBarChart({ bars, onSelect, selectedId, barMinWidth = 0 }: {
  bars: { label: string; count: number; id: string }[];
  onSelect: (id: string | null) => void;
  selectedId: string | null;
  barMinWidth?: number;
}) {
  const max = Math.max(1, ...bars.map(b => b.count));
  return (
    // height: 100% fills the flex parent; items grow to fill
    <div style={{ display: "flex", alignItems: "flex-end", gap: 2, height: "100%", minHeight: 80 }}>
      {bars.map(b => {
        const pct = (b.count / max) * 100;
        const active = selectedId === b.id;
        return (
          <div key={b.id} onClick={() => onSelect(active ? null : b.id)}
            style={{ display: "flex", flexDirection: "column", alignItems: "center",
              gap: 2, cursor: "pointer", flex: barMinWidth ? `0 0 ${barMinWidth}px` : 1, minWidth: barMinWidth || 0, height: "100%" }}>
            {/* spacer + count */}
            <div style={{ flex: 1, display: "flex", flexDirection: "column",
              justifyContent: "flex-end", width: "100%" }}>
              <div style={{ fontSize: 12, fontFamily: "monospace", textAlign: "center",
                color: active ? "#93c5fd" : "#71717a", marginBottom: 2 }}>{b.count}</div>
              <div style={{
                width: "100%", height: `${Math.max(3, pct)}%`, borderRadius: "3px 3px 0 0",
                background: active ? "#3B82F6" : "#d4d4d8",
                border: `1px solid ${active ? "#60a5fa" : "#9ca3af"}`,
                transition: "background 0.15s",
              }} />
            </div>
            <div style={{ fontSize: 12, fontFamily: "monospace",
              color: active ? "#18181b" : "#71717a", textAlign: "center",
              whiteSpace: "nowrap", overflow: "hidden", width: "100%",
              textOverflow: "ellipsis", flexShrink: 0 }}>{b.label}</div>
          </div>
        );
      })}
    </div>
  );
}

// ─── Schema attr lookup (seed nodes only, matched by name) ───────────────────
const SCHEMA_LOOKUP: Record<string, readonly string[]> = Object.fromEntries(
  DBPEDIA_NODES.map(n => [n.name, n.schema])
);
const GROUP_COLOR: Record<string, string> = {
  Person: "#3B82F6", Film: "#8B5CF6", City: "#F59E0B",
  Book: "#10B981", Organisation: "#e84545",
};

// ─── canonical schema key ─────────────────────────────────────────────────────
const canonicalKey = (schema: string[]) => [...new Set(schema)].sort().join("|");

// ─── Schema Distribution Panel ────────────────────────────────────────────────
function SchemaDistPanel({ data, activeRange, onRange, onHighlightIds }: {
  data: GraphData | null;
  activeRange: { min: number; max: number } | null;
  onRange: (r: { min: number; max: number } | null) => void;
  onHighlightIds: (ids: Set<string> | null) => void;
}) {
  const nodes = data?.nodes ?? [];
  const [selectedSchemaKey, setSelectedSchemaKey] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);

  // Chart 1: attribute count buckets
  const attrBars = ATTR_BUCKETS.map(b => ({
    label: b.label,
    count: nodes.filter(n => n.schemaSize >= b.min && n.schemaSize <= b.max).length,
    id: `${b.min}-${b.max}`,
    min: b.min, max: b.max,
  }));
  const activeRangeId = activeRange ? `${activeRange.min}-${activeRange.max}` : null;

  // Chart 2: group by canonical schema identity
  const schemaGroupMap = new Map<string, { count: number; nodeIds: string[]; schemaSize: number }>();
  for (const n of nodes) {
    const key = n.schema.length > 0 ? canonicalKey(n.schema) : `__solo_${n.id}`;
    const entry = schemaGroupMap.get(key) ?? { count: 0, nodeIds: [], schemaSize: n.schemaSize };
    entry.count++;
    entry.nodeIds.push(n.id);
    schemaGroupMap.set(key, entry);
  }
  const uniqueBars = Array.from(schemaGroupMap.entries())
    .sort((a, b) => b[1].count - a[1].count)
    .map(([key, { count, schemaSize }]) => ({
      label: String(schemaSize),
      count,
      id: key,
    }));

  const handleAttrSelect = (id: string | null) => {
    const b = attrBars.find(x => x.id === id);
    onRange(b ? { min: b.min, max: b.max } : null);
    onHighlightIds(null);
    setSelectedSchemaKey(null);
    setSelectedNodeId(null);
  };

  const handleUniqueSelect = (id: string | null) => {
    setSelectedSchemaKey(id);
    setSelectedNodeId(null);
    onRange(null);
    if (id) {
      const ids = schemaGroupMap.get(id)?.nodeIds ?? [];
      onHighlightIds(new Set(ids));
    } else {
      onHighlightIds(null);
    }
  };

  // Nodes for the selected schema (exact match by canonical key)
  const matchedNodes = selectedSchemaKey != null
    ? nodes.filter(n => {
        const key = n.schema.length > 0 ? canonicalKey(n.schema) : `__solo_${n.id}`;
        return key === selectedSchemaKey;
      })
    : [];

  const Divider = ({ style }: { style?: React.CSSProperties }) =>
    <div style={{ height: 1, background: "#e5e7eb50", flexShrink: 0, ...style }} />;

  const StatCard = ({ v, l, accent }: { v: string; l: string; accent?: boolean }) => (
    <div style={{ padding: "9px 10px", background: "#fff", borderRadius: 7,
      border: `1px solid ${accent ? "#e8454520" : "#f0f1f3"}` }}>
      <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "monospace",
        color: accent ? "#e84545" : "#18181b", lineHeight: 1, whiteSpace: "nowrap" }}>{v}</div>
      <div style={{ fontSize: 12, color: "#71717a", marginTop: 4, whiteSpace: "nowrap" }}>{l}</div>
    </div>
  );

  const SectionLabel = ({ children }: { children: React.ReactNode }) => (
    <div style={{ fontSize: 14, fontWeight: 600, color: "#52525b", letterSpacing: "0.04em",
      marginBottom: 8, textTransform: "uppercase" }}>
      {children}
    </div>
  );

  return (
    <div style={{
      width: 340, flexShrink: 0, alignSelf: "center",
      display: "flex", flexDirection: "column", gap: 0,
      overflow: "hidden",
      background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
      padding: "14px 16px",
    }}>
      {/* DBpedia full */}
      <div style={{ flexShrink: 0 }}>
        <SectionLabel>DBpedia (Full Dataset)</SectionLabel>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1.5fr", gap: 7 }}>
          <StatCard v="77 M"    l="nodes" />
          <StatCard v="388 M"   l="edges" />
          <StatCard v="282,764" l="unique attr. sets" accent />
        </div>
      </div>

      <Divider style={{ margin: "10px 0" }} />

      {/* Sample */}
      <div style={{ flexShrink: 0 }}>
        <SectionLabel>This Sample</SectionLabel>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1.5fr", gap: 7 }}>
          <StatCard v={String(nodes.length)}          l="nodes" />
          <StatCard v={String(data?.edges.length ?? 0)} l="edges" />
          <StatCard v={String(uniqueBars.length)}     l="unique attr. sets" />
        </div>
      </div>

      <Divider style={{ margin: "10px 0" }} />

      {/* Chart 1 */}
      <div style={{ flexShrink: 0 }}>
        <SectionLabel>
          # Attributes → # Nodes
          <span style={{ color: "#b4b4b8", fontWeight: 400, marginLeft: 5, fontSize: 13, letterSpacing: 0 }}>click to filter</span>
        </SectionLabel>
        <div style={{ height: 120 }}>
          <MiniBarChart bars={attrBars} onSelect={handleAttrSelect} selectedId={activeRangeId} />
        </div>
      </div>

      <Divider style={{ margin: "10px 0" }} />

      {/* Chart 2 */}
      <div style={{ flexShrink: 0 }}>
        <SectionLabel>
          Unique Attribute Set → # Nodes
          <span style={{ color: "#b4b4b8", fontWeight: 400, marginLeft: 5, fontSize: 13, letterSpacing: 0 }}>click to filter</span>
        </SectionLabel>
        <div style={{ height: 120, overflowX: "auto", overflowY: "hidden" }} className="thin-scrollbar">
          <MiniBarChart bars={uniqueBars} onSelect={handleUniqueSelect} selectedId={selectedSchemaKey} barMinWidth={26} />
        </div>
      </div>

      {/* Schema Detail Drawer */}
      <AnimatePresence>
        {selectedSchemaKey != null && matchedNodes.length > 0 && (
          <motion.div
            key={selectedSchemaKey}
            initial={{ opacity: 0, height: 0, marginTop: 0 }}
            animate={{ opacity: 1, height: "auto", marginTop: 12 }}
            exit={{ opacity: 0, height: 0, marginTop: 0 }}
            transition={{ duration: 0.22, ease: "easeInOut" }}
            style={{ overflow: "hidden" }}
          >
            <Divider style={{ marginBottom: 12 }} />

            {/* Node list */}
            <div style={{ fontSize: 12, fontWeight: 600, color: "#6b7280",
              letterSpacing: "0.06em", textTransform: "uppercase", marginBottom: 6 }}>
              Nodes
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 3, marginBottom: 12 }}>
              {matchedNodes.map(n => {
                const color = GROUP_COLOR[n.group] ?? "#71717a";
                const isActive = selectedNodeId === n.id;
                return (
                  <div key={n.id}
                    onClick={() => setSelectedNodeId(isActive ? null : n.id)}
                    style={{ display: "flex", alignItems: "center", gap: 8,
                      padding: "5px 8px", borderRadius: 6, cursor: "pointer",
                      background: isActive ? "#f0f1f3" : "#f8f9fa",
                      border: `1px solid ${isActive ? color + "60" : "#f0f1f3"}`,
                      transition: "all 0.15s" }}>
                    <div style={{ width: 7, height: 7, borderRadius: "50%",
                      background: color, flexShrink: 0 }} />
                    <span style={{ fontSize: 14, color: isActive ? "#18181b" : "#52525b",
                      flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {n.name}
                    </span>
                  </div>
                );
              })}
            </div>

            {/* Schema (shown when a node is selected) */}
            <AnimatePresence>
              {selectedNodeId && (() => {
                const n = matchedNodes.find(x => x.id === selectedNodeId);
                const schema = n?.schema?.length ? n.schema : (n ? SCHEMA_LOOKUP[n.name] : null);
                const color = n ? GROUP_COLOR[n.group] ?? "#71717a" : "#71717a";
                return (
                  <motion.div key={selectedNodeId}
                    initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: 6 }} transition={{ duration: 0.15 }}>
                    <div style={{ fontSize: 12, fontWeight: 600, color: "#6b7280",
                      letterSpacing: "0.06em", textTransform: "uppercase", marginBottom: 6 }}>
                      Schema
                    </div>
                    <div style={{ background: "#f8f9fa", borderRadius: 7,
                      border: `1px solid ${color}40`, padding: "10px 10px" }}>
                      {schema?.length ? (
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
                          {schema.map(attr => (
                            <span key={attr} style={{
                              fontSize: 12, fontFamily: "monospace",
                              background: "#f0f1f3", border: "1px solid #d4d4d8",
                              color: "#52525b", padding: "2px 6px", borderRadius: 4,
                            }}>{attr}</span>
                          ))}
                        </div>
                      ) : (
                        <span style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace" }}>
                          · · · {n?.schemaSize} props
                        </span>
                      )}
                    </div>
                  </motion.div>
                );
              })()}
            </AnimatePresence>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ─── Main Graph Component ────────────────────────────────────────────────────
function DBpediaGraph({ step, data, schemaRange, highlightIds }: {
  step: number; data: GraphData | null;
  schemaRange?: { min: number; max: number } | null;
  highlightIds?: Set<string> | null;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ w: 0, h: 0 });
  const [selected, setSelected] = useState<SimNode | null>(null);
  const [zoom, setZoom] = useState(1.4);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [isPanning, setIsPanning] = useState(false);
  const panningRef = useRef<{ startX: number; startY: number; panX: number; panY: number } | null>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const { nodes, tick, startDrag, moveDrag, endDrag } = useForce(data, size.w, size.h);

  void tick;

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect;
      setSize({ w: Math.floor(width), h: Math.floor(height) });
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Screen → world coords (inverse of full transform: pan then zoom around center)
  const getSVGPt = (e: React.MouseEvent) => {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect) return { x: 0, y: 0 };
    const sx = e.clientX - rect.left, sy = e.clientY - rect.top;
    const cx = size.w / 2, cy = size.h / 2;
    return {
      x: (sx - pan.x - cx) / zoom + cx,
      y: (sy - pan.y - cy) / zoom + cy,
    };
  };

  const handleWheel = (e: React.WheelEvent) => {
    e.preventDefault();
    setZoom(z => Math.max(0.4, Math.min(4.0, z * (1 - e.deltaY * 0.001))));
  };

  // Pan handlers — only fire when mousedown lands on SVG background (not a node)
  const handleBgMouseDown = (e: React.MouseEvent) => {
    if (e.button !== 0) return;
    panningRef.current = { startX: e.clientX, startY: e.clientY, panX: pan.x, panY: pan.y };
    setIsPanning(true);
  };
  const handleMouseMove = (e: React.MouseEvent) => {
    moveDrag(getSVGPt(e).x, getSVGPt(e).y);
    if (panningRef.current) {
      const { startX, startY, panX, panY } = panningRef.current;
      setPan({ x: panX + e.clientX - startX, y: panY + e.clientY - startY });
    }
  };
  const handleMouseUp = () => { endDrag(); panningRef.current = null; setIsPanning(false); };

  const groups = Array.from(new Set((data?.nodes ?? []).map(n => n.group)));

  // Full transform: pan offset + zoom around center
  const cx = size.w / 2, cy = size.h / 2;
  const viewTransform = `translate(${cx + pan.x},${cy + pan.y}) scale(${zoom}) translate(${-cx},${-cy})`;


  return (
    <div ref={containerRef} style={{ position: "relative", flex: 1, minWidth: 0, minHeight: 0, overflow: "hidden" }}>
      <svg ref={svgRef} width={size.w} height={size.h}
        style={{ display: "block", cursor: isPanning ? "grabbing" : "grab" }}
        onMouseDown={handleBgMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
        onWheel={handleWheel}>

        {/* Background dots — always dim */}
        <g opacity={0.06} transform={viewTransform}>
          {BG_DOTS.map((d, i) => (
            <circle key={i}
              cx={d.x * size.w / 100} cy={d.y * size.h / 100} r={d.r}
              fill="#52525b" />
          ))}
        </g>

        {/* Sample graph */}
        {data && (
          <g transform={viewTransform}>
            {/* Cluster ellipses — only on step 1+ */}
            {step >= 1 && groups.map(group => {
              const b = clusterBounds(nodes, group);
              const col = data.nodes.find(n => n.group === group)?.color ?? "#71717a";
              if (!b) return null;
              return (
                <g key={group}>
                  <ellipse cx={b.cx} cy={b.cy} rx={b.rx} ry={b.ry}
                    fill="none" stroke={col} strokeWidth={1}
                    strokeDasharray="5 4" opacity={0.3} />
                  <text x={b.cx - b.rx + 8} y={b.cy - b.ry + 13}
                    fontSize={12} fill={col} opacity={0.6}
                    fontFamily="monospace" fontWeight={600}>{group}</text>
                </g>
              );
            })}

            {/* Edges */}
            {data.edges.map((e, i) => {
              const src = nodes[e.s], dst = nodes[e.d];
              if (!src || !dst) return null;
              const dx = dst.x - src.x, dy = dst.y - src.y;
              const len = Math.sqrt(dx * dx + dy * dy) || 1;
              const sr = src.isSeed ? 14 : 8, dr = dst.isSeed ? 14 : 8;
              const x1 = src.x + dx / len * sr, y1 = src.y + dy / len * sr;
              const x2 = dst.x - dx / len * dr, y2 = dst.y - dy / len * dr;
              const mx = (x1 + x2) / 2, my = (y1 + y2) / 2;
              const showLabel = len > 55;
              // Rotate label along edge direction
              const angle = Math.atan2(dy, dx) * 180 / Math.PI;
              const flip = Math.abs(angle) > 90;
              return (
                <g key={i}>
                  <line x1={x1} y1={y1} x2={x2} y2={y2}
                    stroke="#9ca3af" strokeWidth={1} opacity={0.6} />
                  {/* Arrowhead */}
                  <circle cx={x2} cy={y2} r={2} fill="#6b7280" opacity={0.5} />
                  {/* Edge label */}
                  {showLabel && (
                    <text textAnchor="middle" fontSize={8.5} fill="#6b7280" fontFamily="monospace"
                      transform={`translate(${mx},${my}) rotate(${flip ? angle + 180 : angle})`}
                      dy={-3} style={{ pointerEvents: "none", userSelect: "none" }}>
                      {e.l}
                    </text>
                  )}
                </g>
              );
            })}

            {/* Nodes */}
            {nodes.map((n, i) => {
              const r = n.isSeed ? 14 : 8;
              const isSel = selected?.id === n.id;
              const col = n.color;
              const hasFilter = highlightIds != null || schemaRange != null;
              const dimmed = highlightIds != null
                ? !highlightIds.has(n.id)
                : schemaRange != null && !(n.schemaSize >= schemaRange.min && n.schemaSize <= schemaRange.max);
              const highlighted = hasFilter && !dimmed;
              const label = n.name.length > 12 ? n.name.slice(0, 11) + "…" : n.name;
              const effectiveR = highlighted ? r * 1.6 : r;
              return (
                <g key={n.id} style={{ cursor: "pointer", opacity: dimmed ? 0.08 : 1, transition: "opacity 0.25s" }}
                  onMouseDown={e => { e.preventDefault(); e.stopPropagation(); const p = getSVGPt(e); startDrag(i, p.x, p.y); }}
                  onClick={() => { if (step === 1) setSelected(s => s?.id === n.id ? null : n); }}>
                  {/* Glow for highlighted or seed nodes */}
                  {(n.isSeed || highlighted) && (
                    <circle cx={n.x} cy={n.y} r={effectiveR + (highlighted ? 8 : 5)}
                      fill={col + (highlighted ? "30" : "15")} stroke="none" />
                  )}
                  <circle cx={n.x} cy={n.y} r={effectiveR}
                    fill={col + (highlighted ? "55" : n.isSeed ? "22" : "15")}
                    stroke={isSel ? col : col + (highlighted ? "ee" : n.isSeed ? "cc" : "66")}
                    strokeWidth={isSel ? 2.5 : highlighted ? 2.5 : n.isSeed ? 1.5 : 1} />
                  {/* Node label inside */}
                  <text x={n.x} y={n.y} textAnchor="middle" dominantBaseline="middle"
                    fontSize={n.isSeed ? 8 : 7} fill={n.isSeed ? col + "ee" : col + "bb"}
                    fontFamily="monospace" fontWeight={600}
                    style={{ pointerEvents: "none", userSelect: "none" }}>
                    {n.isSeed ? label : n.group.slice(0, 1)}
                  </text>
                  {/* Schema size badge on seed nodes */}
                  {n.isSeed && step === 0 && (
                    <text x={n.x + r + 3} y={n.y - r + 3} textAnchor="start"
                      fontSize={8} fill="#71717a" fontFamily="monospace">
                      {n.schemaSize}p
                    </text>
                  )}
                </g>
              );
            })}
          </g>
        )}

        {/* Schema hint (step 1) */}
        {step === 1 && !selected && (
          <text x={size.w - 10} y={size.h - 10} textAnchor="end"
            fontSize={9.5} fill="#9ca3af" fontFamily="monospace">
            Click any seed node to see its schema →
          </text>
        )}
      </svg>


      {/* Zoom controls */}
      <div style={{ position: "absolute", bottom: 10, left: 10, display: "flex",
        flexDirection: "column", gap: 3 }}>
        {[{ label: "+", delta: 0.2 }, { label: "−", delta: -0.2 }].map(btn => (
          <button key={btn.label} onClick={() => setZoom(z => Math.max(0.5, Math.min(3, z + btn.delta)))}
            style={{ width: 26, height: 26, borderRadius: 5, border: "1px solid #9ca3af",
              background: "#f8f9fa", color: "#52525b", fontSize: 17, lineHeight: 1,
              cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}>
            {btn.label}
          </button>
        ))}
        <button onClick={() => setZoom(1)} style={{ width: 26, height: 14, borderRadius: 4,
          border: "1px solid #d4d4d8", background: "#f0f1f3", color: "#6b7280",
          fontSize: 8, cursor: "pointer", fontFamily: "monospace" }}>
          1:1
        </button>
      </div>

      {/* Schema Panel */}
      {step === 1 && selected && (
        <motion.div
          key={selected.id}
          initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0 }}
          style={{
            position: "absolute", top: 0, right: 0, bottom: 0, width: 210,
            background: "#f8f9fa", border: `1px solid ${selected.color}40`,
            borderRadius: 12, padding: "14px 12px", overflow: "auto", zIndex: 10,
          }}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 10 }}>
            <div>
              <div style={{ fontSize: 14, fontWeight: 600, color: "#18181b" }}>{selected.name}</div>
              <div style={{ fontSize: 12, color: selected.color, marginTop: 2 }}>
                {selected.group} · {selected.schemaSize} props
              </div>
            </div>
            <button onClick={() => setSelected(null)}
              style={{ background: "none", border: "none", color: "#6b7280", cursor: "pointer", fontSize: 17 }}>×</button>
          </div>
          <div style={{ fontSize: 12, color: "#6b7280", fontFamily: "monospace" }}>
            {selected.schemaSize} unique properties in this node&apos;s schema
          </div>
          <div style={{ marginTop: 8, display: "flex", flexDirection: "column", gap: 4 }}>
            {Array.from({ length: Math.min(selected.schemaSize, 18) }).map((_, i) => (
              <div key={i} style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <div style={{ height: 3, borderRadius: 2, background: selected.color + "55",
                  width: Math.max(8, 80 - i * 4) }} />
                <div style={{ height: 3, borderRadius: 2, background: "#d4d4d8",
                  flex: 1 }} />
              </div>
            ))}
          </div>
          {selected.schemaSize > 18 && (
            <div style={{ fontSize: 9, color: "#6b7280", marginTop: 6, fontFamily: "monospace" }}>
              +{selected.schemaSize - 18} more properties...
            </div>
          )}
        </motion.div>
      )}
    </div>
  );
}

// ─── Shared query bar ────────────────────────────────────────────────────────
function QueryBar({ attr, onAttr, onRun, running }: {
  attr: string; onAttr: (a: string) => void;
  onRun?: () => void; running?: boolean;
}) {
  return (
    <div style={{ flexShrink: 0, display: "flex", alignItems: "center", gap: 10,
      padding: "7px 7px 7px 14px", background: "#f0f1f3", borderRadius: 9,
      border: "1px solid #d4d4d8", fontFamily: "monospace", fontSize: 14 }}>
      <span style={{ color: "#6b7280", whiteSpace: "nowrap" }}>
        SELECT * FROM{" "}
        <span style={{ color: "#f97316", fontWeight: 600 }}>DBPEDIA</span>
        {" "}WHERE
      </span>
      <div style={{ display: "flex", gap: 4 }}>
        {QUERY_ATTRS.map(a => (
          <button key={a.key} onClick={() => onAttr(a.key)}
            style={{ padding: "3px 10px", borderRadius: 5, cursor: "pointer",
              fontSize: 14, fontWeight: 600, fontFamily: "monospace",
              border: `1px solid ${attr === a.key ? "#3B82F6" : "#d4d4d8"}`,
              background: attr === a.key ? "#1d3a5f" : "transparent",
              color: attr === a.key ? "#93c5fd" : "#6b7280",
              transition: "all 0.15s" }}>
            {a.label}
          </button>
        ))}
      </div>
      <span style={{ color: "#6b7280", whiteSpace: "nowrap" }}>IS NOT NULL</span>
      {onRun && (
        <button onClick={onRun} disabled={running}
          style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 5,
            padding: "5px 13px", borderRadius: 6, border: "none", cursor: running ? "default" : "pointer",
            background: running ? "#f0f1f3" : "#e84545",
            color: running ? "#6b7280" : "#fff",
            fontSize: 14, fontWeight: 700, fontFamily: "monospace",
            letterSpacing: "0.06em", transition: "all 0.2s",
            opacity: running ? 0.6 : 1 }}>
          {running
            ? <><span style={{ fontSize: 9, animation: "spin 1s linear infinite" }}>◌</span> running</>
            : <>▶ RUN</>}
        </button>
      )}
    </div>
  );
}

// ─── Split View (Step 2) ──────────────────────────────────────────────────────
function SplitView() {
  const [attr, setAttr] = useState<string>(QUERY_ATTRS[0].key);
  const [scanIdx, setScanIdx] = useState(-1);
  const [results, setResults] = useState<Record<string, boolean>>({});
  const [done, setDone] = useState(false);
  const [running, setRunning] = useState(false);
  const cancelRef = useRef(false);
  const rowRefs = useRef<(HTMLDivElement | null)[]>([]);

  const runScan = useCallback((a: string) => {
    cancelRef.current = true;
    setScanIdx(-1); setResults({}); setDone(false); setRunning(true);
    setTimeout(() => {
      cancelRef.current = false;
      const timeouts: ReturnType<typeof setTimeout>[] = [];
      const perNode = 150;
      DEMO_PERSONS.forEach((p, i) => {
        timeouts.push(setTimeout(() => {
          if (cancelRef.current) return;
          setScanIdx(i);
        }, i * perNode));
        timeouts.push(setTimeout(() => {
          if (cancelRef.current) return;
          const hit = (p.schema as readonly string[]).includes(a);
          setResults(r => ({ ...r, [p.id]: hit }));
        }, i * perNode + 110));
      });
      timeouts.push(setTimeout(() => {
        if (!cancelRef.current) { setScanIdx(-1); setDone(true); setRunning(false); }
      }, DEMO_PERSONS.length * perNode + 150));
      return () => timeouts.forEach(clearTimeout);
    }, 50);
  }, []);

  // auto-scroll to scanning node
  useEffect(() => {
    if (scanIdx >= 0 && rowRefs.current[scanIdx]) {
      rowRefs.current[scanIdx]!.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
  }, [scanIdx]);

  // reset when attr changes (but don't auto-run)
  useEffect(() => {
    cancelRef.current = true;
    setScanIdx(-1); setResults({}); setDone(false); setRunning(false);
  }, [attr]);

  const matchCount = Object.values(results).filter(Boolean).length;
  const totalSchemaOps = DEMO_PERSONS.reduce((s, p) => s + p.schema.length, 0);
  const avgOps = Math.round(totalSchemaOps / DEMO_PERSONS.length);

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", gap: 10 }}>
      <QueryBar attr={attr} onAttr={setAttr} onRun={() => runScan(attr)} running={running} />

      {/* Node list */}
      <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 3 }}>
        {DEMO_PERSONS.map((p, i) => {
          const isScanning = scanIdx === i;
          const result = results[p.id];
          const hasResult = p.id in results;
          const hit = hasResult && result;
          const miss = hasResult && !result;
          const hasAttr = (p.schema as readonly string[]).includes(attr);

          return (
            <motion.div key={p.id}
              ref={(el: HTMLDivElement | null) => { rowRefs.current[i] = el; }}
              animate={{ borderColor: isScanning ? "#3B82F6" : hit ? "#22c55e50" : "#f0f1f3",
                         background: isScanning ? "#3B82F612" : hit ? "#22c55e08" : "#f8f9fa" }}
              transition={{ duration: 0.12 }}
              style={{ display: "flex", alignItems: "center", gap: 10,
                padding: "5px 12px", borderRadius: 7, border: "1px solid #f0f1f3",
                opacity: miss ? 0.4 : 1 }}>
              <div style={{ width: 180, flexShrink: 0, fontSize: 15, fontWeight: 600,
                color: "#27272a", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {p.name}
              </div>
              <div style={{ flex: 1, display: "flex", flexWrap: "wrap", gap: 3, minWidth: 0 }}>
                {(p.schema as readonly string[]).map(s => (
                  <span key={s} style={{
                    fontSize: 14, fontFamily: "monospace", padding: "1px 6px", borderRadius: 3,
                    background: s === attr ? (hasAttr ? "#22c55e20" : "#f0f1f3") : "#f0f1f3",
                    border: `1px solid ${s === attr ? (hasAttr ? "#22c55e70" : "#9ca3af") : "#d4d4d8"}`,
                    color: s === attr ? (hasAttr ? "#86efac" : "#52525b") : "#6b7280",
                    fontWeight: s === attr ? 700 : 400,
                  }}>{s}</span>
                ))}
              </div>
              <div style={{ width: 84, flexShrink: 0, textAlign: "right", fontFamily: "monospace", fontSize: 14 }}>
                {isScanning && <span style={{ color: "#60a5fa" }}>scanning…</span>}
                {hit  && <span style={{ color: "#4ade80" }}>✓ found</span>}
                {miss && <span style={{ color: "#6b7280" }}>✗ skip</span>}
              </div>
            </motion.div>
          );
        })}
      </div>

      {/* Stats boxes */}
      <AnimatePresence>
        {done && (
          <motion.div key="stats" initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}
            style={{ flexShrink: 0, display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 8 }}>
            <div style={{ background: "#f0faf5", border: "1px solid #22c55e40", borderRadius: 8, padding: "10px 14px" }}>
              <div style={{ fontSize: 26, fontWeight: 700, color: "#4ade80", fontFamily: "monospace" }}>
                {matchCount} <span style={{ fontSize: 15, color: "#71717a" }}>/ {DEMO_PERSONS.length}</span>
              </div>
              <div style={{ fontSize: 14, color: "#71717a", marginTop: 2 }}>nodes matched</div>
            </div>
            <div style={{ background: "#f8f9fa", border: "1px solid #d4d4d8", borderRadius: 8, padding: "10px 14px" }}>
              <div style={{ fontSize: 26, fontWeight: 700, color: "#18181b", fontFamily: "monospace" }}>
                {totalSchemaOps}
              </div>
              <div style={{ fontSize: 14, color: "#71717a", marginTop: 2 }}>
                attr checks ({DEMO_PERSONS.length} nodes × avg {avgOps})
              </div>
            </div>
            <div style={{ background: "#fef2f2", border: "1px solid #e8454540", borderRadius: 8, padding: "10px 14px" }}>
              <div style={{ fontSize: 26, fontWeight: 700, color: "#e84545", fontFamily: "monospace" }}>
                ~{Math.round(77_000_000 * avgOps / 1_000_000)}M
              </div>
              <div style={{ fontSize: 14, color: "#71717a", marginTop: 2 }}>attr checks/query at 77M nodes</div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ─── NULL Table (Step 3) ──────────────────────────────────────────────────────
function NullTableView() {
  const [attr, setAttr] = useState<string>(QUERY_ATTRS[0].key);
  const [scanRow, setScanRow] = useState(-1);
  const [done, setDone] = useState(false);
  const [running, setRunning] = useState(false);
  const cancelRef = useRef(false);
  const tableRef = useRef<HTMLDivElement>(null);
  const rowRefs2 = useRef<(HTMLTableRowElement | null)[]>([]);

  // auto-scroll table to scanning row
  useEffect(() => {
    if (scanRow >= 0 && rowRefs2.current[scanRow] && tableRef.current) {
      rowRefs2.current[scanRow]!.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
  }, [scanRow]);

  const ROWS = DEMO_PERSONS.map(p => ({
    ...p,
    cells: PERSON_TABLE_COLS.map(col => (p.schema as readonly string[]).includes(col)),
  }));
  const TOTAL_CELLS = ROWS.length * PERSON_TABLE_COLS.length;
  const NULL_COUNT  = ROWS.reduce((s, r) => s + r.cells.filter(v => !v).length, 0);
  const attrColIdx  = PERSON_TABLE_COLS.indexOf(attr as typeof PERSON_TABLE_COLS[number]);
  const matchInCol  = attrColIdx >= 0 ? ROWS.filter(r => r.cells[attrColIdx]).length : 0;

  const runScan = useCallback((a: string) => {
    cancelRef.current = true;
    setScanRow(-1); setDone(false); setRunning(true);
    setTimeout(() => {
      cancelRef.current = false;
      const perRow = 120;
      const ts: ReturnType<typeof setTimeout>[] = [];
      ROWS.forEach((_, i) => {
        ts.push(setTimeout(() => { if (!cancelRef.current) setScanRow(i); }, i * perRow));
      });
      ts.push(setTimeout(() => {
        if (!cancelRef.current) { setScanRow(-1); setDone(true); setRunning(false); }
      }, ROWS.length * perRow + 150));
      return () => ts.forEach(clearTimeout);
    }, 50);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    cancelRef.current = true;
    setScanRow(-1); setDone(false); setRunning(false);
  }, [attr]);

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", gap: 10 }}>
      <QueryBar attr={attr} onAttr={setAttr} onRun={() => runScan(attr)} running={running} />

      {/* Table */}
      <div ref={tableRef} style={{ flex: 1, minHeight: 0, overflow: "auto", borderRadius: 8,
        border: "1px solid #d4d4d8", background: "#f0f1f3" }}>
        <table style={{ fontSize: 15, fontFamily: "monospace", borderCollapse: "collapse", width: "100%" }}>
          <thead>
            <tr>
              <th style={{ padding: "6px 12px", color: "#71717a", textAlign: "left",
                borderBottom: "1px solid #d4d4d8", whiteSpace: "nowrap",
                position: "sticky", top: 0, background: "#f0f1f3", zIndex: 1, minWidth: 150 }}>
                name
              </th>
              {PERSON_TABLE_COLS.map((c, ci) => {
                const isActive = c === attr;
                return (
                  <th key={c} style={{ padding: "6px 10px", textAlign: "center",
                    borderBottom: `1px solid ${isActive ? "#3B82F6" : "#d4d4d8"}`,
                    whiteSpace: "nowrap", position: "sticky", top: 0, zIndex: 1,
                    background: isActive ? "#1d3a5f" : "#f0f1f3",
                    color: isActive ? "#93c5fd" : "#6b7280",
                    fontWeight: isActive ? 700 : 500 }}>
                    {c.length > 8 ? c.slice(0, 7) + "…" : c}
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {ROWS.map((row, ri) => {
              const isScanning = scanRow === ri;
              return (
                <motion.tr key={row.id}
                  ref={(el: HTMLTableRowElement | null) => { rowRefs2.current[ri] = el; }}
                  animate={{ background: isScanning ? "#3B82F60e" : "transparent" }}
                  transition={{ duration: 0.1 }}
                  style={{ borderBottom: "1px solid #f0f1f3" }}>
                  <td style={{ padding: "4px 12px", color: "#52525b", whiteSpace: "nowrap", fontWeight: 600 }}>
                    {row.name.length > 18 ? row.name.slice(0, 17) + "…" : row.name}
                  </td>
                  {row.cells.map((present, ci) => {
                    const isActiveCol = ci === attrColIdx;
                    const scanned = done || (running && ri < scanRow);
                    return (
                      <td key={ci} style={{ padding: "4px 10px", textAlign: "center", whiteSpace: "nowrap",
                        background: isActiveCol
                          ? (present ? "#22c55e12" : (scanned ? "#e8454512" : "transparent"))
                          : "transparent",
                        color: present
                          ? (isActiveCol ? "#4ade80" : "#6b7280")
                          : (isActiveCol && scanned ? "#e84545aa" : "#9ca3af"),
                        transition: "color 0.12s, background 0.12s",
                        fontSize: 15 }}>
                        {present ? (isActiveCol ? "✓" : "·") : "∅"}
                      </td>
                    );
                  })}
                </motion.tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Stats boxes */}
      <AnimatePresence>
        {done && (
          <motion.div key="stats" initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}
            style={{ flexShrink: 0, display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 8 }}>
            <div style={{ background: "#f0faf5", border: "1px solid #22c55e40", borderRadius: 8, padding: "10px 14px" }}>
              <div style={{ fontSize: 26, fontWeight: 700, color: "#4ade80", fontFamily: "monospace" }}>
                {matchInCol} <span style={{ fontSize: 15, color: "#71717a" }}>/ {ROWS.length}</span>
              </div>
              <div style={{ fontSize: 14, color: "#71717a", marginTop: 2 }}>rows have <em>{attr}</em></div>
            </div>
            <div style={{ background: "#fef2f2", border: "1px solid #e8454540", borderRadius: 8, padding: "10px 14px" }}>
              <div style={{ fontSize: 26, fontWeight: 700, color: "#e84545", fontFamily: "monospace" }}>
                {NULL_COUNT} <span style={{ fontSize: 15, color: "#71717a" }}>{Math.round(NULL_COUNT / TOTAL_CELLS * 100)}%</span>
              </div>
              <div style={{ fontSize: 14, color: "#71717a", marginTop: 2 }}>NULL cells — wasted storage</div>
            </div>
            <div style={{ background: "#fef2f2", border: "1px solid #e8454540", borderRadius: 8, padding: "10px 14px" }}>
              <div style={{ fontSize: 26, fontWeight: 700, color: "#e84545", fontFamily: "monospace" }}>
                ~740 GB
              </div>
              <div style={{ fontSize: 14, color: "#71717a", marginTop: 2 }}>
                merged table (raw: ~24 GB → 31× bloat)
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}


// ─── Main ─────────────────────────────────────────────────────────────────────
export default function S0_Problem({ step }: Props) {
  const data = useGraphData();
  const [schemaRange, setSchemaRange] = useState<{ min: number; max: number } | null>(null);
  const [highlightIds, setHighlightIds] = useState<Set<string> | null>(null);

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden",
      padding: "22px 0 18px" }}>
    <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column",
      width: "100%", maxWidth: 1440, margin: "0 auto", padding: "0 48px", gap: 13, overflow: "hidden" }}>
      {/* Content */}
      <div style={{ flex: 1, minHeight: 0, display: "flex", gap: 12, overflow: "hidden" }}>
        <DBpediaGraph step={0} data={data} schemaRange={schemaRange} highlightIds={highlightIds} />
        <SchemaDistPanel data={data} activeRange={schemaRange} onRange={setSchemaRange} onHighlightIds={setHighlightIds} />
      </div>
    </div>
    </div>
  );
}
