"use client";
import React, { useState, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface Props { step: number; onStep: (n: number) => void; }

// ─── Shared Data (same graphlets as S2 query plan) ──────────────────────────
const BLOAT_GROUPS = [
  { label: "Person",  gls: ["GL-1", "GL-2", "GL-3"], color: "#DC2626" },
  { label: "City",    gls: ["GL-6", "GL-7"],          color: "#0891B2" },
  { label: "Country", gls: ["GL-9", "GL-10"],         color: "#B45309" },
];
const EDGES = [":birthPlace", ":country"];
const NAIVE_PRODUCT = BLOAT_GROUPS.reduce((p, g) => p * g.gls.length, 1); // 3×2×2 = 12

// ─── Step 0: Join Bloating ──────────────────────────────────────────────────

const BLOAT_SCALES = [
  { counts: [3, 2, 2],    label: "This query" },
  { counts: [10, 8, 5],   label: "10× diversity" },
  { counts: [50, 30, 20], label: "DBpedia-scale" },
];

function CombinationSVG({ counts }: { counts: number[] }) {
  const W = 540, H = 300;
  const colX = [90, 270, 450];
  const maxVis = 8;

  const getYs = (count: number) => {
    const vis = Math.min(count, maxVis);
    const sp = Math.min(34, (H - 80) / (vis + 1));
    const y0 = 50 + ((H - 80) - (vis - 1) * sp) / 2;
    return Array.from({ length: vis }, (_, i) => y0 + i * sp);
  };

  const cols = BLOAT_GROUPS.map((g, i) => ({
    ...g, x: colX[i], count: counts[i],
    ys: getYs(counts[i]),
    hasMore: counts[i] > maxVis,
  }));

  const lineEls: React.ReactNode[] = [];
  for (let ci = 0; ci < 2; ci++) {
    const L = cols[ci], R = cols[ci + 1];
    const nLines = L.ys.length * R.ys.length;
    const opacity = Math.max(0.06, Math.min(0.4, 8 / nLines));
    L.ys.forEach((y1, li) => R.ys.forEach((y2, ri) => {
      lineEls.push(
        <line key={`${ci}-${li}-${ri}`} x1={L.x + 16} y1={y1} x2={R.x - 16} y2={y2}
          stroke={L.color} strokeWidth={1.5} opacity={opacity} />
      );
    }));
  }

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "100%" }}
      preserveAspectRatio="xMidYMid meet">
      {lineEls}
      {cols.map((col, ci) => (
        <g key={ci}>
          <text x={col.x} y={22} textAnchor="middle" fontSize={15} fontWeight={700}
            fill={col.color} fontFamily="monospace">{col.label} ({col.count})</text>
          {col.ys.map((y, di) => (
            <g key={di}>
              <rect x={col.x - 30} y={y - 12} width={60} height={24} rx={6}
                fill={col.color + "14"} stroke={col.color} strokeWidth={1.2} />
              <text x={col.x} y={y + 5} textAnchor="middle" fontSize={12} fill={col.color}
                fontFamily="monospace" fontWeight={600}>
                {di < BLOAT_GROUPS[ci].gls.length ? BLOAT_GROUPS[ci].gls[di] : `GL-${di + 1}`}
              </text>
            </g>
          ))}
          {col.hasMore && (
            <text x={col.x} y={col.ys[col.ys.length - 1] + 30} textAnchor="middle"
              fontSize={13} fill="#9ca3af" fontFamily="monospace">+{col.count - maxVis} more</text>
          )}
        </g>
      ))}
    </svg>
  );
}

function Step0() {
  const [scaleIdx, setScaleIdx] = useState(0);
  const s = BLOAT_SCALES[scaleIdx];
  const product = s.counts.reduce((a, b) => a * b, 1);
  const isExploding = product > 100;

  const PIPELINE = [
    { label: "Person",  count: s.counts[0], color: "#DC2626", gls: BLOAT_GROUPS[0].gls, edge: ":birthPlace" },
    { label: "City",    count: s.counts[1], color: "#0891B2", gls: BLOAT_GROUPS[1].gls, edge: ":country" },
    { label: "Country", count: s.counts[2], color: "#B45309", gls: BLOAT_GROUPS[2].gls, edge: null },
  ];

  return (
    <div style={{ display: "flex", gap: 24, height: "100%", overflow: "hidden" }}>
      {/* Left: Plan pipeline recap from S2 */}
      <div style={{ width: "30%", flexShrink: 0, display: "flex", flexDirection: "column", gap: 12 }}>
        <div style={{
          fontSize: 14, color: "#9ca3af", fontFamily: "monospace",
          background: "#fafbfc", borderRadius: 8, padding: "8px 14px",
          border: "1px solid #e5e7eb",
        }}>
          (p)-[:birthPlace]-&gt;(c)-[:country]-&gt;(co)
        </div>

        <div style={{ fontSize: 15, color: "#6b7280", fontFamily: "monospace", lineHeight: 1.5 }}>
          From the <span style={{ color: "#3B82F6", fontWeight: 600 }}>Query Plan</span>:
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 0, flex: 1 }}>
          {PIPELINE.map((p, i) => (
            <div key={i}>
              <div style={{
                background: p.color + "0C", border: `1.5px solid ${p.color}35`,
                borderRadius: 8, padding: "10px 14px",
              }}>
                <div style={{
                  fontSize: 15, fontWeight: 700, color: p.color,
                  fontFamily: "monospace", marginBottom: 6,
                }}>
                  UnionAll — {p.label}
                </div>
                <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
                  {(scaleIdx === 0 ? p.gls : Array.from({ length: Math.min(p.count, 5) }, (_, j) => `GL-${j + 1}`)).map(gl => (
                    <span key={gl} style={{
                      fontSize: 14, fontFamily: "monospace", color: p.color,
                      background: p.color + "18", padding: "2px 8px", borderRadius: 4,
                    }}>{gl}</span>
                  ))}
                  {p.count > 5 && (
                    <span style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace" }}>
                      +{p.count - 5} more
                    </span>
                  )}
                </div>
                <div style={{
                  fontSize: 14, color: p.color + "90", fontFamily: "monospace", marginTop: 5,
                }}>
                  {p.count} graphlet{p.count !== 1 ? "s" : ""}
                </div>
              </div>
              {p.edge && (
                <div style={{
                  textAlign: "center", padding: "5px 0",
                  color: "#9ca3af", fontFamily: "monospace", fontSize: 14,
                }}>
                  ↓ <span style={{ color: "#6b7280" }}>{p.edge}</span>
                </div>
              )}
            </div>
          ))}
        </div>

        <div style={{
          fontSize: 14, color: "#6b7280", fontFamily: "monospace", lineHeight: 1.6,
        }}>
          Optimizer evaluates <strong style={{ color: "#18181b" }}>every combination</strong> of
          graphlet assignments across these joins.
        </div>
      </div>

      {/* Right: Combination explosion */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 12, overflow: "hidden" }}>
        <div style={{
          flex: 1, minHeight: 0,
          background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
          overflow: "hidden",
        }}>
          <CombinationSVG counts={s.counts} />
        </div>

        <div style={{ textAlign: "center", padding: "2px 0" }}>
          <div style={{
            display: "flex", justifyContent: "center", alignItems: "baseline", gap: 10,
          }}>
            <span style={{ fontSize: 28, fontWeight: 800, fontFamily: "monospace", color: "#DC2626" }}>
              {s.counts[0]}
            </span>
            <span style={{ fontSize: 20, color: "#9ca3af" }}>×</span>
            <span style={{ fontSize: 28, fontWeight: 800, fontFamily: "monospace", color: "#0891B2" }}>
              {s.counts[1]}
            </span>
            <span style={{ fontSize: 20, color: "#9ca3af" }}>×</span>
            <span style={{ fontSize: 28, fontWeight: 800, fontFamily: "monospace", color: "#B45309" }}>
              {s.counts[2]}
            </span>
            <span style={{ fontSize: 20, color: "#9ca3af" }}>=</span>
            <motion.span
              key={product}
              initial={{ scale: 1.4, color: "#e84545" }}
              animate={{ scale: 1, color: isExploding ? "#e84545" : "#18181b" }}
              transition={{ type: "spring", stiffness: 400, damping: 20 }}
              style={{ fontSize: 40, fontWeight: 800, fontFamily: "monospace" }}
            >
              {product.toLocaleString()}
            </motion.span>
            <span style={{ fontSize: 16, color: "#6b7280", fontFamily: "monospace" }}>
              optimizer plans
            </span>
          </div>
        </div>

        <div style={{ display: "flex", gap: 8, justifyContent: "center", flexShrink: 0 }}>
          {BLOAT_SCALES.map((sc, i) => (
            <button key={i} onClick={() => setScaleIdx(i)} style={{
              padding: "8px 22px", borderRadius: 8, cursor: "pointer",
              fontSize: 15, fontFamily: "monospace", fontWeight: 600,
              border: `1.5px solid ${scaleIdx === i ? "#e84545" : "#d4d4d8"}`,
              background: scaleIdx === i ? "#e8454510" : "transparent",
              color: scaleIdx === i ? "#e84545" : "#71717a",
              transition: "all 0.2s",
            }}>
              {sc.label}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─── Step 1: Graphlet Early Merge with GOO ──────────────────────────────────

// Graphlet row counts (from LDBC SF1 catalog statistics)
// Person graphlets: GL-1 (44200), GL-2 (38100), GL-3 (12000)
// City graphlets: GL-6 (15000), GL-7 (8400)
// Country graphlets: GL-9 (200), GL-10 (50)
const GL_SIZES: Record<string, number> = {
  "GL-1": 44200, "GL-2": 38100, "GL-3": 12000,
  "GL-6": 15000, "GL-7": 8400,
  "GL-9": 200, "GL-10": 50,
};

// Edge selectivities: sel = output_rows / (left_rows * right_rows)
// :birthPlace — each person born in one city: ~1/total_cities ≈ 1/23400
// :country — each city in one country: ~1/total_countries ≈ 1/250
const EDGE_SEL_BP = 1 / 23400;  // :birthPlace
const EDGE_SEL_CO = 1 / 250;    // :country

// GOO join tree node
interface JoinNode {
  kind: "leaf" | "join";
  label: string;        // leaf: "p"/"c"/"co", join: "⋈"
  color: string;
  size: number;
  left?: JoinNode;
  right?: JoinNode;
}

// GOO edge definition
interface GOOEdge {
  i: number;  // component index
  j: number;  // component index
  sel: number; // selectivity
}

// Run GOO algorithm (Union-Find greedy, from CJoinOrderGEM.cpp)
function runGOO(
  nodes: JoinNode[],
  edges: GOOEdge[],
): { tree: JoinNode; cost: number } {
  const n = nodes.length;
  const size = nodes.map(nd => nd.size);
  const tree: JoinNode[] = nodes.map(nd => ({ ...nd }));

  // Union-Find
  const parent = Array.from({ length: n }, (_, i) => i);
  const ufRank = new Array(n).fill(0);

  function find(x: number): number {
    while (parent[x] !== x) { parent[x] = parent[parent[x]]; x = parent[x]; }
    return x;
  }
  function union(a: number, b: number): number {
    if (ufRank[a] < ufRank[b]) { parent[a] = b; return b; }
    if (ufRank[a] > ufRank[b]) { parent[b] = a; return a; }
    parent[b] = a; ufRank[a]++; return a;
  }

  let totalCost = 0;
  let numSets = n;

  while (numSets > 1) {
    let minCost = Infinity;
    let minI = 0, minJ = 0;

    for (const edge of edges) {
      const ri = find(edge.i), rj = find(edge.j);
      if (ri === rj) continue;
      const cost = size[ri] * size[rj] * edge.sel;
      if (cost < minCost) {
        minCost = cost;
        // Larger set as left child (matches CJoinOrderGEM behavior)
        if (size[ri] > size[rj]) { minI = ri; minJ = rj; }
        else { minI = rj; minJ = ri; }
      }
    }

    // Create join node: left=minJ (smaller), right=minI (larger)
    // In GOO: pexprLeft = tree[ulMinJ], pexprRight = tree[ulMinI]
    const joinNode: JoinNode = {
      kind: "join", label: "⋈", color: "#6b7280",
      size: minCost,
      left: tree[minJ],
      right: tree[minI],
    };

    totalCost += minCost;
    const newRoot = union(minI, minJ);
    size[newRoot] = minCost;
    tree[newRoot] = joinNode;
    numSets--;
  }

  const root = find(0);
  return { tree: tree[root], cost: Math.round(totalCost) };
}

// Build GOO inputs for a particular VG combination
// splitTarget: which node type is split (0=Person, 1=City, 2=Country)
// vgAssign: A/B assignment for the split target's graphlets
// vgGroup: which group to compute ("A" or "B")
function buildAndRunGOO(
  splitTarget: number,
  assigns: ("A" | "B")[],
  vgGroup: "A" | "B",
): { tree: JoinNode; cost: number } {
  const groups = BLOAT_GROUPS;
  // Compute sizes for each node type in this VG combo
  const nodeSizes = groups.map((g, gi) => {
    if (gi === splitTarget) {
      // Only sum graphlets in this VG group
      return g.gls
        .filter((_, i) => assigns[i] === vgGroup)
        .reduce((s, gl) => s + (GL_SIZES[gl] || 1000), 0);
    }
    // Non-split types: all graphlets together
    return g.gls.reduce((s, gl) => s + (GL_SIZES[gl] || 1000), 0);
  });

  // Build leaf nodes: p=0, c=1, co=2
  const leaves: JoinNode[] = [
    { kind: "leaf", label: "p", color: "#DC2626", size: nodeSizes[0] },
    { kind: "leaf", label: "c", color: "#0891B2", size: nodeSizes[1] },
    { kind: "leaf", label: "co", color: "#B45309", size: nodeSizes[2] },
  ];

  // Edges: p--c (:birthPlace), c--co (:country)
  const gooEdges: GOOEdge[] = [
    { i: 0, j: 1, sel: EDGE_SEL_BP },  // p ⋈ c
    { i: 1, j: 2, sel: EDGE_SEL_CO },  // c ⋈ co
  ];

  return runGOO(leaves, gooEdges);
}

// Describe the join order from a GOO tree
function describeOrder(node: JoinNode): string {
  if (node.kind === "leaf") return node.label;
  const l = describeOrder(node.left!);
  const r = describeOrder(node.right!);
  return `(${l} ⋈ ${r})`;
}

type GemGroup = "A" | "B";
const GEM_COLOR: Record<GemGroup, string> = { A: "#3B82F6", B: "#10B981" };

interface VG {
  grp: GemGroup;
  gls: string[];
  color: string;
}

function computeVGs(splitTarget: number, assigns: GemGroup[]): VG[][] {
  return BLOAT_GROUPS.map((g, gi) => {
    if (gi !== splitTarget) {
      // Non-split: single VG with all graphlets
      return [{ grp: "A" as GemGroup, gls: [...g.gls], color: "#71717a" }];
    }
    const glsA = g.gls.filter((_, i) => assigns[i] === "A");
    const glsB = g.gls.filter((_, i) => assigns[i] === "B");
    const vgs: VG[] = [];
    if (glsA.length > 0) vgs.push({ grp: "A", gls: glsA, color: GEM_COLOR.A });
    if (glsB.length > 0) vgs.push({ grp: "B", gls: glsB, color: GEM_COLOR.B });
    return vgs;
  });
}

function VirtualGroupSVG({ vgCols }: { vgCols: VG[][] }) {
  const W = 540, H = 300;
  const colX = [90, 270, 450];
  const vgBoxW = 100;

  const positioned = vgCols.map((vgs, ci) => {
    const boxH = vgs.map(vg => 28 + vg.gls.length * 20);
    const totalH = boxH.reduce((s, h) => s + h, 0) + (vgs.length - 1) * 16;
    let y = Math.max(40, (H - totalH) / 2);
    return vgs.map((vg, vi) => {
      const top = y;
      const h = boxH[vi];
      y += h + 16;
      return { ...vg, x: colX[ci], y: top, h, cy: top + h / 2 };
    });
  });

  const lineEls: React.ReactNode[] = [];
  for (let ci = 0; ci < 2; ci++) {
    const L = positioned[ci], R = positioned[ci + 1];
    L.forEach((lv, li) => R.forEach((rv, ri) => {
      const opacity = Math.max(0.15, 0.5 / (L.length * R.length));
      lineEls.push(
        <line key={`${ci}-${li}-${ri}`}
          x1={lv.x + vgBoxW / 2 + 2} y1={lv.cy}
          x2={rv.x - vgBoxW / 2 - 2} y2={rv.cy}
          stroke={lv.color === "#71717a" ? "#d4d4d8" : lv.color} strokeWidth={2.5} opacity={opacity}
          strokeDasharray={L.length * R.length <= 2 ? "none" : "6 3"} />
      );
    }));
  }

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "100%" }}
      preserveAspectRatio="xMidYMid meet">
      {BLOAT_GROUPS.map((g, i) => (
        <text key={`h-${i}`} x={colX[i]} y={22} textAnchor="middle" fontSize={15}
          fontWeight={700} fill={g.color} fontFamily="monospace">
          {g.label} ({vgCols[i].length} VG{vgCols[i].length !== 1 ? "s" : ""})
        </text>
      ))}
      {lineEls}
      {positioned.flat().map((vg, i) => (
        <g key={i}>
          <rect x={vg.x - vgBoxW / 2} y={vg.y} width={vgBoxW} height={vg.h}
            rx={8} fill={vg.color + "10"} stroke={vg.color} strokeWidth={1.5} />
          <text x={vg.x} y={vg.y + 18} textAnchor="middle" fontSize={13}
            fontWeight={700} fill={vg.color} fontFamily="monospace">
            VG-{vg.grp}
          </text>
          {vg.gls.map((gl, gli) => (
            <text key={gli} x={vg.x} y={vg.y + 36 + gli * 20} textAnchor="middle"
              fontSize={12} fill={vg.color + "99"} fontFamily="monospace">
              {gl}
            </text>
          ))}
        </g>
      ))}
    </svg>
  );
}

// ─── Recursive join tree rendering ──────────────────────────────────────────

interface TreeLayout {
  x: number;
  y: number;
  w: number;
}

function layoutTree(node: JoinNode, x: number, y: number, w: number): { node: JoinNode; layout: TreeLayout; left?: ReturnType<typeof layoutTree>; right?: ReturnType<typeof layoutTree> } {
  if (node.kind === "leaf") return { node, layout: { x, y, w } };
  const gap = 8;
  const lw = w * 0.5 - gap / 2;
  const rw = w * 0.5 - gap / 2;
  const childY = y + 56;
  return {
    node,
    layout: { x, y, w },
    left: layoutTree(node.left!, x - w * 0.25, childY, lw),
    right: layoutTree(node.right!, x + w * 0.25, childY, rw),
  };
}

function renderTreeSVG(
  lt: ReturnType<typeof layoutTree>,
  elements: React.ReactNode[],
  keyPrefix: string,
) {
  const { node, layout } = lt;
  const { x, y } = layout;

  if (node.kind === "leaf") {
    const leafW = 22, leafH = 24;
    elements.push(
      <g key={`${keyPrefix}-leaf`}>
        <rect x={x - leafW} y={y - 12} width={leafW * 2} height={leafH} rx={6}
          fill={node.color + "18"} stroke={node.color} strokeWidth={1.2} />
        <text x={x} y={y + 5} textAnchor="middle" fontSize={13} fill={node.color}
          fontFamily="monospace" fontWeight={700}>{node.label}</text>
        <text x={x} y={y + 24} textAnchor="middle" fontSize={9} fill="#9ca3af"
          fontFamily="monospace">{Math.round(node.size).toLocaleString()}</text>
      </g>
    );
    return;
  }

  // Join node
  const joinR = 14;
  elements.push(
    <g key={`${keyPrefix}-join`}>
      <circle cx={x} cy={y} r={joinR} fill="#f0f1f3" stroke="#9ca3af" strokeWidth={1.2} />
      <text x={x} y={y + 5} textAnchor="middle" fontSize={14} fill="#6b7280"
        fontFamily="monospace" fontWeight={700}>⋈</text>
    </g>
  );

  // Edges to children
  const childTop = (child: ReturnType<typeof layoutTree>) =>
    child.node.kind === "leaf" ? child.layout.y - 14 : child.layout.y - joinR;
  if (lt.left) {
    elements.push(
      <line key={`${keyPrefix}-el`} x1={x} y1={y + joinR}
        x2={lt.left.layout.x} y2={childTop(lt.left)}
        stroke="#d4d4d8" strokeWidth={1.5} />
    );
    renderTreeSVG(lt.left, elements, `${keyPrefix}-l`);
  }
  if (lt.right) {
    elements.push(
      <line key={`${keyPrefix}-er`} x1={x} y1={y + joinR}
        x2={lt.right.layout.x} y2={childTop(lt.right)}
        stroke="#d4d4d8" strokeWidth={1.5} />
    );
    renderTreeSVG(lt.right, elements, `${keyPrefix}-r`);
  }
}

// ─── Unified Plan SVG with GOO results ──────────────────────────────────────

interface GOOResult {
  vgLabel: string;
  vgColor: string;
  tree: JoinNode;
  cost: number;
  orderDesc: string;
}

function UnifiedPlanSVG({ results }: { results: GOOResult[] }) {
  const W = 580, H = 300;
  const cx = W / 2;
  const projY = 16, unionY = 52;
  const branchY = unionY + 26;

  const treeW = Math.min(220, (W - 40) / results.length);
  const gap = 24;
  const totalW = results.length * treeW + (results.length - 1) * gap;
  const startX = (W - totalW) / 2;

  const treeElements: React.ReactNode[] = [];

  // Projection + UnionAll
  treeElements.push(
    <g key="top">
      <rect x={cx - 50} y={projY - 10} width={100} height={24} rx={6}
        fill="#71717a18" stroke="#71717a" strokeWidth={1.2} />
      <text x={cx} y={projY + 6} textAnchor="middle" fontSize={13} fontWeight={700}
        fill="#71717a" fontFamily="monospace">Projection</text>
      <line x1={cx} y1={projY + 14} x2={cx} y2={unionY - 12}
        stroke="#d4d4d8" strokeWidth={1.5} />
      <rect x={cx - 55} y={unionY - 12} width={110} height={24} rx={6}
        fill="#10B98118" stroke="#10B981" strokeWidth={1.2} />
      <text x={cx} y={unionY + 4} textAnchor="middle" fontSize={13} fontWeight={700}
        fill="#10B981" fontFamily="monospace">UnionAll</text>
    </g>
  );

  results.forEach((res, ci) => {
    const treeCx = startX + ci * (treeW + gap) + treeW / 2;
    const treeTop = branchY + 30;

    // GOO join tree
    const lt = layoutTree(res.tree, treeCx, treeTop, treeW - 20);

    // Dashed scope box around entire sub-tree + cost
    const boxX = treeCx - treeW / 2 + 2;
    const boxW = treeW - 4;
    const boxY = treeTop - 24;
    const boxH = 56 * 2 + 24 + 24 + 50; // 2 join levels + leaf + top pad + bottom pad

    // Background + dashed border (render before tree)
    treeElements.push(
      <rect key={`scopebg-${ci}`} x={boxX} y={boxY} width={boxW} height={boxH} rx={10}
        fill={res.vgColor + "08"} stroke="none" />
    );
    treeElements.push(
      <rect key={`scope-${ci}`} x={boxX} y={boxY} width={boxW} height={boxH} rx={10}
        fill="none" stroke={res.vgColor} opacity={0.35}
        strokeWidth={1.8} strokeDasharray="7 4" />
    );

    // VG label badge centered on top border
    const lblW = Math.max(120, res.vgLabel.length * 7.5 + 20);
    treeElements.push(
      <g key={`vglabel-${ci}`}>
        <rect x={treeCx - lblW / 2} y={boxY - 10} width={lblW} height={20} rx={10}
          fill="#fafbfc" stroke="none" />
        <rect x={treeCx - lblW / 2} y={boxY - 10} width={lblW} height={20} rx={10}
          fill={res.vgColor + "18"} stroke={res.vgColor} strokeWidth={1} opacity={0.6} />
        <text x={treeCx} y={boxY + 4} textAnchor="middle" fontSize={11} fontWeight={700}
          fill={res.vgColor} fontFamily="monospace">{res.vgLabel}</text>
      </g>
    );

    // Connector from UnionAll to top join node
    treeElements.push(
      <path key={`conn-${ci}`}
        d={`M${cx},${unionY + 12} Q${(cx + treeCx) / 2},${treeTop - 20} ${treeCx},${treeTop - 14}`}
        fill="none" stroke="#d4d4d8" strokeWidth={1.5} />
    );

    renderTreeSVG(lt, treeElements, `tree-${ci}`);

    // Cost badge inside dashed box at bottom
    const costY = boxY + boxH - 40;
    treeElements.push(
      <g key={`cost-${ci}`}>
        <rect x={treeCx - 52} y={costY} width={104} height={24} rx={6}
          fill="#10B98115" stroke="#10B98150" strokeWidth={1} />
        <text x={treeCx} y={costY + 16} textAnchor="middle" fontSize={12} fontWeight={700}
          fill="#10B981" fontFamily="monospace">
          cost {res.cost.toLocaleString()}
        </text>
      </g>
    );
  });

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "100%" }}
      preserveAspectRatio="xMidYMid meet">
      {treeElements}
    </svg>
  );
}

// ─── Step 1 ─────────────────────────────────────────────────────────────────

function Step1() {
  const [splitTarget, setSplitTarget] = useState(0); // 0=Person, 1=City, 2=Country
  const [assigns, setAssigns] = useState<GemGroup[]>(() => {
    const gls = BLOAT_GROUPS[0].gls;
    const mid = Math.ceil(gls.length / 2);
    return gls.map((_, i) => (i < mid ? "A" : "B") as GemGroup);
  });
  const [optimizing, setOptimizing] = useState(false);

  // Reset assigns when split target changes
  const changeSplitTarget = (idx: number) => {
    setSplitTarget(idx);
    const gls = BLOAT_GROUPS[idx].gls;
    const mid = Math.ceil(gls.length / 2);
    setAssigns(gls.map((_, i) => (i < mid ? "A" : "B") as GemGroup));
    setOptimizing(false);
  };

  const toggle = (gli: number) => {
    setAssigns(prev => {
      const next = [...prev];
      next[gli] = next[gli] === "A" ? "B" : "A";
      return next;
    });
    setOptimizing(false);
  };

  const vgCols = computeVGs(splitTarget, assigns);
  const vgCounts = vgCols.map(vgs => vgs.length);
  const gemProduct = vgCounts.reduce((p, c) => p * c, 1);
  const savings = Math.round((1 - gemProduct / NAIVE_PRODUCT) * 100);

  // Run GOO for each VG combination
  const gooResults = useMemo<GOOResult[]>(() => {
    const splitVGs = vgCols[splitTarget];
    return splitVGs.map(vg => {
      const { tree, cost } = buildAndRunGOO(splitTarget, assigns, vg.grp);
      return {
        vgLabel: `VG-${vg.grp} (${vg.gls.join(", ")})`,
        vgColor: vg.color,
        tree,
        cost,
        orderDesc: describeOrder(tree),
      };
    });
  }, [splitTarget, assigns, vgCols]);

  // Check if VG groups produce different join orders
  const hasDifferentOrders = gooResults.length >= 2 &&
    gooResults[0].orderDesc !== gooResults[gooResults.length - 1].orderDesc;

  return (
    <div style={{ display: "flex", gap: 24, height: "100%", overflow: "hidden" }}>

      {/* Left panel */}
      <div style={{ width: "30%", flexShrink: 0, display: "flex", flexDirection: "column", gap: 10 }}>
        <div style={{
          fontSize: 14, color: "#9ca3af", fontFamily: "monospace",
          background: "#fafbfc", borderRadius: 8, padding: "8px 14px",
          border: "1px solid #e5e7eb",
        }}>
          (p)-[:birthPlace]-&gt;(c)-[:country]-&gt;(co)
        </div>

        {/* Split target selector */}
        <div style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace" }}>
          Split target:
        </div>
        <div style={{ display: "flex", gap: 6 }}>
          {BLOAT_GROUPS.map((g, i) => (
            <button key={i} onClick={() => changeSplitTarget(i)} style={{
              flex: 1, padding: "6px 0", borderRadius: 6, cursor: "pointer",
              fontSize: 14, fontFamily: "monospace", fontWeight: 700,
              border: `1.5px solid ${splitTarget === i ? g.color : "#d4d4d8"}`,
              background: splitTarget === i ? g.color + "12" : "transparent",
              color: splitTarget === i ? g.color : "#9ca3af",
              transition: "all 0.15s",
            }}>
              {g.label}
            </button>
          ))}
        </div>

        {/* Graphlet A/B assignment for split target */}
        <div style={{ display: "flex", flexDirection: "column", gap: 0, flex: 1, overflowY: "auto" }}>
          {BLOAT_GROUPS.map((group, gi) => (
            <div key={gi}>
              <div style={{
                background: group.color + "0C", border: `1.5px solid ${group.color}35`,
                borderRadius: 8, padding: "10px 14px",
              }}>
                <div style={{
                  fontSize: 15, fontWeight: 700, color: group.color,
                  fontFamily: "monospace", marginBottom: 6,
                  display: "flex", justifyContent: "space-between", alignItems: "center",
                }}>
                  <span>UnionAll — {group.label}</span>
                  <span style={{ fontSize: 14, fontWeight: 600, color: "#6b7280" }}>
                    {gi === splitTarget ? `→ ${vgCounts[gi]} VG${vgCounts[gi] > 1 ? "s" : ""}` : "1 VG"}
                  </span>
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  {group.gls.map((gl, gli) => {
                    const isSplit = gi === splitTarget;
                    const grp = isSplit ? assigns[gli] : null;
                    const gc = grp ? GEM_COLOR[grp] : "#71717a";
                    const sizeVal = GL_SIZES[gl] || 0;
                    return (
                      <button key={gl}
                        onClick={() => isSplit && toggle(gli)}
                        disabled={!isSplit}
                        style={{
                          display: "flex", alignItems: "center", gap: 8,
                          padding: "5px 10px", borderRadius: 6,
                          cursor: isSplit ? "pointer" : "default",
                          border: `1.5px solid ${isSplit ? gc + "50" : "#e5e7eb"}`,
                          background: isSplit ? gc + "12" : "#fafbfc",
                          textAlign: "left",
                          transition: "all 0.15s",
                          opacity: isSplit ? 1 : 0.6,
                        }}>
                        <span style={{
                          width: 8, height: 8, borderRadius: 2,
                          background: group.color, flexShrink: 0,
                        }} />
                        <span style={{
                          fontSize: 14, fontFamily: "monospace",
                          color: group.color, fontWeight: 700, flex: 1,
                        }}>{gl}</span>
                        <span style={{
                          fontSize: 12, color: "#9ca3af", fontFamily: "monospace",
                        }}>{sizeVal.toLocaleString()}</span>
                        {isSplit && grp && (
                          <span style={{
                            fontSize: 14, fontWeight: 700, color: gc,
                            fontFamily: "monospace",
                            background: gc + "25", padding: "1px 8px",
                            borderRadius: 4, minWidth: 20, textAlign: "center",
                          }}>
                            {grp}
                          </span>
                        )}
                      </button>
                    );
                  })}
                </div>
              </div>
              {gi < BLOAT_GROUPS.length - 1 && (
                <div style={{
                  textAlign: "center", padding: "5px 0",
                  color: "#9ca3af", fontFamily: "monospace", fontSize: 14,
                }}>
                  ↓ <span style={{ color: "#6b7280" }}>{EDGES[gi]}</span>
                </div>
              )}
            </div>
          ))}
        </div>

        <div style={{
          fontSize: 14, color: "#6b7280", fontFamily: "monospace", lineHeight: 1.5,
        }}>
          {optimizing
            ? <>GOO finds <span style={{ color: "#10B981", fontWeight: 600 }}>optimal join order</span> per VG — cost = size × size × selectivity</>
            : <>GEM splits <span style={{ color: BLOAT_GROUPS[splitTarget].color, fontWeight: 600 }}>{BLOAT_GROUPS[splitTarget].label}</span> into virtual groups — click graphlets to reassign</>
          }
        </div>
      </div>

      {/* Right panel */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 10, overflowY: "auto", minHeight: 0 }}>
        <div style={{
          flex: 1, minHeight: 260,
          background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
          overflow: "hidden",
        }}>
          <AnimatePresence mode="wait">
            {!optimizing ? (
              <motion.div key="vg" initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }} transition={{ duration: 0.3 }}
                style={{ width: "100%", height: "100%" }}>
                <VirtualGroupSVG vgCols={vgCols} />
              </motion.div>
            ) : (
              <motion.div key="join" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: 20 }} transition={{ duration: 0.3 }}
                style={{ width: "100%", height: "100%" }}>
                <UnifiedPlanSVG results={gooResults} />
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Formula row */}
        <div style={{
          display: "flex", justifyContent: "center", alignItems: "baseline", gap: 10,
          flexShrink: 0, padding: "2px 0",
        }}>
          {vgCounts.map((c, i) => (
            <React.Fragment key={i}>
              {i > 0 && <span style={{ fontSize: 18, color: "#9ca3af" }}>×</span>}
              <span style={{
                fontSize: 24, fontWeight: 800, fontFamily: "monospace",
                color: BLOAT_GROUPS[i].color,
              }}>
                {c}
              </span>
            </React.Fragment>
          ))}
          <span style={{ fontSize: 18, color: "#9ca3af" }}>=</span>
          <motion.span
            key={gemProduct}
            initial={{ scale: 1.3 }}
            animate={{ scale: 1 }}
            transition={{ type: "spring", stiffness: 400, damping: 20 }}
            style={{ fontSize: 32, fontWeight: 800, fontFamily: "monospace", color: "#10B981" }}
          >
            {gemProduct}
          </motion.span>
          <span style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace" }}>
            plans
          </span>
          <span style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace" }}>
            (was <span style={{ textDecoration: "line-through", color: "#e84545" }}>{NAIVE_PRODUCT}</span>, −{savings}%)
          </span>
        </div>

        {/* Optimize button */}
        <div style={{ display: "flex", gap: 10, justifyContent: "center", flexShrink: 0 }}>
          <button
            onClick={() => setOptimizing(!optimizing)}
            style={{
              padding: "9px 28px", borderRadius: 8, cursor: "pointer",
              fontSize: 15, fontFamily: "monospace", fontWeight: 700,
              border: "none",
              background: optimizing ? "#e5e7eb" : "#10B981",
              color: optimizing ? "#6b7280" : "#fff",
              transition: "all 0.2s",
            }}
          >
            {optimizing ? "← Back to Groups" : "Run GOO Optimizer →"}
          </button>
        </div>

        {/* Key insight when optimizing */}
        {optimizing && gooResults.length >= 2 && (
          <motion.div
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.3, duration: 0.3 }}
            style={{
              fontSize: 14, color: "#6b7280", fontFamily: "monospace",
              textAlign: "center", lineHeight: 1.5, flexShrink: 0,
            }}
          >
            {hasDifferentOrders ? (
              <>
                <span style={{ color: GEM_COLOR.A, fontWeight: 700 }}>VG-A</span>: {gooResults[0].orderDesc} (cost {gooResults[0].cost.toLocaleString()})
                {" vs "}
                <span style={{ color: GEM_COLOR.B, fontWeight: 700 }}>VG-B</span>: {gooResults[gooResults.length - 1].orderDesc} (cost {gooResults[gooResults.length - 1].cost.toLocaleString()})
                {" — "}different sizes → different optimal orders!
              </>
            ) : (
              <>
                Both VGs → same order: {gooResults[0].orderDesc}
                {" — "}try different groupings to see divergent plans
              </>
            )}
          </motion.div>
        )}
      </div>
    </div>
  );
}

// ─── Titles ───────────────────────────────────────────────────────────────────
const STEP_TITLES    = ["Join Bloating Problem", "Graphlet Early Merge"];
const STEP_SUBTITLES = [
  "From the Query Plan — why naïve push-down explodes",
  "Group graphlets into virtual groups to reduce search space",
];

export default function S3_GEM({ step }: Props) {
  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{ maxWidth: 1440, margin: "0 auto", padding: "28px 48px", height: "100%", display: "flex", flexDirection: "column", boxSizing: "border-box", gap: 16 }}>
        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} transition={{ duration: 0.25 }} style={{ flexShrink: 0 }}>
            <div style={{ fontSize: 14, color: "#10B981", fontFamily: "monospace", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.08em" }}>
              GEM — {STEP_SUBTITLES[step]}
            </div>
            <h2 style={{ fontSize: 26, fontWeight: 700, color: "#18181b", margin: 0 }}>{STEP_TITLES[step]}</h2>
          </motion.div>
        </AnimatePresence>

        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            transition={{ duration: 0.25 }} style={{ flex: 1, minHeight: 0 }}>
            {step === 0 && <Step0 />}
            {step === 1 && <Step1 />}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}
