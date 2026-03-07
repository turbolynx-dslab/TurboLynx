"use client";
import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface Props { step: number; onStep: (n: number) => void; }

// ─── Data ─────────────────────────────────────────────────────────────────────
const P_GLS = [
  { id: "GL-p1", nodes: 44200, color: "#8B5CF6" },
  { id: "GL-p2", nodes: 38100, color: "#a78bfa" },
  { id: "GL-p3", nodes: 12000, color: "#7C3AED" },
  { id: "GL-p4", nodes:  8500, color: "#9061f9" },
  { id: "GL-p5", nodes:  5200, color: "#c4b5fd" },
];
const F_GLS = [
  { id: "GL-f1", nodes: 15000, color: "#F59E0B" },
  { id: "GL-f2", nodes:  8400, color: "#fbbf24" },
  { id: "GL-f3", nodes:  3200, color: "#D97706" },
  { id: "GL-f4", nodes:  1900, color: "#fcd34d" },
];
const L_GLS = [
  { id: "GL-l1", nodes: 9800, color: "#10B981" },
  { id: "GL-l2", nodes: 5400, color: "#34d399" },
  { id: "GL-l3", nodes: 2100, color: "#6ee7b7" },
];

// ─── Shared plan row ──────────────────────────────────────────────────────────
function PlanRow({ op, color, detail, rows, indent = 0 }: {
  op: string; color: string; detail?: string; rows?: string; indent?: number;
}) {
  return (
    <div style={{
      display: "flex", alignItems: "center", gap: 8,
      marginLeft: indent * 16, marginBottom: 2,
      background: "#0a0a0d",
      border: `1px solid ${color}25`,
      borderLeft: `3px solid ${color}`,
      borderRadius: 4, padding: "5px 10px",
    }}>
      <span style={{ fontSize: 14, fontWeight: 700, color, fontFamily: "monospace", flexShrink: 0 }}>{op}</span>
      {detail && (
        <span style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {detail}
        </span>
      )}
      {rows && (
        <span style={{ fontSize: 13, color: color + "90", fontFamily: "monospace", flexShrink: 0, marginLeft: "auto" }}>
          {rows}
        </span>
      )}
    </div>
  );
}

// ─── Step 0 presets ───────────────────────────────────────────────────────────
type GL = { id: string; nodes: number; color: string };

interface GEMPreset {
  label: string;
  query: string;
  edge1: string;
  edge2: string;
  maxPush: number;       // max push levels (2 for single-hop, 3 for two-hop)
  pGls: GL[];
  fGls: GL[];
  lGls: GL[];
  // ordered node vars for Step1 grouping (no type labels — just var names from query)
  nodeVars: { v: string; gls: GL[] }[];
}

const GEM_PRESETS: GEMPreset[] = [
  {
    label: "Directed",
    query: `MATCH (p)-[:directed]->(f)\nWHERE p.birthDate IS NOT NULL\n  AND f.runtime IS NOT NULL\nRETURN p.name, f.title`,
    edge1: "[:directed]", edge2: "",
    maxPush: 2,
    pGls: P_GLS, fGls: F_GLS, lGls: [],
    nodeVars: [{ v: "p", gls: P_GLS }, { v: "f", gls: F_GLS }],
  },
  {
    label: "Director",
    query: `MATCH (p)-[:director]->(f)-[:filmed_in]->(l)\nWHERE p.birthDate IS NOT NULL\n  AND f.runtime IS NOT NULL\nRETURN p.name, f.title, l.country`,
    edge1: "[:director]", edge2: "[:filmed_in]",
    maxPush: 3,
    pGls: P_GLS, fGls: F_GLS, lGls: L_GLS,
    nodeVars: [{ v: "p", gls: P_GLS }, { v: "f", gls: F_GLS }, { v: "l", gls: L_GLS }],
  },
  {
    label: "Starring",
    query: `MATCH (p)-[:starring]->(f)-[:filmed_in]->(l)\nWHERE p.nationality IS NOT NULL\n  AND f.genre IS NOT NULL\nRETURN p.name, f.title, l.country`,
    edge1: "[:starring]", edge2: "[:filmed_in]",
    maxPush: 3,
    pGls: P_GLS.slice(0, 3), fGls: F_GLS.slice(0, 2), lGls: L_GLS.slice(0, 2),
    nodeVars: [{ v: "p", gls: P_GLS.slice(0, 3) }, { v: "f", gls: F_GLS.slice(0, 2) }, { v: "l", gls: L_GLS.slice(0, 2) }],
  },
];

// ─── Branch cards ─────────────────────────────────────────────────────────────
function CompactCard({ pGls, fGls, lGls, edge1, edge2 }: { pGls: GL[]; fGls: GL[]; lGls: GL[]; edge1: string; edge2: string }) {
  const sumK = (arr: GL[]) => `${(arr.reduce((s, g) => s + g.nodes, 0) / 1000).toFixed(0)}k`;
  const twoHop = lGls.length > 0;
  return (
    <div style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "12px 14px" }}>
      <div style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 8 }}>
        Compact — 1 plan
      </div>
      <PlanRow op="Projection" color="#71717a" detail="…" />
      <PlanRow op="Join" color="#8B5CF6" detail={`⋈ ${edge1}`} indent={1} />
      <PlanRow op="UnionAll" color="#8B5CF6" detail={pGls.map(g => g.id).join(" · ")} rows={sumK(pGls)} indent={2} />
      {twoHop ? (
        <>
          <PlanRow op="Join" color="#F59E0B" detail={`⋈ ${edge2}`} indent={2} />
          <PlanRow op="UnionAll" color="#F59E0B" detail={fGls.map(g => g.id).join(" · ")} rows={sumK(fGls)} indent={3} />
          <PlanRow op="UnionAll" color="#10B981" detail={lGls.map(g => g.id).join(" · ")} rows={sumK(lGls)} indent={3} />
        </>
      ) : (
        <PlanRow op="UnionAll" color="#F59E0B" detail={fGls.map(g => g.id).join(" · ")} rows={sumK(fGls)} indent={2} />
      )}
    </div>
  );
}

function Push1Card({ p, fGls, lGls, edge1, edge2 }: { p: GL; fGls: GL[]; lGls: GL[]; edge1: string; edge2: string }) {
  const twoHop = lGls.length > 0;
  return (
    <div style={{ background: "#131316", border: `1px solid ${p.color}35`, borderRadius: 8, padding: "10px 12px" }}>
      <div style={{ fontSize: 13, fontFamily: "monospace", fontWeight: 700, color: p.color, marginBottom: 8 }}>{p.id}</div>
      <PlanRow op="Join" color={p.color} detail={`${p.id} ⋈ ${edge1}`} />
      <PlanRow op="Scan" color={p.color} detail={p.id} rows={p.nodes.toLocaleString()} indent={1} />
      {twoHop ? (
        <>
          <PlanRow op="Join" color="#F59E0B" detail={`⋈ ${edge2}`} indent={1} />
          <PlanRow op="UnionAll" color="#F59E0B" detail={fGls.map(g => g.id).join(" · ")} indent={2} />
          <PlanRow op="UnionAll" color="#10B981" detail={lGls.map(g => g.id).join(" · ")} indent={2} />
        </>
      ) : (
        <PlanRow op="UnionAll" color="#F59E0B" detail={fGls.map(g => g.id).join(" · ")} indent={1} />
      )}
    </div>
  );
}

function Push2Card({ p, f, lGls, edge1, edge2 }: { p: GL; f: GL; lGls: GL[]; edge1: string; edge2: string }) {
  const twoHop = lGls.length > 0;
  return (
    <div style={{ background: "#131316", border: `1px solid ${p.color}30`, borderRadius: 8, padding: "8px 10px" }}>
      <div style={{ display: "flex", gap: 4, alignItems: "center", marginBottom: 7 }}>
        <span style={{ fontSize: 13, fontFamily: "monospace", fontWeight: 700, color: p.color }}>{p.id}</span>
        <span style={{ fontSize: 10, color: "#3f3f46" }}>×</span>
        <span style={{ fontSize: 13, fontFamily: "monospace", fontWeight: 700, color: f.color }}>{f.id}</span>
      </div>
      <PlanRow op="Join" color={p.color} detail={edge1} />
      <PlanRow op="Scan" color={p.color} detail={p.id} indent={1} />
      {twoHop ? (
        <>
          <PlanRow op="Join" color={f.color} detail={edge2} indent={1} />
          <PlanRow op="Scan" color={f.color} detail={f.id} indent={2} />
          <PlanRow op="UnionAll" color="#10B981" detail={lGls.map(g => g.id).join(" · ")} indent={2} />
        </>
      ) : (
        <PlanRow op="Scan" color={f.color} detail={f.id} indent={1} />
      )}
    </div>
  );
}

function Push3Card({ p, f, l, edge1, edge2 }: { p: GL; f: GL; l: GL; edge1: string; edge2: string }) {
  return (
    <div style={{ background: "#131316", border: `1px solid ${p.color}28`, borderRadius: 6, padding: "7px 9px" }}>
      <div style={{ display: "flex", gap: 3, alignItems: "center", marginBottom: 5, flexWrap: "wrap" }}>
        <span style={{ fontSize: 12, fontFamily: "monospace", fontWeight: 700, color: p.color }}>{p.id}</span>
        <span style={{ fontSize: 9, color: "#3f3f46" }}>×</span>
        <span style={{ fontSize: 12, fontFamily: "monospace", fontWeight: 700, color: f.color }}>{f.id}</span>
        <span style={{ fontSize: 9, color: "#3f3f46" }}>×</span>
        <span style={{ fontSize: 12, fontFamily: "monospace", fontWeight: 700, color: l.color }}>{l.id}</span>
      </div>
      <PlanRow op="Join" color={p.color} detail={edge1} />
      <PlanRow op="Scan" color={p.color} detail={p.id} indent={1} />
      <PlanRow op="Join" color={f.color} detail={edge2} indent={1} />
      <PlanRow op="Scan" color={f.color} detail={f.id} indent={2} />
      <PlanRow op="Scan" color={l.color} detail={l.id} indent={2} />
    </div>
  );
}

// ─── Step 0: Query + Bloating ─────────────────────────────────────────────────
function Step0() {
  const [presetIdx, setPresetIdx] = useState<number | null>(0);
  const [customText, setCustomText] = useState("");
  const [pushLevel, setPushLevel] = useState(0);

  const isCustom = presetIdx === null;
  const preset = isCustom ? null : GEM_PRESETS[presetIdx];
  const pGls  = preset ? preset.pGls  : P_GLS;
  const fGls  = preset ? preset.fGls  : F_GLS;
  const lGls  = preset ? preset.lGls  : L_GLS;
  const edge1 = preset ? preset.edge1 : "[:edge1]";
  const edge2 = preset ? preset.edge2 : "[:edge2]";
  const queryText = isCustom ? customText : preset!.query;

  const MAX_PUSH = preset ? preset.maxPush : 2;
  const totalBranches =
    pushLevel === 0 ? 1 :
    pushLevel === 1 ? pGls.length :
    pushLevel === 2 ? pGls.length * fGls.length :
                     pGls.length * fGls.length * lGls.length;
  const cols =
    pushLevel === 0 ? 1 :
    pushLevel === 1 ? Math.min(pGls.length, 3) :
    pushLevel === 2 ? Math.min(pGls.length, 4) :
                     Math.min(pGls.length * fGls.length, 5);

  const handlePreset = (i: number) => { setPresetIdx(i); setPushLevel(0); };
  const handleCustom = () => { setPresetIdx(null); setCustomText(""); setPushLevel(0); };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12, height: "100%", overflow: "hidden" }}>

      {/* Preset row + query */}
      <div style={{ flexShrink: 0 }}>
        <div style={{ display: "flex", gap: 6, alignItems: "center", marginBottom: 7 }}>
          <span style={{ fontSize: 14, color: "#52525b", fontFamily: "monospace" }}>Presets:</span>
          {GEM_PRESETS.map((p, i) => (
            <button key={p.label} onClick={() => handlePreset(i)} style={{
              padding: "4px 14px", borderRadius: 5, cursor: "pointer", fontSize: 15, fontFamily: "monospace",
              border: `1px solid ${presetIdx === i ? "#8B5CF6" : "#27272a"}`,
              background: presetIdx === i ? "#8B5CF618" : "transparent",
              color: presetIdx === i ? "#a78bfa" : "#71717a",
            }}>{p.label}</button>
          ))}
          <button onClick={handleCustom} style={{
            padding: "4px 14px", borderRadius: 5, cursor: "pointer", fontSize: 15, fontFamily: "monospace",
            border: `1px solid ${isCustom ? "#10B981" : "#27272a"}`,
            background: isCustom ? "#10B98118" : "transparent",
            color: isCustom ? "#34d399" : "#71717a",
          }}>Custom ✎</button>
        </div>
        <textarea
          readOnly={!isCustom}
          value={queryText}
          onChange={e => { setCustomText(e.target.value); setPushLevel(0); }}
          placeholder={isCustom ? "MATCH (p)-[:edge]->(f)\nWHERE …\nRETURN …" : undefined}
          style={{
            background: "#131316",
            border: `1px solid ${isCustom ? "#3f3f46" : "#27272a"}`,
            borderRadius: 8, padding: "10px 14px", fontSize: 16, color: "#f4f4f5",
            fontFamily: "monospace", lineHeight: 1.65, resize: "none", height: 120,
            outline: "none", width: "100%", boxSizing: "border-box",
            cursor: isCustom ? "text" : "default",
          }}
        />
      </div>

      {/* Control bar */}
      <div style={{ display: "flex", gap: 8, alignItems: "center", flexShrink: 0 }}>
        <button
          onClick={() => setPushLevel(l => Math.min(l + 1, MAX_PUSH))}
          disabled={pushLevel >= MAX_PUSH}
          style={{
            padding: "7px 16px", borderRadius: 6, cursor: pushLevel >= MAX_PUSH ? "not-allowed" : "pointer",
            border: `1px solid ${pushLevel >= MAX_PUSH ? "#1f1f23" : "#8B5CF660"}`,
            background: pushLevel >= MAX_PUSH ? "transparent" : "#8B5CF615",
            color: pushLevel >= MAX_PUSH ? "#3f3f46" : "#a78bfa",
            fontSize: 15, fontFamily: "monospace", fontWeight: 600,
          }}
        >
          {pushLevel >= MAX_PUSH ? "✓ fully pushed" : `PushJoinBelowUnionAll ×${pushLevel + 1} →`}
        </button>

        <button
          onClick={() => setPushLevel(0)}
          disabled={pushLevel === 0}
          style={{
            padding: "7px 14px", borderRadius: 6, cursor: pushLevel === 0 ? "not-allowed" : "pointer",
            border: `1px solid ${pushLevel === 0 ? "#1f1f23" : "#e8454560"}`,
            background: pushLevel === 0 ? "transparent" : "#e8454512",
            color: pushLevel === 0 ? "#3f3f46" : "#f87171",
            fontSize: 15, fontFamily: "monospace", fontWeight: 600,
          }}
        >
          ↺ Reset
        </button>

        <div style={{ marginLeft: "auto", display: "flex", alignItems: "baseline", gap: 8 }}>
          <motion.span
            key={totalBranches}
            initial={{ scale: 1.4, color: "#e84545" }}
            animate={{ scale: 1, color: pushLevel >= 2 ? "#e84545" : "#f4f4f5" }}
            transition={{ type: "spring", stiffness: 400, damping: 20 }}
            style={{ fontSize: 32, fontWeight: 800, fontFamily: "monospace", lineHeight: 1 }}
          >
            {totalBranches}
          </motion.span>
          <span style={{ fontSize: 14, color: "#52525b", fontFamily: "monospace" }}>
            {totalBranches === 1 ? "plan (compact)" : "plan branches"}
            {pushLevel > 0 && <span style={{ color: pushLevel >= 2 ? "#e84545" : "#71717a" }}>
              {" "}({
                pushLevel === 1 ? `${pGls.length}` :
                pushLevel === 2 ? `${pGls.length}×${fGls.length}` :
                `${pGls.length}×${fGls.length}×${lGls.length}`
              })
            </span>}
          </span>
        </div>
      </div>

      {/* Cards */}
      <div style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
        <div style={{
          display: "grid",
          gridTemplateColumns: `repeat(${cols}, 1fr)`,
          gap: 8,
          alignItems: "start",
        }}>
          <AnimatePresence mode="popLayout">
            {pushLevel === 0 && (
              <motion.div key="compact" initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }}>
                <CompactCard pGls={pGls} fGls={fGls} lGls={lGls} edge1={edge1} edge2={edge2} />
              </motion.div>
            )}
            {pushLevel === 1 && pGls.map((p, i) => (
              <motion.div key={p.id} initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} transition={{ delay: i * 0.05, duration: 0.2 }}>
                <Push1Card p={p} fGls={fGls} lGls={lGls} edge1={edge1} edge2={edge2} />
              </motion.div>
            ))}
            {pushLevel === 2 && pGls.flatMap((p, pi) => fGls.map((f, fi) => (
              <motion.div key={`${p.id}-${f.id}`} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} transition={{ delay: (pi * fGls.length + fi) * 0.03, duration: 0.18 }}>
                <Push2Card p={p} f={f} lGls={lGls} edge1={edge1} edge2={edge2} />
              </motion.div>
            )))}
            {pushLevel === 3 && pGls.flatMap((p, pi) => fGls.flatMap((f, fi) => lGls.map((l, li) => (
              <motion.div key={`${p.id}-${f.id}-${l.id}`} initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
                transition={{ delay: (pi * fGls.length * lGls.length + fi * lGls.length + li) * 0.015, duration: 0.15 }}>
                <Push3Card p={p} f={f} l={l} edge1={edge1} edge2={edge2} />
              </motion.div>
            ))))}
          </AnimatePresence>
        </div>
      </div>

    </div>
  );
}

// ─── Step 1: Interactive Grouping ─────────────────────────────────────────────
type Group = "A" | "B";
const GROUP_COLOR: Record<Group, string> = { A: "#3B82F6", B: "#10B981" };
const GROUP_BG:    Record<Group, string> = { A: "#3B82F615", B: "#10B98115" };

// Parse node variable names from a Cypher MATCH clause (order-preserving, deduplicated)
function parseQueryVars(query: string): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const m of query.matchAll(/\((\w+)\)/g)) {
    if (m[1] && !seen.has(m[1])) { seen.add(m[1]); result.push(m[1]); }
  }
  return result;
}

type VarAssign = Record<string, Group[]>;

function defaultAssign(nodeVars: { v: string; gls: GL[] }[]): VarAssign {
  const out: VarAssign = {};
  for (const { v, gls } of nodeVars) {
    const mid = Math.ceil(gls.length / 2);
    out[v] = gls.map((_, i) => i < mid ? "A" : "B");
  }
  return out;
}

function getCombos<T>(arrays: T[][]): T[][] {
  return arrays.reduce<T[][]>(
    (acc, arr) => acc.flatMap(prev => arr.map(item => [...prev, item])),
    [[]]
  );
}

// ─── Tree plan types & renderer ───────────────────────────────────────────────
interface PlanNode {
  op: string;
  color: string;
  detail?: string;
  rows?: string;
  children?: PlanNode[];
}

function TreePlan({ node, prefix = "", isLast = true, isRoot = false }: {
  node: PlanNode; prefix?: string; isLast?: boolean; isRoot?: boolean;
}) {
  const connector = isRoot ? "" : (isLast ? "└─ " : "├─ ");
  const childPrefix = isRoot ? "" : prefix + (isLast ? "   " : "│  ");
  return (
    <>
      <div style={{ display: "flex", alignItems: "center", marginBottom: 2, minWidth: 0 }}>
        {!isRoot && (
          <span style={{ fontSize: 12, color: "#3a3a3f", fontFamily: "monospace", whiteSpace: "pre", flexShrink: 0, lineHeight: "22px" }}>
            {prefix}{connector}
          </span>
        )}
        <div style={{
          display: "flex", alignItems: "center", gap: 8, flex: 1, minWidth: 0,
          background: "#0a0a0d", border: `1px solid ${node.color}25`,
          borderLeft: `3px solid ${node.color}`, borderRadius: 4, padding: "4px 10px",
        }}>
          <span style={{ fontSize: 13, fontWeight: 700, color: node.color, fontFamily: "monospace", flexShrink: 0 }}>{node.op}</span>
          {node.detail && (
            <span style={{ fontSize: 12, color: "#52525b", fontFamily: "monospace", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {node.detail}
            </span>
          )}
          {node.rows && (
            <span style={{ fontSize: 12, color: node.color + "90", fontFamily: "monospace", flexShrink: 0, marginLeft: "auto" }}>
              {node.rows}
            </span>
          )}
        </div>
      </div>
      {node.children?.map((child, i) => (
        <TreePlan
          key={i}
          node={child}
          prefix={childPrefix}
          isLast={i === (node.children?.length ?? 0) - 1}
        />
      ))}
    </>
  );
}

// Build a join tree for one combination, swapping left/right based on node counts
function buildComboTree(
  combo: { grp: Group; gls: GL[] }[],
  nodeVars: { v: string; gls: GL[] }[],
  edges: string[],
): PlanNode {
  const sum = (g: { gls: GL[] }) => g.gls.reduce((s, gl) => s + gl.nodes, 0);
  const makeVG = (varIdx: number, vg: { grp: Group; gls: GL[] }): PlanNode => ({
    op: "UnionAll",
    color: GROUP_COLOR[vg.grp],
    detail: `VG-(${nodeVars[varIdx].v})-${vg.grp}`,
    rows: sum(vg).toLocaleString(),
    children: vg.gls.map(gl => ({ op: "Scan", color: gl.color, detail: gl.id, rows: gl.nodes.toLocaleString() })),
  });
  const edgeScan = (idx: number): PlanNode => ({ op: "Scan", color: "#3f3f46", detail: edges[idx] || `[:edge${idx + 1}]` });
  const joinNode = (color: string, left: PlanNode, right: PlanNode): PlanNode => ({ op: "Join", color, children: [left, right] });

  const pVG = combo[0], fVG = combo[1];
  const pNode = makeVG(0, pVG), fNode = makeVG(1, fVG);
  const pSum = sum(pVG), fSum = sum(fVG);
  const pColor = GROUP_COLOR[pVG.grp], fColor = GROUP_COLOR[fVG.grp];

  if (combo.length === 2) {
    // 1-hop: larger side goes LEFT, edge+other-side on RIGHT
    return pSum >= fSum
      ? joinNode(pColor, pNode, joinNode(fColor, edgeScan(0), fNode))
      : joinNode(fColor, fNode, joinNode(pColor, edgeScan(0), pNode));
  } else {
    // 2-hop: first build inner (f ⋈ edge1 ⋈ l), then wrap with outer (p ⋈ edge0 ⋈ inner)
    const lVG = combo[2];
    const lNode = makeVG(2, lVG);
    const lSum = sum(lVG);
    const lColor = GROUP_COLOR[lVG.grp];
    const innerJoin = fSum >= lSum
      ? joinNode(fColor, fNode, joinNode(lColor, edgeScan(1), lNode))
      : joinNode(lColor, lNode, joinNode(fColor, edgeScan(1), fNode));
    const innerColor = fSum >= lSum ? fColor : lColor;
    const innerSum = fSum + lSum;
    return pSum >= innerSum
      ? joinNode(pColor, pNode, joinNode(innerColor, edgeScan(0), innerJoin))
      : joinNode(innerColor, innerJoin, joinNode(pColor, edgeScan(0), pNode));
  }
}

function Step1() {
  const [presetIdx, setPresetIdx] = useState<number | null>(0);
  const [customText, setCustomText] = useState("");
  const [assign, setAssign] = useState<VarAssign>(() => defaultAssign(GEM_PRESETS[0].nodeVars));

  const isCustom = presetIdx === null;
  const preset = isCustom ? null : GEM_PRESETS[presetIdx];
  const queryText = isCustom ? customText : preset!.query;

  // Derive node vars from preset or parse from custom query text
  const GL_POOLS = [P_GLS, F_GLS, L_GLS];
  const nodeVars: { v: string; gls: GL[] }[] = preset
    ? preset.nodeVars
    : parseQueryVars(customText).slice(0, 3).map((v, i) => ({ v, gls: GL_POOLS[i] ?? P_GLS }));

  // Fill in any missing assignments with defaults
  const effectiveAssign: VarAssign = {};
  for (const { v, gls } of nodeVars) {
    effectiveAssign[v] = (assign[v]?.length === gls.length) ? assign[v] : defaultAssign([{ v, gls }])[v];
  }

  const handlePreset = (i: number) => {
    setPresetIdx(i);
    setAssign(defaultAssign(GEM_PRESETS[i].nodeVars));
  };
  const handleCustom = () => {
    setPresetIdx(null); setCustomText(""); setAssign({});
  };
  const handleCustomChange = (text: string) => {
    setCustomText(text);
    const vars = parseQueryVars(text).slice(0, 3).map((v, i) => ({ v, gls: GL_POOLS[i] ?? P_GLS }));
    setAssign(defaultAssign(vars));
  };
  const toggle = (v: string, i: number) => {
    setAssign(prev => {
      const cur = prev[v] ?? effectiveAssign[v];
      const next = [...cur]; next[i] = next[i] === "A" ? "B" : "A";
      return { ...prev, [v]: next };
    });
  };

  // Stats: naive = product of all gl counts; gem = product of virtual group counts per var
  const naiveCombinations = nodeVars.reduce((p, { gls }) => p * gls.length, 1);
  const gemCombinations = nodeVars.reduce((p, { v, gls }) => {
    const a = effectiveAssign[v];
    const nVirtual = (gls.some((_, i) => a[i] === "A") ? 1 : 0) + (gls.some((_, i) => a[i] === "B") ? 1 : 0);
    return p * (nVirtual || 1);
  }, 1);
  const savings = Math.round((1 - gemCombinations / naiveCombinations) * 100);

  // Virtual groups per var — for cross-product combo rendering
  const varVGS = nodeVars.map(({ v, gls }) => {
    const a = effectiveAssign[v];
    const glsA = gls.filter((_, i) => a[i] === "A");
    const glsB = gls.filter((_, i) => a[i] === "B");
    const groups: { grp: Group; gls: GL[] }[] = [];
    if (glsA.length > 0) groups.push({ grp: "A" as Group, gls: glsA });
    if (glsB.length > 0) groups.push({ grp: "B" as Group, gls: glsB });
    return { v, groups };
  });
  const combos = getCombos(varVGS.map(vvg => vvg.groups));
  const edges = preset ? [preset.edge1, preset.edge2].filter(Boolean) : [];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12, height: "100%", overflow: "hidden" }}>

      {/* Preset selector + query */}
      <div style={{ flexShrink: 0 }}>
        <div style={{ display: "flex", gap: 6, alignItems: "center", marginBottom: 7 }}>
          <span style={{ fontSize: 14, color: "#52525b", fontFamily: "monospace" }}>Presets:</span>
          {GEM_PRESETS.map((p, i) => (
            <button key={p.label} onClick={() => handlePreset(i)} style={{
              padding: "4px 14px", borderRadius: 5, cursor: "pointer", fontSize: 15, fontFamily: "monospace",
              border: `1px solid ${presetIdx === i ? "#8B5CF6" : "#27272a"}`,
              background: presetIdx === i ? "#8B5CF618" : "transparent",
              color: presetIdx === i ? "#a78bfa" : "#71717a",
            }}>{p.label}</button>
          ))}
          <button onClick={handleCustom} style={{
            padding: "4px 14px", borderRadius: 5, cursor: "pointer", fontSize: 15, fontFamily: "monospace",
            border: `1px solid ${isCustom ? "#10B981" : "#27272a"}`,
            background: isCustom ? "#10B98118" : "transparent",
            color: isCustom ? "#34d399" : "#71717a",
          }}>Custom ✎</button>
        </div>
        <textarea
          readOnly={!isCustom}
          value={queryText}
          onChange={e => handleCustomChange(e.target.value)}
          placeholder={isCustom ? "MATCH (p)-[:edge]->(f)\nWHERE …\nRETURN …" : undefined}
          style={{
            background: "#131316",
            border: `1px solid ${isCustom ? "#3f3f46" : "#27272a"}`,
            borderRadius: 8, padding: "10px 14px", fontSize: 16, color: "#f4f4f5",
            fontFamily: "monospace", lineHeight: 1.65, resize: "none", height: 120,
            outline: "none", width: "100%", boxSizing: "border-box",
            cursor: isCustom ? "text" : "default",
          }}
        />
      </div>

      {/* Main area */}
      <div style={{ flex: 1, minHeight: 0, display: "flex", gap: 12, overflow: "hidden" }}>

        {/* Left: grouping per node var */}
        <div style={{ width: 270, flexShrink: 0, display: "flex", flexDirection: "column", gap: 10, overflow: "auto" }}>
          {nodeVars.length === 0 && (
            <div style={{ fontSize: 14, color: "#3f3f46", fontFamily: "monospace", textAlign: "center", paddingTop: 24 }}>
              Type a MATCH query above to see node variables
            </div>
          )}
          {nodeVars.map(({ v, gls }) => {
            const a = effectiveAssign[v];
            return (
              <div key={v} style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "12px 14px" }}>
                <div style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.07em", marginBottom: 10 }}>
                  ({v}) graphlets — click to reassign
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
                  {gls.map((g, i) => {
                    const grp = a[i];
                    const gc = GROUP_COLOR[grp];
                    return (
                      <button key={g.id} onClick={() => toggle(v, i)} style={{
                        display: "flex", alignItems: "center", gap: 10,
                        padding: "7px 11px", borderRadius: 6, cursor: "pointer",
                        border: `1px solid ${gc}50`, background: GROUP_BG[grp], textAlign: "left",
                      }}>
                        <span style={{ width: 8, height: 8, borderRadius: 2, background: g.color, flexShrink: 0 }} />
                        <span style={{ fontSize: 15, fontFamily: "monospace", color: g.color, fontWeight: 700, flex: 1 }}>{g.id}</span>
                        <span style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace" }}>{g.nodes.toLocaleString()}</span>
                        <span style={{ fontSize: 14, fontWeight: 700, color: gc, fontFamily: "monospace", background: gc + "25", padding: "1px 7px", borderRadius: 4 }}>
                          {grp}
                        </span>
                      </button>
                    );
                  })}
                </div>
              </div>
            );
          })}
        </div>

        {/* Right: stats + virtual plan */}
        <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 10, overflowY: "auto" }}>

          {/* Stats */}
          <div style={{ display: "flex", gap: 10, flexShrink: 0 }}>
            {[
              { label: "Naïve combinations", value: String(naiveCombinations), color: "#e84545" },
              { label: "GEM combinations",   value: String(gemCombinations),   color: "#10B981" },
              { label: "Search space savings", value: `−${savings}%`,          color: "#3B82F6" },
            ].map(s => (
              <div key={s.label} style={{
                flex: 1, background: "#131316", border: `1px solid ${s.color}25`,
                borderRadius: 8, padding: "10px 14px", textAlign: "center",
              }}>
                <motion.div
                  key={s.value} initial={{ scale: 1.2 }} animate={{ scale: 1 }}
                  transition={{ type: "spring", stiffness: 500, damping: 25 }}
                  style={{ fontSize: 32, fontWeight: 800, fontFamily: "monospace", color: s.color }}
                >
                  {s.value}
                </motion.div>
                <div style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", marginTop: 3 }}>{s.label}</div>
              </div>
            ))}
          </div>

          {/* Virtual plan */}
          <div style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "12px 16px", flexShrink: 0 }}>
            <div style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.07em", marginBottom: 10 }}>
              GEM virtual plan — {gemCombinations} combination{gemCombinations !== 1 ? "s" : ""}
            </div>
            <AnimatePresence mode="wait">
              <motion.div
                key={JSON.stringify(effectiveAssign)}
                initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                transition={{ duration: 0.18 }}
              >
                <TreePlan isRoot node={{
                  op: "Projection", color: "#71717a", detail: "…",
                  children: [{
                    op: "UnionAll", color: "#52525b",
                    detail: `${gemCombinations} virtual combination${gemCombinations !== 1 ? "s" : ""}`,
                    children: combos.map((combo, ci) => ({
                      op: `Combo ${ci + 1}`, color: "#3f3f46",
                      detail: nodeVars.map((nv, j) => `VG-(${nv.v})-${combo[j].grp}`).join(" ⋈ "),
                      children: [buildComboTree(combo, nodeVars, edges)],
                    })),
                  }],
                }} />
              </motion.div>
            </AnimatePresence>
          </div>

          <div style={{ fontSize: 14, color: "#52525b", fontFamily: "monospace", lineHeight: 1.65, flexShrink: 0 }}>
            GEM groups graphlets with similar join preferences into <span style={{ color: "#f4f4f5" }}>virtual graphlets</span>.
            Orca sees <span style={{ color: "#10B981" }}>{gemCombinations}</span> combination{gemCombinations !== 1 ? "s" : ""} instead of{" "}
            <span style={{ color: "#e84545" }}>{naiveCombinations}</span> — search space reduced by <span style={{ color: "#3B82F6" }}>{savings}%</span>.
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Titles ───────────────────────────────────────────────────────────────────
const STEP_TITLES    = ["Join Bloating Problem", "Graphlet Early Merge"];
const STEP_SUBTITLES = [
  "Naïve join pushdown explodes the plan space",
  "Group graphlets to tame Orca's search space",
];

export default function S3_GEM({ step }: Props) {
  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{ maxWidth: 1440, margin: "0 auto", padding: "28px 48px", height: "100%", display: "flex", flexDirection: "column", boxSizing: "border-box", gap: 16 }}>
        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} transition={{ duration: 0.25 }} style={{ flexShrink: 0 }}>
            <div style={{ fontSize: 13, color: "#10B981", fontFamily: "monospace", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.08em" }}>
              GEM — {STEP_SUBTITLES[step]}
            </div>
            <h2 style={{ fontSize: 26, fontWeight: 700, color: "#f4f4f5", margin: 0 }}>{STEP_TITLES[step]}</h2>
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
