"use client";
import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { CGC_GRAPHLETS } from "@/lib/demo-data";

interface Props { step: number; onStep: (n: number) => void; }

// ─── Index construction ───────────────────────────────────────────────────────
const GL = [...CGC_GRAPHLETS];
const ALL_SCHEMA_ATTRS = Array.from(new Set(GL.flatMap(g => [...g.schema])));

function getMatchingGLs(attrs: string[]) {
  if (!attrs.length) return new Set(GL.map(g => g.id));
  const s = new Set<string>();
  GL.forEach(g => {
    if (attrs.every(a => (g.schema as readonly string[]).includes(a))) s.add(g.id);
  });
  return s;
}

function extractAttrs(text: string): string[] {
  return ALL_SCHEMA_ATTRS.filter(a => text.includes(a));
}

// ─── Concept page UNION ALL tree ─────────────────────────────────────────────
function UnionAllTree({ matchedGLs, queryClause }: { matchedGLs: Set<string>; queryClause: string }) {
  const matched = GL.filter(g => matchedGLs.has(g.id));
  const pruned  = GL.filter(g => !matchedGLs.has(g.id));
  return (
    <div style={{ fontFamily: "monospace", fontSize: 15, lineHeight: 1.8 }}>
      <div style={{ color: "#a78bfa" }}>{queryClause}</div>
      <div style={{ marginLeft: 14 }}>
        <div style={{ color: "#8B5CF6" }}>└ UnionAll ({matched.length} graphlet{matched.length !== 1 ? "s" : ""})</div>
        {matched.map((g, i) => (
          <div key={g.id} style={{ marginLeft: 24, color: g.color }}>
            {i < matched.length - 1 ? "├" : pruned.length ? "├" : "└"} Scan({g.id})
          </div>
        ))}
        {pruned.map((g, i) => (
          <div key={g.id} style={{ marginLeft: 24, color: "#3f3f46", opacity: 0.6 }}>
            {i < pruned.length - 1 ? "├" : "└"} <s>{g.id}</s> (pruned)
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Step 0: Concept view ────────────────────────────────────────────────────
const PLAN_MATCHED    = getMatchingGLs([]);
const CONCEPT_ATTR    = "team";
const CONCEPT_MATCHED = getMatchingGLs([CONCEPT_ATTR]);
const MATRIX_ATTRS    = ["birthDate","birthPlace","team","award","almaMater","deathDate","spouse","occupation"];

function ConceptView() {
  return (
    <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 12 }}>
      <div style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "10px 14px", flexShrink: 0 }}>
        <div style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.06em" }}>graphlet schemas</div>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 14, fontFamily: "monospace" }}>
          <thead><tr>
            <th style={{ padding: "2px 8px", textAlign: "left", color: "#3f3f46", fontWeight: 400, width: 90 }} />
            {GL.map(g => <th key={g.id} style={{ padding: "2px 8px", textAlign: "center", color: g.color, fontWeight: 700 }}>{g.id}</th>)}
          </tr></thead>
          <tbody>{MATRIX_ATTRS.map(attr => {
            const isIdx = attr === CONCEPT_ATTR;
            return (
              <tr key={attr}>
                <td style={{ padding: "2px 8px", color: isIdx ? "#a78bfa" : "#52525b", fontWeight: isIdx ? 600 : 400 }}>
                  {isIdx && <span style={{ marginRight: 4 }}>②</span>}{attr}
                </td>
                {GL.map(g => {
                  const has = (g.schema as readonly string[]).includes(attr);
                  return (
                    <td key={g.id} style={{ padding: "2px 8px", textAlign: "center" }}>
                      <span style={{ color: has ? (isIdx ? g.color : "#52525b") : "#27272a" }}>{has ? "✓" : "—"}</span>
                    </td>
                  );
                })}
              </tr>
            );
          })}</tbody>
        </table>
      </div>

      <div style={{ display: "flex", gap: 16, alignItems: "stretch", flexShrink: 0 }}>
        <div style={{ width: 210, flexShrink: 0, display: "flex", flexDirection: "column", justifyContent: "center", gap: 6 }}>
          <div style={{ fontSize: 15, fontWeight: 700, color: "#f4f4f5", fontFamily: "monospace" }}>① UNION ALL Plan</div>
          <div style={{ fontSize: 13, color: "#71717a", lineHeight: 1.6 }}>
            TurboLynx generates one <span style={{ color: "#f4f4f5" }}>Scan</span> per graphlet, all combined under a <span style={{ color: "#8B5CF6" }}>UnionAll</span>. Results are merged at query time.
          </div>
        </div>
        <div style={{ flex: 1, background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "12px 16px", display: "flex", gap: 20 }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.06em" }}>generated plan</div>
            <UnionAllTree matchedGLs={PLAN_MATCHED} queryClause="MATCH (n)" />
          </div>
          <div style={{ width: 1, background: "#1f1f23", flexShrink: 0 }} />
          <div style={{ width: 170, flexShrink: 0, display: "flex", flexDirection: "column", gap: 5, justifyContent: "center" }}>
            {GL.map(g => {
              const hit = PLAN_MATCHED.has(g.id);
              return (
                <div key={g.id} style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 15, fontFamily: "monospace" }}>
                  <span style={{ color: hit ? g.color : "#3f3f46" }}>●</span>
                  <span style={{ color: hit ? g.color : "#3f3f46" }}>{g.id}</span>
                  <span style={{ marginLeft: "auto", fontSize: 14, color: hit ? "#10B981" : "#52525b" }}>{hit ? "Scan" : "pruned"}</span>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 10, flexShrink: 0 }}>
        <div style={{ flex: 1, height: 1, background: "#1f1f23" }} />
        <span style={{ fontSize: 14, color: "#3f3f46", fontFamily: "monospace" }}>+ Schema Index enables predicate-driven pruning</span>
        <div style={{ flex: 1, height: 1, background: "#1f1f23" }} />
      </div>

      <div style={{ display: "flex", gap: 16, alignItems: "stretch", flexShrink: 0 }}>
        <div style={{ width: 210, flexShrink: 0, display: "flex", flexDirection: "column", justifyContent: "center", gap: 6 }}>
          <div style={{ fontSize: 15, fontWeight: 700, color: "#f4f4f5", fontFamily: "monospace" }}>② Schema Index</div>
          <div style={{ fontSize: 13, color: "#71717a", lineHeight: 1.6 }}>
            An inverted index maps each attribute to graphlets containing it. Predicates are resolved in <span style={{ color: "#f4f4f5" }}>microseconds</span> — only matching graphlets enter the plan.
          </div>
          <div style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", background: "#0e0e10", border: "1px solid #27272a", borderRadius: 5, padding: "5px 10px" }}>
            <span style={{ color: "#71717a" }}>WHERE </span><span style={{ color: "#a78bfa" }}>team</span><span style={{ color: "#71717a" }}> IS NOT NULL</span>
          </div>
        </div>
        <div style={{ flex: 1, background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "12px 16px", display: "flex", gap: 20 }}>
          <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 5 }}>
            <div style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.06em" }}>index lookup: team</div>
            {GL.map(g => {
              const hit = CONCEPT_MATCHED.has(g.id);
              return (
                <div key={g.id} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ fontSize: 15, fontFamily: "monospace", fontWeight: 600, color: hit ? g.color : "#3f3f46", minWidth: 50 }}>{g.id}</span>
                  <div style={{ flex: 1, height: 1, borderTop: `1px dashed ${hit ? g.color + "50" : "#1f1f23"}` }} />
                  <span style={{ fontSize: 16, color: hit ? g.color : "#3f3f46" }}>{hit ? "✓" : "—"}</span>
                </div>
              );
            })}
            <div style={{ marginTop: 4, fontSize: 15, color: "#10B981", fontFamily: "monospace" }}>⚡ {GL.length - CONCEPT_MATCHED.size}/{GL.length} graphlets pruned</div>
          </div>
          <div style={{ width: 1, background: "#1f1f23", flexShrink: 0 }} />
          <div style={{ width: 200, flexShrink: 0, display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.06em" }}>resulting plan</div>
            <UnionAllTree matchedGLs={CONCEPT_MATCHED} queryClause="MATCH (n) WHERE team IS NOT NULL" />
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Plan types ───────────────────────────────────────────────────────────────
interface PlanNode {
  op: string;
  color: string;
  detail?: string;
  rows?: string;
  children?: PlanNode[];
}

// Scan plan: resolves attrs via Schema Index → Scan per graphlet, UnionAll if multiple
// No type labels — only graphlet IDs
function scanNode(attrs: string[], filterDetail?: string): PlanNode {
  const m = GL.filter(g => getMatchingGLs(attrs).has(g.id));
  const mkScan = (g: typeof GL[number]) => ({
    op: "Scan", color: g.color,
    detail: filterDetail ? `${g.id}  ${filterDetail}` : g.id,
    rows: `${g.nodeIds.length}`,
  });
  if (m.length === 1) return mkScan(m[0]);
  const total = m.reduce((s, g) => s + g.nodeIds.length, 0);
  return {
    op: "UnionAll", color: "#10B981",
    detail: `${m.map(g => g.id).join(" · ")}  (Schema Index)`,
    rows: `${total}`,
    children: m.map(mkScan),
  };
}

// ─── Node detail content (shown on click) ────────────────────────────────────
interface DetailRow { label: string; value: string; highlight?: boolean }

function getNodeDetails(node: PlanNode): DetailRow[] {
  switch (node.op) {
    case "Projection":
      return [
        { label: "Output columns", value: node.detail || "all" },
        { label: "Note", value: "No computation — column selection only" },
      ];
    case "Filter":
      return [
        { label: "Predicate", value: node.detail || "—" },
        { label: "Applied", value: "Per row, after child operator" },
        { label: "Optimization", value: "Physical plan pushes filter into scan/seek ops" },
      ];
    case "Join":
      return [
        { label: "Join type", value: "Logical join" },
        { label: "Key", value: node.detail || "—" },
        { label: "Physical", value: "AdjIdxJoin (edge) → IdSeek (node)" },
        { label: "Note", value: "Two logical joins collapse to one pipeline" },
      ];
    case "AdjIdxJoin":
      return [
        { label: "Join type", value: "Adjacency Index Join" },
        { label: "Edge", value: node.detail || "—" },
        { label: "Strategy", value: "For each left node, probe adjacency index" },
        { label: "Output", value: "Left node + right node ID from edge", highlight: true },
      ];
    case "IdSeek":
      return [
        { label: "Lookup", value: "Node by ID (from AdjIdxJoin)" },
        { label: "Filter", value: node.detail || "none" },
        { label: "Strategy", value: "O(1) direct ID lookup into node store" },
        { label: "Note", value: "Replaces full right-side scan", highlight: true },
      ];
    case "UnionAll":
      return [
        { label: "Branches", value: String((node.children || []).length) },
        { label: "Total rows", value: node.rows || "?" },
        { label: "Selection", value: node.detail || "" },
        { label: "Note", value: "Schema Index pruned irrelevant graphlets" },
      ];
    case "Scan":
      return [
        { label: "Graphlet", value: node.detail || "" },
        { label: "Rows", value: `${node.rows || "?"} nodes` },
        { label: "Schema guarantee", value: "All attrs present — no null checks", highlight: true },
        { label: "Cost", value: "Minimal — direct graphlet access" },
      ];
    case "SeqScan":
      return [
        { label: "Scan mode", value: "Sequential — no index" },
        { label: "Scope", value: node.rows ? `${node.rows} nodes` : "all nodes" },
        { label: "Warning", value: "⚠ Reads every node — expensive at scale", highlight: true },
      ];
    default:
      return [{ label: "Info", value: node.detail || "" }];
  }
}

// ─── Plan bar (Dalibo-style horizontal bar, clickable) ────────────────────────
function PlanBar({
  node, nodeId, selectedId, onSelect,
}: {
  node: PlanNode; nodeId: string;
  selectedId: string | null; onSelect: (id: string | null) => void;
}) {
  const isSelected = selectedId === nodeId;
  const details    = getNodeDetails(node);

  return (
    <div style={{ display: "flex", flexDirection: "column" }}>
      {/* Horizontal bar */}
      <div
        onClick={e => { e.stopPropagation(); onSelect(isSelected ? null : nodeId); }}
        style={{
          display: "flex", alignItems: "center", gap: 10,
          background: isSelected ? node.color + "18" : "#0a0a0d",
          border: `1px solid ${isSelected ? node.color + "55" : node.color + "30"}`,
          borderLeft: `4px solid ${node.color}`,
          borderRadius: isSelected ? "6px 6px 0 0" : 6,
          padding: "8px 12px",
          cursor: "pointer",
          transition: "background 0.15s, border-color 0.15s, box-shadow 0.15s",
          boxShadow: isSelected ? `0 0 10px ${node.color}20` : "none",
          userSelect: "none",
        }}
      >
        <span style={{ width: 7, height: 7, borderRadius: 2, background: node.color, flexShrink: 0 }} />
        <span style={{ fontSize: 15, fontWeight: 700, color: node.color, fontFamily: "monospace", flexShrink: 0 }}>{node.op}</span>
        {node.detail && (
          <span style={{ fontSize: 14, color: "#52525b", fontFamily: "monospace", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
            {node.detail}
          </span>
        )}
        {node.rows && (
          <span style={{ fontSize: 13, color: node.color + "90", fontFamily: "monospace", flexShrink: 0 }}>
            {node.rows} rows
          </span>
        )}
        <span style={{ fontSize: 10, color: "#2a2a30", flexShrink: 0, marginLeft: 2 }}>
          {isSelected ? "▲" : "▼"}
        </span>
      </div>

      {/* Expanded detail panel */}
      <AnimatePresence>
        {isSelected && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.15 }}
            style={{ overflow: "hidden" }}
          >
            <div style={{
              background: node.color + "0c",
              border: `1px solid ${node.color}35`,
              borderTop: "none",
              borderRadius: "0 0 6px 6px",
              padding: "6px 12px 8px",
              display: "flex", flexDirection: "column", gap: 3,
            }}>
              {details.map((d, i) => (
                <div key={i} style={{ display: "flex", gap: 8, fontSize: 13, fontFamily: "monospace" }}>
                  <span style={{ color: "#3f3f46", minWidth: 130, flexShrink: 0 }}>{d.label}</span>
                  <span style={{ color: d.highlight ? node.color : "#71717a" }}>{d.value}</span>
                </div>
              ))}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ─── Plan tree: horizontal bars with connector lines ─────────────────────────
function PlanTreeNode({
  node, nodeId = "0", selectedId, onSelect, depth = 0,
}: {
  node: PlanNode; nodeId?: string;
  selectedId: string | null; onSelect: (id: string | null) => void;
  depth?: number;
}) {
  const children = node.children ?? [];
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
      <PlanBar node={node} nodeId={nodeId} selectedId={selectedId} onSelect={onSelect} />
      {children.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
          {children.map((c, i) => {
            const isLast = i === children.length - 1;
            return (
              <div key={i} style={{ display: "flex", alignItems: "stretch" }}>
                {/* Vertical + horizontal connector */}
                <div style={{ width: 28, flexShrink: 0, position: "relative" }}>
                  {/* vertical line (full height except last child stops at midpoint) */}
                  <div style={{
                    position: "absolute",
                    top: 0,
                    bottom: isLast ? "50%" : 0,
                    left: 14,
                    width: 1,
                    background: "#2a2a30",
                  }} />
                  {/* horizontal stub to child */}
                  <div style={{
                    position: "absolute",
                    top: "50%",
                    left: 14,
                    width: 14,
                    height: 1,
                    background: "#2a2a30",
                  }} />
                </div>
                {/* Child subtree */}
                <div style={{ flex: 1, paddingTop: 4, paddingBottom: isLast ? 0 : 4 }}>
                  <PlanTreeNode
                    node={c} nodeId={`${nodeId}-${i}`}
                    selectedId={selectedId} onSelect={onSelect}
                    depth={depth + 1}
                  />
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ─── Schema bar ───────────────────────────────────────────────────────────────
function SchemaBar() {
  return (
    <div style={{
      background: "#131316", border: "1px solid #27272a", borderRadius: 8,
      padding: "10px 14px", flexShrink: 0, display: "flex", gap: 20,
    }}>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 7 }}>Node Graphlets</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          {GL.map(g => (
            <div key={g.id} style={{ display: "flex", alignItems: "baseline", gap: 8, fontSize: 14, fontFamily: "monospace" }}>
              <span style={{ color: g.color, fontWeight: 700, minWidth: 42, flexShrink: 0 }}>{g.id}</span>
              <span style={{ color: "#3f3f46" }}>·</span>
              <span style={{ color: "#52525b", lineHeight: 1.5 }}>
                {[...g.schema].map((a, i) => (
                  <span key={a}>
                    <span style={{ color: "#71717a" }}>{a}</span>
                    {i < g.schema.length - 1 && <span style={{ color: "#2a2a30", margin: "0 3px" }}>·</span>}
                  </span>
                ))}
              </span>
            </div>
          ))}
        </div>
      </div>
      <div style={{ width: 1, background: "#1f1f23", flexShrink: 0 }} />
      <div style={{ flexShrink: 0 }}>
        <div style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 7 }}>Edge Graphlets</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
          {["[:birthPlace]", "[:rdf:type]", "[:nationality]"].map(e => (
            <span key={e} style={{
              fontSize: 14, fontFamily: "monospace", color: "#a1a1aa",
              background: "#1a1a1e", padding: "2px 8px", borderRadius: 4,
              border: "1px solid #27272a", display: "inline-block",
            }}>{e}</span>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─── Results table ────────────────────────────────────────────────────────────
function ResultsTable({ rows }: { rows: Record<string, string>[] }) {
  if (!rows.length) return null;
  const cols = Object.keys(rows[0]);
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 14, fontFamily: "monospace" }}>
        <thead><tr>
          {cols.map(c => (
            <th key={c} style={{ textAlign: "left", padding: "5px 12px", color: "#52525b", borderBottom: "1px solid #27272a", fontWeight: 600, whiteSpace: "nowrap" }}>{c}</th>
          ))}
        </tr></thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? "transparent" : "#0e0e10" }}>
              {cols.map(c => (
                <td key={c} style={{ padding: "5px 12px", color: "#a1a1aa", borderBottom: "1px solid #1a1a1e", whiteSpace: "nowrap" }}>{row[c]}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Plan skeleton loader ─────────────────────────────────────────────────────
function PlanSkeleton() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {[{ ml: 0 }, { ml: 22 }, { ml: 44 }, { ml: 66 }, { ml: 66 }].map((s, i) => (
        <motion.div key={i}
          animate={{ opacity: [0.1, 0.26, 0.1] }}
          transition={{ duration: 1.1, repeat: Infinity, delay: i * 0.14 }}
          style={{
            height: 38, marginLeft: s.ml,
            background: "#10B98114",
            borderRadius: 6,
            borderLeft: "4px solid #10B98140",
          }}
        />
      ))}
    </div>
  );
}

// ─── Preset definitions ───────────────────────────────────────────────────────
interface Preset {
  label: string;
  cypher: string;
  attrs: string[];
  logical: PlanNode;
  physical: PlanNode;
  results: Record<string, string>[];
  stats: { rows: number; ms: string; scanned: string };
}

const PRESETS: Preset[] = [
  {
    label: "Goalkeeper",
    cypher: `MATCH (p)-[:birthPlace]->(c)
WHERE p.position = 'Goalkeeper'
  AND c.populationTotal > 1000000
RETURN p.name, p.birthDate,
       p.birthPlace, c.populationTotal`,
    attrs: ["birthDate", "birthPlace"],
    // Logical: left-deep tree, explicit Filter nodes (no pushdown yet)
    logical: {
      op: "Projection", color: "#71717a", detail: "name · birthDate · birthPlace · pop",
      children: [{
        op: "Filter", color: "#3B82F6", detail: "pop > 1,000,000",
        children: [{
          op: "Join", color: "#8B5CF6", detail: "⋈ [:birthPlace]",
          children: [
            {
              op: "Filter", color: "#3B82F6", detail: "position = 'Goalkeeper'",
              children: [{
                op: "UnionAll", color: "#10B981", detail: "GL-1 · GL-2 · GL-3  (Schema Index)", rows: "16",
                children: [
                  { op: "Scan", color: "#e84545", detail: "GL-1", rows: "6" },
                  { op: "Scan", color: "#3B82F6", detail: "GL-2", rows: "6" },
                  { op: "Scan", color: "#8B5CF6", detail: "GL-3", rows: "4" },
                ],
              }],
            },
            {
              op: "UnionAll", color: "#10B981", detail: "GL-C  (Schema Index)", rows: "∞",
              children: [
                { op: "Scan", color: "#F59E0B", detail: "GL-C", rows: "∞" },
              ],
            },
          ],
        }],
      }],
    },
    // Physical: filter pushdown — pos='GK' into person Scans, pop>1M into city Scan
    physical: {
      op: "Projection", color: "#71717a", detail: "name · birthDate · birthPlace · pop",
      children: [{
        op: "AdjIdxJoin", color: "#8B5CF6", detail: "[:birthPlace]",
        children: [
          {
            op: "UnionAll", color: "#10B981", detail: "GL-1 · GL-2 · GL-3  (Schema Index)", rows: "16",
            children: [
              { op: "Scan", color: "#e84545", detail: "GL-1  [pos = 'GK']", rows: "6" },
              { op: "Scan", color: "#3B82F6", detail: "GL-2  [pos = 'GK']", rows: "6" },
              { op: "Scan", color: "#8B5CF6", detail: "GL-3  [pos = 'GK']", rows: "4" },
            ],
          },
          {
            op: "UnionAll", color: "#10B981", detail: "GL-C  (Schema Index)", rows: "∞",
            children: [
              { op: "Scan", color: "#F59E0B", detail: "GL-C  [pop > 1,000,000]", rows: "∞" },
            ],
          },
        ],
      }],
    },
    results: [
      { name: "Gianluigi Buffon", birthDate: "1978-01-28", birthPlace: "Carrara",        pop: "65,497"     },
      { name: "Oliver Kahn",      birthDate: "1969-06-15", birthPlace: "Karlsruhe",      pop: "308,436"    },
      { name: "Petr Čech",        birthDate: "1982-05-20", birthPlace: "Plzeň",          pop: "170,548"    },
      { name: "Manuel Neuer",     birthDate: "1986-03-27", birthPlace: "Gelsenkirchen",  pop: "260,654"    },
      { name: "Iker Casillas",    birthDate: "1981-05-20", birthPlace: "Madrid",         pop: "3,223,334"  },
    ],
    stats: { rows: 5, ms: "14", scanned: "3/5" },
  },
  {
    label: "Scholar",
    cypher: `MATCH (s)
WHERE s.almaMater IS NOT NULL
  AND s.award IS NOT NULL
  AND s.occupation IS NOT NULL
RETURN s.name, s.award,
       s.almaMater, s.occupation`,
    attrs: ["almaMater", "award", "occupation"],
    // Logical: Schema Index → only GL-2 has all three attrs; Filter still present logically
    logical: {
      op: "Projection", color: "#71717a", detail: "name · award · almaMater · occupation",
      children: [{
        op: "Filter", color: "#3B82F6", detail: "almaMater IS NOT NULL ∧ award IS NOT NULL",
        children: [scanNode(["almaMater", "award", "occupation"])],
      }],
    },
    // Physical: IS NOT NULL trivially true (schema guarantee) → filter eliminated; plain Scan
    physical: {
      op: "Projection", color: "#71717a", detail: "name · award · almaMater · occupation",
      children: [scanNode(["almaMater", "award", "occupation"])],
    },
    results: [
      { name: "Marie Curie",          award: "Nobel Prize (×2)", almaMater: "Univ. of Paris", occupation: "Physicist" },
      { name: "Albert Einstein",      award: "Nobel Prize",      almaMater: "ETH Zürich",     occupation: "Physicist" },
      { name: "Florence Nightingale", award: "Royal Red Cross",  almaMater: "self-taught",    occupation: "Nurse"     },
      { name: "Simone de Beauvoir",   award: "Prix Goncourt",    almaMater: "Paris-Sorbonne", occupation: "Author"    },
    ],
    stats: { rows: 4, ms: "2", scanned: "1/5" },
  },
  {
    label: "Athlete",
    cypher: `MATCH (a)
WHERE a.team IS NOT NULL
  AND a.award IS NOT NULL
RETURN a.name, a.team,
       a.award, a.nationality`,
    attrs: ["team", "award"],
    // Logical: Schema Index → GL-1 + GL-3; Filter still present logically
    logical: {
      op: "Projection", color: "#71717a", detail: "name · team · award · nationality",
      children: [{
        op: "Filter", color: "#3B82F6", detail: "team IS NOT NULL ∧ award IS NOT NULL",
        children: [scanNode(["team", "award"])],
      }],
    },
    // Physical: IS NOT NULL trivially true (schema guarantee) → filter eliminated
    physical: {
      op: "Projection", color: "#71717a", detail: "name · team · award · nationality",
      children: [scanNode(["team", "award"])],
    },
    results: [
      { name: "Gianluigi Buffon", team: "Juventus FC",   award: "Serie A MVP",    nationality: "Italian"   },
      { name: "Michael Jordan",   team: "Chicago Bulls", award: "NBA MVP ×5",     nationality: "American"  },
      { name: "Serena Williams",  team: "USA",           award: "Grand Slam ×23", nationality: "American"  },
      { name: "Pelé",             team: "Santos FC",     award: "World Cup ×3",   nationality: "Brazilian" },
      { name: "Manuel Neuer",     team: "Bayern Munich", award: "Yashin Trophy",  nationality: "German"    },
      { name: "Iker Casillas",    team: "Real Madrid",   award: "UEFA Best GK",   nationality: "Spanish"   },
    ],
    stats: { rows: 6, ms: "8", scanned: "2/5" },
  },
];

// ─── Plan panel ───────────────────────────────────────────────────────────────
function PlanPanel({
  title, titleColor, borderColor, bgColor,
  node, loading, hint,
}: {
  title: string; titleColor: string; borderColor: string; bgColor: string;
  node: PlanNode | null; loading: boolean; hint?: string;
}) {
  const [selectedId, setSelectedId] = useState<string | null>(null);

  return (
    <div style={{ background: bgColor, border: `1px solid ${borderColor}`, borderRadius: 8, padding: "12px 16px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
        <span style={{ fontSize: 13, color: titleColor, fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.07em", fontWeight: 600 }}>
          {title}
        </span>
        {loading && hint && (
          <motion.span
            animate={{ opacity: [1, 0.3, 1] }}
            transition={{ duration: 0.8, repeat: Infinity }}
            style={{ fontSize: 13, color: titleColor, fontFamily: "monospace", opacity: 0.7 }}
          >
            {hint}
          </motion.span>
        )}
        {!loading && node && (
          <span style={{ fontSize: 10, color: "#3f3f46", fontFamily: "monospace" }}>
            click a node to inspect
          </span>
        )}
      </div>
      {loading ? (
        <PlanSkeleton />
      ) : node ? (
        <PlanTreeNode node={node} selectedId={selectedId} onSelect={setSelectedId} />
      ) : null}
    </div>
  );
}

// ─── Interactive view ─────────────────────────────────────────────────────────
// phase: 0=idle  1=logical  2=loading-physical  3=physical  4=results
type Phase = 0 | 1 | 2 | 3 | 4;

function InteractiveView() {
  const [presetIdx, setPresetIdx] = useState<number | null>(0);
  const [editorText, setEditorText] = useState(PRESETS[0].cypher);
  const [phase, setPhase] = useState<Phase>(0);
  const [runPreset, setRunPreset] = useState<Preset | null>(null);
  const [runAttrs, setRunAttrs]   = useState<string[]>([]);

  const loadPreset = (i: number) => {
    setPresetIdx(i); setEditorText(PRESETS[i].cypher); setPhase(0);
  };
  const openCustom = () => {
    setPresetIdx(null); setEditorText(""); setPhase(0);
  };

  const handleRun = () => {
    if (phase === 1 || phase === 2) return;
    const preset = presetIdx !== null ? PRESETS[presetIdx] : null;
    const attrs  = preset ? preset.attrs : extractAttrs(editorText);
    setRunPreset(preset); setRunAttrs(attrs); setPhase(0);
    requestAnimationFrame(() => {
      setPhase(1);
      setTimeout(() => setPhase(2), 900);
      setTimeout(() => setPhase(3), 2200);
      setTimeout(() => setPhase(4), 2600);
    });
  };

  const matchedGLs = getMatchingGLs(runAttrs);

  // Custom query plans
  const customLogical: PlanNode = {
    op: "Projection", color: "#71717a", detail: "...",
    children: [{
      op: "Filter", color: "#3B82F6", detail: runAttrs.map(a => `${a} IS NOT NULL`).join(" ∧ ") || "—",
      children: [runAttrs.length > 0
        ? scanNode(runAttrs)
        : { op: "SeqScan", color: "#ef4444", detail: "All Nodes", rows: "25" }
      ],
    }],
  };
  const customPhysical: PlanNode = {
    op: "Projection", color: "#71717a", detail: "...",
    children: [matchedGLs.size === 0
      ? { op: "NoMatch", color: "#3f3f46", detail: "no graphlets matched" }
      : scanNode(runAttrs)],
  };

  const logical  = runPreset ? runPreset.logical  : customLogical;
  const physical = runPreset ? runPreset.physical : customPhysical;
  const isRunning = phase === 1 || phase === 2;

  return (
    <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 8 }}>

      <SchemaBar />

      {/* Editor row */}
      <div style={{ flexShrink: 0, display: "flex", flexDirection: "column", gap: 5 }}>
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span style={{ fontSize: 14, color: "#52525b", fontFamily: "monospace" }}>Presets:</span>
          {PRESETS.map((p, i) => (
            <button key={p.label} onClick={() => loadPreset(i)} style={{
              padding: "4px 14px", borderRadius: 5, cursor: "pointer", fontSize: 15, fontFamily: "monospace",
              border: `1px solid ${presetIdx === i ? "#3B82F6" : "#27272a"}`,
              background: presetIdx === i ? "#3B82F618" : "transparent",
              color: presetIdx === i ? "#60a5fa" : "#71717a",
            }}>{p.label}</button>
          ))}
          <button onClick={openCustom} style={{
            padding: "4px 14px", borderRadius: 5, cursor: "pointer", fontSize: 15, fontFamily: "monospace",
            border: `1px solid ${presetIdx === null ? "#8B5CF6" : "#27272a"}`,
            background: presetIdx === null ? "#8B5CF618" : "transparent",
            color: presetIdx === null ? "#c4b5fd" : "#71717a",
          }}>Custom ✎</button>
          <button onClick={handleRun} disabled={isRunning} style={{
            marginLeft: "auto", padding: "4px 20px", borderRadius: 5,
            cursor: isRunning ? "not-allowed" : "pointer",
            fontSize: 15, fontFamily: "monospace", fontWeight: 700, border: "none",
            background: isRunning ? "#7f1d1d" : "#e84545", color: "#fff",
            opacity: isRunning ? 0.65 : 1,
          }}>
            {isRunning ? "Running…" : "▶ Run"}
          </button>
        </div>
        <textarea
          value={editorText}
          readOnly={presetIdx !== null}
          onChange={e => { setEditorText(e.target.value); setPresetIdx(null); setPhase(0); }}
          placeholder={"MATCH (p)\nWHERE p.birthDate IS NOT NULL\nRETURN p.name, p.birthDate"}
          style={{
            background: "#131316",
            border: `1px solid ${presetIdx !== null ? "#27272a" : "#3f3f46"}`,
            borderRadius: 8, padding: "10px 14px", fontSize: 16, color: "#f4f4f5",
            fontFamily: "monospace", lineHeight: 1.65, resize: "none", height: 160,
            outline: "none", width: "100%", boxSizing: "border-box",
            cursor: presetIdx !== null ? "default" : "text",
          }}
        />
      </div>

      {/* Sequential plan + results */}
      <div style={{ flex: 1, minHeight: 0, overflowY: "auto", display: "flex", flexDirection: "column", gap: 10 }}>
        {phase === 0 && (
          <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", color: "#3f3f46", fontSize: 16, fontFamily: "monospace" }}>
            ▶ Run a query to see the execution plan
          </div>
        )}

        {/* Logical Plan */}
        <AnimatePresence>
          {phase >= 1 && (
            <motion.div key="logical" initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.3 }}>
              <PlanPanel
                title="Logical Plan"
                titleColor="#52525b"
                borderColor="#27272a"
                bgColor="#131316"
                node={logical}
                loading={false}
              />
            </motion.div>
          )}
        </AnimatePresence>

        {/* Physical Plan */}
        <AnimatePresence>
          {phase >= 2 && (
            <motion.div key="physical" initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.3 }}>
              <PlanPanel
                title="GEM-Optimized Physical Plan"
                titleColor="#10B981"
                borderColor="#10B98132"
                bgColor="#0b1410"
                node={phase >= 3 ? physical : null}
                loading={phase === 2}
                hint="GEM optimizer running…"
              />
            </motion.div>
          )}
        </AnimatePresence>

        {/* Results */}
        <AnimatePresence>
          {phase >= 4 && (
            <motion.div key="results" initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.3 }}>
              <div style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "12px 16px" }}>
                <div style={{ display: "flex", gap: 16, marginBottom: 10, alignItems: "center" }}>
                  <span style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.07em", fontWeight: 600 }}>
                    Results
                  </span>
                  {runPreset ? (
                    <>
                      <span style={{ fontSize: 15, fontFamily: "monospace", color: "#10B981" }}>{runPreset.stats.rows} rows</span>
                      <span style={{ fontSize: 15, fontFamily: "monospace", color: "#a1a1aa" }}>{runPreset.stats.ms} ms</span>
                      <span style={{ fontSize: 15, fontFamily: "monospace", color: "#a1a1aa" }}>{runPreset.stats.scanned} graphlets scanned</span>
                      <span style={{ marginLeft: "auto", fontSize: 15, fontFamily: "monospace", color: "#10B981" }}>
                        ⚡ {GL.length - matchedGLs.size}/{GL.length} pruned by Schema Index
                      </span>
                    </>
                  ) : (
                    <span style={{ fontSize: 15, fontFamily: "monospace", color: "#71717a" }}>
                      {matchedGLs.size}/{GL.length} graphlets matched
                    </span>
                  )}
                </div>
                {runPreset
                  ? <ResultsTable rows={runPreset.results} />
                  : <div style={{ fontSize: 14, color: "#52525b", fontFamily: "monospace" }}>Results preview unavailable for custom queries.</div>
                }
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

    </div>
  );
}

// ─── Main scene ───────────────────────────────────────────────────────────────
const STEP_TITLES    = ["Schema Index + UNION ALL Plan", "Live Query Demo"];
const STEP_SUBTITLES = ["How TurboLynx executes graph queries", "Watch the index in action"];

export default function S2_Query({ step }: Props) {
  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{ maxWidth: 1440, margin: "0 auto", padding: "28px 48px", height: "100%", display: "flex", flexDirection: "column", boxSizing: "border-box", gap: 16 }}>
        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} transition={{ duration: 0.25 }}>
            <div style={{ fontSize: 13, color: "#3B82F6", fontFamily: "monospace", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.08em" }}>
              Query — {STEP_SUBTITLES[step]}
            </div>
            <h2 style={{ fontSize: 26, fontWeight: 700, color: "#f4f4f5", margin: 0 }}>{STEP_TITLES[step]}</h2>
          </motion.div>
        </AnimatePresence>

        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            transition={{ duration: 0.25 }} style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
            {step === 0 && <ConceptView />}
            {step === 1 && <InteractiveView />}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}
