"use client";
import { useState, useEffect, useRef, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface Props { step: number; onStep: (n: number) => void; }

// ─── Plan node (shared by S2-style visual tree) ───────────────────────────────
interface PlanNode {
  op: string; color: string; detail?: string; time?: string;
  children?: PlanNode[];
}

// ─── SVG Plan Diagram (reused from S2) ────────────────────────────────────────
const CW = 160, CH = 54, HG = 12, VG = 26;

interface LNode {
  op: string; color: string; detail?: string; time?: string;
  x: number; y: number; parentIdx: number;
}

function layoutTree(root: PlanNode): LNode[] {
  const result: LNode[] = [];
  const wCache = new Map<PlanNode, number>();
  function getW(n: PlanNode): number {
    if (wCache.has(n)) return wCache.get(n)!;
    const w = !n.children?.length
      ? CW
      : Math.max(CW, n.children.map(getW).reduce((a, b) => a + b, 0) + (n.children.length - 1) * HG);
    wCache.set(n, w);
    return w;
  }
  function walk(n: PlanNode, left: number, top: number, pi: number) {
    const sw = getW(n);
    const idx = result.length;
    result.push({
      op: n.op, color: n.color, detail: n.detail, time: n.time,
      x: left + sw / 2, y: top, parentIdx: pi,
    });
    if (n.children?.length) {
      const cws = n.children.map(getW);
      const total = cws.reduce((a, b) => a + b, 0) + (cws.length - 1) * HG;
      let cx = left + (sw - total) / 2;
      n.children.forEach((c, i) => { walk(c, cx, top + CH + VG, idx); cx += cws[i] + HG; });
    }
  }
  walk(root, 0, 0, -1);
  return result;
}

function PlanDiagram({ root }: { root: PlanNode }) {
  const nodes = useMemo(() => layoutTree(root), [root]);
  if (nodes.length === 0) return null;
  const w = Math.max(...nodes.map(n => n.x)) + CW / 2;
  const h = Math.max(...nodes.map(n => n.y)) + CH;
  const pad = 16;
  return (
    <svg viewBox={`${-pad} ${-pad} ${w + pad * 2} ${h + pad * 2}`}
      preserveAspectRatio="xMidYMid meet"
      style={{ width: "100%", height: "100%", display: "block" }}>
      {nodes.map((n, i) => {
        if (n.parentIdx < 0) return null;
        const p = nodes[n.parentIdx];
        const my = (p.y + CH + n.y) / 2;
        return (
          <path key={`e-${i}`}
            d={`M${p.x},${p.y + CH} C${p.x},${my} ${n.x},${my} ${n.x},${n.y}`}
            fill="none" stroke="#d4d4d8" strokeWidth={1.5} />
        );
      })}
      {nodes.map((n, i) => {
        const lx = n.x - CW / 2;
        return (
          <g key={i}>
            <rect x={lx + 1.5} y={n.y + 1.5} width={CW} height={CH} rx={7} fill="#00000005" />
            <rect x={lx} y={n.y} width={CW} height={CH} rx={7}
              fill="white" stroke={n.color + "35"} strokeWidth={1} />
            <rect x={lx} y={n.y + 5} width={3.5} height={CH - 10} rx={2} fill={n.color} />
            <text x={lx + 12} y={n.y + 21} fontSize={14} fontWeight={700}
              fontFamily="monospace" fill={n.color}>{n.op}</text>
            {n.time && (
              <text x={lx + CW - 6} y={n.y + 21} fontSize={11} fontWeight={600}
                fontFamily="monospace" fill="#9ca3af" textAnchor="end">{n.time}</text>
            )}
            {n.detail && (
              <text x={lx + 12} y={n.y + 40} fontSize={12}
                fontFamily="monospace" fill="#6b7280">
                {n.detail.length > 18 ? n.detail.slice(0, 16) + "\u2026" : n.detail}
              </text>
            )}
          </g>
        );
      })}
    </svg>
  );
}

// ─── Preset queries ──────────────────────────────────────────────────────────
interface Preset {
  label: string; query: string;
  cols: string[]; rows: string[][];
  totalRows: number; ms: number;
  plan: PlanNode;
  // Execution pipeline metadata
  pipeline: {
    parse: number;       // ms
    schema: string;      // e.g. "Person(5 GLs) × Film(4 GLs)"
    schemaMs: number;
    gem: string;         // e.g. "Split → 5×4 = 20 VG combos"
    gemMs: number;
    planMs: number;
    execMs: number;
  };
}

const PRESETS: Preset[] = [
  {
    label: "Person → Film",
    query: "MATCH (p)-[:directed]->(f)\nWHERE p.birthDate IS NOT NULL\nRETURN p.name, f.title\nLIMIT 20",
    cols: ["p.name", "f.title"],
    rows: [
      ["Christopher Nolan",  "Inception"],
      ["Steven Spielberg",   "Schindler's List"],
      ["Martin Scorsese",    "The Departed"],
      ["David Fincher",      "Fight Club"],
      ["Ridley Scott",       "Gladiator"],
    ],
    totalRows: 20, ms: 483,
    plan: {
      op: "Projection", color: "#71717a", detail: "p.name, f.title",
      children: [{
        op: "UnionAll", color: "#8B5CF6", detail: "20 VG combos",
        children: [
          { op: "NLJoin", color: "#a78bfa", detail: "GL-p1 ⋈ GL-f1",
            children: [
              { op: "Scan", color: "#DC2626", detail: "GL-p1  44.2K", time: "12.4ms" },
              { op: "Scan", color: "#F59E0B", detail: "GL-f1  15.0K", time: "4.1ms" },
            ]},
          { op: "NLJoin", color: "#a78bfa", detail: "GL-p1 ⋈ GL-f2" },
          { op: "…", color: "#9ca3af", detail: "18 more combos" },
        ],
      }],
    },
    pipeline: {
      parse: 0.2,
      schema: "Person(5 GLs) × Film(4 GLs)",
      schemaMs: 0.1,
      gem: "Split → 5×4 = 20 VG combos, GOO join ordering",
      gemMs: 1.2,
      planMs: 0.3,
      execMs: 481,
    },
  },
  {
    label: "Film → Location",
    query: "MATCH (f)-[:filmed_in]->(l)\nWHERE f.year >= 2000\nRETURN f.title, f.year, l.name\nORDER BY f.year DESC\nLIMIT 15",
    cols: ["f.title", "f.year", "l.name"],
    rows: [
      ["Oppenheimer",        "2023", "United States"],
      ["The Batman",         "2022", "United Kingdom"],
      ["Dune",               "2021", "Jordan"],
      ["Parasite",           "2019", "South Korea"],
      ["Avengers: Endgame",  "2019", "United States"],
    ],
    totalRows: 15, ms: 312,
    plan: {
      op: "Sort", color: "#71717a", detail: "f.year DESC",
      children: [{
        op: "Projection", color: "#71717a", detail: "f.title, f.year, l.name",
        children: [{
          op: "UnionAll", color: "#8B5CF6", detail: "8 VG combos",
          children: [
            { op: "NLJoin", color: "#a78bfa", detail: "GL-f1 ⋈ GL-l1",
              children: [
                { op: "Scan", color: "#F59E0B", detail: "GL-f1  15.0K", time: "6.2ms" },
                { op: "Scan", color: "#10B981", detail: "GL-l1  9.8K", time: "2.1ms" },
              ]},
            { op: "…", color: "#9ca3af", detail: "7 more combos" },
          ],
        }],
      }],
    },
    pipeline: {
      parse: 0.15,
      schema: "Film(4 GLs) × Location(2 GLs)",
      schemaMs: 0.08,
      gem: "Split → 4×2 = 8 VG combos, GOO join ordering",
      gemMs: 0.6,
      planMs: 0.2,
      execMs: 311,
    },
  },
  {
    label: "2-hop: Person → Film → Location",
    query: 'MATCH (p)-[:starring]->(f)-[:filmed_in]->(l)\nWHERE p.nationality = "American"\nRETURN p.name, f.title, l.name\nLIMIT 20',
    cols: ["p.name", "f.title", "l.name"],
    rows: [
      ["Brad Pitt",         "Fight Club",    "Germany"],
      ["Tom Hanks",         "Cast Away",     "United States"],
      ["Meryl Streep",      "The Iron Lady", "United Kingdom"],
      ["Cate Blanchett",    "Carol",         "United States"],
      ["Denzel Washington", "Training Day",  "United States"],
    ],
    totalRows: 20, ms: 891,
    plan: {
      op: "Projection", color: "#71717a", detail: "p.name, f.title, l.name",
      children: [{
        op: "UnionAll", color: "#8B5CF6", detail: "12 VG combos",
        children: [
          { op: "NLJoin", color: "#a78bfa", detail: "GL-f1 ⋈ GL-l1",
            children: [
              { op: "NLJoin", color: "#a78bfa", detail: "GL-p1 ⋈ GL-f1",
                children: [
                  { op: "Scan", color: "#DC2626", detail: "GL-p1  44.2K", time: "12.4ms" },
                  { op: "Scan", color: "#F59E0B", detail: "GL-f1  15.0K", time: "4.1ms" },
                ]},
              { op: "Scan", color: "#10B981", detail: "GL-l1  9.8K", time: "2.1ms" },
            ]},
          { op: "…", color: "#9ca3af", detail: "11 more combos" },
        ],
      }],
    },
    pipeline: {
      parse: 0.3,
      schema: "Person(3 GLs) × Film(2 GLs) × Location(2 GLs)",
      schemaMs: 0.15,
      gem: "Split → 3×2×2 = 12 VG combos, GOO per VG",
      gemMs: 2.1,
      planMs: 0.5,
      execMs: 888,
    },
  },
];

const CUSTOM_MOCK: Preset = {
  label: "Custom", query: "",
  cols: ["result"], rows: [["(query executed on TurboLynx)"]],
  totalRows: 1, ms: 247,
  plan: {
    op: "Projection", color: "#71717a", detail: "custom output",
    children: [{ op: "Scan", color: "#8B5CF6", detail: "DBpedia graph" }],
  },
  pipeline: { parse: 0.1, schema: "auto-resolved", schemaMs: 0.05, gem: "auto-optimized", gemMs: 0.8, planMs: 0.2, execMs: 246 },
};

// ─── Execution Pipeline Steps ─────────────────────────────────────────────────
interface PipelineStep {
  label: string; color: string; ms: number; detail?: string;
  type: "text" | "plan";
}

function buildPipeline(p: Preset): PipelineStep[] {
  return [
    { label: "Parse Cypher", color: "#3B82F6", ms: p.pipeline.parse, type: "text", detail: "Tokenize + AST" },
    { label: "Schema Resolve", color: "#8B5CF6", ms: p.pipeline.schemaMs, type: "text", detail: p.pipeline.schema },
    { label: "GEM Optimize", color: "#F59E0B", ms: p.pipeline.gemMs, type: "text", detail: p.pipeline.gem },
    { label: "Physical Plan", color: "#10B981", ms: p.pipeline.planMs, type: "plan" },
    { label: "Execute", color: "#e84545", ms: p.pipeline.execMs, type: "text", detail: `${p.totalRows} rows in ${p.ms}ms total` },
  ];
}

// ─── Step 0: Live Query Runner ────────────────────────────────────────────────
function LiveQueryRunner() {
  const [presetIdx, setPresetIdx] = useState(0);
  const [query, setQuery] = useState(PRESETS[0].query);
  const [runState, setRunState] = useState<"idle" | "running" | "done">("idle");
  const [result, setResult] = useState<Preset | null>(null);
  const [visibleSteps, setVisibleSteps] = useState(0);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const timersRef = useRef<ReturnType<typeof setTimeout>[]>([]);

  const isCustom = presetIdx === -1;

  const selectPreset = (i: number) => {
    setPresetIdx(i); setQuery(PRESETS[i].query);
    setRunState("idle"); setResult(null); setVisibleSteps(0);
  };
  const selectCustom = () => {
    setPresetIdx(-1); setQuery("");
    setRunState("idle"); setResult(null); setVisibleSteps(0);
    setTimeout(() => textareaRef.current?.focus(), 50);
  };

  const runQuery = () => {
    if (runState === "running") return;
    setRunState("running"); setResult(null); setVisibleSteps(0);
    timersRef.current.forEach(clearTimeout);
    timersRef.current = [];

    const preset = PRESETS.find(p => p.query === query) ?? (isCustom ? CUSTOM_MOCK : PRESETS[0]);
    const steps = buildPipeline(preset);

    // Progressive reveal: each step appears after a delay
    let cumDelay = 150;
    steps.forEach((_, i) => {
      const t = setTimeout(() => setVisibleSteps(i + 1), cumDelay);
      timersRef.current.push(t);
      cumDelay += i === steps.length - 1 ? 300 : 180; // longer pause before execute
    });
    // Done after all steps
    const t = setTimeout(() => { setResult(preset); setRunState("done"); }, cumDelay + 100);
    timersRef.current.push(t);
  };

  useEffect(() => () => timersRef.current.forEach(clearTimeout), []);

  const activePreset = PRESETS.find(p => p.query === query) ?? (isCustom ? CUSTOM_MOCK : PRESETS[0]);
  const pipelineSteps = buildPipeline(activePreset);

  return (
    <div style={{ display: "flex", gap: 16, height: "100%", overflow: "hidden" }}>
      {/* ── Left: Query + Results ── */}
      <div style={{
        width: "42%", flexShrink: 0, display: "flex", flexDirection: "column",
        gap: 10, overflow: "hidden",
      }}>
        {/* Query editor */}
        <div style={{
          flexShrink: 0, background: "#fafbfc", border: "1px solid #e5e7eb",
          borderRadius: 10, padding: "14px 16px",
        }}>
          <div style={{ display: "flex", gap: 5, marginBottom: 8, flexWrap: "wrap", alignItems: "center" }}>
            {PRESETS.map((p, i) => (
              <button key={i} onClick={() => selectPreset(i)} style={{
                padding: "5px 14px", borderRadius: 6, cursor: "pointer",
                fontSize: 14, fontFamily: "monospace", fontWeight: 500,
                border: `1.5px solid ${presetIdx === i ? "#e84545" : "#d4d4d8"}`,
                background: presetIdx === i ? "#e8454510" : "transparent",
                color: presetIdx === i ? "#e84545" : "#52525b",
              }}>{p.label}</button>
            ))}
            <div style={{ width: 1, background: "#d4d4d8", alignSelf: "stretch", margin: "0 2px" }} />
            <button onClick={selectCustom} style={{
              padding: "5px 14px", borderRadius: 6, cursor: "pointer",
              fontSize: 14, fontFamily: "monospace",
              border: `1.5px solid ${isCustom ? "#8B5CF6" : "#d4d4d8"}`,
              background: isCustom ? "#8B5CF610" : "transparent",
              color: isCustom ? "#8B5CF6" : "#52525b",
              fontWeight: isCustom ? 700 : 400,
            }}>Custom</button>
          </div>
          <textarea
            ref={textareaRef}
            value={query}
            onChange={e => { setQuery(e.target.value); setRunState("idle"); setResult(null); setVisibleSteps(0); }}
            placeholder={isCustom ? "MATCH (a)-[:rel]->(b)\nRETURN a.name, b.title\nLIMIT 20" : undefined}
            spellCheck={false}
            style={{
              background: "#fff", border: `1px solid ${isCustom ? "#8B5CF640" : "#e5e7eb"}`,
              borderRadius: 6, padding: "10px 14px", fontSize: 15, color: "#18181b",
              fontFamily: "monospace", lineHeight: 1.7, resize: "none", height: 120,
              outline: "none", boxSizing: "border-box", width: "100%",
            }}
          />
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 10 }}>
            <button onClick={runQuery} disabled={runState === "running"} style={{
              padding: "8px 22px", borderRadius: 7, cursor: runState === "running" ? "not-allowed" : "pointer",
              fontSize: 15, fontFamily: "monospace", fontWeight: 700,
              border: "none",
              background: runState === "running" ? "#f0f1f3" : "#e84545",
              color: runState === "running" ? "#9ca3af" : "#fff",
              transition: "all 0.15s",
            }}>
              {runState === "running" ? "Running…" : "▶  Run on TurboLynx"}
            </button>
            {runState === "running" && (
              <div style={{ flex: 1, height: 3, background: "#e5e7eb", borderRadius: 2, overflow: "hidden" }}>
                <motion.div
                  initial={{ width: "0%" }} animate={{ width: "100%" }}
                  transition={{ duration: 1.2, ease: "linear" }}
                  style={{ height: "100%", background: "#e84545", borderRadius: 2 }}
                />
              </div>
            )}
            {runState === "done" && result && (
              <span style={{ fontSize: 28, fontWeight: 800, color: "#10B981", fontFamily: "monospace" }}>
                {result.ms}ms
              </span>
            )}
          </div>
        </div>

        {/* Results table */}
        <AnimatePresence mode="wait">
          {runState === "done" && result ? (
            <motion.div key="results"
              initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0 }} transition={{ duration: 0.2 }}
              style={{
                flex: 1, minHeight: 0, background: "#fafbfc",
                border: "1px solid #e5e7eb", borderRadius: 10, overflow: "hidden",
                display: "flex", flexDirection: "column",
              }}>
              <div style={{
                padding: "8px 16px", borderBottom: "1px solid #e5e7eb",
                display: "flex", alignItems: "center", justifyContent: "space-between",
                flexShrink: 0,
              }}>
                <span style={{ fontSize: 15, fontFamily: "monospace", fontWeight: 700, color: "#18181b" }}>
                  Results
                </span>
                <span style={{
                  fontSize: 14, fontFamily: "monospace", fontWeight: 600,
                  color: "#10B981", background: "#10B98110", padding: "2px 10px", borderRadius: 5,
                }}>
                  {result.totalRows} rows · {result.ms}ms
                </span>
              </div>
              <div style={{ flex: 1, overflowY: "auto" }}>
                <table style={{
                  fontSize: 14, fontFamily: "monospace", borderCollapse: "collapse",
                  width: "100%", whiteSpace: "nowrap",
                }}>
                  <thead>
                    <tr>
                      {result.cols.map(c => (
                        <th key={c} style={{
                          padding: "6px 14px", textAlign: "left",
                          borderBottom: "1.5px solid #d4d4d8", color: "#71717a",
                          fontWeight: 600, position: "sticky", top: 0, background: "#fafbfc",
                        }}>{c}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.rows.map((row, ri) => (
                      <tr key={ri}>
                        {row.map((v, ci) => (
                          <td key={ci} style={{
                            padding: "5px 14px", color: "#3f3f46",
                            borderBottom: "1px solid #f0f1f3",
                          }}>{v}</td>
                        ))}
                      </tr>
                    ))}
                    <tr>
                      <td colSpan={result.cols.length} style={{
                        padding: "5px 14px", color: "#9ca3af", fontStyle: "italic",
                      }}>
                        … {result.totalRows - result.rows.length} more rows
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </motion.div>
          ) : (
            <motion.div key="placeholder"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
              style={{
                flex: 1, display: "flex", alignItems: "center", justifyContent: "center",
                background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
              }}>
              <span style={{ fontSize: 15, color: "#9ca3af", fontFamily: "monospace" }}>
                {runState === "running" ? "executing…" : "press ▶ to run query"}
              </span>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* ── Right: Execution Pipeline ── */}
      <div style={{
        flex: 1, minWidth: 0, display: "flex", flexDirection: "column",
        background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
        padding: "14px 18px", overflow: "hidden",
      }}>
        <div style={{
          fontSize: 15, color: "#e84545", fontFamily: "monospace", fontWeight: 700,
          textTransform: "uppercase", letterSpacing: "0.07em", marginBottom: 12,
          flexShrink: 0,
        }}>
          Execution Pipeline
        </div>

        <div style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
          {visibleSteps === 0 && runState === "idle" && (
            <div style={{
              height: "100%", display: "flex", alignItems: "center", justifyContent: "center",
            }}>
              <span style={{ fontSize: 15, color: "#9ca3af", fontFamily: "monospace" }}>
                run a query to see the pipeline
              </span>
            </div>
          )}

          {pipelineSteps.map((ps, i) => {
            if (i >= visibleSteps) return null;
            const isDone = runState === "done" || i < visibleSteps - 1;
            return (
              <motion.div key={i}
                initial={{ opacity: 0, x: -12 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ duration: 0.2 }}
                style={{ marginBottom: ps.type === "plan" ? 8 : 10 }}
              >
                {/* Step header */}
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                  <span style={{
                    fontSize: 13, fontWeight: 700, color: isDone ? "#10B981" : ps.color,
                    fontFamily: "monospace",
                  }}>
                    {isDone ? "✓" : "◌"}
                  </span>
                  <span style={{
                    fontSize: 15, fontWeight: 700, color: ps.color,
                    fontFamily: "monospace",
                  }}>
                    {ps.label}
                  </span>
                  <span style={{
                    fontSize: 14, color: "#9ca3af", fontFamily: "monospace",
                    marginLeft: "auto",
                  }}>
                    {ps.ms < 1 ? ps.ms.toFixed(2) : ps.ms.toFixed(1)}ms
                  </span>
                </div>

                {/* Step content */}
                {ps.type === "text" && ps.detail && (
                  <div style={{
                    marginLeft: 21, fontSize: 14, color: "#6b7280",
                    fontFamily: "monospace", lineHeight: 1.5,
                    padding: "4px 10px", background: ps.color + "08",
                    borderLeft: `3px solid ${ps.color}30`, borderRadius: "0 4px 4px 0",
                  }}>
                    {ps.detail}
                  </div>
                )}

                {ps.type === "plan" && (
                  <div style={{
                    marginLeft: 21,
                    background: "#fff", border: "1px solid #e5e7eb",
                    borderRadius: 8, padding: 8, maxHeight: 260, overflow: "auto",
                  }}>
                    <PlanDiagram root={activePreset.plan} />
                  </div>
                )}
              </motion.div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

// ─── Benchmark data — Table 4 ─────────────────────────────────────────────────
type Cell = { c: number | null; t: number | null; x: number | null };
const N = (c: number, t: number, x: number): Cell => ({ c, t, x });
const X = (): Cell => ({ c: null, t: null, x: null });

interface Competitor {
  id: string; name: string; cat: "GDBMS" | "RDBMS"; color: string;
  ldbc_sf1: Cell; ldbc_sf10: Cell; ldbc_sf100: Cell;
  tpch_sf1: Cell; tpch_sf10: Cell; tpch_sf100: Cell;
  dbpedia: Cell;
}

const COMPETITORS: Competitor[] = [
  { id: "neo4j",      name: "Neo4j",      cat: "GDBMS", color: "#22c55e",
    ldbc_sf1: N(248,38,6.46),   ldbc_sf10: N(404,45,9.03),    ldbc_sf100: N(880,84,10.47),
    tpch_sf1: N(2473,173,14.33),tpch_sf10: N(20295,1011,20.07),tpch_sf100: N(192699,12253,15.73),
    dbpedia: N(41596,483,86.14) },
  { id: "memgraph",   name: "Memgraph",   cat: "GDBMS", color: "#3B82F6",
    ldbc_sf1: N(114,36,3.20),   ldbc_sf10: N(256,45,5.72),    ldbc_sf100: N(967,82,11.74),
    tpch_sf1: N(3281,161,20.34),tpch_sf10: N(39591,887,44.63),tpch_sf100: X(),
    dbpedia: X() },
  { id: "kuzu",       name: "Kuzu",       cat: "GDBMS", color: "#8B5CF6",
    ldbc_sf1: N(475,39,12.27),  ldbc_sf10: N(1988,43,45.90),  ldbc_sf100: N(8394,79,106.89),
    tpch_sf1: N(1477,176,8.37), tpch_sf10: N(17187,1003,17.13),tpch_sf100: N(175100,12205,14.34),
    dbpedia: N(8634,483,18.88) },
  { id: "graphscope", name: "GraphScope", cat: "GDBMS", color: "#F59E0B",
    ldbc_sf1: N(213,37,5.78),   ldbc_sf10: N(451,36,12.70),   ldbc_sf100: N(1451,54,26.92),
    tpch_sf1: X(),               tpch_sf10: X(),                tpch_sf100: X(),
    dbpedia: X() },
  { id: "duckpgq",    name: "DuckPGQ",    cat: "GDBMS", color: "#eab308",
    ldbc_sf1: N(180,38,4.71),   ldbc_sf10: N(1289,45,28.83),  ldbc_sf100: N(15905,84,183.9),
    tpch_sf1: N(178,173,1.02),  tpch_sf10: N(1639,1011,1.62), tpch_sf100: N(18924,12253,1.54),
    dbpedia: N(9771,483,20.23) },
  { id: "umbra",      name: "Umbra",      cat: "RDBMS", color: "#94a3b8",
    ldbc_sf1: N(107,38,0.93),   ldbc_sf10: N(684,45,2.53),    ldbc_sf100: N(2319,84,7.74),
    tpch_sf1: N(915,173,0.43),  tpch_sf10: N(10386,1011,0.45),tpch_sf100: N(113512,12253,0.58),
    dbpedia: N(11139,483,23.07) },
  { id: "duckdb",     name: "DuckDB",     cat: "RDBMS", color: "#14B8A6",
    ldbc_sf1: N(110,38,2.87),   ldbc_sf10: N(652,45,14.59),   ldbc_sf100: N(3467,84,41.27),
    tpch_sf1: N(177,173,1.02),  tpch_sf10: N(1652,1011,1.63), tpch_sf100: N(18801,12253,1.53),
    dbpedia: N(9764,483,20.22) },
];

interface BenchView { key: keyof Competitor; label: string; avg: number; group: string }
const BENCH_VIEWS: BenchView[] = [
  { key: "ldbc_sf1",   label: "SF1",   avg: 4.07,  group: "LDBC SNB" },
  { key: "ldbc_sf10",  label: "SF10",  avg: 11.81, group: "LDBC SNB" },
  { key: "ldbc_sf100", label: "SF100", avg: 29.78, group: "LDBC SNB" },
  { key: "tpch_sf1",   label: "SF1",   avg: 3.21,  group: "TPC-H"   },
  { key: "tpch_sf10",  label: "SF10",  avg: 5.13,  group: "TPC-H"   },
  { key: "tpch_sf100", label: "SF100", avg: 3.15,  group: "TPC-H"   },
  { key: "dbpedia",    label: "—",     avg: 27.37, group: "DBpedia"  },
];
const BENCH_GROUPS = ["LDBC SNB", "TPC-H", "DBpedia"];

// ─── Step 1: Benchmark Comparison ─────────────────────────────────────────────
function BenchmarkView() {
  const [group, setGroup] = useState("LDBC SNB");
  const [viewIdx, setViewIdx] = useState(0);
  const [hovered, setHovered] = useState<string | null>(null);

  const groupViews = BENCH_VIEWS.filter(v => v.group === group);
  const view = groupViews[Math.min(viewIdx, groupViews.length - 1)];

  const rows = COMPETITORS.map(comp => ({
    ...comp, cell: comp[view.key as keyof Competitor] as Cell,
  })).filter(r => r.cell.x !== null);

  const validXs = rows.map(r => r.cell.x!).filter(x => x > 0);
  const maxX = validXs.length ? Math.max(...validXs) : 1;

  const CatSection = ({ cat, label }: { cat: "GDBMS" | "RDBMS"; label: string }) => {
    const catRows = rows.filter(r => r.cat === cat);
    if (!catRows.length) return null;
    return (
      <div style={{ marginBottom: 14 }}>
        <div style={{
          fontSize: 14, color: "#6b7280", fontFamily: "monospace",
          textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6,
        }}>{label}</div>
        {catRows.map(r => {
          const x = r.cell.x!;
          const isLoss = x < 1;
          const barPct = Math.sqrt(Math.max(x, 0.05) / maxX) * 100;
          const color = isLoss ? "#e84545" : r.color;
          const isHov = hovered === r.id;
          return (
            <div key={r.id}
              onMouseEnter={() => setHovered(r.id)}
              onMouseLeave={() => setHovered(null)}
              style={{
                display: "flex", alignItems: "center", gap: 14,
                padding: "8px 0", borderBottom: "1px solid #f0f1f3",
                background: isHov ? "#00000003" : "transparent",
              }}>
              <div style={{ width: 110, flexShrink: 0 }}>
                <span style={{ fontSize: 17, fontFamily: "monospace", color: r.color, fontWeight: 600 }}>{r.name}</span>
              </div>
              <div style={{ flex: 1, height: 26, display: "flex", alignItems: "center" }}>
                <motion.div
                  key={`${view.key}-${r.id}`}
                  initial={{ width: 0 }} animate={{ width: `${isLoss ? 3 : barPct}%` }}
                  transition={{ duration: 0.45, ease: "easeOut" }}
                  style={{
                    height: "100%", borderRadius: 4,
                    background: color + (isLoss ? "25" : "30"),
                    border: `1px solid ${color + (isLoss ? "40" : "60")}`,
                    minWidth: 4,
                  }}
                />
              </div>
              <div style={{ flexShrink: 0, textAlign: "right", whiteSpace: "nowrap", width: 180 }}>
                {isHov ? (
                  <span style={{ fontSize: 15, fontFamily: "monospace" }}>
                    <span style={{ color: "#71717a" }}>{r.cell.c?.toLocaleString()}</span>
                    <span style={{ color: "#d4d4d8" }}> / </span>
                    <span style={{ color: "#10B981" }}>{r.cell.t?.toLocaleString()}</span>
                    <span style={{ color: "#9ca3af" }}> ms </span>
                    <span style={{ color, fontWeight: 700 }}>{x >= 10 ? x.toFixed(1) : x.toFixed(2)}×</span>
                  </span>
                ) : (
                  <span style={{ fontSize: 22, fontFamily: "monospace", fontWeight: 700, color }}>
                    {x >= 10 ? x.toFixed(1) : x.toFixed(2)}×
                  </span>
                )}
              </div>
            </div>
          );
        })}
      </div>
    );
  };

  return (
    <div style={{ display: "flex", gap: 24, height: "100%", overflow: "hidden" }}>
      {/* Left: Controls + speed-up badge */}
      <div style={{ width: 260, flexShrink: 0, display: "flex", flexDirection: "column", gap: 14 }}>
        <div style={{
          fontSize: 15, color: "#6b7280", fontFamily: "monospace",
          textTransform: "uppercase", letterSpacing: "0.06em",
        }}>
          Table 4 — TurboLynx vs competitors
        </div>

        {/* Dataset group */}
        <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
          {BENCH_GROUPS.map(g => (
            <button key={g} onClick={() => { setGroup(g); setViewIdx(0); }} style={{
              padding: "8px 18px", borderRadius: 7, cursor: "pointer",
              fontSize: 15, fontFamily: "monospace", fontWeight: 600, textAlign: "left",
              border: `1.5px solid ${group === g ? "#e84545" : "#e5e7eb"}`,
              background: group === g ? "#e8454510" : "transparent",
              color: group === g ? "#e84545" : "#52525b",
            }}>{g}</button>
          ))}
        </div>

        {/* Scale factor */}
        {groupViews.length > 1 && (
          <div style={{ display: "flex", gap: 4 }}>
            {groupViews.map((v, i) => (
              <button key={v.key} onClick={() => setViewIdx(i)} style={{
                padding: "5px 14px", borderRadius: 5, cursor: "pointer",
                fontSize: 14, fontFamily: "monospace",
                border: `1px solid ${viewIdx === i ? "#71717a" : "#d4d4d8"}`,
                background: viewIdx === i ? "#71717a" : "transparent",
                color: viewIdx === i ? "#fff" : "#71717a",
              }}>{v.label}</button>
            ))}
          </div>
        )}

        {/* Big speed-up number */}
        <div style={{
          background: "#e8454508", border: "1px solid #e8454520", borderRadius: 10,
          padding: "16px 20px", textAlign: "center",
        }}>
          <div style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace", marginBottom: 4 }}>
            avg speed-up
          </div>
          <motion.div
            key={view.key}
            initial={{ scale: 1.2 }} animate={{ scale: 1 }}
            transition={{ type: "spring", stiffness: 400, damping: 20 }}
            style={{ fontSize: 48, fontWeight: 800, color: "#e84545", fontFamily: "monospace", lineHeight: 1 }}
          >
            {view.avg}×
          </motion.div>
          <div style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace", marginTop: 4 }}>
            over {rows.length} systems
          </div>
        </div>

        <div style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace", lineHeight: 1.5 }}>
          Bar ∝ √speed-up
          <br />Hover for exact ms
          <br /><span style={{ color: "#e84545" }}>red</span> = TurboLynx slower
        </div>
      </div>

      {/* Right: Bar chart */}
      <div style={{
        flex: 1, minWidth: 0, display: "flex", flexDirection: "column",
        background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
        padding: "18px 24px", overflow: "hidden",
      }}>
        <div style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
          <AnimatePresence mode="wait">
            <motion.div key={view.key}
              initial={{ opacity: 0 }} animate={{ opacity: 1 }}
              exit={{ opacity: 0 }} transition={{ duration: 0.18 }}>
              <CatSection cat="GDBMS" label="Graph DBMS" />
              <CatSection cat="RDBMS" label="Relational DBMS" />
            </motion.div>
          </AnimatePresence>
        </div>
      </div>
    </div>
  );
}

// ─── Main ──────────────────────────────────────────────────────────────────────
const TITLES = ["Live Query Runner", "Benchmark Comparison"];
const SUBS = [
  "Execute queries on TurboLynx and inspect the full execution pipeline",
  "TurboLynx speed-up vs 7 competitors across LDBC, TPC-H, and DBpedia",
];

export default function S5_Performance({ step }: Props) {
  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{
        maxWidth: 1440, margin: "0 auto", padding: "20px 40px",
        height: "100%", display: "flex", flexDirection: "column",
        boxSizing: "border-box", gap: 10,
      }}>
        <AnimatePresence mode="wait">
          <motion.div key={step}
            initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }} transition={{ duration: 0.25 }}
            style={{ flexShrink: 0 }}>
            <div style={{
              fontSize: 15, color: "#e84545", fontFamily: "monospace",
              textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 4,
            }}>
              Performance — {SUBS[step]}
            </div>
            <h2 style={{ fontSize: 30, fontWeight: 700, color: "#18181b", margin: 0 }}>
              {TITLES[step]}
            </h2>
          </motion.div>
        </AnimatePresence>

        <AnimatePresence mode="wait">
          <motion.div key={step}
            initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }} transition={{ duration: 0.25 }}
            style={{ flex: 1, minHeight: 0 }}>
            {step === 0 && <LiveQueryRunner />}
            {step === 1 && <BenchmarkView />}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}
