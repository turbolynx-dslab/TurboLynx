"use client";
import { useState, useEffect, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface Props { step: number; onStep: (n: number) => void; }

// ─── Plan node tree ────────────────────────────────────────────────────────────
interface PlanNode {
  op: string; detail?: string; color?: string;
  children?: PlanNode[];
}

function PlanTree({ node, prefix = "", isLast = true }: { node: PlanNode; prefix?: string; isLast?: boolean }) {
  const connector = isLast ? "└─ " : "├─ ";
  const childPfx   = isLast ? "   " : "│  ";
  const col = node.color ?? "#52525b";
  return (
    <div style={{ fontFamily: "monospace", fontSize: 13, lineHeight: 1.75 }}>
      <div>
        <span style={{ color: "#3f3f46" }}>{prefix}{connector}</span>
        <span style={{ color: col, fontWeight: 600 }}>{node.op}</span>
        {node.detail && <span style={{ color: "#3f3f46" }}> {node.detail}</span>}
      </div>
      {node.children?.map((c, i) => (
        <PlanTree key={i} node={c} prefix={prefix + childPfx} isLast={i === node.children!.length - 1} />
      ))}
    </div>
  );
}

// ─── Preset queries ────────────────────────────────────────────────────────────
interface Preset {
  label: string; query: string;
  cols: string[]; rows: string[][];
  totalRows: number; ms: number;
  plan: PlanNode;
}

const PRESET_QUERIES: Preset[] = [
  {
    label: "Person → Film",
    query: `MATCH (p)-[:directed]->(f)\nWHERE p.birthDate IS NOT NULL\nRETURN p.name, f.title\nLIMIT 20`,
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
      op: "Projection", detail: "[p.name, f.title]", color: "#f4f4f5",
      children: [{
        op: "UnionAll", detail: "[5p × 4f = 20 combos]", color: "#8B5CF6",
        children: [
          { op: "NLJoin", detail: "GL-p1(44.2K) ⋈ GL-f1(15.0K)", color: "#a78bfa",
            children: [
              { op: "Scan", detail: "GL-p1  [p.name, p.born]  44,200 nodes", color: "#8B5CF6" },
              { op: "Scan", detail: "GL-f1  [f.title, f.year, f.genre]  15,000 nodes", color: "#F59E0B" },
            ]},
          { op: "NLJoin", detail: "GL-p1(44.2K) ⋈ GL-f2(8.4K)",  color: "#a78bfa" },
          { op: "…",      detail: "18 more graphlet combos",       color: "#3f3f46" },
        ],
      }],
    },
  },
  {
    label: "Film → Location",
    query: `MATCH (f)-[:filmed_in]->(l)\nWHERE f.year >= 2000\nRETURN f.title, f.year, l.name\nORDER BY f.year DESC\nLIMIT 15`,
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
      op: "Sort", detail: "[f.year DESC]", color: "#f4f4f5",
      children: [{
        op: "Projection", detail: "[f.title, f.year, l.name]", color: "#f4f4f5",
        children: [{
          op: "UnionAll", detail: "[4f × 2l = 8 combos]", color: "#8B5CF6",
          children: [
            { op: "NLJoin", detail: "GL-f1(15.0K) ⋈ GL-l1(9.8K)", color: "#a78bfa",
              children: [
                { op: "Scan", detail: "GL-f1  [filter: year≥2000]  15,000 nodes", color: "#F59E0B" },
                { op: "Scan", detail: "GL-l1  [l.country, l.pop]  9,800 nodes",   color: "#10B981" },
              ]},
            { op: "…", detail: "7 more combos", color: "#3f3f46" },
          ],
        }],
      }],
    },
  },
  {
    label: "2-hop: Person → Film → Location",
    query: `MATCH (p)-[:starring]->(f)-[:filmed_in]->(l)\nWHERE p.nationality = "American"\nRETURN p.name, f.title, l.name\nLIMIT 20`,
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
      op: "Projection", detail: "[p.name, f.title, l.name]", color: "#f4f4f5",
      children: [{
        op: "UnionAll", detail: "[3p × 2f × 2l = 12 combos]", color: "#8B5CF6",
        children: [
          { op: "NLJoin", detail: "GL-f1(15K) ⋈ GL-l1(9.8K)", color: "#a78bfa",
            children: [
              { op: "NLJoin", detail: "GL-p1(44.2K) ⋈ GL-f1(15K) [nat=American]", color: "#a78bfa",
                children: [
                  { op: "Scan", detail: "GL-p1  [filter: nat=American]  44,200 nodes", color: "#8B5CF6" },
                  { op: "Scan", detail: "GL-f1  15,000 nodes",                         color: "#F59E0B" },
                ]},
              { op: "Scan", detail: "GL-l1  9,800 nodes", color: "#10B981" },
            ]},
          { op: "…", detail: "11 more graphlet combos", color: "#3f3f46" },
        ],
      }],
    },
  },
];

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

// ─── Live Query Runner ─────────────────────────────────────────────────────────
// Custom query fallback mock
const CUSTOM_MOCK: Preset = {
  label: "Custom", query: "",
  cols: ["result"],
  rows: [["(query executed on TurboLynx)"]],
  totalRows: 1, ms: 247,
  plan: {
    op: "Projection", detail: "[custom output]", color: "#f4f4f5",
    children: [{ op: "Scan", detail: "DBpedia graph", color: "#8B5CF6" }],
  },
};

function QueryRunner() {
  // presetIdx = -1 → Custom mode
  const [presetIdx, setPresetIdx] = useState(0);
  const [query, setQuery] = useState(PRESET_QUERIES[0].query);
  const [runState, setRunState] = useState<"idle" | "running" | "done">("idle");
  const [result, setResult] = useState<Preset | null>(null);
  const [tab, setTab] = useState<"plan" | "results">("plan");
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const isCustom = presetIdx === -1;

  const selectPreset = (i: number) => {
    setPresetIdx(i);
    setQuery(PRESET_QUERIES[i].query);
    setRunState("idle");
    setResult(null);
  };

  const selectCustom = () => {
    setPresetIdx(-1);
    setQuery("");
    setRunState("idle");
    setResult(null);
    setTimeout(() => textareaRef.current?.focus(), 50);
  };

  const runQuery = () => {
    if (runState === "running") return;
    setRunState("running");
    setResult(null);
    const preset = PRESET_QUERIES.find(p => p.query === query) ?? (isCustom ? CUSTOM_MOCK : PRESET_QUERIES[0]);
    const simDelay = Math.max(320, Math.min(preset.ms * 0.35, 900));
    timerRef.current = setTimeout(() => {
      setResult(preset);
      setRunState("done");
      setTab("plan");
    }, simDelay);
  };

  useEffect(() => () => { if (timerRef.current) clearTimeout(timerRef.current); }, []);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 9, height: "100%", overflow: "hidden" }}>
      {/* Preset + editor */}
      <div style={{ flexShrink: 0 }}>
        <div style={{ display: "flex", gap: 5, marginBottom: 7, flexWrap: "wrap", alignItems: "center" }}>
          {PRESET_QUERIES.map((p, i) => (
            <button key={i} onClick={() => selectPreset(i)} style={{
              padding: "5px 13px", borderRadius: 5, cursor: "pointer", fontSize: 13, fontFamily: "monospace",
              border: `1px solid ${presetIdx === i ? "#e84545" : "#27272a"}`,
              background: presetIdx === i ? "#e8454518" : "transparent",
              color: presetIdx === i ? "#f87171" : "#a1a1aa",
            }}>{p.label}</button>
          ))}
          {/* Custom button — visually separated */}
          <div style={{ width: 1, background: "#3f3f46", alignSelf: "stretch", margin: "0 3px" }} />
          <button onClick={selectCustom} style={{
            padding: "5px 13px", borderRadius: 5, cursor: "pointer", fontSize: 13, fontFamily: "monospace",
            border: `1px solid ${isCustom ? "#8B5CF6" : "#3f3f46"}`,
            background: isCustom ? "#8B5CF625" : "#3f3f4618",
            color: isCustom ? "#c4b5fd" : "#a1a1aa",
            fontWeight: isCustom ? 700 : 400,
          }}>✏ Custom</button>
        </div>
        <textarea
          ref={textareaRef}
          value={query}
          onChange={e => { setQuery(e.target.value); setRunState("idle"); setResult(null); }}
          placeholder={isCustom ? "MATCH (a)-[:rel]->(b)\nWHERE a.prop IS NOT NULL\nRETURN a.name, b.title\nLIMIT 20" : undefined}
          spellCheck={false}
          style={{
            background: "#090909",
            border: `1px solid ${isCustom ? "#8B5CF650" : "#27272a"}`,
            borderRadius: 6,
            padding: "10px 14px", fontSize: 14, color: "#f4f4f5",
            fontFamily: "monospace", lineHeight: 1.7, resize: "none", height: 136,
            outline: "none", boxSizing: "border-box", width: "100%",
            transition: "border-color 0.15s",
          }}
        />
        {isCustom && (
          <div style={{ fontSize: 12, color: "#52525b", fontFamily: "monospace", marginTop: 5 }}>
            DBpedia — no node labels · use edge types + properties only
          </div>
        )}
      </div>

      {/* Run button */}
      <div style={{ flexShrink: 0, display: "flex", alignItems: "center", gap: 10 }}>
        <button onClick={runQuery} disabled={runState === "running"} style={{
          padding: "8px 20px", borderRadius: 6, cursor: runState === "running" ? "not-allowed" : "pointer",
          fontSize: 14, fontFamily: "monospace", fontWeight: 600,
          border: "1px solid #e8454540",
          background: runState === "running" ? "#1a1a1e" : "#e8454520",
          color: runState === "running" ? "#52525b" : "#f87171",
          transition: "all 0.15s",
        }}>
          {runState === "running" ? "⏳ Running…" : "▶ Run on TurboLynx"}
        </button>
        {runState === "running" && (
          <div style={{ flex: 1, height: 3, background: "#27272a", borderRadius: 2, overflow: "hidden" }}>
            <motion.div
              initial={{ width: "0%" }} animate={{ width: "100%" }}
              transition={{ duration: (PRESET_QUERIES.find(p => p.query === query)?.ms ?? 500) / 1000, ease: "linear" }}
              style={{ height: "100%", background: "#e84545", borderRadius: 2 }}
            />
          </div>
        )}
        {runState === "done" && result && (
          <span style={{ fontSize: 22, fontWeight: 800, color: "#10B981", fontFamily: "monospace", lineHeight: 1 }}>
            {result.ms} ms
          </span>
        )}
      </div>

      {/* Plan / Results tabs */}
      <AnimatePresence mode="wait">
        {runState === "done" && result && (
          <motion.div key="output" initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }} transition={{ duration: 0.2 }}
            style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", overflow: "hidden" }}>
            {/* Tab selector */}
            <div style={{ display: "flex", gap: 0, marginBottom: 8, flexShrink: 0, borderBottom: "1px solid #27272a" }}>
              {(["plan", "results"] as const).map(t => (
                <button key={t} onClick={() => setTab(t)} style={{
                  padding: "6px 16px", cursor: "pointer", fontSize: 13, fontFamily: "monospace",
                  border: "none", borderBottom: `2px solid ${tab === t ? "#8B5CF6" : "transparent"}`,
                  background: "transparent", color: tab === t ? "#c4b5fd" : "#71717a",
                  marginBottom: -1, transition: "all 0.12s",
                }}>{t === "plan" ? "Query Plan" : `Results (${result.totalRows} rows)`}</button>
              ))}
              <div style={{ flex: 1 }} />
              <span style={{ fontSize: 12, color: "#3f3f46", fontFamily: "monospace", alignSelf: "center", marginRight: 4 }}>
                ✓ {result.ms} ms · {result.totalRows} rows
              </span>
            </div>

            {/* Tab content */}
            <div style={{ flex: 1, minHeight: 0, overflow: "auto" }}>
              {tab === "plan" && (
                <div style={{ background: "#090909", border: "1px solid #27272a", borderRadius: 6, padding: "10px 12px", minHeight: "100%", boxSizing: "border-box" }}>
                  <PlanTree node={{ op: "", children: [result.plan] }} prefix="" isLast />
                </div>
              )}
              {tab === "results" && (
                <div style={{ background: "#090909", border: "1px solid #27272a", borderRadius: 6, overflow: "hidden" }}>
                  <table style={{ fontSize: 13, fontFamily: "monospace", borderCollapse: "collapse", width: "100%", whiteSpace: "nowrap" }}>
                    <thead>
                      <tr style={{ background: "#131316" }}>
                        {result.cols.map(c => (
                          <th key={c} style={{ padding: "6px 14px", textAlign: "left", color: "#71717a", borderBottom: "1px solid #27272a", fontWeight: 500 }}>{c}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {result.rows.map((row, ri) => (
                        <tr key={ri} style={{ borderBottom: "1px solid #18181b" }}>
                          {row.map((v, ci) => (
                            <td key={ci} style={{ padding: "5px 14px", color: "#d4d4d8" }}>{v}</td>
                          ))}
                        </tr>
                      ))}
                      <tr>
                        <td colSpan={result.cols.length} style={{ padding: "5px 14px", color: "#3f3f46", fontStyle: "italic", fontSize: 12 }}>
                          … {result.totalRows - result.rows.length} more rows
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </motion.div>
        )}
        {runState === "idle" && (
          <motion.div key="idle" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
            style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}>
            <span style={{ fontSize: 13, color: "#3f3f46", fontFamily: "monospace" }}>press ▶ to run</span>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ─── Benchmark Viewer ──────────────────────────────────────────────────────────
function BenchmarkViewer() {
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
      <div style={{ marginBottom: 10 }}>
        <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 4 }}>{label}</div>
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
              style={{ display: "flex", alignItems: "center", gap: 12, padding: "6px 0", borderBottom: "1px solid #18181b", background: isHov ? "#ffffff04" : "transparent" }}>
              <div style={{ width: 100, flexShrink: 0 }}>
                <span style={{ fontSize: 15, fontFamily: "monospace", color: r.color, fontWeight: 600 }}>{r.name}</span>
              </div>
              <div style={{ flex: 1, height: 22, display: "flex", alignItems: "center" }}>
                <motion.div
                  key={`${view.key}-${r.id}`}
                  initial={{ width: 0 }} animate={{ width: `${isLoss ? 3 : barPct}%` }}
                  transition={{ duration: 0.45, ease: "easeOut" }}
                  style={{ height: "100%", borderRadius: 3, background: color + (isLoss ? "30" : "35"), border: `1px solid ${color + (isLoss ? "50" : "70")}`, minWidth: 3 }}
                />
              </div>
              <div style={{ flexShrink: 0, textAlign: "right", whiteSpace: "nowrap" }}>
                {isHov ? (
                  <span style={{ fontSize: 13, fontFamily: "monospace" }}>
                    <span style={{ color: "#71717a" }}>{r.cell.c?.toLocaleString()}</span>
                    <span style={{ color: "#3f3f46" }}> / </span>
                    <span style={{ color: "#10B981" }}>{r.cell.t?.toLocaleString()}</span>
                    <span style={{ color: "#3f3f46" }}> ms  </span>
                    <span style={{ color, fontWeight: 700 }}>{x >= 10 ? x.toFixed(1) : x.toFixed(2)}×</span>
                  </span>
                ) : (
                  <span style={{ fontSize: 17, fontFamily: "monospace", fontWeight: 700, color }}>
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
    <div style={{ display: "flex", flexDirection: "column", gap: 8, height: "100%", overflow: "hidden" }}>
      {/* Selectors */}
      <div style={{ flexShrink: 0 }}>
        <div style={{ fontSize: 12, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 7 }}>
          Table 4 — TurboLynx speed-up vs competitors (geomean, ms)
        </div>
        <div style={{ display: "flex", gap: 5, marginBottom: 6 }}>
          {BENCH_GROUPS.map(g => (
            <button key={g} onClick={() => { setGroup(g); setViewIdx(0); }} style={{
              padding: "5px 16px", borderRadius: 5, cursor: "pointer", fontSize: 13, fontFamily: "monospace",
              border: `1px solid ${group === g ? "#e84545" : "#27272a"}`,
              background: group === g ? "#e8454518" : "transparent",
              color: group === g ? "#f87171" : "#a1a1aa",
            }}>{g}</button>
          ))}
        </div>
        {groupViews.length > 1 && (
          <div style={{ display: "flex", gap: 4 }}>
            {groupViews.map((v, i) => (
              <button key={v.key} onClick={() => setViewIdx(i)} style={{
                padding: "4px 12px", borderRadius: 4, cursor: "pointer", fontSize: 12, fontFamily: "monospace",
                border: `1px solid ${viewIdx === i ? "#71717a" : "#27272a"}`,
                background: viewIdx === i ? "#27272a" : "transparent",
                color: viewIdx === i ? "#f4f4f5" : "#71717a",
              }}>{v.label}</button>
            ))}
          </div>
        )}
      </div>

      {/* Avg badge */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, flexShrink: 0 }}>
        <span style={{ fontSize: 13, color: "#71717a", fontFamily: "monospace" }}>TurboLynx avg speed-up:</span>
        <span style={{ fontSize: 30, fontWeight: 800, color: "#e84545", fontFamily: "monospace", lineHeight: 1 }}>{view.avg}×</span>
        <span style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace" }}>over {rows.length} competitors</span>
      </div>

      {/* Bar chart */}
      <div style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
        <AnimatePresence mode="wait">
          <motion.div key={view.key} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.18 }}>
            <CatSection cat="GDBMS" label="Graph DB" />
            <CatSection cat="RDBMS" label="Relational DB" />
            <div style={{ marginTop: 10, fontSize: 12, color: "#52525b", fontFamily: "monospace" }}>
              Bar ∝ √speed-up · hover row for exact ms · <span style={{ color: "#e84545" }}>red</span> = TurboLynx slower
            </div>
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}

// ─── Main ──────────────────────────────────────────────────────────────────────
export default function S5_Performance({ step }: Props) {
  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{ maxWidth: 1440, margin: "0 auto", padding: "28px 48px", height: "100%", display: "flex", flexDirection: "column", boxSizing: "border-box", gap: 14 }}>
        {/* Header */}
        <div style={{ flexShrink: 0 }}>
          <div style={{ fontSize: 13, color: "#e84545", fontFamily: "monospace", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.08em" }}>
            Performance
          </div>
          <h2 style={{ fontSize: 26, fontWeight: 700, color: "#f4f4f5", margin: 0 }}>Real-world benchmark results</h2>
        </div>

        {/* Two-panel layout: left = live runner, right = benchmark table */}
        <div style={{ flex: 1, minHeight: 0, display: "flex", gap: 16, overflow: "hidden" }}>
          {/* Left: Live query runner */}
          <div style={{
            width: 390, flexShrink: 0, display: "flex", flexDirection: "column",
            background: "#0c0c0e", border: "1px solid #27272a", borderRadius: 10, padding: "14px 16px",
            overflow: "hidden",
          }}>
            <div style={{ fontSize: 11, color: "#e84545", fontFamily: "monospace", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.07em", marginBottom: 10 }}>
              ◎ Live — TurboLynx on DBpedia
            </div>
            <QueryRunner />
          </div>

          {/* Right: Benchmark comparison */}
          <div style={{
            flex: 1, minWidth: 0, display: "flex", flexDirection: "column",
            background: "#0c0c0e", border: "1px solid #27272a", borderRadius: 10, padding: "14px 16px",
            overflow: "hidden",
          }}>
            <BenchmarkViewer />
          </div>
        </div>
      </div>
    </div>
  );
}
