"use client";
import { useEffect, useState, useMemo, useReducer, useRef, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";

import type { QState, MatchPattern, WhereFilter, ReturnItem, OrderByItem, EdgeDirection, WhereOp, AggFn } from "@/lib/query-state";
import { generateCypher, INIT_QSTATE, uid, mkMatch, mkReturn } from "@/lib/query-state";
import type { LiveResult } from "@/app/page";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "https://turbolynx.duckdns.org:8080";

interface Props {
  step: number; onStep: (n: number) => void;
  queryState?: QState; onQueryChange?: (s: QState) => void;
  onLiveResult?: (r: LiveResult) => void;
}

interface CatalogData { edgeTypes: string[]; properties: string[]; }

const mkWhere = (v = "", p = "", op: WhereOp = "IS NOT NULL", val = ""): WhereFilter =>
  ({ id: uid(), variable: v, property: p, operator: op, value: val });
const mkOrderBy = (expr = "", desc = false): OrderByItem => ({ expr, desc });

const INIT = INIT_QSTATE;

// ─── Reducer ─────────────────────────────────────────────────────────────────
type Action =
  | { type: "ADD_MATCH" }
  | { type: "REMOVE_MATCH"; id: string }
  | { type: "UPDATE_MATCH"; id: string; key: string; value: any }
  | { type: "ADD_WHERE" }
  | { type: "REMOVE_WHERE"; id: string }
  | { type: "UPDATE_WHERE"; id: string; key: string; value: any }
  | { type: "ADD_RETURN" }
  | { type: "REMOVE_RETURN"; id: string }
  | { type: "UPDATE_RETURN"; id: string; key: string; value: any }
  | { type: "ADD_ORDERBY" }
  | { type: "REMOVE_ORDERBY"; idx: number }
  | { type: "UPDATE_ORDERBY"; idx: number; key: string; value: any }
  | { type: "SET_LIMIT"; value: number | null }
  | { type: "LOAD_PRESET"; state: QState }
  | { type: "RESET" };

function reducer(state: QState, action: Action): QState {
  switch (action.type) {
    case "ADD_MATCH": return { ...state, matches: [...state.matches, mkMatch()] };
    case "REMOVE_MATCH": return { ...state, matches: state.matches.filter(m => m.id !== action.id) };
    case "UPDATE_MATCH": return { ...state, matches: state.matches.map(m => m.id === action.id ? { ...m, [action.key]: action.value } : m) };
    case "ADD_WHERE": return { ...state, wheres: [...state.wheres, mkWhere()] };
    case "REMOVE_WHERE": return { ...state, wheres: state.wheres.filter(w => w.id !== action.id) };
    case "UPDATE_WHERE": return { ...state, wheres: state.wheres.map(w => w.id === action.id ? { ...w, [action.key]: action.value } : w) };
    case "ADD_RETURN": return { ...state, returns: [...state.returns, mkReturn()] };
    case "REMOVE_RETURN": return { ...state, returns: state.returns.filter(r => r.id !== action.id) };
    case "UPDATE_RETURN": return { ...state, returns: state.returns.map(r => r.id === action.id ? { ...r, [action.key]: action.value } : r) };
    case "ADD_ORDERBY": return { ...state, orderBy: [...state.orderBy, mkOrderBy()] };
    case "REMOVE_ORDERBY": return { ...state, orderBy: state.orderBy.filter((_, i) => i !== action.idx) };
    case "UPDATE_ORDERBY": return { ...state, orderBy: state.orderBy.map((o, i) => i === action.idx ? { ...o, [action.key]: action.value } : o) };
    case "SET_LIMIT": return { ...state, limit: action.value };
    case "LOAD_PRESET": return JSON.parse(JSON.stringify(action.state));
    case "RESET": return JSON.parse(JSON.stringify(INIT));
    default: return state;
  }
}

// ─── Syntax highlight ────────────────────────────────────────────────────────
const KEYWORDS = /\b(MATCH|OPTIONAL MATCH|WHERE|RETURN|ORDER BY|LIMIT|AND|AS|IS NOT NULL|IS NULL|DESC|ASC|COUNT|SUM|AVG|MIN|MAX|CONTAINS|STARTS WITH)\b/g;

function highlightCypher(cypher: string): React.ReactNode[] {
  return cypher.split("\n").map((line, li) => {
    const parts: React.ReactNode[] = [];
    let last = 0;
    const regex = new RegExp(KEYWORDS.source, "g");
    let match;
    while ((match = regex.exec(line)) !== null) {
      if (match.index > last) parts.push(<span key={`${li}-${last}`}>{line.slice(last, match.index)}</span>);
      parts.push(<span key={`${li}-${match.index}`} style={{ color: "#e84545", fontWeight: 700 }}>{match[0]}</span>);
      last = match.index + match[0].length;
    }
    if (last < line.length) parts.push(<span key={`${li}-${last}`}>{line.slice(last)}</span>);
    return <div key={li}>{parts}</div>;
  });
}

// ─── Presets ─────────────────────────────────────────────────────────────────
const PRESETS: { label: string; desc: string; scenario?: string; state: QState }[] = [
  {
    label: "Scenario A: Selective Filter",
    desc: "1-hop with sparse property filter — demonstrates graphlet pruning",
    scenario: "A",
    state: {
      matches: [{ id: "p1", sourceVar: "p", edgeType: "birthPlace", direction: "right", targetVar: "c", optional: false }],
      wheres: [{ id: "w1", variable: "p", property: "birthDate", operator: "IS NOT NULL", value: "" }],
      returns: [
        { id: "r1", variable: "p", property: "name", alias: "", aggregate: "" },
        { id: "r2", variable: "p", property: "birthDate", alias: "", aggregate: "" },
        { id: "r3", variable: "c", property: "name", alias: "", aggregate: "" },
      ],
      orderBy: [],
      limit: null,
    },
  },
  {
    label: "Scenario B: Multi-Hop + Aggregation",
    desc: "2-hop traversal with COUNT — demonstrates GEM + SSRF",
    scenario: "B",
    state: {
      matches: [
        { id: "p1", sourceVar: "p", edgeType: "birthPlace", direction: "right", targetVar: "c", optional: false },
        { id: "p2", sourceVar: "c", edgeType: "country", direction: "right", targetVar: "co", optional: false },
      ],
      wheres: [],
      returns: [
        { id: "r1", variable: "co", property: "name", alias: "", aggregate: "" },
        { id: "r2", variable: "co", property: "abstract", alias: "", aggregate: "" },
        { id: "r3", variable: "co", property: "populationTotal", alias: "", aggregate: "" },
        { id: "r4", variable: "co", property: "areaTotal", alias: "", aggregate: "" },
        { id: "r5", variable: "co", property: "elevation", alias: "", aggregate: "" },
        { id: "r6", variable: "p", property: "", alias: "person_count", aggregate: "COUNT" },
      ],
      orderBy: [{ expr: "person_count", desc: true }],
      limit: null,
    },
  },
  {
    label: "City \u2192 Country (population)",
    desc: "Location hierarchy with numeric filter + ORDER BY",
    state: {
      matches: [{ id: "p1", sourceVar: "c", edgeType: "country", direction: "right", targetVar: "co", optional: false }],
      wheres: [{ id: "w1", variable: "c", property: "populationTotal", operator: ">", value: "1000000" }],
      returns: [
        { id: "r1", variable: "c", property: "name", alias: "", aggregate: "" },
        { id: "r2", variable: "c", property: "populationTotal", alias: "pop", aggregate: "" },
        { id: "r3", variable: "co", property: "name", alias: "country", aggregate: "" },
      ],
      orderBy: [{ expr: "pop", desc: true }],
      limit: null,
    },
  },
];

// ─── Shared styles ───────────────────────────────────────────────────────────
const INPUT: React.CSSProperties = {
  padding: "6px 10px", borderRadius: 5, border: "1px solid #e5e7eb", fontSize: 14,
  fontFamily: "monospace", outline: "none", background: "#fff", color: "#18181b",
};
const SELECT: React.CSSProperties = { ...INPUT, cursor: "pointer", appearance: "none" as any, paddingRight: 24,
  backgroundImage: "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%239ca3af'/%3E%3C/svg%3E\")",
  backgroundRepeat: "no-repeat", backgroundPosition: "right 8px center",
};
const BTN_ADD: React.CSSProperties = {
  padding: "6px 14px", borderRadius: 6, border: "1px dashed #d4d4d8", background: "transparent",
  color: "#71717a", fontSize: 13, fontWeight: 600, cursor: "pointer",
};
const BTN_REMOVE: React.CSSProperties = {
  width: 26, height: 26, borderRadius: 5, border: "none", background: "#fef2f2",
  color: "#e84545", fontSize: 15, fontWeight: 700, cursor: "pointer", flexShrink: 0,
  display: "flex", alignItems: "center", justifyContent: "center",
};
const SECTION_LABEL: React.CSSProperties = {
  fontSize: 12, fontWeight: 700, color: "#9ca3af", textTransform: "uppercase" as any,
  letterSpacing: "0.08em", marginBottom: 6,
};
const ROW: React.CSSProperties = {
  display: "flex", alignItems: "center", gap: 8, padding: "10px 14px",
  background: "#fff", borderRadius: 8, border: "1px solid #e5e7eb",
};

// ─── Searchable dropdown ─────────────────────────────────────────────────────
function SearchSelect({ value, options, onChange, placeholder, width }: {
  value: string; options: string[]; onChange: (v: string) => void; placeholder?: string; width?: number;
}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false); };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const filtered = search
    ? options.filter(o => o.toLowerCase().includes(search.toLowerCase())).slice(0, 50)
    : options.slice(0, 50);

  return (
    <div ref={ref} style={{ position: "relative", display: "inline-block", width: width ?? 160 }}>
      <button onClick={() => { setOpen(!open); setSearch(""); }}
        style={{ ...INPUT, width: "100%", textAlign: "left", cursor: "pointer",
          color: value ? "#18181b" : "#9ca3af", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" as any }}>
        {value || placeholder || "Select..."}
      </button>
      {open && (
        <div style={{
          position: "absolute", top: "100%", left: 0, right: 0, zIndex: 50,
          background: "#fff", border: "1px solid #e5e7eb", borderRadius: 8,
          boxShadow: "0 8px 24px rgba(0,0,0,0.1)", marginTop: 4, overflow: "hidden",
        }}>
          {options.length > 10 && (
            <input value={search} onChange={e => setSearch(e.target.value)}
              placeholder="Search..." autoFocus
              style={{ ...INPUT, width: "100%", border: "none", borderBottom: "1px solid #f0f1f3", borderRadius: 0 }} />
          )}
          <div style={{ maxHeight: 200, overflowY: "auto" }} className="thin-scrollbar">
            {filtered.map(o => (
              <div key={o} onClick={() => { onChange(o); setOpen(false); }}
                style={{
                  padding: "7px 12px", fontSize: 13, fontFamily: "monospace", cursor: "pointer",
                  background: o === value ? "#eff6ff" : "transparent",
                  color: o === value ? "#1e40af" : "#374151",
                }}
                onMouseEnter={e => (e.currentTarget.style.background = "#f8f9fa")}
                onMouseLeave={e => (e.currentTarget.style.background = o === value ? "#eff6ff" : "transparent")}>
                {o}
              </div>
            ))}
            {filtered.length === 0 && <div style={{ padding: "8px 12px", fontSize: 13, color: "#9ca3af" }}>No matches</div>}
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Direction toggle ────────────────────────────────────────────────────────
function DirToggle({ value, onChange }: { value: EdgeDirection; onChange: (d: EdgeDirection) => void }) {
  const cycle = () => onChange(value === "right" ? "left" : value === "left" ? "undirected" : "right");
  const label = value === "right" ? "\u2192" : value === "left" ? "\u2190" : "\u2194";
  return (
    <button onClick={cycle} title={`Direction: ${value}`}
      style={{
        width: 34, height: 30, borderRadius: 5, border: "1px solid #e5e7eb", background: "#fff",
        fontSize: 18, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center",
        color: "#18181b", fontWeight: 600, flexShrink: 0,
      }}>
      {label}
    </button>
  );
}

// ─── NL examples (LDBC Social Network) ──────────────────────────────────────
const NL_EXAMPLES: { nl: string; hint?: string; cachedCypher?: string }[] = [
  {
    nl: "I want to know which people are friends with someone named Deepak Kumar. For each of those friends, show me their first name, last name, and the city where they are located.",
    hint: "2-hop + IS_LOCATED_IN",
    cachedCypher: "MATCH (p:Person {firstName: 'Deepak', lastName: 'Kumar'})-[:KNOWS]-(friend:Person)-[:IS_LOCATED_IN]->(city:Place)\nRETURN friend.firstName, friend.lastName, city.name AS city\nLIMIT 20",
  },
  {
    nl: "Find all the comments that were created by friends of the person whose first name is Yang and last name is Zhang. For each comment, return the friend who wrote it, the comment content, and the creation date, ordered by the most recent comments first. Limit to 15 results.",
    hint: "2-hop + ORDER BY date",
    cachedCypher: "MATCH (p:Person {firstName: 'Yang', lastName: 'Zhang'})-[:KNOWS]->(friend:Person)<-[:HAS_CREATOR]-(c:Comment)\nRETURN friend.firstName, friend.lastName, c.content, c.creationDate\nORDER BY c.creationDate DESC\nLIMIT 15",
  },
  {
    nl: "Show me the top 10 most active commenters in the social network. For each person, display their first name, last name, and the total number of comments they have created, sorted from the most comments to the fewest.",
    hint: "HAS_CREATOR + COUNT",
    cachedCypher: "MATCH (c:Comment)-[:HAS_CREATOR]->(p:Person)\nRETURN p.firstName, p.lastName, count(c) AS commentCount\nORDER BY commentCount DESC\nLIMIT 10",
  },
  {
    nl: "For each forum in the network, count how many distinct tags appear on posts within that forum. Return the forum title and the number of unique tags, ordered by the tag count descending, and show only the top 10 forums with the most diverse topics.",
    hint: "3-hop + COUNT DISTINCT",
    cachedCypher: "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post)-[:HAS_TAG]->(t:Tag)\nRETURN f.title AS forumTitle, COUNT(DISTINCT t) AS uniqueTagCount\nORDER BY uniqueTagCount DESC\nLIMIT 10",
  },
];

// ─── Main ────────────────────────────────────────────────────────────────────
type InputMode = "nl" | "cypher";
const WHERE_OPS: WhereOp[] = ["IS NOT NULL", "IS NULL", "=", "!=", ">", "<", ">=", "<=", "CONTAINS", "STARTS WITH"];
const AGG_FNS: AggFn[] = ["", "COUNT", "SUM", "AVG", "MIN", "MAX"];

export default function S2_QuerySelect({ step, queryState, onQueryChange, onLiveResult }: Props) {
  const [state, dispatch] = useReducer(reducer, queryState ?? INIT);
  const [catalog, setCatalog] = useState<CatalogData | null>(null);
  const [activePreset, setActivePreset] = useState<number | null>(null);
  const [mode, setMode] = useState<InputMode>("nl");
  const [nlText, setNlText] = useState<string>("");
  const [nlCypher, setNlCypher] = useState<string>("");
  const [converting, setConverting] = useState<boolean>(false);
  const [executing, setExecuting] = useState<boolean>(false);
  const [nlError, setNlError] = useState<string>("");

  // Sync state up to parent
  useEffect(() => {
    onQueryChange?.(state);
  }, [state]);

  useEffect(() => {
    fetch(`${process.env.NEXT_PUBLIC_BASE_PATH || ""}/dbpedia_catalog.json`)
      .then(r => r.json())
      .then((raw: any) => {
        const edgeTypes = [...new Set<string>(
          raw.edgePartitions.map((e: any) => (e.short as string).replace(/@NODE@NODE$/, ""))
        )].sort();
        const propSet = new Set<string>();
        for (const g of raw.vertexPartitions[0].graphlets) {
          for (const p of g.schema) propSet.add(p);
        }
        setCatalog({ edgeTypes, properties: [...propSet].sort() });
      })
      .catch(() => {});
  }, []);

  const cypher = useMemo(() => generateCypher(state), [state]);

  // All declared variables
  const vars = useMemo(() => {
    const s = new Set<string>();
    for (const m of state.matches) { if (m.sourceVar) s.add(m.sourceVar); if (m.targetVar) s.add(m.targetVar); }
    return [...s];
  }, [state.matches]);

  const loadPreset = (idx: number) => {
    setActivePreset(idx);
    dispatch({ type: "LOAD_PRESET", state: PRESETS[idx].state });
  };

  // Real NL → Cypher via EC2 API (Claude CLI), with preset cache
  const handleConvert = useCallback(async () => {
    if (!nlText.trim() || converting) return;
    // Check if this matches a cached preset
    const cached = NL_EXAMPLES.find(ex => ex.nl === nlText.trim());
    if (cached?.cachedCypher) {
      setConverting(true);
      setNlError("");
      setNlCypher("");
      await new Promise(r => setTimeout(r, 1500));
      setNlCypher(cached.cachedCypher);
      setConverting(false);
      return;
    }
    setConverting(true);
    setNlError("");
    setNlCypher("");
    try {
      const res = await fetch(`${API_BASE}/api/nl2cypher`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: nlText.trim() }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(err.detail || `HTTP ${res.status}`);
      }
      const data = await res.json();
      setNlCypher(data.cypher);
    } catch (e: any) {
      setNlError(e.message || "NL2Cypher failed");
    } finally {
      setConverting(false);
    }
  }, [nlText, converting]);

  // Execute the generated Cypher on the real backend
  const handleExecute = useCallback(async () => {
    if (!nlCypher.trim() || executing) return;
    setExecuting(true);
    setNlError("");
    try {
      const res = await fetch(`${API_BASE}/api/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cypher: nlCypher.trim(), timeout: 30 }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(err.detail || `HTTP ${res.status}`);
      }
      const data = await res.json();
      onLiveResult?.({
        cypher: nlCypher,
        columns: data.columns,
        rows: data.rows,
        elapsed_ms: data.elapsed_ms,
      });
    } catch (e: any) {
      setNlError(e.message || "Query execution failed");
    } finally {
      setExecuting(false);
    }
  }, [nlCypher, executing, onLiveResult]);

  const loadNLExample = (idx: number) => {
    setNlText(NL_EXAMPLES[idx].nl);
    setNlCypher("");
    setNlError("");
  };

  const needsValue = (op: WhereOp) => op !== "IS NOT NULL" && op !== "IS NULL";

  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{
        maxWidth: 1440, margin: "0 auto", padding: "16px 40px", height: "100%",
        display: "flex", gap: 16, boxSizing: "border-box",
      }}>
        {/* ═══ Left: Builder ═══ */}
        <div style={{ flex: mode === "nl" ? "1 1 100%" : "1 1 58%", display: "flex", flexDirection: "column", overflow: "hidden", gap: 10 }}>

          {/* Mode toggle ─────────────────────────────────────────────── */}
          <div style={{
            display: "flex", alignItems: "center", gap: 6, padding: 4,
            background: "#f0f1f3", borderRadius: 8, width: "fit-content",
          }}>
            {([
              { id: "nl",      label: "Natural Language" },
              { id: "cypher",  label: "Cypher Builder"   },
            ] as { id: InputMode; label: string }[]).map(m => (
              <button key={m.id} onClick={() => setMode(m.id)}
                style={{
                  padding: "7px 16px", borderRadius: 6, border: "none", cursor: "pointer",
                  fontSize: 13, fontWeight: 700,
                  background: mode === m.id ? "#fff" : "transparent",
                  color:      mode === m.id ? "#18181b" : "#71717a",
                  boxShadow:  mode === m.id ? "0 1px 3px rgba(0,0,0,0.08)" : "none",
                  transition: "background 0.15s, color 0.15s",
                }}>
                {m.label}
              </button>
            ))}
          </div>

          {/* ─── NL MODE ─────────────────────────────────────────────── */}
          {mode === "nl" && (
            <div style={{ flex: 1, overflowY: "auto", overflowX: "hidden", paddingRight: 4, display: "flex", flexDirection: "column", gap: 14 }}
                 className="thin-scrollbar">
              <div>
                <div style={SECTION_LABEL}>Ask in English</div>
                <div style={{
                  position: "relative", background: "#fff", borderRadius: 10,
                  border: `1px solid ${
                    nlError ? "#ef4444" : nlCypher ? "#e84545" : "#e5e7eb"
                  }`,
                  transition: "border-color 0.15s",
                }}>
                  <textarea
                    value={nlText}
                    onChange={e => {
                      setNlText(e.target.value);
                      if (nlCypher) setNlCypher("");
                      if (nlError) setNlError("");
                    }}
                    onKeyDown={e => {
                      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
                        e.preventDefault();
                        handleConvert();
                      }
                    }}
                    placeholder="e.g. Find the top 5 people with the most friends."
                    rows={4}
                    disabled={converting}
                    style={{
                      width: "100%", boxSizing: "border-box", padding: "14px 16px",
                      border: "none", outline: "none", background: "transparent",
                      fontSize: 15, lineHeight: 1.55, color: "#18181b", resize: "none",
                      fontFamily: "'Inter', system-ui, sans-serif", borderRadius: 10,
                    }}
                  />
                  <div style={{
                    padding: "8px 10px 8px 14px", borderTop: "1px solid #f0f1f3",
                    display: "flex", alignItems: "center", justifyContent: "space-between", gap: 10,
                    fontSize: 12,
                    color: nlError ? "#ef4444" : nlCypher ? "#e84545" : "#9ca3af",
                  }}>
                    <span style={{ fontWeight: 600, flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {converting
                        ? "Translating your question…"
                        : nlError
                          ? nlError
                          : nlCypher
                            ? "Cypher generated — click Execute to run on live data."
                            : nlText.trim().length > 0
                              ? "Press Convert to translate into Cypher."
                              : "Type a question, or pick an example below."}
                    </span>
                    <div style={{ display: "flex", alignItems: "center", gap: 6, flexShrink: 0 }}>
                      {nlText && !converting && (
                        <button onClick={() => { setNlText(""); setNlCypher(""); setNlError(""); }}
                          style={{
                            background: "transparent", border: "none", color: "#9ca3af",
                            fontSize: 12, cursor: "pointer", fontWeight: 600, padding: "4px 8px",
                          }}>
                          Clear
                        </button>
                      )}
                      <button
                        onClick={handleConvert}
                        disabled={converting || !nlText.trim()}
                        style={{
                          padding: "7px 16px", borderRadius: 6, border: "none",
                          background: converting || !nlText.trim() ? "#e5e7eb" : "#e84545",
                          color: converting || !nlText.trim() ? "#9ca3af" : "#fff",
                          fontSize: 13, fontWeight: 700,
                          cursor: converting || !nlText.trim() ? "not-allowed" : "pointer",
                          display: "flex", alignItems: "center", gap: 6,
                          transition: "background 0.15s",
                        }}>
                        {converting && (
                          <motion.span
                            animate={{ rotate: 360 }}
                            transition={{ duration: 0.8, repeat: Infinity, ease: "linear" }}
                            style={{
                              width: 12, height: 12, borderRadius: "50%",
                              border: "2px solid #9ca3af", borderTopColor: "transparent",
                              display: "inline-block",
                            }}
                          />
                        )}
                        {converting ? "Converting…" : "Convert to Cypher"}
                      </button>
                    </div>
                  </div>
                </div>
              </div>

              {/* Generated Cypher + Execute */}
              {nlCypher && (
                <div>
                  <div style={SECTION_LABEL}>Generated Cypher</div>
                  <div style={{
                    padding: "14px 16px", borderRadius: 10, background: "#18181b",
                    fontFamily: "'JetBrains Mono', monospace", fontSize: 13, color: "#e5e7eb",
                    lineHeight: 1.6, whiteSpace: "pre-wrap", wordBreak: "break-word",
                  }}>
                    {nlCypher}
                  </div>
                  <button
                    onClick={handleExecute}
                    disabled={executing}
                    style={{
                      marginTop: 10, padding: "10px 0", borderRadius: 7, border: "none", width: "100%",
                      background: executing ? "#9ca3af" : "#10B981",
                      color: "#fff", fontSize: 14, fontWeight: 700,
                      cursor: executing ? "default" : "pointer",
                      display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
                      transition: "background 0.15s",
                    }}>
                    {executing && (
                      <motion.span
                        animate={{ rotate: 360 }}
                        transition={{ duration: 0.8, repeat: Infinity, ease: "linear" }}
                        style={{
                          width: 14, height: 14, borderRadius: "50%",
                          border: "2px solid rgba(255,255,255,0.4)", borderTopColor: "#fff",
                          display: "inline-block",
                        }}
                      />
                    )}
                    {executing ? "Executing on LDBC…" : "\u25b6 Execute on Live Database"}
                  </button>
                </div>
              )}

              <div>
                <div style={SECTION_LABEL}>Try an example</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {NL_EXAMPLES.map((ex, i) => (
                    <motion.button key={i} onClick={() => loadNLExample(i)}
                      whileHover={{ scale: 1.005 }} whileTap={{ scale: 0.995 }}
                      style={{
                        padding: "12px 14px", borderRadius: 10, textAlign: "left", cursor: "pointer",
                        border: nlText === ex.nl ? "2px solid #e84545" : "1px solid #e5e7eb",
                        background: nlText === ex.nl ? "#fef2f2" : "#fafbfc",
                        transition: "border 0.15s",
                      }}>
                      <div style={{ fontSize: 14, color: "#18181b", lineHeight: 1.5 }}>
                        &ldquo;{ex.nl}&rdquo;
                      </div>
                      {ex.hint && (
                        <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 4, fontFamily: "monospace" }}>
                          {ex.hint}
                        </div>
                      )}
                    </motion.button>
                  ))}
                </div>
              </div>

              <div style={{
                padding: "10px 14px", background: "#f8fafc", borderRadius: 8,
                border: "1px dashed #cbd5e1", fontSize: 12, color: "#64748b", lineHeight: 1.6,
              }}>
                <strong style={{ color: "#475569" }}>How it works:</strong> Your question is parsed into
                the same graph pattern that the Cypher builder emits. Switch to <em>Cypher Builder</em>
                to inspect or tweak the translated structure; the preview on the right stays in sync.
              </div>
            </div>
          )}

          {/* ─── CYPHER BUILDER MODE ────────────────────────────────── */}
          {mode === "cypher" && (
          <div style={{ flex: 1, overflowY: "auto", overflowX: "hidden", paddingRight: 4 }} className="thin-scrollbar">

            {/* MATCH */}
            <div style={{ marginBottom: 16 }}>
              <div style={SECTION_LABEL}>MATCH Patterns</div>
              <AnimatePresence>
                {state.matches.map(m => (
                  <motion.div key={m.id} initial={{ opacity: 0, y: -8 }} animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, height: 0, marginBottom: 0 }} transition={{ duration: 0.15 }}
                    style={{ ...ROW, marginBottom: 6 }}>
                    {m.optional && <span style={{ fontSize: 11, fontWeight: 700, color: "#F59E0B", marginRight: 2 }}>OPTIONAL</span>}
                    <span style={{ fontSize: 15, color: "#9ca3af" }}>(</span>
                    <input value={m.sourceVar} onChange={e => dispatch({ type: "UPDATE_MATCH", id: m.id, key: "sourceVar", value: e.target.value })}
                      placeholder="var" style={{ ...INPUT, width: 50, textAlign: "center" }} />
                    <span style={{ fontSize: 15, color: "#9ca3af" }}>)</span>

                    <DirToggle value={m.direction} onChange={d => dispatch({ type: "UPDATE_MATCH", id: m.id, key: "direction", value: d })} />

                    <span style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace" }}>[:</span>
                    {catalog ? (
                      <SearchSelect value={m.edgeType} options={catalog.edgeTypes}
                        onChange={v => dispatch({ type: "UPDATE_MATCH", id: m.id, key: "edgeType", value: v })}
                        placeholder="edge type" width={180} />
                    ) : (
                      <input value={m.edgeType} onChange={e => dispatch({ type: "UPDATE_MATCH", id: m.id, key: "edgeType", value: e.target.value })}
                        placeholder="edgeType" style={{ ...INPUT, width: 140 }} />
                    )}
                    <span style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace" }}>]</span>

                    <span style={{ fontSize: 15, color: "#9ca3af" }}>(</span>
                    <input value={m.targetVar} onChange={e => dispatch({ type: "UPDATE_MATCH", id: m.id, key: "targetVar", value: e.target.value })}
                      placeholder="var" style={{ ...INPUT, width: 50, textAlign: "center" }} />
                    <span style={{ fontSize: 15, color: "#9ca3af" }}>)</span>

                    <button onClick={() => dispatch({ type: "UPDATE_MATCH", id: m.id, key: "optional", value: !m.optional })}
                      title="Toggle OPTIONAL MATCH"
                      style={{ ...BTN_REMOVE, background: m.optional ? "#FEF3C7" : "#f0f1f3", color: m.optional ? "#D97706" : "#9ca3af", fontSize: 10, fontWeight: 700, width: "auto", padding: "0 6px" }}>
                      OPTIONAL
                    </button>
                    <button onClick={() => dispatch({ type: "REMOVE_MATCH", id: m.id })} style={BTN_REMOVE}>&times;</button>
                  </motion.div>
                ))}
              </AnimatePresence>
              <button onClick={() => dispatch({ type: "ADD_MATCH" })} style={BTN_ADD}>+ Add Pattern</button>
            </div>

            {/* WHERE */}
            <div style={{ marginBottom: 16 }}>
              <div style={SECTION_LABEL}>WHERE Filters</div>
              <AnimatePresence>
                {state.wheres.map(w => (
                  <motion.div key={w.id} initial={{ opacity: 0, y: -8 }} animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, height: 0 }} transition={{ duration: 0.15 }}
                    style={{ ...ROW, marginBottom: 6 }}>
                    <select value={w.variable} onChange={e => dispatch({ type: "UPDATE_WHERE", id: w.id, key: "variable", value: e.target.value })}
                      style={{ ...SELECT, width: 70 }}>
                      <option value="">var</option>
                      {vars.map(v => <option key={v} value={v}>{v}</option>)}
                    </select>
                    <span style={{ color: "#9ca3af" }}>.</span>
                    {catalog ? (
                      <SearchSelect value={w.property} options={catalog.properties}
                        onChange={v => dispatch({ type: "UPDATE_WHERE", id: w.id, key: "property", value: v })}
                        placeholder="property" width={180} />
                    ) : (
                      <input value={w.property} onChange={e => dispatch({ type: "UPDATE_WHERE", id: w.id, key: "property", value: e.target.value })}
                        placeholder="property" style={{ ...INPUT, width: 140 }} />
                    )}
                    <select value={w.operator} onChange={e => dispatch({ type: "UPDATE_WHERE", id: w.id, key: "operator", value: e.target.value })}
                      style={{ ...SELECT, width: 130 }}>
                      {WHERE_OPS.map(op => <option key={op} value={op}>{op}</option>)}
                    </select>
                    {needsValue(w.operator) && (
                      <input value={w.value} onChange={e => dispatch({ type: "UPDATE_WHERE", id: w.id, key: "value", value: e.target.value })}
                        placeholder="value" style={{ ...INPUT, width: 120 }} />
                    )}
                    <button onClick={() => dispatch({ type: "REMOVE_WHERE", id: w.id })} style={BTN_REMOVE}>&times;</button>
                  </motion.div>
                ))}
              </AnimatePresence>
              <button onClick={() => dispatch({ type: "ADD_WHERE" })} style={BTN_ADD}>+ Add Filter</button>
            </div>

            {/* RETURN */}
            <div style={{ marginBottom: 16 }}>
              <div style={SECTION_LABEL}>RETURN</div>
              <AnimatePresence>
                {state.returns.map(r => (
                  <motion.div key={r.id} initial={{ opacity: 0, y: -8 }} animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, height: 0 }} transition={{ duration: 0.15 }}
                    style={{ ...ROW, marginBottom: 6 }}>
                    <select value={r.aggregate} onChange={e => dispatch({ type: "UPDATE_RETURN", id: r.id, key: "aggregate", value: e.target.value })}
                      style={{ ...SELECT, width: 80 }}>
                      {AGG_FNS.map(a => <option key={a} value={a}>{a || "none"}</option>)}
                    </select>
                    <select value={r.variable} onChange={e => dispatch({ type: "UPDATE_RETURN", id: r.id, key: "variable", value: e.target.value })}
                      style={{ ...SELECT, width: 70 }}>
                      <option value="">var</option>
                      {vars.map(v => <option key={v} value={v}>{v}</option>)}
                    </select>
                    <span style={{ color: "#9ca3af" }}>.</span>
                    {catalog ? (
                      <SearchSelect value={r.property} options={["", ...catalog.properties]}
                        onChange={v => dispatch({ type: "UPDATE_RETURN", id: r.id, key: "property", value: v })}
                        placeholder="(all)" width={160} />
                    ) : (
                      <input value={r.property} onChange={e => dispatch({ type: "UPDATE_RETURN", id: r.id, key: "property", value: e.target.value })}
                        placeholder="property" style={{ ...INPUT, width: 120 }} />
                    )}
                    <span style={{ color: "#9ca3af", fontSize: 13 }}>AS</span>
                    <input value={r.alias} onChange={e => dispatch({ type: "UPDATE_RETURN", id: r.id, key: "alias", value: e.target.value })}
                      placeholder="alias" style={{ ...INPUT, width: 80 }} />
                    <button onClick={() => dispatch({ type: "REMOVE_RETURN", id: r.id })} style={BTN_REMOVE}>&times;</button>
                  </motion.div>
                ))}
              </AnimatePresence>
              <button onClick={() => dispatch({ type: "ADD_RETURN" })} style={BTN_ADD}>+ Add Return</button>
            </div>

            {/* ORDER BY + LIMIT */}
            <div style={{ marginBottom: 16 }}>
              <div style={SECTION_LABEL}>ORDER BY / LIMIT</div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                {state.orderBy.map((o, i) => (
                  <div key={i} style={{ display: "flex", gap: 6, alignItems: "center" }}>
                    <input value={o.expr} onChange={e => dispatch({ type: "UPDATE_ORDERBY", idx: i, key: "expr", value: e.target.value })}
                      placeholder="expr" style={{ ...INPUT, width: 120 }} />
                    <button onClick={() => dispatch({ type: "UPDATE_ORDERBY", idx: i, key: "desc", value: !o.desc })}
                      style={{ ...INPUT, width: 50, cursor: "pointer", textAlign: "center", fontWeight: 600, color: "#71717a" }}>
                      {o.desc ? "DESC" : "ASC"}
                    </button>
                    <button onClick={() => dispatch({ type: "REMOVE_ORDERBY", idx: i })} style={BTN_REMOVE}>&times;</button>
                  </div>
                ))}
                <button onClick={() => dispatch({ type: "ADD_ORDERBY" })} style={{ ...BTN_ADD, padding: "4px 10px" }}>+ ORDER BY</button>
                <div style={{ display: "flex", alignItems: "center", gap: 6, marginLeft: 8 }}>
                  <span style={{ fontSize: 13, fontWeight: 600, color: "#71717a" }}>LIMIT</span>
                  <input value={state.limit ?? ""} type="number" min={0}
                    onChange={e => dispatch({ type: "SET_LIMIT", value: e.target.value ? Number(e.target.value) : null })}
                    style={{ ...INPUT, width: 70, textAlign: "center" }} />
                </div>
              </div>
            </div>

            {/* Reset — inside scrollable area, right after ORDER BY */}
            <button onClick={() => { dispatch({ type: "RESET" }); setActivePreset(null); }}
              style={{
                padding: "10px 0", borderRadius: 8, border: "none",
                background: "#f0f1f3", color: "#71717a", fontSize: 13, fontWeight: 600,
                cursor: "pointer", width: "100%", marginTop: 4,
              }}>
              Reset Query
            </button>
          </div>
          )}
        </div>

        {/* ═══ Right: Preview + Presets (Cypher Builder mode only) ═══ */}
        {mode === "cypher" && (
        <div style={{ flex: "1 1 42%", display: "flex", flexDirection: "column", gap: 14, overflow: "hidden" }}>
          {/* Cypher preview */}
          <div style={{
            background: "#18181b", borderRadius: 10, padding: "18px 20px",
            fontFamily: "monospace", fontSize: 15, lineHeight: 1.7, color: "#e5e7eb",
            flexShrink: 0, minHeight: 100, position: "relative",
          }}>
            <div style={{ fontSize: 11, color: "#71717a", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 8 }}>
              Generated Cypher
            </div>
            <div style={{ opacity: converting ? 0.15 : 1, transition: "opacity 0.25s" }}>
              {cypher ? highlightCypher(cypher) : <span style={{ color: "#52525b" }}>Build a query to see Cypher here...</span>}
            </div>
            {converting && (
              <div style={{
                position: "absolute", inset: 0, display: "flex", flexDirection: "column",
                alignItems: "center", justifyContent: "center", gap: 10, zIndex: 10,
              }}>
                <motion.div
                  animate={{ rotate: 360 }}
                  transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
                  style={{
                    width: 28, height: 28, borderRadius: "50%",
                    border: "3px solid #52525b", borderTopColor: "#e84545",
                  }}
                />
                <span style={{ fontSize: 13, color: "#9ca3af", fontWeight: 600 }}>
                  Translating…
                </span>
              </div>
            )}
            {!converting && cypher && (
              <button onClick={() => navigator.clipboard.writeText(cypher)}
                style={{
                  position: "absolute", top: 12, right: 12,
                  padding: "4px 10px", borderRadius: 5, border: "1px solid #52525b",
                  background: "transparent", color: "#9ca3af", fontSize: 12, cursor: "pointer",
                }}>
                Copy
              </button>
            )}
          </div>

          {/* Presets */}
          <div style={{ flex: 1, overflowY: "auto" }} className="thin-scrollbar">
            <div style={SECTION_LABEL}>Presets</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              {PRESETS.map((p, i) => (
                <motion.button key={i} onClick={() => loadPreset(i)}
                  whileHover={{ scale: 1.005 }} whileTap={{ scale: 0.995 }}
                  style={{
                    padding: "14px 16px", borderRadius: 10, textAlign: "left", cursor: "pointer",
                    border: activePreset === i ? "2px solid #e84545" : "1px solid #e5e7eb",
                    background: activePreset === i ? "#fef2f2" : "#fafbfc",
                    transition: "border 0.15s",
                  }}>
                  <div style={{ fontSize: 16, fontWeight: 700, color: "#18181b", marginBottom: 3 }}>{p.label}</div>
                  <div style={{ fontSize: 13, color: "#71717a" }}>{p.desc}</div>
                </motion.button>
              ))}
            </div>
          </div>

        </div>
        )}
      </div>
    </div>
  );
}
