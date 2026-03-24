"use client";
import React, { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface Props { step: number; onStep: (n: number) => void; }

// Render "GL_{p}-1" as GL<sub>p</sub>-1
function GLLabel({ id, style }: { id: string; style?: React.CSSProperties }) {
  const m = id.match(/^GL_\{(.+)\}-(.+)$/);
  if (!m) return <span style={style}>{id}</span>;
  return <span style={style}>GL<sub style={{ fontSize: "0.75em" }}>{m[1]}</sub>-{m[2]}</span>;
}

// Schema attributes as pills
function SchemaPills({ cols, color }: { cols: string[]; color: string }) {
  return (
    <span style={{ display: "inline-flex", gap: 3, marginLeft: 4 }}>
      {cols.map(c => (
        <span key={c} style={{
          fontSize: 13, fontFamily: "monospace", padding: "1px 7px",
          borderRadius: 4, background: color + "12", color: color + "cc",
        }}>{c}</span>
      ))}
    </span>
  );
}

// ─── Data ────────────────────────────────────────────────────────────────────

const A_SCHEMAS = [
  { id: "GL_{p}-1", schema: ["name","birthDate","team"], sparseCols: ["birth","team"], color: "#DC2626" },
  { id: "GL_{p}-2", schema: ["name","birthDate"],        sparseCols: ["birth"],        color: "#DC2626" },
  { id: "GL_{p}-3", schema: ["name"],                    sparseCols: [],               color: "#DC2626" },
];
const B_SCHEMAS = [
  { id: "GL_{c}-1", schema: ["name","population","area"], sparseCols: ["pop","area"], color: "#0891B2" },
  { id: "GL_{c}-2", schema: ["name"],                     sparseCols: [],             color: "#0891B2" },
];
const D_SCHEMAS = [
  { id: "GL_{co}-1", schema: ["name","gdp","continent"], sparseCols: ["gdp","cont."], color: "#B45309" },
  { id: "GL_{co}-2", schema: ["name"],                   sparseCols: [],              color: "#B45309" },
];

const PC = "#DC2626", CC = "#0891B2", COC = "#B45309";

// Query highlight segments
const QUERY_SEGMENTS = [
  { text: "(p)", stage: 0 },
  { text: "-[:birthPlace]→(c)", stage: 1 },
  { text: "-[:country]→(co)", stage: 2 },
];

function QueryHighlight({ stage, setStage }: { stage: number; setStage: (n: number) => void }) {
  return (
    <div style={{
      fontFamily: "monospace", fontSize: 16,
      background: "#fafbfc", borderRadius: 8, border: "1px solid #e5e7eb",
      overflow: "hidden", userSelect: "none",
    }}>
      <div style={{ display: "flex", alignItems: "center", padding: "8px 10px" }}>
        {QUERY_SEGMENTS.map((seg, i) => (
          <motion.span key={i} onClick={() => setStage(i)}
            animate={{ color: stage >= i ? "#F59E0B" : "#d4d4d8", fontWeight: stage >= i ? 700 : 400 }}
            transition={{ duration: 0.25 }}
            style={{ cursor: "pointer", padding: "2px 2px" }}
            whileHover={{ color: stage >= i ? "#d97706" : "#9ca3af" }}
          >{seg.text}</motion.span>
        ))}
      </div>
      <div style={{ height: 6, background: "#e5e7eb", display: "flex" }}>
        {QUERY_SEGMENTS.map((_, i) => (
          <div key={i} onClick={() => setStage(i)}
            style={{ flex: 1, cursor: "pointer", position: "relative", borderRight: i < 2 ? "1px solid #fafbfc" : "none" }}>
            <motion.div animate={{ scaleX: stage >= i ? 1 : 0, opacity: stage >= i ? 1 : 0 }}
              transition={{ duration: 0.3, ease: "easeOut" }}
              style={{ position: "absolute", inset: 0, background: "#F59E0B", transformOrigin: "left" }} />
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Row data ────────────────────────────────────────────────────────────────

// 8 rows: 3 Person schemas × 2 City schemas, heavy on sparser combos
// (p)→(c) cols: p.name, birth, team, c.name, pop, area = 6 cols
// NULLs per combo: p1×c1=0, p1×c2=2, p2×c1=1, p2×c2=3, p3×c1=2, p3×c2=4
// Distribution: 0+2+1+3+4+4+4+4 = 22/48 = 46%
const ROWS = [
  { p: "Alice", c: "Seoul",  co: "Korea",  pGL: "GL_{p}-1", cGL: "GL_{c}-1", coGL: "GL_{co}-1",
    pCols: ["Alice","1990","FCB"], cSparse: ["9.7M","316km²"], coSparse: ["1.6T","Asia"] },
  { p: "Alice", c: "Busan",  co: "Korea",  pGL: "GL_{p}-1", cGL: "GL_{c}-2", coGL: "GL_{co}-2",
    pCols: ["Alice","1990","FCB"], cSparse: [],                coSparse: [] },
  { p: "Bob",   c: "Seoul",  co: "Korea",  pGL: "GL_{p}-2", cGL: "GL_{c}-1", coGL: "GL_{co}-2",
    pCols: ["Bob","1988",null],    cSparse: ["9.7M","316km²"], coSparse: [] },
  { p: "Bob",   c: "Busan",  co: "Korea",  pGL: "GL_{p}-2", cGL: "GL_{c}-2", coGL: "GL_{co}-1",
    pCols: ["Bob","1988",null],    cSparse: [],                coSparse: ["1.6T","Asia"] },
  { p: "Carol", c: "Busan",  co: "Korea",  pGL: "GL_{p}-3", cGL: "GL_{c}-2", coGL: "GL_{co}-2",
    pCols: ["Carol",null,null],    cSparse: [],                coSparse: [] },
  { p: "Dave",  c: "Paris",  co: "France", pGL: "GL_{p}-3", cGL: "GL_{c}-2", coGL: "GL_{co}-2",
    pCols: ["Dave",null,null],     cSparse: [],                coSparse: [] },
  { p: "Eve",   c: "Paris",  co: "France", pGL: "GL_{p}-3", cGL: "GL_{c}-2", coGL: "GL_{co}-1",
    pCols: ["Eve",null,null],      cSparse: [],                coSparse: ["2.7T","Europe"] },
  { p: "Frank", c: "Busan",  co: "Korea",  pGL: "GL_{p}-3", cGL: "GL_{c}-2", coGL: "GL_{co}-2",
    pCols: ["Frank",null,null],    cSparse: [],                coSparse: [] },
];

// Naive columns per stage
const NAIVE_COLS = [
  ["p.name","birth","team"],
  ["p.name","birth","team","c.name","pop","area"],
  ["p.name","birth","team","c.name","pop","area","co.name","gdp","cont."],
];
const BASE_IDX = [[0],[0,3],[0,3,6]];

function getNaive(row: typeof ROWS[0], stage: number): (string|null)[] {
  const p = row.pCols;
  if (stage === 0) return p;
  const b = B_SCHEMAS.find(s => s.id === row.cGL)!;
  const cVals: (string|null)[] = [row.c,
    ...["pop","area"].map(c => b.sparseCols.includes(c) ? row.cSparse[b.sparseCols.indexOf(c)] ?? null : null)];
  if (stage === 1) return [...p, ...cVals];
  const d = D_SCHEMAS.find(s => s.id === row.coGL)!;
  const coVals: (string|null)[] = [row.co,
    ...["gdp","cont."].map(c => d.sparseCols.includes(c) ? row.coSparse[d.sparseCols.indexOf(c)] ?? null : null)];
  return [...p, ...cVals, ...coVals];
}

// Right-operand SchemaInfos (City, Country only — Person stays columnar)
const RIGHT_SCHEMA_INFOS = [
  { group: "City", color: CC, schemas: B_SCHEMAS, sparseCols: ["pop","area"] },
  { group: "Country", color: COC, schemas: D_SCHEMAS, sparseCols: ["gdp","cont."] },
];

// ─── Main Scene ──────────────────────────────────────────────────────────────

export default function S4_SSRF({ step }: Props) {
  const [stage, setStage] = useState(0);
  const [hovered, setHovered] = useState<string | null>(null);

  const naiveCols = NAIVE_COLS[stage];
  const baseIdx = BASE_IDX[stage];
  const naiveData = ROWS.map(r => getNaive(r, stage));
  const nullCount = naiveData.reduce((s, r) => s + r.filter(v => v === null).length, 0);
  const totalCells = naiveData.length * naiveCols.length;
  const nullPct = totalCells > 0 ? Math.round(nullCount / totalCells * 100) : 0;

  // Naive schemas: multiplicative
  const naiveSchemas = stage === 0 ? A_SCHEMAS.length
    : stage === 1 ? A_SCHEMAS.length * B_SCHEMAS.length
    : A_SCHEMAS.length * B_SCHEMAS.length * D_SCHEMAS.length;
  // SSRF schemas: additive (Person stays columnar, only right operands add)
  const ssrfSchemas = A_SCHEMAS.length
    + (stage >= 1 ? B_SCHEMAS.length : 0)
    + (stage >= 2 ? D_SCHEMAS.length : 0);

  // Active right-operand groups
  const activeRight = RIGHT_SCHEMA_INFOS.slice(0, Math.max(0, stage));

  // SSRF null count: same Person NULLs as naive (columnar), but right side = 0
  const ssrfNullCount = ROWS.reduce((s, r) => s + r.pCols.filter(v => v === null).length, 0);
  const ssrfTotalCells = ROWS.length * (3 + (stage >= 1 ? 1 : 0) + (stage >= 2 ? 1 : 0)); // base cols only for denominator
  // For display: person NULLs exist but right-side NULLs = 0
  const rightNullsSaved = nullCount - ssrfNullCount;

  // TupleStore entries — one per hop (each join creates its own row_major_store)
  const tsCityEntries = stage >= 1
    ? ROWS.flatMap((row, ri) => row.cSparse.map(v => ({ value: v, color: CC, ri })))
    : [];
  const tsCountryEntries = stage >= 2
    ? ROWS.flatMap((row, ri) => row.coSparse.map(v => ({ value: v, color: COC, ri })))
    : [];

  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{
        maxWidth: 1440, margin: "0 auto", padding: "20px 40px",
        height: "100%", display: "flex", flexDirection: "column",
        boxSizing: "border-box", gap: 12,
      }}>
        {/* Top: 2-column grid — left: title + query, right: graphlet schemas */}
        <div style={{
          flexShrink: 0, display: "grid",
          gridTemplateColumns: "auto 1fr", gridTemplateRows: "auto auto",
          gap: "6px 16px",
        }}>
          {/* Row 1, Col 1: Title */}
          <div style={{ gridColumn: 1, gridRow: 1 }}>
            <div style={{
              fontSize: 13, color: "#F59E0B", fontFamily: "monospace",
              marginBottom: 2, textTransform: "uppercase", letterSpacing: "0.08em",
            }}>
              SSRF — Shared Schema Row Format
            </div>
            <h2 style={{ fontSize: 22, fontWeight: 700, color: "#18181b", margin: 0 }}>
              Solving Schema Explosion in Join Intermediates
            </h2>
          </div>

          {/* Row 1-2, Col 2: Graphlet schemas (spans 2 rows) */}
          <div style={{ gridColumn: 2, gridRow: "1 / 3", display: "flex", gap: 10, alignItems: "stretch" }}>
            {[
              { label: "Person", schemas: A_SCHEMAS, color: PC, dim: false, sub: "columnar" },
              { label: "City", schemas: B_SCHEMAS, color: CC, dim: stage < 1, sub: "row-packed" },
              { label: "Country", schemas: D_SCHEMAS, color: COC, dim: stage < 2, sub: "row-packed" },
            ].map((g, gi) => (
              <div key={gi} style={{
                background: g.color + "06", border: `1px solid ${g.color}15`,
                borderRadius: 7, padding: "8px 12px", flex: 1, minWidth: 0,
                opacity: g.dim ? 0.25 : 1, transition: "opacity 0.3s",
              }}>
                <div style={{ fontSize: 14, fontFamily: "monospace", color: g.color, fontWeight: 700, marginBottom: 4 }}>
                  {g.label} <span style={{ fontWeight: 400, color: "#9ca3af", fontSize: 11 }}>{g.sub}</span>
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                  {g.schemas.map(s => (
                    <div key={s.id} style={{ display: "flex", alignItems: "center", fontSize: 14, fontFamily: "monospace", color: s.color, fontWeight: 600 }}>
                      <GLLabel id={s.id} />
                      <SchemaPills cols={s.schema} color={s.color} />
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>

          {/* Row 2, Col 1: Query Highlight */}
          <div style={{ gridColumn: 1, gridRow: 2 }}>
            <QueryHighlight stage={stage} setStage={setStage} />
          </div>
        </div>

        {/* Main 2-column layout */}
        <div style={{ flex: 1, minHeight: 0, display: "flex", gap: 14, overflow: "hidden" }}>

          {/* ═══ CENTER: Naive Columnar ═══ */}
          <div style={{
            flex: 1, display: "flex", flexDirection: "column",
            background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
            padding: "12px 14px", overflow: "hidden", minWidth: 0,
          }}>
            <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 6, flexShrink: 0 }}>
              <span style={{ fontSize: 16, color: "#e84545", fontFamily: "monospace", fontWeight: 700 }}>Columnar</span>
              <span style={{ fontSize: 13, color: "#6b7280", fontFamily: "monospace" }}>— unified wide table</span>
            </div>

            <div style={{ flex: 1, minHeight: 0, overflow: "auto" }}>
              <AnimatePresence mode="wait">
                <motion.table key={stage}
                  initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                  style={{ fontSize: 14, fontFamily: "monospace", borderCollapse: "collapse", whiteSpace: "nowrap", width: "100%" }}>
                  <thead>
                    <tr>
                      {naiveCols.map((col, i) => (
                        <th key={col} style={{
                          padding: "4px 6px", textAlign: "left", borderBottom: "1.5px solid #d4d4d8",
                          color: baseIdx.includes(i) ? "#71717a" : "#F59E0B", fontSize: 13,
                        }}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {naiveData.map((vals, ri) => (
                      <tr key={ri} style={{ background: vals.some(v => v === null) ? "#e8454504" : "transparent" }}>
                        {vals.map((v, ci) => (
                          <td key={ci} style={{
                            padding: "4px 6px", borderBottom: "1px solid #e5e7eb",
                            color: v === null ? "#e8454570" : "#18181b",
                            background: v === null ? "#e8454508" : "transparent",
                            fontStyle: v === null ? "italic" : "normal",
                            fontSize: 13,
                          }}>
                            {v ?? "NULL"}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </motion.table>
              </AnimatePresence>
            </div>

            <div style={{ marginTop: 6, flexShrink: 0, textAlign: "center", fontFamily: "monospace", fontSize: 16 }}>
              <span style={{ color: "#6b7280" }}>Schemas: </span>
              <span style={{ color: stage === 0 ? "#18181b" : "#e84545", fontWeight: 700, fontSize: 24 }}>{naiveSchemas}</span>
              {stage > 0 && <>
                <span style={{ color: "#6b7280", marginLeft: 14 }}>NULLs: </span>
                <span style={{ color: "#e84545", fontWeight: 700, fontSize: 20 }}>{nullPct}%</span>
              </>}
            </div>
          </div>

          {/* ═══ RIGHT: SSRF ═══ */}
          <div style={{
            flex: 1.2, display: "flex", flexDirection: "column",
            background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
            padding: "12px 14px", overflow: "hidden", minWidth: 0,
          }}>
            <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 6, flexShrink: 0 }}>
              <span style={{ fontSize: 16, color: "#10B981", fontFamily: "monospace", fontWeight: 700 }}>SSRF</span>
              <span style={{ fontSize: 13, color: "#6b7280", fontFamily: "monospace" }}>— left columnar + right row-packed</span>
            </div>

            <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 8 }}>
              {/* SSRF table: Person cols (columnar, NULLs OK) + right schema ptrs + packed */}
              <AnimatePresence mode="wait">
                <motion.div key={stage} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                  <table style={{ fontSize: 14, fontFamily: "monospace", borderCollapse: "collapse", whiteSpace: "nowrap", width: "100%" }}>
                    <thead>
                      <tr>
                        {/* Person columns — columnar, may have NULLs */}
                        <th style={{ padding: "4px 6px", borderBottom: "1.5px solid #d4d4d8", color: "#71717a", fontSize: 13 }}>p.name</th>
                        <th style={{ padding: "4px 6px", borderBottom: "1.5px solid #d4d4d8", color: PC, fontSize: 13 }}>birth</th>
                        <th style={{ padding: "4px 6px", borderBottom: "1.5px solid #d4d4d8", color: PC, fontSize: 13 }}>team</th>
                        {/* Right operand: base + schema ptr */}
                        {stage >= 1 && <>
                          <th style={{ padding: "4px 6px", borderBottom: "1.5px solid #d4d4d8", color: "#71717a", fontSize: 13 }}>c.name</th>
                          <th style={{ padding: "4px 6px", borderBottom: "1.5px solid #d4d4d8", color: CC, fontSize: 13, textAlign: "center" }}>SchPtr<sub style={{ fontSize: "0.75em" }}>c</sub></th>
                        </>}
                        {stage >= 2 && <>
                          <th style={{ padding: "4px 6px", borderBottom: "1.5px solid #d4d4d8", color: "#71717a", fontSize: 13 }}>co.name</th>
                          <th style={{ padding: "4px 6px", borderBottom: "1.5px solid #d4d4d8", color: COC, fontSize: 13, textAlign: "center" }}>SchPtr<sub style={{ fontSize: "0.75em" }}>co</sub></th>
                        </>}
                      </tr>
                    </thead>
                    <tbody>
                      {ROWS.map((row, ri) => (
                        <tr key={ri}>
                          {/* Person columns — columnar with NULLs */}
                          {row.pCols.map((v, ci) => (
                            <td key={ci} style={{
                              padding: "4px 6px", borderBottom: "1px solid #e5e7eb",
                              color: v === null ? "#e8454570" : "#18181b",
                              background: v === null ? "#e8454508" : "transparent",
                              fontStyle: v === null ? "italic" : "normal", fontSize: 13,
                            }}>{v ?? "NULL"}</td>
                          ))}
                          {/* City */}
                          {stage >= 1 && <>
                            <td style={{ padding: "4px 6px", borderBottom: "1px solid #e5e7eb", color: "#52525b", fontSize: 13 }}>{row.c}</td>
                            <td style={{ padding: "4px 6px", borderBottom: "1px solid #e5e7eb", textAlign: "center" }}>
                              <span style={{ padding: "1px 6px", borderRadius: 3, background: CC + "15", color: CC, fontWeight: 700, fontSize: 12 }}>
                                <GLLabel id={row.cGL} />
                              </span>
                            </td>
                          </>}
                          {/* Country */}
                          {stage >= 2 && <>
                            <td style={{ padding: "4px 6px", borderBottom: "1px solid #e5e7eb", color: "#52525b", fontSize: 13 }}>{row.co}</td>
                            <td style={{ padding: "4px 6px", borderBottom: "1px solid #e5e7eb", textAlign: "center" }}>
                              <span style={{ padding: "1px 6px", borderRadius: 3, background: COC + "15", color: COC, fontWeight: 700, fontSize: 12 }}>
                                <GLLabel id={row.coGL} />
                              </span>
                            </td>
                          </>}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </motion.div>
              </AnimatePresence>

              {/* SchemaInfos — right operands only */}
              {stage >= 1 && (
                <div>
                  <div style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace", marginBottom: 4, textTransform: "uppercase" }}>
                    SchemaInfos <span style={{ fontSize: 12, textTransform: "none", color: "#9ca3af" }}>— right operands only</span>
                  </div>
                  <div style={{ display: "flex", gap: 10 }}>
                    {activeRight.map((group, gi) => (
                      <div key={gi} style={{ flex: 1, background: group.color + "06", borderRadius: 6, padding: "6px 10px", border: `1px solid ${group.color}15` }}>
                        <div style={{ fontSize: 13, color: group.color, fontFamily: "monospace", fontWeight: 700, marginBottom: 3 }}>
                          {group.group}
                        </div>
                        {group.schemas.map(s => {
                          const offsets = group.sparseCols.map(c =>
                            s.sparseCols.includes(c) ? s.sparseCols.indexOf(c) * 8 : -1);
                          return (
                            <div key={s.id} style={{ fontSize: 13, fontFamily: "monospace", display: "flex", gap: 4, alignItems: "center", marginBottom: 1 }}>
                              <span style={{ color: group.color, fontWeight: 700 }}><GLLabel id={s.id} /></span>
                              <span style={{ color: "#71717a" }}>
                                [{offsets.map((o, i) => (
                                  <React.Fragment key={i}>
                                    {i > 0 && ","}
                                    <span style={{ color: o < 0 ? "#d4d4d8" : "#10B981", fontWeight: o >= 0 ? 700 : 400 }}>
                                      {o < 0 ? "-1" : o}
                                    </span>
                                  </React.Fragment>
                                ))}]
                              </span>
                            </div>
                          );
                        })}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* TupleStores — one per join hop */}
              {stage >= 1 && (
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {[
                    { label: "City", color: CC, entries: tsCityEntries, show: stage >= 1 },
                    { label: "Country", color: COC, entries: tsCountryEntries, show: stage >= 2 },
                  ].filter(ts => ts.show).map(ts => (
                    <div key={ts.label}>
                      <div style={{ fontSize: 13, color: ts.color, fontFamily: "monospace", marginBottom: 2, fontWeight: 600 }}>
                        TupleStore<sub style={{ fontSize: "0.75em" }}>{ts.label.toLowerCase()}</sub>
                        <span style={{ color: "#10B981", fontWeight: 400, marginLeft: 6 }}>row-packed, zero NULLs</span>
                      </div>
                      <div style={{
                        display: "flex", flexWrap: "wrap", gap: 3,
                        borderRadius: 6, border: `1px solid ${ts.color}30`,
                        padding: 5, minHeight: 24,
                      }}>
                        <AnimatePresence>
                          {ts.entries.map((e, i) => (
                            <motion.span key={`${ts.label}-${i}`}
                              initial={{ opacity: 0, scale: 0.8 }}
                              animate={{ opacity: 1, scale: 1 }}
                              transition={{ delay: i * 0.02, duration: 0.2 }}
                              style={{
                                fontSize: 12, fontFamily: "monospace", fontWeight: 600,
                                color: "#18181b", background: e.color + "20",
                                padding: "2px 6px", borderRadius: 3,
                                border: `1px solid ${e.color}30`,
                              }}
                            >{e.value}</motion.span>
                          ))}
                        </AnimatePresence>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div style={{ marginTop: 6, flexShrink: 0, textAlign: "center", fontFamily: "monospace", fontSize: 16 }}>
              <span style={{ color: "#6b7280" }}>Schemas: </span>
              <span style={{ color: "#10B981", fontWeight: 700, fontSize: 24 }}>{ssrfSchemas}</span>
              {stage > 0 && <>
                <span style={{ color: "#6b7280", marginLeft: 14 }}>Right-side NULLs: </span>
                <span style={{ color: "#10B981", fontWeight: 700, fontSize: 20 }}>0</span>
              </>}
            </div>
          </div>
        </div>

      </div>
    </div>
  );
}
