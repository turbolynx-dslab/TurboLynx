"use client";
import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface Props { step: number; onStep: (n: number) => void; }

// ─── Graphlet data ─────────────────────────────────────────────────────────────
type GL = { id: string; nodes: number; color: string; cols: string[] };

const P_GLS: GL[] = [
  { id: "GL-p1", nodes: 44200, color: "#8B5CF6", cols: ["p.name", "p.born"] },
  { id: "GL-p2", nodes: 38100, color: "#a78bfa", cols: ["p.name", "p.born", "p.nat"] },
  { id: "GL-p3", nodes: 12000, color: "#7C3AED", cols: ["p.name"] },
  { id: "GL-p4", nodes:  8500, color: "#9061f9", cols: ["p.name", "p.born", "p.died"] },
  { id: "GL-p5", nodes:  5200, color: "#c4b5fd", cols: ["p.name", "p.born", "p.nat", "p.died"] },
];
const F_GLS: GL[] = [
  { id: "GL-f1", nodes: 15000, color: "#F59E0B", cols: ["f.title", "f.year", "f.genre"] },
  { id: "GL-f2", nodes:  8400, color: "#fbbf24", cols: ["f.title", "f.year"] },
  { id: "GL-f3", nodes:  3200, color: "#D97706", cols: ["f.title", "f.genre", "f.runtime"] },
  { id: "GL-f4", nodes:  1900, color: "#fcd34d", cols: ["f.title"] },
];
const L_GLS: GL[] = [
  { id: "GL-l1", nodes: 9800, color: "#10B981", cols: ["l.country", "l.pop"] },
  { id: "GL-l2", nodes: 5400, color: "#34d399", cols: ["l.country"] },
  { id: "GL-l3", nodes: 2100, color: "#6ee7b7", cols: ["l.country", "l.pop", "l.area"] },
];

interface SSRFPreset { label: string; query: string; pGls: GL[]; fGls: GL[]; lGls: GL[]; }
const SSRF_PRESETS: SSRFPreset[] = [
  {
    label: "Directed",
    query: `MATCH (p)-[:directed]->(f)\nWHERE p.birthDate IS NOT NULL\n  AND f.runtime IS NOT NULL\nRETURN p.name, f.title`,
    pGls: P_GLS, fGls: F_GLS, lGls: [],
  },
  {
    label: "Director",
    query: `MATCH (p)-[:director]->(f)-[:filmed_in]->(l)\nWHERE p.birthDate IS NOT NULL\n  AND f.runtime IS NOT NULL\nRETURN p.name, f.title, l.country`,
    pGls: P_GLS, fGls: F_GLS, lGls: L_GLS,
  },
  {
    label: "Starring",
    query: `MATCH (p)-[:starring]->(f)-[:filmed_in]->(l)\nWHERE p.nationality IS NOT NULL\n  AND f.genre IS NOT NULL\nRETURN p.name, f.title, l.country`,
    pGls: P_GLS.slice(0, 3), fGls: F_GLS.slice(0, 2), lGls: L_GLS.slice(0, 2),
  },
];

function mergedCols(a: string[], b: string[]): string[] {
  const seen = new Set(a);
  return [...a, ...b.filter(c => !seen.has(c))];
}

// ─── Preset Query ──────────────────────────────────────────────────────────────
function PresetQuery({ presetIdx, onChange }: { presetIdx: number; onChange?: (i: number) => void }) {
  const preset = SSRF_PRESETS[presetIdx];
  const readOnly = !onChange;
  return (
    <div style={{ flexShrink: 0 }}>
      <div style={{ display: "flex", gap: 6, alignItems: "center", marginBottom: 7 }}>
        {readOnly ? (
          <>
            <span style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace" }}>Preset:</span>
            <span style={{ fontSize: 14, color: "#F59E0B", fontFamily: "monospace", fontWeight: 700 }}>{preset.label}</span>
            <span style={{ fontSize: 11, color: "#3f3f46", fontFamily: "monospace" }}>· carry-over from Step 1</span>
          </>
        ) : (
          <>
            <span style={{ fontSize: 14, color: "#52525b", fontFamily: "monospace" }}>Presets:</span>
            {SSRF_PRESETS.map((p, i) => (
              <button key={p.label} onClick={() => onChange!(i)} style={{
                padding: "4px 14px", borderRadius: 5, cursor: "pointer", fontSize: 15, fontFamily: "monospace",
                border: `1px solid ${presetIdx === i ? "#F59E0B" : "#27272a"}`,
                background: presetIdx === i ? "#F59E0B18" : "transparent",
                color: presetIdx === i ? "#fbbf24" : "#71717a",
              }}>{p.label}</button>
            ))}
          </>
        )}
      </div>
      <textarea readOnly value={preset.query} style={{
        background: "#131316", border: "1px solid #27272a",
        borderRadius: 8, padding: "10px 14px", fontSize: 16, color: "#f4f4f5",
        fontFamily: "monospace", lineHeight: 1.65, resize: "none", height: 130,
        outline: "none", width: "100%", boxSizing: "border-box", cursor: "default",
      }} />
    </div>
  );
}

// ─── Step 0: Schema Bloating Matrix ───────────────────────────────────────────
function Step0({ presetIdx, onPresetChange }: { presetIdx: number; onPresetChange: (i: number) => void }) {
  const { pGls, fGls } = SSRF_PRESETS[presetIdx];
  const total = pGls.length * fGls.length;
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12, height: "100%", overflow: "hidden" }}>
      <PresetQuery presetIdx={presetIdx} onChange={onPresetChange} />
      <div style={{ display: "flex", alignItems: "center", gap: 10, flexShrink: 0 }}>
        <span style={{ fontSize: 15, color: "#52525b", fontFamily: "monospace" }}>
          {pGls.length} (p)-graphlets × {fGls.length} (f)-graphlets =
        </span>
        <motion.span
          key={total}
          initial={{ scale: 1.5, color: "#e84545" }}
          animate={{ scale: 1, color: "#e84545" }}
          transition={{ type: "spring", stiffness: 400, damping: 18 }}
          style={{ fontSize: 34, fontWeight: 800, fontFamily: "monospace", lineHeight: 1 }}
        >{total}</motion.span>
        <span style={{ fontSize: 15, color: "#e84545", fontFamily: "monospace" }}>distinct intermediate schemas</span>
        <span style={{ fontSize: 13, color: "#52525b", fontFamily: "monospace", marginLeft: 4 }}>(each needs its own expression tree)</span>
      </div>
      <div style={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
        <AnimatePresence mode="wait">
          <motion.div key={presetIdx} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }}>
            <div style={{ overflowX: "auto" }}>
              <table style={{ borderCollapse: "separate", borderSpacing: 5 }}>
                <thead>
                  <tr>
                    <th style={{ minWidth: 100 }} />
                    {fGls.map(f => (
                      <th key={f.id} style={{ padding: "3px 6px", textAlign: "center", verticalAlign: "bottom" }}>
                        <div style={{ fontSize: 13, fontFamily: "monospace", fontWeight: 700, color: f.color }}>{f.id}</div>
                        <div style={{ fontSize: 11, fontFamily: "monospace", color: "#52525b", marginTop: 1 }}>{f.cols.join(" · ")}</div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pGls.map((p, pi) => (
                    <tr key={p.id}>
                      <td style={{ padding: "3px 6px", verticalAlign: "middle" }}>
                        <div style={{ fontSize: 13, fontFamily: "monospace", fontWeight: 700, color: p.color }}>{p.id}</div>
                        <div style={{ fontSize: 11, fontFamily: "monospace", color: "#52525b", marginTop: 1 }}>{p.cols.join(" · ")}</div>
                      </td>
                      {fGls.map((f, fi) => {
                        const merged = mergedCols(p.cols, f.cols);
                        return (
                          <motion.td key={f.id}
                            initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: (pi * fGls.length + fi) * 0.04, duration: 0.18 }}
                            style={{ padding: 0, verticalAlign: "top" }}>
                            <div style={{ background: "#131316", border: `1px solid ${p.color}35`, borderRadius: 6, padding: "7px 10px", minWidth: 105 }}>
                              <div style={{ fontSize: 14, fontWeight: 700, color: "#f4f4f5", fontFamily: "monospace", marginBottom: 4 }}>{merged.length} cols</div>
                              <div style={{ display: "flex", flexDirection: "column", gap: 1 }}>
                                {merged.map(c => (
                                  <span key={c} style={{ fontSize: 11, fontFamily: "monospace", color: c.startsWith("p.") ? p.color + "cc" : f.color + "cc" }}>{c}</span>
                                ))}
                              </div>
                            </div>
                          </motion.td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </motion.div>
        </AnimatePresence>
        <div style={{ marginTop: 8, fontSize: 14, color: "#52525b", fontFamily: "monospace" }}>
          Even after GEM groups graphlets — within each JOIN, every (p-schema × f-schema) pair produces a distinct output schema
        </div>
      </div>
    </div>
  );
}

// ─── Step 1: SSRF Layout ──────────────────────────────────────────────────────
// Base cols: always present in all join results — stored OUTSIDE SSRF
const BASE_COLS = ["p.name", "f.title"];

// Sparse cols: nullable, vary by graphlet combo — these go into SSRF TupleStore
const SPARSE_COLS = ["p.born", "p.nat", "f.year", "f.genre"];

// All cols for Naïve wide table
const ALL_COLS = [...BASE_COLS, ...SPARSE_COLS];

// ── SchemaInfos ──────────────────────────────────────────────────────────────
// Each entry holds: total_size + offset_infos vector (one entry per SPARSE_COL)
// offset_infos[i] = byte offset of SPARSE_COLS[i] within the compact tuple, or -1 if absent
// Multiple tuples with identical (p-graphlet × f-graphlet) schema share ONE entry
interface SchemaInfo { id: string; glPair: string; color: string; offsets: number[]; totalSize: number }
const SCHEMA_INFOS: SchemaInfo[] = [
  // GL-p1 has [p.born], GL-f1 has [f.year, f.genre] → sparse present: p.born, f.year, f.genre
  { id: "S0", glPair: "GL-p1 × GL-f1", color: "#F59E0B", offsets: [ 0, -1,  8, 16], totalSize: 24 },
  // GL-p1 has [p.born], GL-f2 has [f.year] → sparse present: p.born, f.year
  { id: "S1", glPair: "GL-p1 × GL-f2", color: "#fbbf24", offsets: [ 0, -1,  8, -1], totalSize: 16 },
  // GL-p2 has [p.born, p.nat], GL-f1 has [f.year, f.genre] → all four present
  { id: "S2", glPair: "GL-p2 × GL-f1", color: "#D97706", offsets: [ 0,  8, 16, 24], totalSize: 32 },
  // GL-p3 has [] (only p.name), GL-f2 has [f.year] → sparse present: f.year only
  { id: "S3", glPair: "GL-p3 × GL-f2", color: "#fcd34d", offsets: [-1, -1,  0, -1], totalSize:  8 },
];

// ── TupleStore ────────────────────────────────────────────────────────────────
// Stores ONLY the non-null sparse column values (no base cols, no NULLs)
// Multiple tuples referencing the same SchemaInfo — schema is SHARED
const SSRF_TUPLES = [
  { schema: "S0", sparseVals: ["1978", "2000", "Action"    ], offset:   0 }, // Alice + Gladiator → shares S0
  { schema: "S0", sparseVals: ["1985", "2010", "Drama"     ], offset:  24 }, // Bob   + Inception → shares S0 ←
  { schema: "S1", sparseVals: ["1978", "1997"              ], offset:  48 }, // Alice + Titanic   → shares S1
  { schema: "S1", sparseVals: ["1985", "2009"              ], offset:  64 }, // Bob   + Avatar    → shares S1 ←
  { schema: "S2", sparseVals: ["1964", "EN", "1994", "Drama"], offset: 80 }, // Diana + Amadeus
  { schema: "S3", sparseVals: ["1997"                      ], offset: 112 }, // Carol + Titanic
];

// Base col values per tuple (shown alongside TupleStore for clarity)
const BASE_VALS = [
  ["Alice", "Gladiator"], ["Bob", "Inception"],
  ["Alice", "Titanic"],   ["Bob", "Avatar"],
  ["Diana", "Amadeus"],   ["Carol", "Titanic"],
];

// ── Naïve wide table ─────────────────────────────────────────────────────────
// [p.name, f.title, p.born, p.nat, f.year, f.genre]
const NAIVE_ROWS: (string | null)[][] = [
  ["Alice", "Gladiator",  "1978", null, "2000", "Action"],
  ["Bob",   "Inception",  "1985", null, "2010", "Drama" ],
  ["Alice", "Titanic",    "1978", null, "1997", null    ],
  ["Bob",   "Avatar",     "1985", null, "2009", null    ],
  ["Diana", "Amadeus",    "1964", "EN", "1994", "Drama" ],
  ["Carol", "Titanic",    null,   null, "1997", null    ],
];
const NAIVE_NULLS = NAIVE_ROWS.reduce((s, r) => s + r.filter(v => v === null).length, 0);
const NAIVE_TOTAL = NAIVE_ROWS.length * ALL_COLS.length;

function Step1({ presetIdx }: { presetIdx: number }) {
  const [hovered, setHovered] = useState<string | null>(null);
  const { pGls, fGls } = SSRF_PRESETS[presetIdx];
  const total = pGls.length * fGls.length;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", overflow: "hidden" }}>
      <PresetQuery presetIdx={presetIdx} />

      {/* 2-col grid — rows auto-matched in height */}
      <div style={{
        flex: 1, minHeight: 0,
        display: "grid",
        gridTemplateColumns: "1fr 1.55fr",
        gridTemplateRows: "auto 1fr auto auto",
        gap: "6px 14px",
        overflow: "hidden",
      }}>

        {/* ── Row 1: Section headers ─────────────────────────────────────── */}
        <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
          <span style={{ fontSize: 13, color: "#e84545", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.07em" }}>Naïve</span>
          <span style={{ fontSize: 12, color: "#52525b", fontFamily: "monospace" }}>— unified wide schema, {total} variants pre-built</span>
        </div>
        <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
          <span style={{ fontSize: 13, color: "#10B981", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.07em" }}>SSRF</span>
          <span style={{ fontSize: 12, color: "#52525b", fontFamily: "monospace" }}>
            — sparse cols only in TupleStore:{" "}
            <span style={{ color: "#F59E0B" }}>{SPARSE_COLS.join(", ")}</span>
          </span>
        </div>

        {/* ── Row 2: Main tables ─────────────────────────────────────────── */}

        {/* Naïve: wide table */}
        <div style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "10px 14px", overflow: "auto" }}>
          <table style={{ fontSize: 12, fontFamily: "monospace", borderCollapse: "collapse", whiteSpace: "nowrap" }}>
            <thead>
              <tr>
                {ALL_COLS.map((c, ci) => (
                  <th key={c} style={{
                    padding: "3px 8px", textAlign: "left", borderBottom: "1px solid #27272a",
                    color: ci < BASE_COLS.length ? "#52525b" : "#71717a",
                    fontStyle: ci >= BASE_COLS.length ? "italic" : "normal",
                  }}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {NAIVE_ROWS.map((row, ri) => {
                const si = SCHEMA_INFOS[ri] ?? SCHEMA_INFOS[SCHEMA_INFOS.length - 1];
                return (
                  <tr key={ri}
                    onMouseEnter={() => setHovered(si.id)}
                    onMouseLeave={() => setHovered(null)}
                    style={{ borderBottom: "1px solid #18181b", background: hovered === si.id ? si.color + "12" : "transparent", transition: "background 0.12s" }}>
                    {row.map((v, ci) => (
                      <td key={ci} style={{
                        padding: "4px 8px",
                        color: v === null ? "#e8454566" : ci < BASE_COLS.length ? "#a1a1aa" : "#f4f4f5",
                        background: v === null ? "#e8454508" : "transparent",
                        fontStyle: v === null ? "italic" : "normal",
                      }}>
                        {v ?? "NULL"}
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* SSRF: SchemaInfos with offset_infos vector */}
        <div style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "10px 14px", overflow: "auto" }}>
          <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", marginBottom: 6, display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ textTransform: "uppercase", letterSpacing: "0.06em" }}>SchemaInfos</span>
            <span style={{ color: "#3f3f46" }}>—</span>
            <span>created lazily · shared across tuples</span>
            <span style={{ color: "#3f3f46" }}>·</span>
            <span>base cols (<span style={{ color: "#a1a1aa" }}>{BASE_COLS.join(", ")}</span>) stored separately</span>
          </div>
          <table style={{ fontSize: 12, fontFamily: "monospace", borderCollapse: "collapse", whiteSpace: "nowrap", width: "100%" }}>
            <thead>
              <tr>
                <th style={{ padding: "2px 8px", color: "#52525b", textAlign: "left", borderBottom: "1px solid #27272a" }}>id</th>
                <th style={{ padding: "2px 8px", color: "#52525b", textAlign: "left", borderBottom: "1px solid #27272a" }}>graphlet pair</th>
                <th style={{ padding: "2px 8px", color: "#52525b", textAlign: "right", borderBottom: "1px solid #27272a" }}>total_size</th>
                {/* offset_infos vector header */}
                <th colSpan={SPARSE_COLS.length} style={{ padding: "2px 8px", color: "#F59E0B", textAlign: "center", borderBottom: "1px solid #27272a", letterSpacing: "0.04em" }}>
                  offset_infos[ ]
                </th>
              </tr>
              <tr>
                <th colSpan={3} style={{ borderBottom: "1px solid #27272a" }} />
                {SPARSE_COLS.map(c => (
                  <th key={c} style={{ padding: "1px 8px", color: "#52525b", textAlign: "center", borderBottom: "1px solid #27272a", fontSize: 11 }}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {SCHEMA_INFOS.map(si => (
                <tr key={si.id}
                  onMouseEnter={() => setHovered(si.id)}
                  onMouseLeave={() => setHovered(null)}
                  style={{
                    borderBottom: "1px solid #18181b",
                    background: hovered === si.id ? si.color + "18" : "transparent",
                    transition: "background 0.12s", cursor: "default",
                  }}>
                  <td style={{ padding: "4px 8px", color: si.color, fontWeight: 700 }}>{si.id}</td>
                  <td style={{ padding: "4px 8px", color: "#3f3f46", fontSize: 11 }}>{si.glPair}</td>
                  <td style={{ padding: "4px 8px", color: "#71717a", textAlign: "right" }}>{si.totalSize}B</td>
                  {si.offsets.map((off, oi) => (
                    <td key={oi} style={{
                      padding: "4px 8px", textAlign: "center",
                      color: off === -1 ? "#3f3f46" : "#10B981",
                      fontWeight: off !== -1 ? 600 : 400,
                    }}>
                      {off === -1 ? "−1" : `+${off}`}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* ── Row 3: Secondary ──────────────────────────────────────────── */}

        {/* Naïve: pre-built schema variants */}
        <div style={{ background: "#131316", border: "1px solid #e8454520", borderRadius: 8, padding: "10px 14px", overflow: "hidden" }}>
          <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", marginBottom: 5, textTransform: "uppercase", letterSpacing: "0.06em" }}>
            {total} schemas — all pre-materialized
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
            {pGls.flatMap((p, pi) => fGls.map((f, fi) => (
              <div key={`${pi}-${fi}`} style={{
                fontSize: 11, fontFamily: "monospace",
                background: "#e8454510", border: "1px solid #e8454330",
                borderRadius: 3, padding: "2px 6px", color: "#f87171",
              }}>
                {p.id}×{f.id}
              </div>
            )))}
          </div>
        </div>

        {/* SSRF: OffsetArr + TupleStore */}
        <div style={{ display: "flex", gap: 8, overflow: "hidden" }}>
          {/* OffsetArr: byte offset + schema pointer per tuple */}
          <div style={{ flexShrink: 0 }}>
            <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", marginBottom: 5 }}>OffsetArr</div>
            <div style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 6, overflow: "hidden" }}>
              {SSRF_TUPLES.map((t, i) => {
                const si = SCHEMA_INFOS.find(s => s.id === t.schema)!;
                return (
                  <div key={i}
                    onMouseEnter={() => setHovered(t.schema)}
                    onMouseLeave={() => setHovered(null)}
                    style={{
                      padding: "4px 10px", display: "flex", alignItems: "center", gap: 6,
                      borderBottom: i < SSRF_TUPLES.length - 1 ? "1px solid #18181b" : "none",
                      background: hovered === t.schema ? si.color + "15" : "transparent",
                      transition: "all 0.12s", cursor: "default",
                    }}>
                    <span style={{ fontSize: 12, fontFamily: "monospace", color: hovered === t.schema ? "#f4f4f5" : "#52525b" }}>
                      [{t.offset}]
                    </span>
                    <span style={{ fontSize: 10, color: "#3f3f46" }}>→</span>
                    <span style={{ fontSize: 12, fontFamily: "monospace", color: si.color, fontWeight: 700 }}>{si.id}</span>
                  </div>
                );
              })}
            </div>
          </div>

          {/* TupleStore: ONLY sparse col values, no NULLs */}
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", marginBottom: 5 }}>
              TupleStore — sparse cols only, <span style={{ color: "#10B981" }}>zero NULLs</span>
            </div>
            <div style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 6, overflow: "hidden" }}>
              {SSRF_TUPLES.map((t, i) => {
                const si = SCHEMA_INFOS.find(s => s.id === t.schema)!;
                // Find sibling tuples sharing same schema
                const sameSchema = SSRF_TUPLES.filter(x => x.schema === t.schema);
                const isShared = sameSchema.length > 1;
                return (
                  <div key={i}
                    onMouseEnter={() => setHovered(t.schema)}
                    onMouseLeave={() => setHovered(null)}
                    style={{
                      display: "flex", alignItems: "center", gap: 6, padding: "4px 10px",
                      borderBottom: i < SSRF_TUPLES.length - 1 ? "1px solid #18181b" : "none",
                      background: hovered === t.schema ? si.color + "12" : "transparent",
                      transition: "background 0.12s", cursor: "default",
                    }}>
                    {/* Schema id badge */}
                    <span style={{ fontSize: 11, color: si.color, fontFamily: "monospace", fontWeight: 700, flexShrink: 0, minWidth: 22 }}>{si.id}</span>
                    {/* Base cols (outside SSRF, shown faded) */}
                    <div style={{ display: "flex", gap: 2 }}>
                      {BASE_VALS[i].map((v, vi) => (
                        <span key={vi} style={{ fontSize: 11, fontFamily: "monospace", color: "#3f3f46", background: "#ffffff06", padding: "1px 5px", borderRadius: 3 }}>{v}</span>
                      ))}
                    </div>
                    <span style={{ fontSize: 10, color: "#27272a" }}>|</span>
                    {/* Sparse vals (in SSRF TupleStore) */}
                    <div style={{ display: "flex", gap: 3, flex: 1, flexWrap: "wrap" }}>
                      {t.sparseVals.map((v, vi) => (
                        <span key={vi} style={{ fontSize: 12, fontFamily: "monospace", color: "#f4f4f5", background: si.color + "22", padding: "1px 5px", borderRadius: 3 }}>{v}</span>
                      ))}
                    </div>
                    {/* Shared schema indicator */}
                    {isShared && (
                      <span style={{ fontSize: 10, fontFamily: "monospace", color: si.color + "99", flexShrink: 0, whiteSpace: "nowrap" }}>
                        shared
                      </span>
                    )}
                    <span style={{ fontSize: 11, color: "#3f3f46", fontFamily: "monospace", flexShrink: 0 }}>{si.totalSize}B</span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* ── Row 4: Metrics (full width) ───────────────────────────────── */}
        <div style={{ gridColumn: "1 / -1", display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10 }}>
          {/* NULL waste */}
          <div style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "10px 16px", display: "flex", gap: 12, alignItems: "center" }}>
            <div style={{ flex: 1, textAlign: "center" }}>
              <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 3 }}>NULL cells</div>
              <div style={{ fontSize: 24, fontWeight: 800, color: "#e84545", fontFamily: "monospace", lineHeight: 1 }}>{NAIVE_NULLS}/{NAIVE_TOTAL}</div>
              <div style={{ fontSize: 11, color: "#e84545", fontFamily: "monospace", marginTop: 2 }}>Naïve ({Math.round(NAIVE_NULLS / NAIVE_TOTAL * 100)}% waste)</div>
            </div>
            <div style={{ width: 1, alignSelf: "stretch", background: "#27272a" }} />
            <div style={{ flex: 1, textAlign: "center" }}>
              <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 3 }}>NULL cells</div>
              <div style={{ fontSize: 24, fontWeight: 800, color: "#10B981", fontFamily: "monospace", lineHeight: 1 }}>0</div>
              <div style={{ fontSize: 11, color: "#10B981", fontFamily: "monospace", marginTop: 2 }}>SSRF (eliminated)</div>
            </div>
          </div>

          {/* Schema entries */}
          <div style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "10px 16px", display: "flex", gap: 12, alignItems: "center" }}>
            <div style={{ flex: 1, textAlign: "center" }}>
              <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 3 }}>Schema variants</div>
              <div style={{ fontSize: 24, fontWeight: 800, color: "#e84545", fontFamily: "monospace", lineHeight: 1 }}>{total}</div>
              <div style={{ fontSize: 11, color: "#e84545", fontFamily: "monospace", marginTop: 2 }}>Naïve (pre-built)</div>
            </div>
            <div style={{ width: 1, alignSelf: "stretch", background: "#27272a" }} />
            <div style={{ flex: 1, textAlign: "center" }}>
              <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 3 }}>Schema variants</div>
              <div style={{ fontSize: 24, fontWeight: 800, color: "#10B981", fontFamily: "monospace", lineHeight: 1 }}>{SCHEMA_INFOS.length}</div>
              <div style={{ fontSize: 11, color: "#10B981", fontFamily: "monospace", marginTop: 2 }}>SSRF (lazy + shared)</div>
            </div>
          </div>

          {/* Schema sharing */}
          <div style={{ background: "#131316", border: "1px solid #27272a", borderRadius: 8, padding: "10px 16px", display: "flex", gap: 12, alignItems: "center" }}>
            <div style={{ flex: 1, textAlign: "center" }}>
              <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 3 }}>Tuples</div>
              <div style={{ fontSize: 24, fontWeight: 800, color: "#f4f4f5", fontFamily: "monospace", lineHeight: 1 }}>{SSRF_TUPLES.length}</div>
              <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", marginTop: 2 }}>total rows</div>
            </div>
            <div style={{ width: 1, alignSelf: "stretch", background: "#27272a" }} />
            <div style={{ flex: 1, textAlign: "center" }}>
              <div style={{ fontSize: 11, color: "#52525b", fontFamily: "monospace", textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 3 }}>Schema entries</div>
              <div style={{ fontSize: 24, fontWeight: 800, color: "#10B981", fontFamily: "monospace", lineHeight: 1 }}>{SCHEMA_INFOS.length}</div>
              <div style={{ fontSize: 11, color: "#10B981", fontFamily: "monospace", marginTop: 2 }}>shared across {SSRF_TUPLES.length} tuples</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Titles ───────────────────────────────────────────────────────────────────
const STEP_TITLES    = ["Schema Bloating Problem", "Shared Schema Row Format"];
const STEP_SUBTITLES = [
  "Binary joins multiply schema variants exponentially",
  "Compact tuples + lazy shared schema metadata",
];

export default function S4_SSRF({ step }: Props) {
  const [presetIdx, setPresetIdx] = useState(0);
  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{ maxWidth: 1440, margin: "0 auto", padding: "28px 48px", height: "100%", display: "flex", flexDirection: "column", boxSizing: "border-box", gap: 16 }}>
        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} transition={{ duration: 0.25 }} style={{ flexShrink: 0 }}>
            <div style={{ fontSize: 13, color: "#F59E0B", fontFamily: "monospace", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.08em" }}>
              SSRF — {STEP_SUBTITLES[step]}
            </div>
            <h2 style={{ fontSize: 26, fontWeight: 700, color: "#f4f4f5", margin: 0 }}>{STEP_TITLES[step]}</h2>
          </motion.div>
        </AnimatePresence>
        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            transition={{ duration: 0.25 }} style={{ flex: 1, minHeight: 0 }}>
            {step === 0 && <Step0 presetIdx={presetIdx} onPresetChange={setPresetIdx} />}
            {step === 1 && <Step1 presetIdx={presetIdx} />}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}
