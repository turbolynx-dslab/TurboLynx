"use client";
import React, { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface Props { step: number; onStep: (n: number) => void; }

// ─── Shared Data (same graphlets as S2/S3, now with column schemas) ─────────
const GROUPS = [
  { label: "Person", color: "#DC2626", gls: [
    { id: "GL-1", cols: ["name", "birthDate"] },
    { id: "GL-2", cols: ["name", "birthDate", "nat"] },
    { id: "GL-3", cols: ["name"] },
  ]},
  { label: "City", color: "#0891B2", gls: [
    { id: "GL-6", cols: ["name", "pop"] },
    { id: "GL-7", cols: ["name"] },
  ]},
];
const EDGES = [":birthPlace", ":country"];

// Unified schema for Person ⋈ City intermediate result
const UNI_COLS = ["p.name", "p.birth", "p.nat", "c.name", "c.pop"];
const UNI_SHORT = ["name", "birth", "nat", "name", "pop"];
const BASE_IDX = [0, 3]; // p.name, c.name — always present
const SPARSE_COLS = ["p.birth", "p.nat", "c.pop"];

// Compute which columns are present per Person×City combo
const P = GROUPS[0].gls, C = GROUPS[1].gls;
const COMBOS = P.flatMap(p => C.map(c => {
  const have = new Set([...p.cols.map(x => `p.${x === "birthDate" ? "birth" : x}`), ...c.cols.map(x => `c.${x}`)]);
  const present = UNI_COLS.map(u => have.has(u));
  return { pGL: p.id, cGL: c.id, present, nulls: present.filter(x => !x).length };
}));

// Scale presets for schema explosion
const SCALES = [
  { label: "This query", counts: [3, 2] },
  { label: "10× diversity", counts: [10, 8] },
  { label: "DBpedia-scale", counts: [1410, 800] },
];

// ─── Step 0: Schema Bloating in Joins ───────────────────────────────────────

function Step0() {
  const [scaleIdx, setScaleIdx] = useState(0);
  const s = SCALES[scaleIdx];
  const product = s.counts[0] * s.counts[1];

  return (
    <div style={{ display: "flex", gap: 24, height: "100%", overflow: "hidden" }}>
      {/* Left: Graphlet column schemas */}
      <div style={{ width: "30%", flexShrink: 0, display: "flex", flexDirection: "column", gap: 10 }}>
        <div style={{
          fontSize: 15, color: "#9ca3af", fontFamily: "monospace",
          background: "#fafbfc", borderRadius: 8, padding: "8px 14px",
          border: "1px solid #e5e7eb",
        }}>
          (p)-[:birthPlace]-&gt;(c)-[:country]-&gt;(co)
        </div>

        <div style={{ fontSize: 15, color: "#6b7280", fontFamily: "monospace" }}>
          Each graphlet has <span style={{ color: "#F59E0B", fontWeight: 600 }}>different columns</span>:
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 0, flex: 1 }}>
          {GROUPS.map((group, gi) => (
            <div key={gi}>
              <div style={{
                background: group.color + "0C", border: `1.5px solid ${group.color}35`,
                borderRadius: 8, padding: "10px 14px",
              }}>
                <div style={{
                  fontSize: 15, fontWeight: 700, color: group.color,
                  fontFamily: "monospace", marginBottom: 6,
                }}>
                  {group.label}
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  {group.gls.map(gl => (
                    <div key={gl.id} style={{
                      display: "flex", alignItems: "center", gap: 8,
                      padding: "6px 12px", borderRadius: 6,
                      border: "1px solid #e5e7eb", background: "#fff",
                    }}>
                      <span style={{
                        fontSize: 15, fontFamily: "monospace",
                        color: group.color, fontWeight: 700, minWidth: 36,
                      }}>{gl.id}</span>
                      <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                        {gl.cols.map(col => (
                          <span key={col} style={{
                            fontSize: 14, fontFamily: "monospace", padding: "2px 8px",
                            borderRadius: 3, background: group.color + "15", color: group.color,
                          }}>{col === "birthDate" ? "birth" : col}</span>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
              {gi < GROUPS.length - 1 && (
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
          Join result schema = <span style={{ color: "#F59E0B", fontWeight: 600 }}>union of both sides</span>.
          Different combos → different schemas.
        </div>
      </div>

      {/* Right: Matrix + Scale */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 12, overflow: "hidden" }}>

        {scaleIdx === 0 ? (
          <div style={{
            flex: 1, minHeight: 0, overflow: "auto",
            background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
            padding: "16px 20px",
          }}>
            {/* Legend: unified column names */}
            <div style={{ display: "flex", gap: 4, marginBottom: 10, paddingLeft: 80 }}>
              <span style={{ fontSize: 15, color: "#9ca3af", fontFamily: "monospace", marginRight: 6 }}>
                Unified schema:
              </span>
              {UNI_COLS.map((col, i) => (
                <span key={i} style={{
                  fontSize: 14, fontFamily: "monospace", padding: "2px 8px", borderRadius: 3,
                  background: BASE_IDX.includes(i) ? "#71717a15" : "#F59E0B15",
                  color: BASE_IDX.includes(i) ? "#71717a" : "#F59E0B",
                  border: `1px solid ${BASE_IDX.includes(i) ? "#71717a25" : "#F59E0B30"}`,
                }}>{col}</span>
              ))}
            </div>

            {/* Column headers */}
            <div style={{ display: "grid", gridTemplateColumns: "80px 1fr 1fr", gap: 8, marginBottom: 8 }}>
              <div />
              {C.map(c => (
                <div key={c.id} style={{ textAlign: "center" }}>
                  <span style={{ fontSize: 16, fontFamily: "monospace", fontWeight: 700, color: "#0891B2" }}>
                    {c.id}
                  </span>
                  <span style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace", marginLeft: 6 }}>
                    {c.cols.map(x => x === "birthDate" ? "birth" : x).join(", ")}
                  </span>
                </div>
              ))}
            </div>

            {/* Matrix rows */}
            {P.map((p, pi) => (
              <div key={p.id} style={{
                display: "grid", gridTemplateColumns: "80px 1fr 1fr",
                gap: 8, marginBottom: 8,
              }}>
                {/* Row header */}
                <div style={{ display: "flex", flexDirection: "column", justifyContent: "center" }}>
                  <span style={{ fontSize: 16, fontFamily: "monospace", fontWeight: 700, color: "#DC2626" }}>
                    {p.id}
                  </span>
                  <span style={{ fontSize: 14, color: "#9ca3af", fontFamily: "monospace" }}>
                    {p.cols.map(x => x === "birthDate" ? "birth" : x).join(", ")}
                  </span>
                </div>

                {/* Combo cells */}
                {C.map((c, ci) => {
                  const combo = COMBOS[pi * C.length + ci];
                  const hasNull = combo.nulls > 0;
                  return (
                    <motion.div key={c.id}
                      initial={{ opacity: 0, y: 6 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ delay: (pi * C.length + ci) * 0.1, duration: 0.2 }}
                      style={{
                        background: "#fff",
                        border: `1.5px solid ${hasNull ? "#e8454530" : "#10B98130"}`,
                        borderRadius: 8, padding: "10px 12px",
                      }}
                    >
                      <div style={{
                        display: "flex", alignItems: "center", justifyContent: "space-between",
                        marginBottom: 6,
                      }}>
                        <span style={{
                          fontSize: 20, fontWeight: 700, fontFamily: "monospace",
                          color: hasNull ? "#e84545" : "#10B981",
                        }}>
                          {combo.present.filter(Boolean).length}/{UNI_COLS.length}
                        </span>
                        {hasNull && (
                          <span style={{ fontSize: 15, color: "#e84545", fontFamily: "monospace" }}>
                            {combo.nulls} NULL{combo.nulls > 1 ? "s" : ""}
                          </span>
                        )}
                      </div>
                      {/* Column presence pills */}
                      <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                        {UNI_COLS.map((col, ui) => (
                          <div key={ui} title={col} style={{
                            padding: "3px 8px", borderRadius: 4,
                            fontSize: 14, fontFamily: "monospace", fontWeight: 600,
                            background: combo.present[ui] ? "#10B98118" : "#e8454510",
                            border: `1px solid ${combo.present[ui] ? "#10B98140" : "#e8454525"}`,
                            color: combo.present[ui] ? "#10B981" : "#e8454570",
                          }}>
                            {combo.present[ui] ? UNI_SHORT[ui] : "NULL"}
                          </div>
                        ))}
                      </div>
                    </motion.div>
                  );
                })}
              </div>
            ))}

            <div style={{
              fontSize: 14, color: "#6b7280", fontFamily: "monospace",
              marginTop: 8, lineHeight: 1.5,
            }}>
              Unified Schema forces <span style={{ color: "#e84545", fontWeight: 600 }}>all 5 columns</span> on every row —
              sparse columns are valid but mostly NULL.
            </div>
          </div>
        ) : (
          /* Scaled view: just the big number */
          <div style={{
            flex: 1, display: "flex", alignItems: "center", justifyContent: "center",
            background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
          }}>
            <div style={{ textAlign: "center", fontFamily: "monospace" }}>
              <div style={{ fontSize: 16, color: "#6b7280", marginBottom: 12 }}>
                <span style={{ color: "#DC2626", fontWeight: 700 }}>{s.counts[0].toLocaleString()}</span> Person GLs
                {" "}×{" "}
                <span style={{ color: "#0891B2", fontWeight: 700 }}>{s.counts[1].toLocaleString()}</span> City GLs
              </div>
              <motion.div
                key={product}
                initial={{ scale: 1.3, color: "#e84545" }}
                animate={{ scale: 1, color: "#e84545" }}
                transition={{ type: "spring", stiffness: 400, damping: 20 }}
                style={{ fontSize: 52, fontWeight: 800, lineHeight: 1 }}
              >
                {product.toLocaleString()}
              </motion.div>
              <div style={{ fontSize: 16, color: "#e84545", marginTop: 8 }}>
                distinct intermediate schemas
              </div>
              <div style={{ fontSize: 14, color: "#9ca3af", marginTop: 12 }}>
                Each needs its own expression tree + column mappings
              </div>
            </div>
          </div>
        )}

        {/* Formula row */}
        <div style={{
          display: "flex", justifyContent: "center", alignItems: "baseline", gap: 10,
          flexShrink: 0,
        }}>
          <span style={{ fontSize: 28, fontWeight: 800, fontFamily: "monospace", color: "#DC2626" }}>
            {s.counts[0].toLocaleString()}
          </span>
          <span style={{ fontSize: 20, color: "#9ca3af" }}>×</span>
          <span style={{ fontSize: 28, fontWeight: 800, fontFamily: "monospace", color: "#0891B2" }}>
            {s.counts[1].toLocaleString()}
          </span>
          <span style={{ fontSize: 20, color: "#9ca3af" }}>=</span>
          <motion.span
            key={product}
            initial={{ scale: 1.4, color: "#e84545" }}
            animate={{ scale: 1, color: product > 100 ? "#e84545" : "#18181b" }}
            transition={{ type: "spring", stiffness: 400, damping: 20 }}
            style={{ fontSize: 36, fontWeight: 800, fontFamily: "monospace" }}
          >
            {product.toLocaleString()}
          </motion.span>
          <span style={{ fontSize: 14, color: "#6b7280", fontFamily: "monospace" }}>
            schema variants
          </span>
        </div>

        {/* Scale buttons */}
        <div style={{ display: "flex", gap: 8, justifyContent: "center", flexShrink: 0 }}>
          {SCALES.map((sc, i) => (
            <button key={i} onClick={() => setScaleIdx(i)} style={{
              padding: "8px 22px", borderRadius: 8, cursor: "pointer",
              fontSize: 15, fontFamily: "monospace", fontWeight: 600,
              border: `1.5px solid ${scaleIdx === i ? "#F59E0B" : "#d4d4d8"}`,
              background: scaleIdx === i ? "#F59E0B10" : "transparent",
              color: scaleIdx === i ? "#F59E0B" : "#71717a",
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

// ─── Step 1: Naïve vs SSRF ─────────────────────────────────────────────────

// Naïve table: [p.name, p.birth, p.nat, c.name, c.pop]
// 20 NULLs / 40 cells = 50%
const NAIVE: { vals: (string | null)[] }[] = [
  { vals: ["Alice", "1990", null,  "Seoul", "9.7M"] },  // GL-1 × GL-6
  { vals: ["Alice", "1990", null,  "Busan", null  ] },  // GL-1 × GL-7
  { vals: ["Carol", null,   null,  "Seoul", "9.7M"] },  // GL-3 × GL-6
  { vals: ["Carol", null,   null,  "Busan", null  ] },  // GL-3 × GL-7
  { vals: ["Dave",  null,   null,  "Paris", null  ] },  // GL-3 × GL-7
  { vals: ["Eve",   null,   null,  "Rome",  null  ] },  // GL-3 × GL-7
  { vals: ["Frank", null,   null,  "London",null  ] },  // GL-3 × GL-7
  { vals: ["Grace", null,   null,  "Berlin",null  ] },  // GL-3 × GL-7
];
const N_NULLS = NAIVE.reduce((s, r) => s + r.vals.filter(v => v === null).length, 0);
const N_TOTAL = NAIVE.length * UNI_COLS.length;

// SSRF tuples: base cols (always filled) + sparse (packed) + TupleStore offset
const SSRF: { si: string; base: string[]; sparse: string[]; offset: number }[] = [
  { si: "S0", base: ["Alice", "Seoul"],  sparse: ["1990", "9.7M"], offset: 0  },
  { si: "S1", base: ["Alice", "Busan"],  sparse: ["1990"],         offset: 16 },
  { si: "S2", base: ["Carol", "Seoul"],  sparse: ["9.7M"],         offset: 24 },
  { si: "S3", base: ["Carol", "Busan"],  sparse: [],               offset: 32 },
  { si: "S3", base: ["Dave",  "Paris"],  sparse: [],               offset: 32 },
  { si: "S3", base: ["Eve",   "Rome"],   sparse: [],               offset: 32 },
  { si: "S3", base: ["Frank", "London"], sparse: [],               offset: 32 },
  { si: "S3", base: ["Grace", "Berlin"], sparse: [],               offset: 32 },
];
const TS_TOTAL = 32; // total TupleStore size in bytes

// SchemaInfos: offset_infos for sparse cols [p.birth, p.nat, c.pop]
// -1 = absent, ≥0 = byte offset within tuple's sparse region
// Multiple tuples share the same SchemaInfo → "Shared Schema"
const S_INFOS: { id: string; offsets: number[]; size: number }[] = [
  { id: "S0", offsets: [0,  -1, 8 ], size: 16 },
  { id: "S1", offsets: [0,  -1, -1], size: 8  },
  { id: "S2", offsets: [-1, -1, 0 ], size: 8  },
  { id: "S3", offsets: [-1, -1, -1], size: 0  },
];

const SI_COLOR_MAP: Record<string, string> = { S0: "#F59E0B", S1: "#fbbf24", S2: "#D97706", S3: "#b45309" };

function Step1() {
  const [hovered, setHovered] = useState<string | null>(null);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12, height: "100%", overflow: "hidden" }}>
      {/* Context bar */}
      <div style={{
        display: "flex", alignItems: "center", gap: 16, flexShrink: 0,
      }}>
        <div style={{
          fontSize: 14, color: "#9ca3af", fontFamily: "monospace",
          background: "#fafbfc", borderRadius: 8, padding: "6px 14px",
          border: "1px solid #e5e7eb",
        }}>
          (p)-[:birthPlace]-&gt;(c)
        </div>
        <div style={{ fontSize: 15, color: "#6b7280", fontFamily: "monospace" }}>
          Base cols: <span style={{ color: "#71717a", fontWeight: 600 }}>p.name, c.name</span> (always present)
          {" · "}Sparse cols: <span style={{ color: "#F59E0B", fontWeight: 600 }}>p.birth, p.nat, c.pop</span> (vary per combo)
        </div>
      </div>

      {/* Side-by-side comparison */}
      <div style={{
        flex: 1, minHeight: 0, display: "grid",
        gridTemplateColumns: "1fr 1.5fr", gap: 16, overflow: "hidden",
      }}>
        {/* Naïve: Unified Wide Table */}
        <div style={{
          display: "flex", flexDirection: "column",
          background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
          padding: "14px 16px", overflow: "hidden",
        }}>
          <div style={{
            display: "flex", alignItems: "baseline", gap: 8, marginBottom: 10, flexShrink: 0,
          }}>
            <span style={{
              fontSize: 16, color: "#e84545", fontFamily: "monospace",
              textTransform: "uppercase", letterSpacing: "0.07em", fontWeight: 700,
            }}>Naïve</span>
            <span style={{ fontSize: 15, color: "#6b7280", fontFamily: "monospace" }}>
              — unified wide table
            </span>
          </div>

          <div style={{ flex: 1, minHeight: 0, overflow: "auto" }}>
            <table style={{
              fontSize: 15, fontFamily: "monospace", borderCollapse: "collapse",
              whiteSpace: "nowrap", width: "100%",
            }}>
              <thead>
                <tr>
                  {UNI_COLS.map((col, i) => (
                    <th key={col} style={{
                      padding: "4px 8px", textAlign: "left",
                      borderBottom: "1.5px solid #d4d4d8",
                      color: BASE_IDX.includes(i) ? "#71717a" : "#F59E0B",
                      fontSize: 15,
                    }}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {NAIVE.map((row, ri) => {
                  const hasNull = row.vals.some(v => v === null);
                  return (
                    <tr key={ri}
                      style={{
                        background: hasNull ? "#e8454506" : "transparent",
                      }}>
                      {row.vals.map((v, ci) => (
                        <td key={ci} style={{
                          padding: "6px 10px",
                          borderBottom: "1px solid #e5e7eb",
                          color: v === null ? "#e8454580" : BASE_IDX.includes(ci) ? "#52525b" : "#18181b",
                          background: v === null ? "#e8454510" : "transparent",
                          fontStyle: v === null ? "italic" : "normal",
                          fontWeight: v === null ? 400 : BASE_IDX.includes(ci) ? 400 : 600,
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

          <div style={{
            marginTop: 10, textAlign: "center", flexShrink: 0,
            fontSize: 15, fontFamily: "monospace",
          }}>
            <span style={{ color: "#e84545", fontWeight: 700 }}>{N_NULLS}/{N_TOTAL}</span>
            <span style={{ color: "#6b7280" }}> cells = </span>
            <span style={{ color: "#e84545", fontWeight: 700 }}>{Math.round(N_NULLS / N_TOTAL * 100)}%</span>
            <span style={{ color: "#e84545" }}> NULL waste</span>
          </div>
        </div>

        {/* SSRF: Shared Schema Row Format */}
        <div style={{
          display: "flex", flexDirection: "column",
          background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
          padding: "14px 16px", overflow: "hidden",
        }}>
          <div style={{
            display: "flex", alignItems: "baseline", gap: 8, marginBottom: 8, flexShrink: 0,
          }}>
            <span style={{
              fontSize: 18, color: "#10B981", fontFamily: "monospace",
              textTransform: "uppercase", letterSpacing: "0.07em", fontWeight: 700,
            }}>SSRF</span>
            <span style={{ fontSize: 16, color: "#6b7280", fontFamily: "monospace" }}>
              — shared schema row format
            </span>
          </div>

          <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 10 }}>
            {/* Top: Per-tuple table + SchemaInfos side by side */}
            <div style={{ display: "flex", gap: 12 }}>
              {/* Left: Base Cols + OffsetArr + SchemaPtrArr */}
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{
                  fontSize: 15, color: "#6b7280", fontFamily: "monospace",
                  marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.06em",
                }}>
                  Base Cols · OffsetArr · SchemaPtrArr
                </div>
                <table style={{
                  fontSize: 15, fontFamily: "monospace", borderCollapse: "collapse",
                  width: "100%", whiteSpace: "nowrap",
                }}>
                  <thead>
                    <tr>
                      <th style={{ padding: "4px 7px", textAlign: "left", borderBottom: "1.5px solid #d4d4d8", color: "#71717a", fontWeight: 600 }}>p.name</th>
                      <th style={{ padding: "4px 7px", textAlign: "left", borderBottom: "1.5px solid #d4d4d8", color: "#71717a", fontWeight: 600 }}>c.name</th>
                      <th style={{ padding: "4px 7px", textAlign: "right", borderBottom: "1.5px solid #d4d4d8", color: "#8B5CF6", fontWeight: 600 }}>offset</th>
                      <th style={{ padding: "4px 7px", textAlign: "center", borderBottom: "1.5px solid #d4d4d8", color: "#F59E0B", fontWeight: 600 }}>schema</th>
                    </tr>
                  </thead>
                  <tbody>
                    {SSRF.map((row, i) => {
                      const siColor = SI_COLOR_MAP[row.si] || "#71717a";
                      const isHov = hovered === row.si;
                      return (
                        <tr key={i}
                          onMouseEnter={() => setHovered(row.si)}
                          onMouseLeave={() => setHovered(null)}
                          style={{
                            background: isHov ? siColor + "12" : "transparent",
                            transition: "background 0.12s",
                          }}>
                          <td style={{ padding: "4px 7px", color: "#52525b" }}>{row.base[0]}</td>
                          <td style={{ padding: "4px 7px", color: "#52525b" }}>{row.base[1]}</td>
                          <td style={{ padding: "4px 7px", textAlign: "right", color: "#8B5CF6", fontWeight: 600 }}>{row.offset}</td>
                          <td style={{ padding: "4px 7px", textAlign: "center" }}>
                            <span style={{
                              display: "inline-block", padding: "1px 8px", borderRadius: 4,
                              background: siColor + "20", color: siColor, fontWeight: 700,
                              fontSize: 14,
                            }}>{row.si}</span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {/* Right: SchemaInfos */}
              <div style={{ width: 240, flexShrink: 0 }}>
                <div style={{
                  fontSize: 15, color: "#6b7280", fontFamily: "monospace",
                  marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.06em",
                }}>
                  SchemaInfos
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                  {S_INFOS.map((si, i) => {
                    const siColor = SI_COLOR_MAP[si.id] || "#71717a";
                    const isHov = hovered === si.id;
                    return (
                      <div key={si.id}
                        onMouseEnter={() => setHovered(si.id)}
                        onMouseLeave={() => setHovered(null)}
                        style={{
                          display: "flex", alignItems: "center", gap: 5,
                          padding: "3px 8px", borderRadius: 5,
                          background: isHov ? siColor + "15" : "transparent",
                          border: `1px solid ${isHov ? siColor + "40" : "transparent"}`,
                          transition: "all 0.12s", cursor: "default",
                          fontSize: 14, fontFamily: "monospace",
                        }}>
                        <span style={{ fontWeight: 700, color: siColor, minWidth: 22 }}>{si.id}</span>
                        <span style={{ color: "#71717a" }}>
                          {"{"}
                          {si.offsets.map((o, oi) => (
                            <React.Fragment key={oi}>
                              {oi > 0 && ","}
                              <span style={{
                                color: o < 0 ? "#d4d4d8" : "#10B981",
                                fontWeight: o >= 0 ? 700 : 400,
                              }}>
                                {o < 0 ? "-1" : o}
                              </span>
                            </React.Fragment>
                          ))}
                          {"}"}
                        </span>
                        <span style={{ color: "#9ca3af", marginLeft: "auto", fontSize: 13 }}>{si.size}B</span>
                      </div>
                    );
                  })}
                </div>
                <div style={{
                  marginTop: 5, fontSize: 13, color: "#9ca3af", fontFamily: "monospace",
                  lineHeight: 1.4,
                }}>
                  offset_infos: [{SPARSE_COLS.map((c, i) => (
                    <React.Fragment key={i}>{i > 0 && ", "}<span style={{ color: "#F59E0B" }}>{c}</span></React.Fragment>
                  ))}]
                  <br />
                  <span style={{ color: "#d4d4d8" }}>-1</span> = absent, <span style={{ color: "#10B981" }}>≥0</span> = byte offset
                </div>
              </div>
            </div>

            {/* Bottom: TupleStore — contiguous byte buffer */}
            <div>
              <div style={{
                fontSize: 15, color: "#6b7280", fontFamily: "monospace",
                marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.06em",
              }}>
                TupleStore — <span style={{ color: "#10B981" }}>contiguous buffer, zero NULLs</span>
              </div>
              <div style={{
                display: "flex", borderRadius: 6, overflow: "hidden",
                border: "1px solid #10B98130", height: 48,
              }}>
                {SSRF.filter(r => r.sparse.length > 0).map((row) => {
                  const si = S_INFOS.find(s => s.id === row.si)!;
                  const siColor = SI_COLOR_MAP[row.si] || "#71717a";
                  const isHov = hovered === row.si;
                  const widthPct = (si.size / TS_TOTAL) * 100;
                  return (
                    <div key={row.si}
                      onMouseEnter={() => setHovered(row.si)}
                      onMouseLeave={() => setHovered(null)}
                      style={{
                        width: `${widthPct}%`, padding: "2px 3px",
                        background: isHov ? siColor + "30" : siColor + "15",
                        borderRight: "1px solid #ffffff80",
                        display: "flex", flexDirection: "column", alignItems: "center",
                        justifyContent: "center", gap: 1,
                        transition: "background 0.12s", cursor: "default",
                      }}>
                      <div style={{ display: "flex", gap: 2, flexWrap: "wrap", justifyContent: "center" }}>
                        {row.sparse.map((v, vi) => (
                          <span key={vi} style={{
                            fontSize: 13, fontFamily: "monospace", fontWeight: 600,
                            color: "#18181b", background: siColor + "35",
                            padding: "1px 5px", borderRadius: 2,
                          }}>{v}</span>
                        ))}
                      </div>
                      <span style={{ fontSize: 11, color: siColor, fontFamily: "monospace", fontWeight: 600 }}>
                        {row.si} · {row.offset}B
                      </span>
                    </div>
                  );
                })}
              </div>
              {/* Byte offset scale */}
              <div style={{
                display: "flex", fontSize: 12, color: "#9ca3af", fontFamily: "monospace",
                marginTop: 2, position: "relative", height: 14,
              }}>
                {SSRF.filter(r => r.sparse.length > 0).map((row) => {
                  const leftPct = (row.offset / TS_TOTAL) * 100;
                  return (
                    <span key={row.si} style={{
                      position: "absolute", left: `${leftPct}%`,
                      transform: "translateX(-50%)",
                    }}>{row.offset}</span>
                  );
                })}
                <span style={{ position: "absolute", right: 0 }}>{TS_TOTAL}B</span>
              </div>
            </div>
          </div>

          <div style={{
            marginTop: 6, textAlign: "center", flexShrink: 0,
            fontSize: 15, fontFamily: "monospace",
          }}>
            <span style={{ color: "#10B981", fontWeight: 700 }}>0</span>
            <span style={{ color: "#6b7280" }}> NULLs — sparse cols packed, schemas </span>
            <span style={{ color: "#10B981", fontWeight: 700 }}>shared</span>
          </div>
        </div>
      </div>

      {/* Bottom metric */}
      <div style={{
        background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 8,
        padding: "10px 28px", display: "flex", gap: 16, alignItems: "center",
        justifyContent: "center", flexShrink: 0,
      }}>
        <div style={{ textAlign: "center" }}>
          <div style={{
            fontSize: 14, color: "#6b7280", fontFamily: "monospace",
            textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 2,
          }}>NULL cells</div>
          <div style={{ fontSize: 34, fontWeight: 800, color: "#e84545", fontFamily: "monospace" }}>
            {Math.round(N_NULLS / N_TOTAL * 100)}%
          </div>
          <div style={{ fontSize: 14, color: "#e84545", fontFamily: "monospace" }}>Naïve unified table</div>
        </div>
        <div style={{ fontSize: 24, color: "#9ca3af", padding: "0 8px" }}>→</div>
        <div style={{ textAlign: "center" }}>
          <div style={{
            fontSize: 14, color: "#6b7280", fontFamily: "monospace",
            textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 2,
          }}>NULL cells</div>
          <div style={{ fontSize: 34, fontWeight: 800, color: "#10B981", fontFamily: "monospace" }}>0%</div>
          <div style={{ fontSize: 14, color: "#10B981", fontFamily: "monospace" }}>SSRF — sparse cols packed</div>
        </div>
      </div>
    </div>
  );
}

// ─── Titles ─────────────────────────────────────────────────────────────────
const STEP_TITLES = ["Schema Bloating in Joins", "Shared Schema Row Format"];
const STEP_SUBTITLES = [
  "Each graphlet combo produces a different intermediate schema",
  "Separate schema info from tuples — zero NULLs, shared metadata",
];

export default function S4_SSRF({ step }: Props) {
  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{
        maxWidth: 1440, margin: "0 auto", padding: "28px 48px",
        height: "100%", display: "flex", flexDirection: "column",
        boxSizing: "border-box", gap: 16,
      }}>
        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }} transition={{ duration: 0.25 }} style={{ flexShrink: 0 }}>
            <div style={{
              fontSize: 14, color: "#F59E0B", fontFamily: "monospace",
              marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.08em",
            }}>
              SSRF — {STEP_SUBTITLES[step]}
            </div>
            <h2 style={{ fontSize: 26, fontWeight: 700, color: "#18181b", margin: 0 }}>
              {STEP_TITLES[step]}
            </h2>
          </motion.div>
        </AnimatePresence>

        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }} transition={{ duration: 0.25 }}
            style={{ flex: 1, minHeight: 0 }}>
            {step === 0 && <Step0 />}
            {step === 1 && <Step1 />}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}
