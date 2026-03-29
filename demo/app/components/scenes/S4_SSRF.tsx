"use client";
import React, { useState, useEffect, useMemo, useRef, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { QState } from "@/lib/query-state";
import {
  GraphletInfo,
  BatchResult,
  ReplayResult,
  DEFAULT_SOURCE_SCHEMA,
  simulateReplay,
  generateSampleRows,
} from "@/lib/replay-engine";

interface Props {
  step: number;
  onStep: (n: number) => void;
  queryState?: QState;
}

// ---------------------------------------------------------------------------
// Catalog types (vertex partition from dbpedia_catalog.json)
// ---------------------------------------------------------------------------
interface CatalogGraphlet {
  id: number;
  rows: number;
  extents: number;
  cols: number;
  schema: string[];
  schemaTruncated?: boolean;
}
interface CatalogVertexPartition {
  label?: string;
  numColumns?: number;
  graphlets: CatalogGraphlet[];
  numGraphlets: number;
  totalRows: number;
}
interface Catalog {
  vertexPartitions: CatalogVertexPartition[];
  edgePartitions: { type: string; short: string; totalRows: number; numGraphlets: number }[];
  summary: { totalNodes: number; totalEdges: number; totalGraphlets: number };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function fmt(n: number): string {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(n);
}


// ---------------------------------------------------------------------------
// Mini plan tree
// ---------------------------------------------------------------------------
const PLAN_NODES = [
  { id: "result", op: "Result", detail: "", color: "#71717a", children: ["project"] },
  { id: "project", op: "Project", detail: "p.name, p.birthDate, c.*", color: "#71717a", children: ["idseek"] },
  { id: "idseek", op: "IdSeek", detail: "fetch target c", color: "#0891B2", children: ["adjidx"] },
  { id: "adjidx", op: "AdjIdxJoin", detail: "birthPlace", color: "#DC2626", children: ["scan"] },
  { id: "scan", op: "GraphletScan", detail: "p: Person", color: "#3B82F6", children: [] },
];

function MiniPlanTree({ selectedOp, onSelect }: { selectedOp: string; onSelect: (op: string) => void }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 2 }}>
      {PLAN_NODES.map((node, i) => (
        <React.Fragment key={node.id}>
          {i > 0 && (
            <div style={{ width: 2, height: 12, background: "#d4d4d8" }} />
          )}
          <motion.div
            onClick={() => {
              if (node.id === "idseek" || node.id === "adjidx") onSelect(node.id);
            }}
            whileHover={
              node.id === "idseek" || node.id === "adjidx"
                ? { scale: 1.04, boxShadow: "0 2px 8px rgba(0,0,0,0.12)" }
                : {}
            }
            style={{
              display: "flex", alignItems: "center", gap: 8,
              padding: "4px 14px", borderRadius: 6,
              border: selectedOp === node.id ? `2px solid ${node.color}` : "1px solid #e5e7eb",
              background: selectedOp === node.id ? node.color + "10" : "#fff",
              cursor: node.id === "idseek" || node.id === "adjidx" ? "pointer" : "default",
              fontFamily: "monospace", fontSize: 13, minWidth: 200,
              opacity: node.id === "idseek" || node.id === "adjidx" ? 1 : 0.6,
            }}
          >
            <span style={{ fontWeight: 700, color: node.color }}>{node.op}</span>
            <span style={{ color: "#9ca3af", fontSize: 11 }}>{node.detail}</span>
          </motion.div>
        </React.Fragment>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Stats bar (cumulative comparison)
// ---------------------------------------------------------------------------
function StatsBar({ replay, currentBatch }: { replay: ReplayResult; currentBatch: number }) {
  const wideNulls = replay.cumulativeWideNulls[currentBatch] ?? 0;
  const ssrfNulls = replay.cumulativeSsrfNulls[currentBatch] ?? 0;
  const schemas = replay.cumulativeSchemas[currentBatch] ?? 0;
  const batch = replay.batches[currentBatch];
  const wideCells = replay.batches.slice(0, currentBatch + 1).reduce((s, b) => s + b.wide.totalCells, 0);
  const memorySaved = wideCells > 0 ? Math.round(((wideNulls - ssrfNulls) / wideCells) * 100) : 0;

  return (
    <div style={{
      display: "flex", gap: 20, alignItems: "center", justifyContent: "center",
      padding: "8px 16px", background: "#fafbfc", borderRadius: 8,
      border: "1px solid #e5e7eb", fontFamily: "monospace", fontSize: 13,
    }}>
      {/* NULL comparison bar */}
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <span style={{ color: "#6b7280" }}>NULLs:</span>
        <div style={{ display: "flex", alignItems: "center", gap: 4, minWidth: 180 }}>
          <span style={{ color: "#e84545", fontWeight: 700 }}>SSRF OFF</span>
          <div style={{ width: 100, height: 14, background: "#fee2e2", borderRadius: 3, position: "relative", overflow: "hidden" }}>
            <motion.div
              animate={{ width: `${Math.min(100, (wideNulls / Math.max(1, wideCells)) * 100)}%` }}
              transition={{ duration: 0.3 }}
              style={{ height: "100%", background: "#e84545", borderRadius: 3 }}
            />
          </div>
          <span style={{ color: "#e84545", fontWeight: 700, minWidth: 48, textAlign: "right" }}>{fmt(wideNulls)}</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 4, minWidth: 140 }}>
          <span style={{ color: "#10B981", fontWeight: 700 }}>SSRF ON</span>
          <div style={{ width: 100, height: 14, background: "#d1fae5", borderRadius: 3, position: "relative", overflow: "hidden" }}>
            <motion.div
              animate={{ width: "0%" }}
              transition={{ duration: 0.3 }}
              style={{ height: "100%", background: "#10B981", borderRadius: 3 }}
            />
          </div>
          <span style={{ color: "#10B981", fontWeight: 700, minWidth: 48, textAlign: "right" }}>0</span>
        </div>
      </div>

      <div style={{ width: 1, height: 20, background: "#e5e7eb" }} />

      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <span style={{ color: "#6b7280" }}>Schemas:</span>
        <span style={{ color: "#F59E0B", fontWeight: 700 }}>{schemas}</span>
      </div>

      <div style={{ width: 1, height: 20, background: "#e5e7eb" }} />

      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <span style={{ color: "#6b7280" }}>Memory saved:</span>
        <span style={{ color: "#10B981", fontWeight: 700, fontSize: 15 }}>{memorySaved}%</span>
      </div>

      <div style={{ width: 1, height: 20, background: "#e5e7eb" }} />

      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <span style={{ color: "#6b7280" }}>Batch:</span>
        <span style={{ fontWeight: 700 }}>{currentBatch + 1}/{replay.totalBatches}</span>
        <span style={{ color: "#9ca3af" }}>({batch?.tupleCount ?? 0} tuples)</span>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Wide table panel (SSRF OFF)
// ---------------------------------------------------------------------------
function WideTablePanel({ batch, allColumns, currentBatch }: {
  batch: BatchResult | null;
  allColumns: string[];
  currentBatch: number;
}) {
  if (!batch) return null;
  const rows = generateSampleRows(batch, allColumns, 8);

  return (
    <div style={{
      flex: 1, display: "flex", flexDirection: "column",
      background: "#fff8f8", border: "1px solid #fecaca", borderRadius: 8,
      padding: "10px 12px", overflow: "hidden", minWidth: 0,
    }}>
      <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 6, flexShrink: 0 }}>
        <span style={{ fontSize: 15, color: "#e84545", fontFamily: "monospace", fontWeight: 700 }}>SSRF OFF</span>
        <span style={{ fontSize: 12, color: "#9ca3af", fontFamily: "monospace" }}>wide UNIONALL table</span>
      </div>

      <div style={{ flex: 1, minHeight: 0, overflow: "auto" }}>
        <table style={{
          fontSize: 12, fontFamily: "monospace", borderCollapse: "collapse",
          whiteSpace: "nowrap", width: "100%",
        }}>
          <thead>
            <tr>
              {allColumns.map((col) => (
                <th key={col} style={{
                  padding: "3px 5px", textAlign: "left",
                  borderBottom: "1.5px solid #fca5a5",
                  color: "#b91c1c", fontSize: 11, position: "sticky", top: 0,
                  background: "#fff8f8",
                }}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            <AnimatePresence mode="popLayout">
              {rows.map((row, ri) => (
                <motion.tr
                  key={`b${currentBatch}-r${ri}`}
                  initial={{ opacity: 0, y: 6 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: ri * 0.03, duration: 0.2 }}
                >
                  {row.map((v, ci) => (
                    <td key={ci} style={{
                      padding: "3px 5px", borderBottom: "1px solid #fee2e2",
                      color: v === null ? "#e8454580" : "#18181b",
                      background: v === null ? "#fecaca" : "transparent",
                      fontStyle: v === null ? "italic" : "normal",
                      fontSize: 11, maxWidth: 80, overflow: "hidden", textOverflow: "ellipsis",
                    }}>
                      {v ?? "NULL"}
                    </td>
                  ))}
                </motion.tr>
              ))}
            </AnimatePresence>
          </tbody>
        </table>
      </div>

      <div style={{
        marginTop: 6, flexShrink: 0, display: "flex", justifyContent: "space-between",
        fontFamily: "monospace", fontSize: 12, color: "#6b7280",
      }}>
        <span>Cols: <strong style={{ color: "#e84545" }}>{batch.wide.totalColumns}</strong></span>
        <span>NULLs: <strong style={{ color: "#e84545" }}>{fmt(batch.wide.nullCount)}</strong> ({batch.wide.nullPercent}%)</span>
        <span>Cells: {fmt(batch.wide.totalCells)}</span>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// SSRF table panel (SSRF ON)
// ---------------------------------------------------------------------------
function SSRFTablePanel({ batch, sourceSchema, currentBatch }: {
  batch: BatchResult | null;
  sourceSchema: string[];
  currentBatch: number;
}) {
  if (!batch) return null;

  // SSRF: source columns (columnar) + sPtr column + packed target data
  // Show source cols + "sPtr" column
  const ssrfCols = [...sourceSchema, "sPtr"];

  // Generate sample rows for SSRF side: source values + schema pointer
  const rows: { values: (string | null)[]; schemaLabel: string }[] = [];
  let remaining = 8;
  for (const hit of batch.graphletHits) {
    if (remaining <= 0) break;
    const count = Math.min(hit.count, remaining);
    for (let i = 0; i < count; i++) {
      const vals: (string | null)[] = sourceSchema.map((col) => {
        // Source side: all columns present (columnar), generate values
        return sampleSourceValue(col, hit.graphletId, i);
      });
      rows.push({ values: vals, schemaLabel: `GL-${hit.graphletId}` });
    }
    remaining -= count;
  }

  return (
    <div style={{
      flex: 1, display: "flex", flexDirection: "column",
      background: "#f0fdf4", border: "1px solid #bbf7d0", borderRadius: 8,
      padding: "10px 12px", overflow: "hidden", minWidth: 0,
    }}>
      <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 6, flexShrink: 0 }}>
        <span style={{ fontSize: 15, color: "#10B981", fontFamily: "monospace", fontWeight: 700 }}>SSRF ON</span>
        <span style={{ fontSize: 12, color: "#9ca3af", fontFamily: "monospace" }}>columnar src + row-packed tgt</span>
      </div>

      <div style={{ flex: 1, minHeight: 0, overflow: "auto" }}>
        <table style={{
          fontSize: 12, fontFamily: "monospace", borderCollapse: "collapse",
          whiteSpace: "nowrap", width: "100%",
        }}>
          <thead>
            <tr>
              {ssrfCols.map((col) => (
                <th key={col} style={{
                  padding: "3px 5px", textAlign: "left",
                  borderBottom: "1.5px solid #86efac",
                  color: col === "sPtr" ? "#0891B2" : "#166534",
                  fontSize: 11, position: "sticky", top: 0,
                  background: "#f0fdf4",
                  fontWeight: col === "sPtr" ? 700 : 600,
                }}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            <AnimatePresence mode="popLayout">
              {rows.map((row, ri) => (
                <motion.tr
                  key={`b${currentBatch}-r${ri}`}
                  initial={{ opacity: 0, y: 6 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: ri * 0.03, duration: 0.2 }}
                >
                  {row.values.map((v, ci) => (
                    <td key={ci} style={{
                      padding: "3px 5px", borderBottom: "1px solid #dcfce7",
                      color: "#18181b", fontSize: 11,
                      maxWidth: 80, overflow: "hidden", textOverflow: "ellipsis",
                    }}>
                      {v ?? "..."}
                    </td>
                  ))}
                  {/* Schema pointer cell */}
                  <td style={{
                    padding: "3px 5px", borderBottom: "1px solid #dcfce7",
                    textAlign: "center",
                  }}>
                    <span style={{
                      padding: "1px 6px", borderRadius: 3,
                      background: "#0891B215", color: "#0891B2",
                      fontWeight: 700, fontSize: 10,
                    }}>
                      {row.schemaLabel}
                    </span>
                  </td>
                </motion.tr>
              ))}
            </AnimatePresence>
          </tbody>
        </table>
      </div>

      <div style={{
        marginTop: 6, flexShrink: 0, display: "flex", justifyContent: "space-between",
        fontFamily: "monospace", fontSize: 12, color: "#6b7280",
      }}>
        <span>Src cols: <strong style={{ color: "#10B981" }}>{sourceSchema.length}</strong></span>
        <span>NULLs: <strong style={{ color: "#10B981" }}>0</strong></span>
        <span>Schemas: <strong style={{ color: "#0891B2" }}>{batch.ssrf.schemaCount}</strong></span>
      </div>
    </div>
  );
}

function sampleSourceValue(col: string, graphletId: number, rowIndex: number): string {
  const seed = graphletId * 1000 + rowIndex;
  switch (col) {
    case "name":
      return SOURCE_NAMES[seed % SOURCE_NAMES.length];
    case "birthDate":
      return `${1940 + (seed % 60)}-${String(1 + (seed % 12)).padStart(2, "0")}-${String(1 + (seed % 28)).padStart(2, "0")}`;
    case "occupation":
      return SOURCE_OCCUPATIONS[seed % SOURCE_OCCUPATIONS.length];
    case "abstract":
      return `Abstract...`;
    default:
      return `v${seed % 100}`;
  }
}

const SOURCE_NAMES = [
  "Einstein", "Curie", "Newton", "Tesla", "Darwin", "Galileo",
  "Hawking", "Feynman", "Turing", "Euler", "Gauss", "Planck",
  "Bohr", "Dirac", "Faraday", "Maxwell",
];
const SOURCE_OCCUPATIONS = [
  "Physicist", "Chemist", "Biologist", "Engineer", "Mathematician",
  "Astronomer", "Computer Scientist", "Philosopher",
];

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------
export default function S4_SSRF({ step, onStep, queryState }: Props) {
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [selectedOp, setSelectedOp] = useState<string>("idseek");
  const [currentBatch, setCurrentBatch] = useState(0);
  const [playing, setPlaying] = useState(false);
  const playRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Load catalog on mount
  useEffect(() => {
    fetch("/dbpedia_catalog.json")
      .then((r) => r.json())
      .then((data: Catalog) => setCatalog(data))
      .catch(() => {
        // Fallback: generate synthetic graphlets
        setCatalog(null);
      });
  }, []);

  // Compute target graphlets from catalog
  const targetGraphlets: GraphletInfo[] = useMemo(() => {
    if (!catalog?.vertexPartitions?.length) {
      // Fallback: generate 1304 synthetic graphlets
      return generateSyntheticGraphlets(1304);
    }
    const vp = catalog.vertexPartitions[0];
    return vp.graphlets.map((g) => ({
      id: g.id,
      rows: g.rows,
      cols: g.cols,
      schema: g.schema ?? [],
    }));
  }, [catalog]);

  // Total edges (birthPlace edge partition or fallback)
  const totalEdges = useMemo(() => {
    if (!catalog?.edgePartitions) return 1_600_000;
    const bp = catalog.edgePartitions.find((ep) =>
      ep.short?.includes("birthPlace") || ep.type?.includes("birthPlace")
    );
    return bp?.totalRows ?? 1_600_000;
  }, [catalog]);

  // Source schema
  const sourceSchema = DEFAULT_SOURCE_SCHEMA;

  // Run simulation
  const replay = useMemo(() => {
    return simulateReplay(targetGraphlets, sourceSchema, totalEdges, 1024);
  }, [targetGraphlets, sourceSchema, totalEdges]);

  // All wide columns (union across all batches up to current)
  const allWideColumns = useMemo(() => {
    const cols = new Set<string>();
    for (let b = 0; b <= currentBatch && b < replay.batches.length; b++) {
      for (const hit of replay.batches[b].graphletHits) {
        for (const c of hit.schema) cols.add(c);
      }
    }
    return Array.from(cols).sort();
  }, [replay, currentBatch]);

  // Play/pause logic
  useEffect(() => {
    if (playing) {
      playRef.current = setInterval(() => {
        setCurrentBatch((prev) => {
          if (prev >= replay.totalBatches - 1) {
            setPlaying(false);
            return prev;
          }
          return prev + 1;
        });
      }, 500);
    } else {
      if (playRef.current) clearInterval(playRef.current);
      playRef.current = null;
    }
    return () => {
      if (playRef.current) clearInterval(playRef.current);
    };
  }, [playing, replay.totalBatches]);

  const togglePlay = useCallback(() => {
    if (currentBatch >= replay.totalBatches - 1) {
      setCurrentBatch(0);
      setPlaying(true);
    } else {
      setPlaying((p) => !p);
    }
  }, [currentBatch, replay.totalBatches]);

  const currentBatchData = replay.batches[currentBatch] ?? null;

  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{
        maxWidth: 1440, margin: "0 auto", padding: "14px 32px",
        height: "100%", display: "flex", flexDirection: "column",
        boxSizing: "border-box", gap: 10,
      }}>
        {/* Top bar: title + run selector */}
        <div style={{
          display: "flex", alignItems: "center", justifyContent: "space-between",
          flexShrink: 0,
        }}>
          <h2 style={{ fontSize: 20, fontWeight: 700, color: "#18181b", margin: 0, fontFamily: "system-ui" }}>
            Inspect: Batch-by-Batch Execution Replay
          </h2>
          <div style={{
            display: "flex", gap: 10, fontFamily: "monospace", fontSize: 13,
          }}>
            <span style={{
              padding: "4px 12px", borderRadius: 6,
              background: "#dcfce7", color: "#166534", fontWeight: 700,
              border: "1px solid #bbf7d0",
            }}>Run 1: SSRF ON</span>
            <span style={{ color: "#9ca3af", alignSelf: "center" }}>vs</span>
            <span style={{
              padding: "4px 12px", borderRadius: 6,
              background: "#fee2e2", color: "#b91c1c", fontWeight: 700,
              border: "1px solid #fecaca",
            }}>Run 2: SSRF OFF</span>
          </div>
        </div>

        {/* Middle: plan tree + slider controls */}
        <div style={{
          display: "flex", gap: 20, alignItems: "flex-start", flexShrink: 0,
        }}>
          {/* Mini plan tree */}
          <div style={{
            background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 8,
            padding: "8px 12px",
          }}>
            <div style={{ fontSize: 12, color: "#9ca3af", fontFamily: "monospace", marginBottom: 4, textTransform: "uppercase" }}>
              Physical Plan
            </div>
            <MiniPlanTree selectedOp={selectedOp} onSelect={setSelectedOp} />
          </div>

          {/* Slider + play controls */}
          <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 8 }}>
            <div style={{
              display: "flex", alignItems: "center", gap: 12,
              background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 8,
              padding: "10px 16px",
            }}>
              <motion.button
                onClick={togglePlay}
                whileHover={{ scale: 1.08 }}
                whileTap={{ scale: 0.95 }}
                style={{
                  width: 36, height: 36, borderRadius: "50%",
                  border: "none", cursor: "pointer",
                  background: playing ? "#e84545" : "#10B981",
                  color: "#fff", fontSize: 16, fontWeight: 700,
                  display: "flex", alignItems: "center", justifyContent: "center",
                }}
              >
                {playing ? "\u275A\u275A" : "\u25B6"}
              </motion.button>

              <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 2 }}>
                <input
                  type="range"
                  min={0}
                  max={replay.totalBatches - 1}
                  value={currentBatch}
                  onChange={(e) => {
                    setCurrentBatch(Number(e.target.value));
                    setPlaying(false);
                  }}
                  style={{
                    width: "100%", height: 6, cursor: "pointer",
                    accentColor: "#F59E0B",
                  }}
                />
                <div style={{
                  display: "flex", justifyContent: "space-between",
                  fontFamily: "monospace", fontSize: 11, color: "#9ca3af",
                }}>
                  <span>Batch 0</span>
                  <span style={{ color: "#F59E0B", fontWeight: 700 }}>
                    Batch {currentBatch} / {replay.totalBatches - 1}
                  </span>
                  <span>Batch {replay.totalBatches - 1}</span>
                </div>
              </div>
            </div>

            {/* Batch info summary */}
            {currentBatchData && (
              <div style={{
                display: "flex", gap: 12, fontFamily: "monospace", fontSize: 12,
                color: "#6b7280", flexWrap: "wrap",
              }}>
                <span>Tuples: <strong style={{ color: "#18181b" }}>{currentBatchData.tupleCount}</strong></span>
                <span>Graphlets hit: <strong style={{ color: "#0891B2" }}>{currentBatchData.graphletHits.length}</strong></span>
                <span>Wide cols: <strong style={{ color: "#e84545" }}>{currentBatchData.wide.totalColumns}</strong></span>
                <span>Wide NULLs: <strong style={{ color: "#e84545" }}>{fmt(currentBatchData.wide.nullCount)}</strong> ({currentBatchData.wide.nullPercent}%)</span>
                <span>SSRF NULLs: <strong style={{ color: "#10B981" }}>0</strong></span>
              </div>
            )}
          </div>
        </div>

        {/* Main area: side-by-side tables */}
        <div style={{ flex: 1, minHeight: 0, display: "flex", gap: 12, overflow: "hidden" }}>
          {/* Left: SSRF ON */}
          <SSRFTablePanel
            batch={currentBatchData}
            sourceSchema={sourceSchema}
            currentBatch={currentBatch}
          />

          {/* Right: SSRF OFF (wide) */}
          <WideTablePanel
            batch={currentBatchData}
            allColumns={allWideColumns}
            currentBatch={currentBatch}
          />
        </div>

        {/* Bottom: cumulative stats bar */}
        <StatsBar replay={replay} currentBatch={currentBatch} />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Fallback: synthetic graphlets when catalog is unavailable
// ---------------------------------------------------------------------------
function generateSyntheticGraphlets(count: number): GraphletInfo[] {
  const ALL_PROPS = [
    "id", "uri", "name", "abstract", "birthDate", "deathDate", "birthPlace",
    "nationality", "occupation", "genre", "instrument", "activeYears",
    "country", "population", "areaTotal", "elevation", "timezone",
    "postalCode", "wikiPageID", "wikiPageRevisionID", "award", "almaMater",
    "spouse", "height", "weight", "budget", "director", "starring",
    "releaseDate", "runtime", "language",
  ];

  const graphlets: GraphletInfo[] = [];
  for (let i = 0; i < count; i++) {
    // Deterministic pseudo-random using simple LCG
    const seed = (i * 2654435761) >>> 0;
    const cols = 2 + (seed % 15); // 2 to 16 columns
    const schema: string[] = [];
    for (let c = 0; c < cols && c < ALL_PROPS.length; c++) {
      const idx = (seed + c * 7919) % ALL_PROPS.length;
      if (!schema.includes(ALL_PROPS[idx])) {
        schema.push(ALL_PROPS[idx]);
      }
    }
    // Ensure at least id and uri
    if (!schema.includes("id")) schema.unshift("id");
    if (!schema.includes("uri") && schema.length < 2) schema.push("uri");

    const rows = 100 + ((seed * 31) % 500000);
    graphlets.push({
      id: i,
      rows,
      cols: schema.length,
      schema,
    });
  }
  return graphlets;
}
