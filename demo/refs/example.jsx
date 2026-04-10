import { useState, useEffect, useCallback, useMemo } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  Legend, ResponsiveContainer, Cell
} from "recharts";
import {
  Database, Layers, Zap, Play, RotateCcw,
  ChevronRight, ChevronLeft, AlertTriangle, CheckCircle,
  GitBranch, Search, Table, Box
} from "lucide-react";

// ═══════════════════════════════════════════
// DATA CONSTANTS
// ═══════════════════════════════════════════

const PERSON_NODES = [
  { id: "v0", label: "Person", attrs: { age: 20, FN: "John", LN: "Doe" } },
  { id: "v1", label: "Person", attrs: { FN: "Frank", LN: "Hill" } },
  { id: "v2", label: "Person", attrs: { FN: "Franz" } },
  { id: "v3", label: "Person", attrs: { age: 25, gender: "F", major: "Math" } },
  { id: "v4", label: "Person", attrs: { gender: "M", major: "CS", name: "Mike" } },
  { id: "v5", label: "Person", attrs: { FN: "Sara", LN: "Kim", age: 22 } },
  { id: "v6", label: "Person", attrs: { name: "Alex", major: "Physics", birthday: "95-03" } },
  { id: "v7", label: "Person", attrs: { FN: "Lee", age: 30, url: "dbp.org" } },
  { id: "v8", label: "Person", attrs: { name: "Yuna", age: 28 } },
  { id: "v9", label: "Person", attrs: { FN: "Tom", LN: "Park", gender: "M" } },
];

const ALL_PERSON_ATTRS = ["age", "FN", "LN", "gender", "major", "name", "birthday", "url"];

// Graphlet definitions AFTER CGC clustering
const GRAPHLET_DEFS = [
  {
    id: "gl₁", color: "#3B82F6",
    schema: ["age", "FN", "LN"],
    nodes: ["v0", "v5", "v7"],
    desc: "FN/LN-based"
  },
  {
    id: "gl₂", color: "#8B5CF6",
    schema: ["FN", "LN", "gender"],
    nodes: ["v1", "v2", "v9"],
    desc: "FN-only variants"
  },
  {
    id: "gl₃", color: "#F59E0B",
    schema: ["gender", "major", "name"],
    nodes: ["v3", "v4", "v6"],
    desc: "name/major-based"
  },
  {
    id: "gl₄", color: "#10B981",
    schema: ["name", "age"],
    nodes: ["v8"],
    desc: "minimal schema"
  },
];

// Performance data from the paper
const LDBC_DATA = [
  { system: "Neo4j", speedup: 10.47, type: "GDBMS", color: "#EF4444" },
  { system: "Memgraph", speedup: 11.74, type: "GDBMS", color: "#F97316" },
  { system: "Kuzu", speedup: 106.89, type: "GDBMS", color: "#EAB308" },
  { system: "GraphScope", speedup: 26.92, type: "GDBMS", color: "#84CC16" },
  { system: "DuckPGQ", speedup: 183.9, type: "RDBMS", color: "#22D3EE" },
  { system: "Umbra", speedup: 7.74, type: "RDBMS", color: "#A78BFA" },
  { system: "DuckDB", speedup: 41.27, type: "RDBMS", color: "#818CF8" },
];

const TPCH_DATA = [
  { system: "Neo4j", speedup: 15.73, type: "GDBMS", color: "#EF4444" },
  { system: "Kuzu", speedup: 14.34, type: "GDBMS", color: "#EAB308" },
  { system: "DuckPGQ", speedup: 18.88, type: "RDBMS", color: "#22D3EE" },
  { system: "Umbra", speedup: 0.58, type: "RDBMS", color: "#A78BFA" },
  { system: "DuckDB", speedup: 1.53, type: "RDBMS", color: "#818CF8" },
];

const DBPEDIA_DATA = [
  { system: "Neo4j", speedup: 86.14, type: "GDBMS", color: "#EF4444" },
  { system: "Kuzu", speedup: 18.88, type: "GDBMS", color: "#EAB308" },
  { system: "DuckPGQ", speedup: 20.23, type: "RDBMS", color: "#22D3EE" },
  { system: "Umbra", speedup: 23.07, type: "RDBMS", color: "#A78BFA" },
  { system: "DuckDB", speedup: 20.22, type: "RDBMS", color: "#818CF8" },
];

// ═══════════════════════════════════════════
// TAB NAVIGATION
// ═══════════════════════════════════════════

const TABS = [
  { id: 0, label: "The Challenge", icon: AlertTriangle, emoji: "⚡" },
  { id: 1, label: "Graphlet Chunking", icon: Layers, emoji: "🧩" },
  { id: 2, label: "Query Processing", icon: Search, emoji: "🔍" },
  { id: 3, label: "Performance", icon: Zap, emoji: "🏆" },
];

function TabBar({ active, onChange }) {
  return (
    <div className="flex items-center gap-1 bg-slate-800/80 backdrop-blur-sm p-1.5 rounded-xl border border-slate-700/50 shadow-lg">
      {TABS.map((tab) => {
        const Icon = tab.icon;
        const isActive = active === tab.id;
        return (
          <button
            key={tab.id}
            onClick={() => onChange(tab.id)}
            className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all duration-300 ${
              isActive
                ? "bg-gradient-to-r from-blue-600 to-indigo-600 text-white shadow-lg shadow-blue-500/25"
                : "text-slate-400 hover:text-white hover:bg-slate-700/50"
            }`}
          >
            <Icon size={16} />
            <span className="hidden sm:inline">{tab.label}</span>
          </button>
        );
      })}
    </div>
  );
}

// ═══════════════════════════════════════════
// SECTION 1: THE SCHEMALESS CHALLENGE
// ═══════════════════════════════════════════

function NullCell() {
  return (
    <td className="px-2 py-1.5 text-center">
      <span className="inline-block px-1.5 py-0.5 bg-red-500/20 text-red-400 text-xs rounded font-mono border border-red-500/30">
        NULL
      </span>
    </td>
  );
}

function ValueCell({ value }) {
  return (
    <td className="px-2 py-1.5 text-center">
      <span className="text-emerald-300 text-xs font-mono">{String(value)}</span>
    </td>
  );
}

function SchemalessChallenge() {
  const [storageMode, setStorageMode] = useState("row"); // "row" | "graphlet"
  const [highlightNulls, setHighlightNulls] = useState(true);
  const [animatedNullCount, setAnimatedNullCount] = useState(0);

  const nullCount = useMemo(() => {
    if (storageMode === "row") {
      let count = 0;
      PERSON_NODES.forEach((node) => {
        ALL_PERSON_ATTRS.forEach((attr) => {
          if (!(attr in node.attrs)) count++;
        });
      });
      return count;
    }
    // In graphlet mode, count nulls within each graphlet
    let count = 0;
    GRAPHLET_DEFS.forEach((gl) => {
      gl.nodes.forEach((nid) => {
        const node = PERSON_NODES.find((n) => n.id === nid);
        gl.schema.forEach((attr) => {
          if (!(attr in node.attrs)) count++;
        });
      });
    });
    return count;
  }, [storageMode]);

  useEffect(() => {
    const target = nullCount;
    const duration = 600;
    const startTime = Date.now();
    let startVal = storageMode === "row" ? 6 : 54; // opposite mode's count
    const animate = () => {
      const elapsed = Date.now() - startTime;
      const progress = Math.min(elapsed / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      setAnimatedNullCount(Math.round(startVal + (target - startVal) * eased));
      if (progress < 1) requestAnimationFrame(animate);
    };
    requestAnimationFrame(animate);
  }, [nullCount, storageMode]);

  const totalCells = storageMode === "row"
    ? PERSON_NODES.length * ALL_PERSON_ATTRS.length
    : GRAPHLET_DEFS.reduce((sum, gl) => sum + gl.nodes.length * gl.schema.length, 0);
  const nullRatio = totalCells > 0 ? ((animatedNullCount / totalCells) * 100).toFixed(1) : 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="text-center space-y-2">
        <h2 className="text-2xl font-bold text-white">
          The Schemaless Data Challenge
        </h2>
        <p className="text-slate-400 max-w-2xl mx-auto text-sm">
          In Property Graph Models, nodes with the same label can have completely different attributes.
          How should we store them? Click below to compare approaches.
        </p>
      </div>

      {/* Mode Toggle */}
      <div className="flex justify-center">
        <div className="flex bg-slate-800 rounded-xl p-1 border border-slate-700">
          <button
            onClick={() => setStorageMode("row")}
            className={`flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-medium transition-all ${
              storageMode === "row"
                ? "bg-red-500/20 text-red-300 border border-red-500/40 shadow-lg"
                : "text-slate-400 hover:text-white"
            }`}
          >
            <Table size={16} />
            Single Table (Row Store)
          </button>
          <button
            onClick={() => setStorageMode("graphlet")}
            className={`flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-medium transition-all ${
              storageMode === "graphlet"
                ? "bg-blue-500/20 text-blue-300 border border-blue-500/40 shadow-lg"
                : "text-slate-400 hover:text-white"
            }`}
          >
            <Box size={16} />
            Graphlet-based (TurboLynx)
          </button>
        </div>
      </div>

      {/* Metrics Bar */}
      <div className="flex justify-center gap-6">
        <div className={`px-5 py-3 rounded-xl border transition-all duration-500 ${
          storageMode === "row"
            ? "bg-red-500/10 border-red-500/30"
            : "bg-emerald-500/10 border-emerald-500/30"
        }`}>
          <div className="text-xs text-slate-400 mb-1">NULL Cells</div>
          <div className={`text-2xl font-bold font-mono transition-colors ${
            storageMode === "row" ? "text-red-400" : "text-emerald-400"
          }`}>
            {animatedNullCount}
          </div>
        </div>
        <div className={`px-5 py-3 rounded-xl border transition-all duration-500 ${
          storageMode === "row"
            ? "bg-red-500/10 border-red-500/30"
            : "bg-emerald-500/10 border-emerald-500/30"
        }`}>
          <div className="text-xs text-slate-400 mb-1">NULL Ratio</div>
          <div className={`text-2xl font-bold font-mono transition-colors ${
            storageMode === "row" ? "text-red-400" : "text-emerald-400"
          }`}>
            {nullRatio}%
          </div>
        </div>
        <div className="px-5 py-3 rounded-xl border bg-slate-800/50 border-slate-700">
          <div className="text-xs text-slate-400 mb-1">Total Cells</div>
          <div className="text-2xl font-bold font-mono text-slate-300">{totalCells}</div>
        </div>
        {storageMode === "graphlet" && (
          <div className="px-5 py-3 rounded-xl border bg-blue-500/10 border-blue-500/30">
            <div className="text-xs text-slate-400 mb-1">Graphlets</div>
            <div className="text-2xl font-bold font-mono text-blue-400">{GRAPHLET_DEFS.length}</div>
          </div>
        )}
      </div>

      {/* Table Visualization */}
      <div className="overflow-x-auto">
        {storageMode === "row" ? (
          <div className="space-y-2">
            <div className="bg-slate-800/50 rounded-xl border border-slate-700 p-4 overflow-x-auto">
              <div className="text-xs text-slate-500 mb-2 font-mono">// One giant table for ALL Person nodes — {ALL_PERSON_ATTRS.length} columns</div>
              <table className="w-full text-sm border-collapse">
                <thead>
                  <tr className="border-b border-slate-600">
                    <th className="px-2 py-2 text-left text-slate-400 text-xs font-mono">ID</th>
                    {ALL_PERSON_ATTRS.map((attr) => (
                      <th key={attr} className="px-2 py-2 text-center text-slate-400 text-xs font-mono">{attr}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {PERSON_NODES.map((node, i) => (
                    <tr key={node.id} className={`border-b border-slate-700/50 transition-all ${i % 2 === 0 ? "bg-slate-800/30" : ""}`}>
                      <td className="px-2 py-1.5 text-blue-400 text-xs font-mono font-bold">{node.id}</td>
                      {ALL_PERSON_ATTRS.map((attr) =>
                        attr in node.attrs
                          ? <ValueCell key={attr} value={node.attrs[attr]} />
                          : <NullCell key={attr} />
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {/* Aha callout */}
            <div className="bg-gradient-to-r from-red-500/10 to-orange-500/10 border border-red-500/30 rounded-xl p-4 flex items-start gap-3">
              <AlertTriangle className="text-red-400 mt-0.5 flex-shrink-0" size={20} />
              <div>
                <div className="text-red-300 font-semibold text-sm">Aha! The NULL Explosion</div>
                <div className="text-slate-400 text-xs mt-1">
                  With just 10 nodes and 8 distinct attributes, <span className="text-red-400 font-bold">{nullCount} out of {totalCells} cells are NULL ({nullRatio}%)</span>.
                  On DBpedia (282K unique schemas), this produces <span className="text-red-400 font-bold">~212 billion NULL entries</span> for 77M nodes.
                  Each NULL still requires per-tuple schema interpretation at query time.
                </div>
              </div>
            </div>
          </div>
        ) : (
          <div className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {GRAPHLET_DEFS.map((gl) => {
                const nodes = gl.nodes.map((nid) => PERSON_NODES.find((n) => n.id === nid));
                return (
                  <div
                    key={gl.id}
                    className="bg-slate-800/50 rounded-xl border p-4 transition-all hover:shadow-lg"
                    style={{ borderColor: gl.color + "40" }}
                  >
                    <div className="flex items-center gap-2 mb-2">
                      <div className="w-3 h-3 rounded-full" style={{ backgroundColor: gl.color }} />
                      <span className="text-sm font-bold" style={{ color: gl.color }}>{gl.id}</span>
                      <span className="text-xs text-slate-500">— {gl.desc}</span>
                    </div>
                    <div className="text-xs text-slate-500 font-mono mb-2">
                      schema: [{gl.schema.join(", ")}]
                    </div>
                    <table className="w-full text-sm border-collapse">
                      <thead>
                        <tr className="border-b border-slate-600">
                          <th className="px-2 py-1 text-left text-slate-400 text-xs font-mono">ID</th>
                          {gl.schema.map((attr) => (
                            <th key={attr} className="px-2 py-1 text-center text-xs font-mono" style={{ color: gl.color + "CC" }}>{attr}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {nodes.map((node, i) => (
                          <tr key={node.id} className={`border-b border-slate-700/30 ${i % 2 === 0 ? "bg-slate-800/20" : ""}`}>
                            <td className="px-2 py-1 text-xs font-mono font-bold" style={{ color: gl.color }}>{node.id}</td>
                            {gl.schema.map((attr) =>
                              attr in node.attrs
                                ? <ValueCell key={attr} value={node.attrs[attr]} />
                                : <NullCell key={attr} />
                            )}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                );
              })}
            </div>
            {/* Aha callout */}
            <div className="bg-gradient-to-r from-blue-500/10 to-emerald-500/10 border border-blue-500/30 rounded-xl p-4 flex items-start gap-3">
              <CheckCircle className="text-emerald-400 mt-0.5 flex-shrink-0" size={20} />
              <div>
                <div className="text-emerald-300 font-semibold text-sm">Graphlets eliminate most NULLs!</div>
                <div className="text-slate-400 text-xs mt-1">
                  By clustering nodes with similar schemas into <span className="text-blue-400 font-bold">columnar graphlets</span>,
                  NULLs drop from <span className="text-red-400">54</span> (row store) to just <span className="text-emerald-400 font-bold">{animatedNullCount}</span> ({nullRatio}% ratio).
                  Each graphlet is a compact columnar unit enabling <span className="text-blue-400 font-bold">SIMD vectorization</span>.
                  On DBpedia, CGC achieves up to <span className="text-emerald-400 font-bold">5,319× speedup</span> on scan with selection.
                </div>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Scale callout */}
      <div className="bg-slate-800/30 rounded-xl border border-slate-700/50 p-4">
        <div className="text-xs text-slate-500 text-center">
          📊 <strong className="text-slate-300">At scale:</strong> DBpedia has <span className="text-amber-400">2,796 unique schemas</span> and <span className="text-amber-400">282,764 unique attribute sets</span> across 77M nodes.
          The naive single-table approach wastes enormous space; separating every schema creates too many tables.
          <strong className="text-blue-400"> CGC finds the optimal balance.</strong>
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════
// SECTION 2: GRAPHLET CHUNKING
// ═══════════════════════════════════════════

function GraphletChunking() {
  const [step, setStep] = useState(0); // 0: initial, 1: extract schemas, 2: layering, 3: clustering, 4: final
  const [cSch, setCSch] = useState(100);
  const [cNull, setCNull] = useState(0.3);
  const [cVec, setCVec] = useState(10000);

  const STEPS = [
    { title: "Raw Schemaless Data", desc: "10 Person nodes with 7+ different schemas" },
    { title: "Step 1: Extract Schemas", desc: "Identify each node's unique attribute set" },
    { title: "Step 2: Size-based Layering", desc: "Group provisional graphlets by size into p layers" },
    { title: "Step 3: Agglomerative Clustering", desc: "Merge graphlets using cost-aware similarity (casim)" },
    { title: "Final Graphlets", desc: "Optimal columnar storage units ready for vectorized processing" },
  ];

  const schemas = useMemo(() => {
    const map = {};
    PERSON_NODES.forEach((n) => {
      const key = Object.keys(n.attrs).sort().join(",");
      if (!map[key]) map[key] = { schema: Object.keys(n.attrs).sort(), nodes: [], key };
      map[key].nodes.push(n.id);
    });
    return Object.values(map);
  }, []);

  const nodeColor = useCallback((nid) => {
    if (step < 3) {
      const schIdx = schemas.findIndex((s) => s.nodes.includes(nid));
      const colors = ["#3B82F6", "#8B5CF6", "#F59E0B", "#10B981", "#EF4444", "#EC4899", "#06B6D4", "#F97316", "#84CC16", "#6366F1"];
      return colors[schIdx % colors.length];
    }
    const gl = GRAPHLET_DEFS.find((g) => g.nodes.includes(nid));
    return gl ? gl.color : "#64748B";
  }, [step, schemas]);

  // Node positions
  const positions = useMemo(() => {
    const pos = {};
    if (step <= 1) {
      // Scattered
      const coords = [
        [80,60],[200,40],[320,70],[440,50],[560,65],
        [140,150],[260,140],[380,160],[500,145],[620,155]
      ];
      PERSON_NODES.forEach((n, i) => { pos[n.id] = coords[i]; });
    } else if (step === 2) {
      // Grouped by schema
      let x = 60;
      schemas.forEach((s) => {
        s.nodes.forEach((nid, j) => {
          pos[nid] = [x + j * 40, 100];
        });
        x += s.nodes.length * 40 + 50;
      });
    } else {
      // Grouped by graphlet
      const glPositions = [[100, 70], [300, 70], [500, 70], [650, 70]];
      GRAPHLET_DEFS.forEach((gl, gi) => {
        gl.nodes.forEach((nid, ni) => {
          pos[nid] = [glPositions[gi][0] + ni * 45, glPositions[gi][1] + (ni % 2) * 35];
        });
      });
    }
    return pos;
  }, [step, schemas]);

  return (
    <div className="space-y-6">
      <div className="text-center space-y-2">
        <h2 className="text-2xl font-bold text-white">Cost-based Graphlet Chunking (CGC)</h2>
        <p className="text-slate-400 max-w-2xl mx-auto text-sm">
          CGC clusters nodes with similar schemas into columnar graphlets, balancing schema count,
          NULL overhead, and vectorization potential.
        </p>
      </div>

      {/* Step controls */}
      <div className="flex items-center justify-center gap-3">
        <button
          onClick={() => setStep(Math.max(0, step - 1))}
          disabled={step === 0}
          className="flex items-center gap-1 px-3 py-2 rounded-lg bg-slate-700 text-slate-300 text-sm disabled:opacity-30 hover:bg-slate-600 transition-all"
        >
          <ChevronLeft size={16} /> Back
        </button>

        <div className="flex gap-1.5">
          {STEPS.map((_, i) => (
            <button
              key={i}
              onClick={() => setStep(i)}
              className={`w-8 h-8 rounded-full text-xs font-bold transition-all ${
                i === step
                  ? "bg-blue-600 text-white shadow-lg shadow-blue-500/30"
                  : i < step
                  ? "bg-blue-600/30 text-blue-300"
                  : "bg-slate-700 text-slate-500"
              }`}
            >
              {i}
            </button>
          ))}
        </div>

        <button
          onClick={() => setStep(Math.min(4, step + 1))}
          disabled={step === 4}
          className="flex items-center gap-1 px-3 py-2 rounded-lg bg-blue-600 text-white text-sm disabled:opacity-30 hover:bg-blue-500 transition-all"
        >
          Next <ChevronRight size={16} />
        </button>
      </div>

      {/* Current step info */}
      <div className="text-center">
        <div className="text-lg font-semibold text-white">{STEPS[step].title}</div>
        <div className="text-sm text-slate-400">{STEPS[step].desc}</div>
      </div>

      {/* Visualization */}
      <div className="bg-slate-800/50 rounded-xl border border-slate-700 p-4">
        <svg viewBox="0 0 720 200" className="w-full h-48">
          {/* Graphlet boundaries */}
          {step >= 3 && GRAPHLET_DEFS.map((gl, gi) => {
            const nodePositions = gl.nodes.map((nid) => positions[nid]).filter(Boolean);
            if (nodePositions.length === 0) return null;
            const minX = Math.min(...nodePositions.map(p => p[0])) - 25;
            const maxX = Math.max(...nodePositions.map(p => p[0])) + 25;
            const minY = Math.min(...nodePositions.map(p => p[1])) - 25;
            const maxY = Math.max(...nodePositions.map(p => p[1])) + 25;
            return (
              <g key={gl.id}>
                <rect
                  x={minX} y={minY}
                  width={maxX - minX} height={maxY - minY}
                  rx={12} fill={gl.color + "15"} stroke={gl.color + "40"}
                  strokeWidth={2} strokeDasharray="5,5"
                />
                <text x={minX + 5} y={maxY + 15} fill={gl.color} fontSize={11} fontWeight="bold" fontFamily="monospace">
                  {gl.id}
                </text>
              </g>
            );
          })}

          {/* Nodes */}
          {PERSON_NODES.map((node) => {
            const p = positions[node.id];
            if (!p) return null;
            const color = nodeColor(node.id);
            return (
              <g key={node.id}>
                <circle
                  cx={p[0]} cy={p[1]} r={16}
                  fill={color + "30"} stroke={color} strokeWidth={2}
                />
                <text x={p[0]} y={p[1] + 1} textAnchor="middle" dominantBaseline="central"
                  fill="white" fontSize={9} fontWeight="bold" fontFamily="monospace">
                  {node.id}
                </text>
              </g>
            );
          })}

          {/* Schema labels on step 1 */}
          {step === 1 && PERSON_NODES.map((node) => {
            const p = positions[node.id];
            if (!p) return null;
            const attrs = Object.keys(node.attrs).sort();
            return (
              <text key={`lbl-${node.id}`} x={p[0]} y={p[1] + 28} textAnchor="middle"
                fill="#94A3B8" fontSize={7} fontFamily="monospace">
                {"{" + attrs.join(",") + "}"}
              </text>
            );
          })}
        </svg>
      </div>

      {/* Cost function parameters */}
      {step >= 3 && (
        <div className="bg-slate-800/30 rounded-xl border border-slate-700/50 p-4 space-y-3">
          <div className="text-sm font-semibold text-slate-300 flex items-center gap-2">
            <GitBranch size={14} className="text-blue-400" />
            Cost Function: c(H) = C_sch·|H| + C_null·ΣΓ(gl) + C_vec·ΣΨ(|gl|)
          </div>
          <div className="grid grid-cols-3 gap-4">
            <div className="space-y-1">
              <label className="text-xs text-slate-400 flex justify-between">
                <span>C_sch (schema cost)</span>
                <span className="text-blue-400 font-mono">{cSch}</span>
              </label>
              <input
                type="range" min={1} max={500} value={cSch}
                onChange={(e) => setCSch(+e.target.value)}
                className="w-full accent-blue-500"
              />
              <div className="text-xs text-slate-500">Higher → fewer, coarser graphlets</div>
            </div>
            <div className="space-y-1">
              <label className="text-xs text-slate-400 flex justify-between">
                <span>C_null (null penalty)</span>
                <span className="text-amber-400 font-mono">{cNull}</span>
              </label>
              <input
                type="range" min={0} max={100} value={cNull * 100}
                onChange={(e) => setCNull(+e.target.value / 100)}
                className="w-full accent-amber-500"
              />
              <div className="text-xs text-slate-500">Higher → fewer NULLs (more graphlets)</div>
            </div>
            <div className="space-y-1">
              <label className="text-xs text-slate-400 flex justify-between">
                <span>C_vec (vectorization bonus)</span>
                <span className="text-emerald-400 font-mono">{cVec}</span>
              </label>
              <input
                type="range" min={100} max={50000} value={cVec}
                onChange={(e) => setCVec(+e.target.value)}
                className="w-full accent-emerald-500"
              />
              <div className="text-xs text-slate-500">Higher → larger graphlets for SIMD</div>
            </div>
          </div>
        </div>
      )}

      {/* Aha moments per step */}
      {step === 4 && (
        <div className="bg-gradient-to-r from-blue-500/10 to-purple-500/10 border border-blue-500/30 rounded-xl p-4 flex items-start gap-3">
          <CheckCircle className="text-blue-400 mt-0.5 flex-shrink-0" size={20} />
          <div>
            <div className="text-blue-300 font-semibold text-sm">CGC: The Sweet Spot Between Extremes</div>
            <div className="text-slate-400 text-xs mt-1">
              Merging all into one table (MA) eliminates schema overhead but creates <span className="text-red-400 font-bold">212B NULLs</span> on DBpedia.
              Separating all (SA) removes NULLs but creates <span className="text-red-400 font-bold">282K tiny tables</span>.
              CGC finds the <span className="text-blue-400 font-bold">cost-optimal balance</span>, achieving
              up to <span className="text-emerald-400 font-bold">5,319×</span> speedup on scan+selection and
              <span className="text-emerald-400 font-bold">28×</span> on scans vs. the SA baseline.
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════
// SECTION 3: QUERY PROCESSING (SSRF)
// ═══════════════════════════════════════════

function QueryProcessing() {
  const [qStep, setQStep] = useState(0);
  const [showBloating, setShowBloating] = useState(false);
  const [hopCount, setHopCount] = useState(2);

  const QUERY_STEPS = [
    {
      title: "Cypher Query Arrives",
      code: `MATCH (n:Person)-[:livesIn]->(m:City)
WHERE m.area > 100
  AND n.FN =~ 'Fran.*'
RETURN n, m;`,
      desc: "A graph pattern matching query with filters on both Person and City attributes.",
    },
    {
      title: "Step 1: Schema Index Lookup",
      desc: "The Schema Index (SI) identifies which graphlets contain the required attributes (FN for Person, area for City). Instead of scanning ALL graphlets, only relevant ones are accessed.",
      detail: "SI(Person, FN) → {gl₁, gl₂}  |  SI(City, area) → {gl₁₁, gl₁₂}"
    },
    {
      title: "Step 2: Graphlet Early Merge (GEM)",
      desc: "GEM merges graphlets into virtual graphlets to reduce the plan search space. Without GEM, each graphlet combination generates separate join plans, causing exponential plan explosion.",
      detail: "Virtual graphlet: gl₁₁ ⊎ gl₁₂ → gl' (merged City graphlet)"
    },
    {
      title: "Step 3: Execute with SSRF",
      desc: "The Shared Schema Row Format (SSRF) handles heterogeneous schemas during joins without materializing all schema combinations. Schema info is stored separately from tuple data.",
      detail: "SSRF separates schema definitions from data → no schema bloating!"
    },
  ];

  // Schema bloating calculation
  const personSchemas = 3; // Number of Person graphlet schemas
  const citySchemas = 4; // Number of City graphlet schemas
  const traditionalCombinations = Math.pow(personSchemas * citySchemas, hopCount > 1 ? hopCount - 1 : 1);
  const ssrfCombinations = personSchemas + citySchemas;

  return (
    <div className="space-y-6">
      <div className="text-center space-y-2">
        <h2 className="text-2xl font-bold text-white">Schemaless Query Processing</h2>
        <p className="text-slate-400 max-w-2xl mx-auto text-sm">
          TurboLynx introduces Schema Index, GEM, and SSRF to handle queries efficiently
          over heterogeneous graphlet schemas.
        </p>
      </div>

      {/* Query step navigation */}
      <div className="flex items-center justify-center gap-2">
        {QUERY_STEPS.map((qs, i) => (
          <button
            key={i}
            onClick={() => setQStep(i)}
            className={`px-3 py-2 rounded-lg text-xs font-medium transition-all ${
              i === qStep
                ? "bg-indigo-600 text-white shadow-lg"
                : i < qStep
                ? "bg-indigo-600/20 text-indigo-300"
                : "bg-slate-700 text-slate-500"
            }`}
          >
            {i === 0 ? "Query" : `Step ${i}`}
          </button>
        ))}
      </div>

      {/* Current step content */}
      <div className="bg-slate-800/50 rounded-xl border border-slate-700 p-5 space-y-4">
        <h3 className="text-lg font-semibold text-white">{QUERY_STEPS[qStep].title}</h3>

        {qStep === 0 && (
          <pre className="bg-slate-900 rounded-lg p-4 text-sm font-mono overflow-x-auto border border-slate-700">
            <span className="text-purple-400">MATCH</span>{" "}
            <span className="text-blue-300">(n:Person)</span>
            <span className="text-slate-400">-[</span>
            <span className="text-amber-300">:livesIn</span>
            <span className="text-slate-400">]-&gt;</span>
            <span className="text-emerald-300">(m:City)</span>{"\n"}
            <span className="text-purple-400">WHERE</span>{" "}
            <span className="text-emerald-300">m</span>.area &gt; 100{"\n"}
            {"  "}<span className="text-purple-400">AND</span>{" "}
            <span className="text-blue-300">n</span>.FN =~ <span className="text-amber-300">'Fran.*'</span>{"\n"}
            <span className="text-purple-400">RETURN</span> n, m;
          </pre>
        )}

        <p className="text-slate-400 text-sm">{QUERY_STEPS[qStep].desc}</p>

        {QUERY_STEPS[qStep].detail && (
          <div className="bg-slate-900/50 rounded-lg p-3 border border-slate-700">
            <code className="text-xs font-mono text-emerald-300">{QUERY_STEPS[qStep].detail}</code>
          </div>
        )}

        {/* Schema Index visualization for step 1 */}
        {qStep === 1 && (
          <div className="grid grid-cols-2 gap-4 mt-4">
            <div className="bg-slate-900/50 rounded-lg p-3 border border-blue-500/30">
              <div className="text-xs font-mono text-blue-400 mb-2">Schema Index (Person)</div>
              <div className="space-y-1 text-xs font-mono">
                <div className="flex items-center gap-2">
                  <span className="text-slate-500">age_uint</span> <span className="text-slate-600">→</span>
                  <span className="text-slate-400">gl₁, gl₃</span>
                </div>
                <div className="flex items-center gap-2 bg-blue-500/10 px-2 py-1 rounded">
                  <span className="text-blue-300 font-bold">FN_str</span> <span className="text-slate-600">→</span>
                  <span className="text-blue-300 font-bold">gl₁, gl₂ ✓</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-slate-500">LN_str</span> <span className="text-slate-600">→</span>
                  <span className="text-slate-400">gl₁, gl₂</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-slate-500">gender_str</span> <span className="text-slate-600">→</span>
                  <span className="text-slate-400">gl₃</span>
                </div>
              </div>
            </div>
            <div className="bg-slate-900/50 rounded-lg p-3 border border-emerald-500/30">
              <div className="text-xs font-mono text-emerald-400 mb-2">Schema Index (City)</div>
              <div className="space-y-1 text-xs font-mono">
                <div className="flex items-center gap-2">
                  <span className="text-slate-500">name_str</span> <span className="text-slate-600">→</span>
                  <span className="text-slate-400">gl₁₁, gl₁₂</span>
                </div>
                <div className="flex items-center gap-2 bg-emerald-500/10 px-2 py-1 rounded">
                  <span className="text-emerald-300 font-bold">area_double</span> <span className="text-slate-600">→</span>
                  <span className="text-emerald-300 font-bold">gl₁₁, gl₁₂ ✓</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-slate-500">pop_uint</span> <span className="text-slate-600">→</span>
                  <span className="text-slate-400">gl₁₁</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-slate-500">url_str</span> <span className="text-slate-600">→</span>
                  <span className="text-slate-400">gl₁₂</span>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Schema Bloating Interactive Demo */}
      <div className="bg-slate-800/50 rounded-xl border border-slate-700 p-5 space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-white flex items-center gap-2">
            <AlertTriangle size={18} className="text-amber-400" />
            Schema Bloating Problem vs SSRF
          </h3>
          <button
            onClick={() => setShowBloating(!showBloating)}
            className="text-xs px-3 py-1.5 rounded-lg bg-amber-500/20 text-amber-300 border border-amber-500/30 hover:bg-amber-500/30 transition-all"
          >
            {showBloating ? "Hide Details" : "Show Problem"}
          </button>
        </div>

        {showBloating && (
          <div className="space-y-4">
            <div className="flex items-center gap-4 justify-center">
              <label className="text-sm text-slate-400">Number of hops:</label>
              <div className="flex gap-2">
                {[1, 2, 3, 4, 5].map((h) => (
                  <button
                    key={h}
                    onClick={() => setHopCount(h)}
                    className={`w-8 h-8 rounded-lg text-sm font-bold transition-all ${
                      h === hopCount
                        ? "bg-amber-500 text-white"
                        : "bg-slate-700 text-slate-400 hover:bg-slate-600"
                    }`}
                  >
                    {h}
                  </button>
                ))}
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              {/* Traditional */}
              <div className="bg-red-500/5 rounded-lg p-4 border border-red-500/20">
                <div className="text-sm font-semibold text-red-400 mb-2">Traditional Approach</div>
                <div className="text-xs text-slate-400 mb-3">
                  Full materialization of all schema combinations
                </div>
                <div className="text-center">
                  <div className="text-3xl font-bold text-red-400 font-mono">
                    {traditionalCombinations.toLocaleString()}
                  </div>
                  <div className="text-xs text-slate-500 mt-1">intermediate schema combinations</div>
                </div>
                <div className="text-xs text-slate-500 mt-2 font-mono text-center">
                  ({personSchemas} × {citySchemas}){hopCount > 1 ? `^${hopCount - 1}` : ""} = {traditionalCombinations}
                </div>
              </div>

              {/* SSRF */}
              <div className="bg-emerald-500/5 rounded-lg p-4 border border-emerald-500/20">
                <div className="text-sm font-semibold text-emerald-400 mb-2">SSRF (TurboLynx)</div>
                <div className="text-xs text-slate-400 mb-3">
                  Schema info stored separately; no combinatorial explosion
                </div>
                <div className="text-center">
                  <div className="text-3xl font-bold text-emerald-400 font-mono">
                    {ssrfCombinations}
                  </div>
                  <div className="text-xs text-slate-500 mt-1">schema lists maintained separately</div>
                </div>
                <div className="text-xs text-slate-500 mt-2 font-mono text-center">
                  {personSchemas} + {citySchemas} = {ssrfCombinations} (constant!)
                </div>
              </div>
            </div>

            {traditionalCombinations > 20 && (
              <div className="bg-gradient-to-r from-amber-500/10 to-red-500/10 border border-amber-500/30 rounded-lg p-3 text-center">
                <span className="text-amber-300 text-sm font-semibold">
                  {hopCount}-hop query: Traditional creates {(traditionalCombinations / ssrfCombinations).toFixed(0)}× more schema combinations!
                </span>
              </div>
            )}
          </div>
        )}

        {/* SSRF Architecture */}
        <div className="bg-slate-900/50 rounded-lg p-4 border border-slate-700">
          <div className="text-xs font-mono text-slate-400 mb-3">// SSRF: Shared Schema Row Format</div>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <div className="text-xs font-semibold text-blue-400">TupleStore (row data)</div>
              <div className="bg-slate-800 rounded p-2 text-xs font-mono space-y-1">
                <div className="flex items-center gap-1">
                  <span className="text-emerald-300">c₁</span>
                  <span className="text-emerald-300">c₂</span>
                  <span className="text-slate-600">|</span>
                  <span className="text-amber-300">c₃</span>
                  <span className="text-amber-300">c₄</span>
                  <span className="text-slate-500 ml-2">← only non-sparse data</span>
                </div>
              </div>
            </div>
            <div className="space-y-2">
              <div className="text-xs font-semibold text-purple-400">Schema Infos (shared)</div>
              <div className="bg-slate-800 rounded p-2 text-xs font-mono space-y-1">
                <div><span className="text-purple-300">sch1:</span> total_size=32, offsets=[0,8,16,24]</div>
                <div><span className="text-purple-300">sch2:</span> total_size=8, offsets=[-1,0,-1,-1]</div>
              </div>
            </div>
          </div>
          <div className="text-xs text-slate-500 mt-2 text-center">
            -1 in offset_infos indicates NULL → no storage waste for sparse columns
          </div>
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════
// SECTION 4: PERFORMANCE
// ═══════════════════════════════════════════

function PerformanceSection() {
  const [benchmark, setBenchmark] = useState("ldbc");

  const benchData = {
    ldbc: {
      name: "LDBC SNB Interactive (SF100)",
      data: LDBC_DATA,
      maxSpeedup: "183.9×",
      avgSpeedup: "29.78×",
      desc: "Social network benchmark with 14 complex multi-step graph pattern matching queries"
    },
    tpch: {
      name: "TPC-H (SF100)",
      data: TPCH_DATA,
      maxSpeedup: "18.88×",
      avgSpeedup: "3.15×",
      desc: "Decision support benchmark converted to graph workloads (22 queries)"
    },
    dbpedia: {
      name: "DBpedia (282K unique schemas)",
      data: DBPEDIA_DATA,
      maxSpeedup: "86.14×",
      avgSpeedup: "27.37×",
      desc: "Real-world knowledge graph with 2,796 unique schemas — the ultimate schemaless test"
    },
  };

  const current = benchData[benchmark];
  const chartData = current.data.map((d) => ({
    ...d,
    name: d.system,
    value: d.speedup,
    fill: d.color,
  }));

  const CustomTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const d = payload[0].payload;
      return (
        <div className="bg-slate-800 border border-slate-600 rounded-lg p-3 shadow-xl">
          <div className="font-bold text-white text-sm">{d.system}</div>
          <div className="text-xs text-slate-400">{d.type}</div>
          <div className="text-lg font-bold mt-1" style={{ color: d.color }}>
            {d.speedup}× slower
          </div>
          <div className="text-xs text-slate-500">than TurboLynx</div>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="space-y-6">
      <div className="text-center space-y-2">
        <h2 className="text-2xl font-bold text-white">Performance Results</h2>
        <p className="text-slate-400 max-w-2xl mx-auto text-sm">
          TurboLynx outperforms 5 GDBMSes and 2 RDBMSes across three diverse benchmarks.
          All values show <span className="text-emerald-400">competitor / TurboLynx</span> execution time ratio (higher = TurboLynx wins more).
        </p>
      </div>

      {/* Benchmark selector */}
      <div className="flex justify-center">
        <div className="flex bg-slate-800 rounded-xl p-1 border border-slate-700 gap-1">
          {[
            { key: "ldbc", label: "LDBC SNB", emoji: "🌐" },
            { key: "tpch", label: "TPC-H", emoji: "📊" },
            { key: "dbpedia", label: "DBpedia", emoji: "🧠" },
          ].map((b) => (
            <button
              key={b.key}
              onClick={() => setBenchmark(b.key)}
              className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all ${
                benchmark === b.key
                  ? "bg-gradient-to-r from-blue-600 to-indigo-600 text-white shadow-lg"
                  : "text-slate-400 hover:text-white hover:bg-slate-700/50"
              }`}
            >
              <span>{b.emoji}</span> {b.label}
            </button>
          ))}
        </div>
      </div>

      {/* Headline stats */}
      <div className="flex justify-center gap-6">
        <div className="px-5 py-3 rounded-xl border bg-emerald-500/10 border-emerald-500/30 text-center">
          <div className="text-xs text-slate-400 mb-1">Max Speedup</div>
          <div className="text-2xl font-bold text-emerald-400 font-mono">{current.maxSpeedup}</div>
        </div>
        <div className="px-5 py-3 rounded-xl border bg-blue-500/10 border-blue-500/30 text-center">
          <div className="text-xs text-slate-400 mb-1">Avg Speedup</div>
          <div className="text-2xl font-bold text-blue-400 font-mono">{current.avgSpeedup}</div>
        </div>
        <div className="px-5 py-3 rounded-xl border bg-slate-800/50 border-slate-700 text-center">
          <div className="text-xs text-slate-400 mb-1">Systems Compared</div>
          <div className="text-2xl font-bold text-slate-300 font-mono">{current.data.length}</div>
        </div>
      </div>

      {/* Benchmark description */}
      <div className="text-center text-sm text-slate-500">{current.desc}</div>

      {/* Bar Chart */}
      <div className="bg-slate-800/50 rounded-xl border border-slate-700 p-4">
        <div className="text-xs text-slate-500 mb-2 text-center font-mono">
          Speedup over TurboLynx (×) — higher bars = TurboLynx wins by more
        </div>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={chartData} margin={{ top: 20, right: 20, bottom: 5, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis
              dataKey="name"
              tick={{ fill: "#94A3B8", fontSize: 12 }}
              axisLine={{ stroke: "#475569" }}
            />
            <YAxis
              tick={{ fill: "#94A3B8", fontSize: 11 }}
              axisLine={{ stroke: "#475569" }}
              label={{
                value: "Speedup (×)",
                angle: -90,
                position: "insideLeft",
                fill: "#64748B",
                fontSize: 12,
              }}
            />
            <Tooltip content={<CustomTooltip />} cursor={{ fill: "rgba(255,255,255,0.05)" }} />
            <Bar dataKey="value" radius={[6, 6, 0, 0]} maxBarSize={60}>
              {chartData.map((entry, i) => (
                <Cell key={i} fill={entry.color} fillOpacity={0.8} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        {/* TurboLynx baseline annotation */}
        <div className="text-center mt-2">
          <span className="inline-flex items-center gap-2 bg-blue-500/10 border border-blue-500/30 rounded-full px-4 py-1.5 text-xs text-blue-300">
            <Zap size={12} /> TurboLynx = 1.0× (baseline) — all bars show how much slower competitors are
          </span>
        </div>
      </div>

      {/* Key insight cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        <div className="bg-gradient-to-br from-blue-500/10 to-indigo-500/10 rounded-xl border border-blue-500/20 p-4">
          <div className="text-blue-400 font-bold text-sm mb-1">Storage Layer</div>
          <div className="text-xs text-slate-400">
            Columnar graphlets via CGC eliminate per-tuple schema interpretation,
            enabling up to <span className="text-emerald-400 font-bold">5,319×</span> faster scans with selection vs. separating all schemas.
          </div>
        </div>
        <div className="bg-gradient-to-br from-purple-500/10 to-pink-500/10 rounded-xl border border-purple-500/20 p-4">
          <div className="text-purple-400 font-bold text-sm mb-1">Query Processing</div>
          <div className="text-xs text-slate-400">
            SSRF reduces join traversal cost by up to <span className="text-emerald-400 font-bold">2.1×</span> with
            no penalty on aggregations. Schema bloating is eliminated entirely.
          </div>
        </div>
        <div className="bg-gradient-to-br from-amber-500/10 to-orange-500/10 rounded-xl border border-amber-500/20 p-4">
          <div className="text-amber-400 font-bold text-sm mb-1">Query Optimization</div>
          <div className="text-xs text-slate-400">
            GEM reduces compilation overhead by <span className="text-emerald-400 font-bold">6.2%</span> and
            query execution time by <span className="text-emerald-400 font-bold">26.7%</span> through
            early graphlet merging.
          </div>
        </div>
      </div>

      {/* Architecture summary */}
      <div className="bg-slate-800/30 rounded-xl border border-slate-700/50 p-4">
        <div className="text-xs text-center text-slate-500">
          <strong className="text-slate-300">Built on:</strong>{" "}
          DuckDB (expression evaluator, Vector, DataChunk) + Orca (query optimizer) +
          <span className="text-blue-400"> 57K LOC</span> of native graph-centric storage,
          graph-aware operators, and retrofitted optimizer rules.
          Total: ~246K LOC. Open source at{" "}
          <span className="text-blue-400 underline">github.com/turbolynx-dslab/TurboLynx</span>
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════
// MAIN APP
// ═══════════════════════════════════════════

export default function TurboLynxDemo() {
  const [activeTab, setActiveTab] = useState(0);

  const sections = [
    <SchemalessChallenge key={0} />,
    <GraphletChunking key={1} />,
    <QueryProcessing key={2} />,
    <PerformanceSection key={3} />,
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950 text-white">
      {/* Header */}
      <div className="border-b border-slate-800 bg-slate-900/80 backdrop-blur-sm sticky top-0 z-50">
        <div className="max-w-5xl mx-auto px-4 py-3">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-3">
              <div className="flex items-center justify-center w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 shadow-lg shadow-blue-500/25">
                <Database size={20} className="text-white" />
              </div>
              <div>
                <h1 className="text-lg font-bold bg-gradient-to-r from-blue-400 to-indigo-400 bg-clip-text text-transparent">
                  TurboLynx
                </h1>
                <p className="text-xs text-slate-500 -mt-0.5">
                  Schemaless Graph Engine for General-Purpose Analytics
                </p>
              </div>
            </div>
            <div className="hidden md:flex items-center gap-2 text-xs text-slate-500">
              <span className="px-2 py-1 rounded bg-slate-800 border border-slate-700">VLDB 2026</span>
              <span className="px-2 py-1 rounded bg-slate-800 border border-slate-700">PVLDB Vol.19 No.6</span>
            </div>
          </div>
          <TabBar active={activeTab} onChange={setActiveTab} />
        </div>
      </div>

      {/* Content */}
      <div className="max-w-5xl mx-auto px-4 py-6">
        {sections[activeTab]}
      </div>

      {/* Footer */}
      <div className="border-t border-slate-800 mt-8 py-4 text-center text-xs text-slate-600">
        POSTECH DB Lab — T. Lee, J. Ha, B. Tak, W.-S. Han —
        <span className="text-blue-500"> github.com/turbolynx-dslab/TurboLynx</span>
      </div>
    </div>
  );
}