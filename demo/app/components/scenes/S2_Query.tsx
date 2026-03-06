"use client";
import { motion, AnimatePresence } from "framer-motion";
import { HERO_RESULTS } from "@/lib/demo-data";

interface Props { step: number; onStep: (n: number) => void; }

const CYPHER = `MATCH (p)-[:birthPlace]->(c),
      (p)-[:rdf:type]->(t)
WHERE p.position = 'Goalkeeper'
  AND c.populationTotal > 1000000
RETURN p.name      AS name,
       p.birthDate AS born,
       c.name      AS city,
       c.population AS population,
       t.type       AS entity_type`;

// 34 partition dots
const PARTITIONS = Array.from({ length: 34 }, (_, i) => ({
  id: i + 1,
  lit: i === 6 || i === 11, // partition #7 and #12 (0-indexed: 6 and 11)
  color: i === 6 ? "#3B82F6" : i === 11 ? "#F59E0B" : "#27272a",
}));

const COLS = ["name", "born", "city", "population", "entity_type"] as const;

function PartitionDots() {
  const cols = 9;
  return (
    <div>
      <svg viewBox="0 0 720 160" style={{ width: "100%", height: 160 }}>
        {PARTITIONS.map((p, i) => {
          const col = i % cols;
          const row = Math.floor(i / cols);
          const cx = 50 + col * 72;
          const cy = 30 + row * 52;
          return (
            <g key={p.id}>
              <circle cx={cx} cy={cy} r={p.lit ? 18 : 14}
                fill={p.lit ? p.color + "30" : "#1a1a1e"}
                stroke={p.color}
                strokeWidth={p.lit ? 2 : 1}
                opacity={p.lit ? 1 : 0.5} />
              <text x={cx} y={cy + 1} textAnchor="middle" dominantBaseline="middle"
                fontSize={9} fill={p.lit ? "#f4f4f5" : "#52525b"} fontFamily="monospace">
                #{p.id}
              </text>
              {p.lit && (
                <text x={cx} y={cy + 28} textAnchor="middle" fontSize={8} fill={p.color} fontFamily="monospace">
                  {p.id === 7 ? "position" : "popTotal"}
                </text>
              )}
            </g>
          );
        })}
      </svg>
      <div style={{ display: "flex", gap: 20, fontSize: 12, fontFamily: "monospace", marginTop: 8 }}>
        <span style={{ color: "#3B82F6" }}>■ #7 — Goalkeeper-compatible</span>
        <span style={{ color: "#F59E0B" }}>■ #12 — City-compatible</span>
        <span style={{ color: "#52525b" }}>■ 32 partitions skipped</span>
      </div>
      <div style={{ marginTop: 10, fontSize: 12, color: "#a1a1aa", fontFamily: "monospace" }}>
        2/34 partitions scanned · <span style={{ color: "#10B981" }}>212B null-ops skipped</span>
      </div>
    </div>
  );
}

function QueryPlanTree() {
  return (
    <svg viewBox="0 0 680 200" style={{ width: "100%", height: 200 }}>
      {/* Lines */}
      <line x1={340} y1={40} x2={200} y2={88} stroke="#27272a" strokeWidth={1.5} />
      <line x1={340} y1={40} x2={480} y2={88} stroke="#27272a" strokeWidth={1.5} />
      <line x1={200} y1={108} x2={140} y2={148} stroke="#27272a" strokeWidth={1.5} />
      <line x1={200} y1={108} x2={260} y2={148} stroke="#27272a" strokeWidth={1.5} />
      <line x1={480} y1={108} x2={420} y2={148} stroke="#27272a" strokeWidth={1.5} />
      <line x1={480} y1={108} x2={540} y2={148} stroke="#27272a" strokeWidth={1.5} />

      {/* Root */}
      <rect x={250} y={16} width={180} height={28} rx={6} fill="#3B82F620" stroke="#3B82F6" strokeWidth={1.5} />
      <text x={340} y={34} textAnchor="middle" fontSize={11} fill="#93c5fd" fontFamily="monospace">HashJoin [:birthPlace]</text>

      {/* Level 1 */}
      <rect x={110} y={84} width={180} height={26} rx={6} fill="#8B5CF620" stroke="#8B5CF6" strokeWidth={1.5} />
      <text x={200} y={101} textAnchor="middle" fontSize={11} fill="#c4b5fd" fontFamily="monospace">UnionAll (partition #7)</text>

      <rect x={390} y={84} width={180} height={26} rx={6} fill="#F59E0B20" stroke="#F59E0B" strokeWidth={1.5} />
      <text x={480} y={101} textAnchor="middle" fontSize={11} fill="#fcd34d" fontFamily="monospace">UnionAll (partition #12)</text>

      {/* Level 2 */}
      {[
        { x: 100, label: "gl_p1", color: "#8B5CF6" },
        { x: 220, label: "gl_p2", color: "#8B5CF6" },
        { x: 370, label: "gl_c1", color: "#F59E0B" },
        { x: 490, label: "gl_c2", color: "#F59E0B" },
      ].map(item => (
        <g key={item.label}>
          <rect x={item.x} y={138} width={70} height={24} rx={5}
            fill={item.color + "15"} stroke={item.color + "80"} strokeWidth={1} />
          <text x={item.x + 35} y={153} textAnchor="middle" fontSize={10} fill={item.color} fontFamily="monospace">{item.label}</text>
        </g>
      ))}

      <text x={340} y={195} textAnchor="middle" fontSize={10} fill="#52525b" fontFamily="monospace">
        TurboLynx: scans 2 partitions only · vs. 77M nodes full scan (Neo4j)
      </text>
    </svg>
  );
}

function ResultsTable() {
  return (
    <div>
      <div style={{ overflowX: "auto" }}>
        <table style={{ fontSize: 12, fontFamily: "monospace", borderCollapse: "collapse", width: "100%" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #27272a" }}>
              {COLS.map(c => (
                <th key={c} style={{ padding: "6px 12px", color: "#71717a", textAlign: "left", fontWeight: 500 }}>{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {HERO_RESULTS.map((row, i) => (
              <motion.tr key={i}
                initial={{ opacity: 0, y: 4 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: i * 0.07 }}
                style={{ borderBottom: "1px solid #18181b" }}>
                <td style={{ padding: "5px 12px", color: "#f4f4f5" }}>{row.name}</td>
                <td style={{ padding: "5px 12px", color: "#a1a1aa" }}>{row.born}</td>
                <td style={{ padding: "5px 12px", color: "#a1a1aa" }}>{row.city}</td>
                <td style={{ padding: "5px 12px", color: "#a1a1aa" }}>{row.pop}</td>
                <td style={{ padding: "5px 12px", color: "#71717a" }}>Person/Athlete</td>
              </motion.tr>
            ))}
          </tbody>
        </table>
      </div>
      <div style={{ marginTop: 10, display: "flex", alignItems: "center", gap: 12 }}>
        <span style={{ fontSize: 11, color: "#71717a" }}>5 rows · DBpedia Hero Query</span>
        <span style={{ fontSize: 12, fontFamily: "monospace", color: "#10B981", background: "#10B98115", padding: "2px 8px", borderRadius: 6 }}>
          ⚡ 14ms
        </span>
      </div>
    </div>
  );
}

const STEP_TITLES = [
  "The Query",
  "CGC Schema Index Lookup",
  "UnionAll Query Plan",
  "Results",
];

const AHA = "The CGC schema index answered 'which partitions contain position and populationTotal?' in microseconds. 32 of 34 partitions were never touched.";

export default function S2_Query({ step }: Props) {
  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", padding: "32px 48px", gap: 20, overflow: "hidden" }}>
      <AnimatePresence mode="wait">
        <motion.div key={step} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
          transition={{ duration: 0.25 }}>
          <div style={{ fontSize: 11, color: "#3B82F6", fontFamily: "monospace", marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.08em" }}>
            Query — How a Query Runs
          </div>
          <h2 style={{ fontSize: 22, fontWeight: 700, color: "#f4f4f5", margin: 0 }}>{STEP_TITLES[step]}</h2>
        </motion.div>
      </AnimatePresence>

      <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 16, overflow: "auto" }}>
        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            transition={{ duration: 0.25 }}>
            {step === 0 && (
              <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                <pre style={{
                  background: "#131316", border: "1px solid #27272a", borderRadius: 10,
                  padding: "16px 20px", fontSize: 13, color: "#f4f4f5", fontFamily: "monospace",
                  lineHeight: 1.7, margin: 0, overflowX: "auto",
                }}>{CYPHER}</pre>
                <div style={{ fontSize: 13, color: "#71717a" }}>
                  Find soccer players with <span style={{ color: "#f4f4f5" }}>position = Goalkeeper</span> born in cities
                  with <span style={{ color: "#f4f4f5" }}>population &gt; 1M</span> — from DBpedia (77M nodes).
                </div>
              </div>
            )}
            {step === 1 && <PartitionDots />}
            {step === 2 && <QueryPlanTree />}
            {step === 3 && <ResultsTable />}
          </motion.div>
        </AnimatePresence>

        <AnimatePresence>
          {step === 3 && (
            <motion.div initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
              transition={{ delay: 0.4 }}
              style={{ border: "1px solid #3B82F630", background: "#3B82F610", borderRadius: 10, padding: "12px 16px", fontSize: 13, color: "#93c5fd", lineHeight: 1.6, flexShrink: 0 }}>
              <span style={{ color: "#60a5fa", fontWeight: 600 }}>✓ Key Insight: </span>{AHA}
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
