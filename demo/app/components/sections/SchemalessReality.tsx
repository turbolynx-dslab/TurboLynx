"use client";
import { useState } from "react";
import { motion } from "framer-motion";
import { useInView } from "framer-motion";
import { useRef } from "react";
import SectionLabel from "@/components/ui/SectionLabel";
import { NODE_SAMPLES, ATTR_COLORS, DBPEDIA_STATS } from "@/lib/data";

function NodeCard({ node, index }: { node: typeof NODE_SAMPLES[0]; index: number }) {
  const [expanded, setExpanded] = useState(false);
  const entries = Object.entries(node.attrs);
  const preview = entries.slice(0, 3);
  const rest = entries.slice(3);

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.04 }}
      onClick={() => setExpanded(e => !e)}
      className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-lg p-3 cursor-pointer hover:border-[var(--accent)] transition-all"
      style={{ minWidth: 0 }}
    >
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-mono text-[var(--text-secondary)]">{node.id}</span>
        <span className="text-xs px-2 py-0.5 rounded-full bg-[var(--bg-elevated)] text-[var(--text-secondary)] border border-[var(--border)]">
          {node.type}
        </span>
      </div>
      <div className="space-y-1">
        {(expanded ? entries : preview).map(([k, v]) => (
          <div key={k} className="flex items-start gap-1.5 text-xs">
            <span className="font-medium shrink-0" style={{ color: ATTR_COLORS[k] ?? "#a1a1aa" }}>{k}</span>
            <span className="text-[var(--text-secondary)] truncate">{v}</span>
          </div>
        ))}
        {!expanded && rest.length > 0 && (
          <div className="text-xs text-[var(--text-secondary)] opacity-60">+{rest.length} more...</div>
        )}
      </div>
      <div className="mt-2 flex flex-wrap gap-1">
        {(expanded ? entries : preview).map(([k]) => (
          <span key={k} className="w-2 h-2 rounded-full inline-block" style={{ background: ATTR_COLORS[k] ?? "#a1a1aa" }} />
        ))}
      </div>
    </motion.div>
  );
}

const ATTR_FREQ = [
  { key: "wikiPageID", pct: 98 }, { key: "name", pct: 71 }, { key: "abstract", pct: 48 },
  { key: "birthDate", pct: 12 }, { key: "country", pct: 9 }, { key: "genre", pct: 6 },
  { key: "birthPlace", pct: 5 }, { key: "director", pct: 3 }, { key: "instrument", pct: 2 },
  { key: "budget", pct: 1.5 }, { key: "population", pct: 1.2 }, { key: "occupation", pct: 0.8 },
];

export default function SchemalessReality() {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true, margin: "-100px" });

  return (
    <section id="schemaless" ref={ref} className="py-24 px-6 max-w-7xl mx-auto">
      <SectionLabel number="01" label="The Schemaless Reality" />

      <div className="grid lg:grid-cols-2 gap-16">
        {/* Left: Node cards */}
        <div>
          <motion.h2
            initial={{ opacity: 0, y: 16 }}
            animate={inView ? { opacity: 1, y: 0 } : {}}
            transition={{ duration: 0.5 }}
            className="text-3xl md:text-4xl font-bold text-[var(--text-primary)] mb-3"
          >
            {DBPEDIA_STATS.uniqueSchemas.toLocaleString()} unique attribute sets.
          </motion.h2>
          <motion.p
            initial={{ opacity: 0 }}
            animate={inView ? { opacity: 1 } : {}}
            transition={{ delay: 0.2 }}
            className="text-[var(--text-secondary)] mb-8 text-lg"
          >
            Every node in DBpedia can have a completely different set of attributes.
            Hover any card to see the full schema.
          </motion.p>

          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            {NODE_SAMPLES.map((n, i) => <NodeCard key={n.id} node={n} index={i} />)}
          </div>
        </div>

        {/* Right: Histogram */}
        <div>
          <motion.h3
            initial={{ opacity: 0, y: 16 }}
            animate={inView ? { opacity: 1, y: 0 } : {}}
            transition={{ delay: 0.1 }}
            className="text-xl font-bold text-[var(--text-primary)] mb-2"
          >
            Attribute distribution — extreme sparsity
          </motion.h3>
          <p className="text-[var(--text-secondary)] text-sm mb-6">
            If you use one big table: 90%+ of cells are NULL.
            Neo4j handles{" "}
            <span className="text-[var(--accent)] font-semibold">
              {(DBPEDIA_STATS.nullOpsWithoutCGC / 1e9).toFixed(0)}B null operations
            </span>{" "}
            per scan.
          </p>

          <div className="space-y-2">
            {ATTR_FREQ.map((item, i) => (
              <motion.div
                key={item.key}
                initial={{ opacity: 0, x: -20 }}
                animate={inView ? { opacity: 1, x: 0 } : {}}
                transition={{ delay: 0.3 + i * 0.06 }}
                className="flex items-center gap-3"
              >
                <span className="text-xs font-mono w-24 text-right shrink-0" style={{ color: ATTR_COLORS[item.key] ?? "#a1a1aa" }}>
                  {item.key}
                </span>
                <div className="flex-1 bg-[var(--bg-elevated)] rounded-full h-2 overflow-hidden">
                  <motion.div
                    initial={{ width: 0 }}
                    animate={inView ? { width: `${item.pct}%` } : {}}
                    transition={{ delay: 0.4 + i * 0.06, duration: 0.6, ease: "easeOut" }}
                    className="h-full rounded-full"
                    style={{ background: item.pct >= 5 ? ATTR_COLORS[item.key] ?? "#e84545" : "#3f3f46" }}
                  />
                </div>
                <span className="text-xs text-[var(--text-secondary)] w-10 shrink-0">{item.pct}%</span>
                {item.pct < 5 && (
                  <span className="text-xs text-[var(--accent)] opacity-70">waste</span>
                )}
              </motion.div>
            ))}
          </div>

          {/* Red line at 5% */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={inView ? { opacity: 1 } : {}}
            transition={{ delay: 1.2 }}
            className="mt-6 flex items-center gap-2"
          >
            <div className="w-4 h-px bg-[var(--accent)]" />
            <span className="text-xs text-[var(--accent)]">5% threshold — below = mostly wasted storage</span>
          </motion.div>

          {/* The takeaway */}
          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={inView ? { opacity: 1, y: 0 } : {}}
            transition={{ delay: 1.4 }}
            className="mt-8 p-4 rounded-xl border border-[var(--accent)] border-opacity-30 bg-[var(--bg-surface)]"
          >
            <p className="text-sm text-[var(--text-secondary)]">
              <span className="text-[var(--text-primary)] font-semibold">The problem:</span>{" "}
              {DBPEDIA_STATS.nodes.toLocaleString()} nodes × {DBPEDIA_STATS.attributes.toLocaleString()} attributes
              = {(DBPEDIA_STATS.nullOpsWithoutCGC / 1e9).toFixed(0)} billion null checks per scan.
              Existing systems either waste memory or lose vectorization.
            </p>
          </motion.div>
        </div>
      </div>
    </section>
  );
}
