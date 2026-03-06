"use client";
import { useState, useRef, useEffect, useCallback } from "react";
import { motion, AnimatePresence, useInView } from "framer-motion";
import SectionLabel from "@/components/ui/SectionLabel";
import { PRESET_QUERIES, type PresetQuery } from "@/lib/data";

type State = "idle" | "editing" | "running" | "results";

function classifyQuery(q: string): PresetQuery | null {
  const lower = q.toLowerCase();
  let best = PRESET_QUERIES[0];
  let bestScore = 0;
  for (const pq of PRESET_QUERIES) {
    let score = 0;
    if (lower.includes(pq.id.toLowerCase())) score += 10;
    pq.cypher.toLowerCase().split(/\W+/).forEach(w => {
      if (w.length > 3 && lower.includes(w)) score++;
    });
    if (score > bestScore) { bestScore = score; best = pq; }
  }
  return bestScore > 2 ? best : null;
}

function ExecutionTrace({ query, visible }: { query: PresetQuery; visible: boolean }) {
  const steps = [
    { label: "Parse query", time: "2 ms", done: true },
    { label: `Schema Index lookup — matched ${query.matchedGraphlets.toLocaleString()} / ${query.totalGraphlets.toLocaleString()} graphlets`, time: "8 ms", done: true },
    { label: `GEM: virtual graphlets formed`, time: "14 ms", done: true },
    { label: `Plan: ${query.planSummary}`, time: "31 ms", done: true },
    { label: "Executing...", time: "", done: false },
  ];
  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: "auto" }}
          exit={{ opacity: 0, height: 0 }}
          className="mt-3 bg-[var(--bg-base)] border border-[var(--border)] rounded-lg p-4 font-mono text-xs space-y-1.5 overflow-hidden"
        >
          {steps.map((s, i) => (
            <motion.div
              key={s.label}
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: i * 0.18 }}
              className="flex items-start gap-2"
            >
              {s.done
                ? <span className="text-[var(--accent-green)] shrink-0">✓</span>
                : <span className="text-[var(--accent)] shrink-0 animate-pulse">▶</span>}
              <span className="text-[var(--text-secondary)] flex-1">{s.label}</span>
              {s.time && <span className="text-[var(--text-secondary)] opacity-50">{s.time}</span>}
            </motion.div>
          ))}
        </motion.div>
      )}
    </AnimatePresence>
  );
}

function ResultsTable({ query }: { query: PresetQuery }) {
  const cols = Object.keys(query.results[0] ?? {});
  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      className="h-full flex flex-col"
    >
      <div className="overflow-auto rounded-xl border border-[var(--border)] flex-1">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[var(--border)] bg-[var(--bg-elevated)]">
              {cols.map(c => (
                <th key={c} className="text-left px-4 py-2.5 text-xs font-mono text-[var(--accent-blue)] tracking-wide">{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {query.results.map((row, i) => (
              <motion.tr
                key={i}
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.2 + i * 0.07 }}
                className="border-b border-[var(--border)] last:border-0 hover:bg-[var(--bg-elevated)]"
              >
                {cols.map(c => (
                  <td key={c} className="px-4 py-2.5 text-[var(--text-secondary)] text-xs">{row[c]}</td>
                ))}
              </motion.tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="mt-3 flex items-center gap-4 text-xs text-[var(--text-secondary)]">
        <span>{query.results.length} rows</span>
        <span className="text-[var(--accent-green)] font-semibold">{query.turbolynxMs} ms</span>
        <span className="text-[var(--accent)] font-bold">{query.speedup}× faster than Neo4j</span>
        <span className="opacity-50">(naive: {query.naiveMs.toLocaleString()} ms)</span>
      </div>
    </motion.div>
  );
}

function RightPanel({ state, activeQuery }: { state: State; activeQuery: PresetQuery | null }) {
  if (state === "idle") {
    return (
      <div className="h-full flex flex-col items-center justify-center text-center p-8">
        <div className="w-16 h-16 rounded-full border border-[var(--border)] flex items-center justify-center mb-4 opacity-30">
          <span className="text-3xl">⚡</span>
        </div>
        <p className="text-[var(--text-secondary)] text-sm">Pick a preset or type a query, then press Run.</p>
      </div>
    );
  }
  if (state === "running") {
    return (
      <div className="h-full flex flex-col items-center justify-center gap-4">
        <div className="flex gap-1.5">
          {[0, 1, 2].map(i => (
            <motion.div key={i} className="w-2 h-2 rounded-full bg-[var(--accent)]"
              animate={{ scale: [1, 1.5, 1] }}
              transition={{ duration: 0.8, delay: i * 0.15, repeat: Infinity }}
            />
          ))}
        </div>
        <p className="text-sm text-[var(--text-secondary)]">TurboLynx is working...</p>
        {activeQuery && (
          <div className="text-xs font-mono text-[var(--text-secondary)] opacity-50">
            Scanning {activeQuery.matchedGraphlets} / {activeQuery.totalGraphlets} graphlets
          </div>
        )}
      </div>
    );
  }
  if (state === "results" && activeQuery) {
    return <ResultsTable query={activeQuery} />;
  }
  return null;
}

export default function QueryPlayground() {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true, margin: "-80px" });
  const [state, setState] = useState<State>("idle");
  const [text, setText] = useState("");
  const [activeQuery, setActiveQuery] = useState<PresetQuery | null>(null);
  const [traceVisible, setTraceVisible] = useState(false);
  const [recentIds, setRecentIds] = useState<Set<string>>(new Set());
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Typewriter effect for preset loading
  const loadPreset = useCallback((pq: PresetQuery) => {
    setState("editing");
    setActiveQuery(pq);
    setRecentIds(s => new Set(s).add(pq.id));
    let i = 0;
    setText("");
    const chars = pq.cypher.split("");
    const t = setInterval(() => {
      setText(pq.cypher.slice(0, ++i));
      if (i >= chars.length) clearInterval(t);
    }, 18);
  }, []);

  const run = useCallback(() => {
    if (!text.trim()) return;
    const match = classifyQuery(text) ?? activeQuery;
    setActiveQuery(match);
    setState("running");
    setTraceVisible(true);
    setTimeout(() => setState("results"), 1400);
  }, [text, activeQuery]);

  const reset = () => { setState("idle"); setText(""); setActiveQuery(null); setTraceVisible(false); };

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") { e.preventDefault(); run(); }
      if ((e.metaKey || e.ctrlKey) && e.key === "k") { e.preventDefault(); reset(); textareaRef.current?.focus(); }
      if (e.key === "Escape") reset();
      if (!e.metaKey && !e.ctrlKey && ["1","2","3","4"].includes(e.key)) {
        const idx = parseInt(e.key) - 1;
        if (PRESET_QUERIES[idx]) loadPreset(PRESET_QUERIES[idx]);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [run, loadPreset]);

  return (
    <section id="playground" ref={ref} className="py-24 px-6 max-w-7xl mx-auto">
      <SectionLabel number="03" label="Query Playground" />

      <motion.h2
        initial={{ opacity: 0, y: 16 }}
        animate={inView ? { opacity: 1, y: 0 } : {}}
        className="text-3xl md:text-4xl font-bold mb-3"
      >
        Try it live.
      </motion.h2>
      <motion.p
        initial={{ opacity: 0 }}
        animate={inView ? { opacity: 1 } : {}}
        transition={{ delay: 0.2 }}
        className="text-[var(--text-secondary)] mb-8 text-lg"
      >
        Real DBpedia queries. Pre-computed results. Watch the engine work.
      </motion.p>

      <div className="grid lg:grid-cols-2 gap-6">
        {/* Left panel */}
        <div className="flex flex-col gap-4">
          {/* Preset chips */}
          <div className="flex flex-wrap gap-2">
            {PRESET_QUERIES.map((pq, i) => (
              <button
                key={pq.id}
                onClick={() => loadPreset(pq)}
                title={pq.description}
                className={`flex items-center gap-2 px-4 py-2 rounded-full text-sm border transition-all ${
                  activeQuery?.id === pq.id
                    ? "border-[var(--accent)] text-[var(--accent)] bg-[var(--accent)]/10"
                    : "border-[var(--border)] text-[var(--text-secondary)] hover:border-[var(--text-secondary)]"
                }`}
              >
                <span className="text-xs font-mono opacity-60">{i + 1}</span>
                <span className="font-medium">{pq.id}</span>
                <span className="hidden sm:inline opacity-70">{pq.label}</span>
                {recentIds.has(pq.id) && (
                  <span className="w-1.5 h-1.5 rounded-full bg-[var(--accent-green)]" />
                )}
              </button>
            ))}
          </div>

          {/* Editor */}
          <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl overflow-hidden focus-within:border-[var(--accent)] transition-colors">
            <div className="flex items-center justify-between px-4 py-2 border-b border-[var(--border)] bg-[var(--bg-elevated)]">
              <span className="text-xs font-mono text-[var(--text-secondary)]">Cypher</span>
              <div className="flex gap-1.5">
                <div className="w-2.5 h-2.5 rounded-full bg-[#ff5f56]" />
                <div className="w-2.5 h-2.5 rounded-full bg-[#ffbd2e]" />
                <div className="w-2.5 h-2.5 rounded-full bg-[#27c93f]" />
              </div>
            </div>
            <textarea
              ref={textareaRef}
              value={text}
              onChange={e => { setText(e.target.value); if (state === "idle") setState("editing"); }}
              placeholder="// Type a Cypher query, or pick a preset above"
              className="w-full bg-transparent text-[var(--text-primary)] font-mono text-sm p-4 resize-none outline-none placeholder:text-[var(--text-secondary)] placeholder:opacity-40 min-h-[180px]"
              rows={8}
              spellCheck={false}
            />
          </div>

          {/* Buttons */}
          <div className="flex gap-3">
            <button
              onClick={run}
              disabled={!text.trim() || state === "running"}
              className="flex items-center gap-2 bg-[var(--accent)] disabled:opacity-40 text-white font-semibold px-6 py-2.5 rounded-full hover:bg-[#d63a3a] transition-colors text-sm"
            >
              ▶ Run Query
            </button>
            <button
              onClick={reset}
              className="flex items-center gap-2 border border-[var(--border)] text-[var(--text-secondary)] px-5 py-2.5 rounded-full hover:border-[var(--text-secondary)] transition-colors text-sm"
            >
              ↺ Reset
            </button>
          </div>

          {/* Execution trace */}
          {activeQuery && <ExecutionTrace query={activeQuery} visible={traceVisible} />}

          {/* Keyboard shortcuts */}
          <div className="flex flex-wrap gap-3 text-xs text-[var(--text-secondary)] opacity-50">
            <span><kbd className="font-mono">⌘↵</kbd> Run</span>
            <span><kbd className="font-mono">⌘K</kbd> Clear</span>
            <span><kbd className="font-mono">1-4</kbd> Load preset</span>
            <span><kbd className="font-mono">Esc</kbd> Reset</span>
          </div>
        </div>

        {/* Right panel */}
        <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl p-5 min-h-[400px] flex flex-col">
          <RightPanel state={state} activeQuery={activeQuery} />
        </div>
      </div>

      {/* Key message */}
      {activeQuery && state === "results" && (
        <motion.div
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          className="mt-6 p-4 bg-[var(--bg-surface)] border border-[var(--accent)]/30 rounded-xl text-sm text-[var(--text-secondary)]"
        >
          <span className="text-[var(--text-primary)] font-semibold">Schema Index speedup:</span>{" "}
          Without SI: scan all 77M nodes.{" "}
          With SI: scan {activeQuery.matchedGraphlets.toLocaleString()} graphlets ({((activeQuery.matchedGraphlets / activeQuery.totalGraphlets) * 100).toFixed(1)}% of graphlets).{" "}
          <span className="text-[var(--accent)] font-semibold">{Math.round(activeQuery.totalGraphlets / activeQuery.matchedGraphlets)}× less I/O.</span>
        </motion.div>
      )}
    </section>
  );
}
