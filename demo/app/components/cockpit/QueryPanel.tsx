"use client";
import { useState, useRef, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import type { Preset, ResultRow, PhaseTrace } from "@/lib/pipeline-data";
import { PRESETS } from "@/lib/pipeline-data";

interface QueryPanelProps {
  onRun: (preset: Preset) => void;
  results: ResultRow[] | null;
  running: boolean;
  activePresetId: string | null;
  executionMs: number | null;
  trace?: PhaseTrace | null;
}

const RESULT_COLS: (keyof ResultRow)[] = ["name", "born", "city", "population", "entity_type"];

export default function QueryPanel({ onRun, results, running, activePresetId, executionMs, trace }: QueryPanelProps) {
  const [cypher, setCypher] = useState(PRESETS[2].cypher); // default: Q10
  const [typingTarget, setTypingTarget] = useState<string | null>(null);
  const [typingIdx, setTypingIdx] = useState(0);
  const [pendingPreset, setPendingPreset] = useState<Preset>(PRESETS[2]); // track selected preset
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Typewriter animation when preset is clicked
  useEffect(() => {
    if (typingTarget === null) return;
    if (typingIdx >= typingTarget.length) {
      setTypingTarget(null);
      return;
    }
    const t = setTimeout(() => {
      setCypher(typingTarget.slice(0, typingIdx + 1));
      setTypingIdx(i => i + 1);
    }, 6);
    return () => clearTimeout(t);
  }, [typingTarget, typingIdx]);

  const selectPreset = (preset: Preset) => {
    setPendingPreset(preset);
    setCypher("");
    setTypingIdx(0);
    setTypingTarget(preset.cypher);
  };

  const handleRun = () => {
    // Prefer exact match; fallback to pendingPreset (works during typewriter)
    const match = PRESETS.find(p => p.cypher.trim() === cypher.trim()) ?? pendingPreset;
    onRun(match);
  };

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Presets strip */}
      <div className="flex items-center gap-1.5 px-3 pt-3 pb-2 flex-shrink-0 border-b border-[var(--border)]">
        <span className="text-xs text-[var(--text-secondary)] mr-1">Preset:</span>
        {PRESETS.map(p => (
          <button
            key={p.id}
            onClick={() => selectPreset(p)}
            className={`text-xs px-2.5 py-1 rounded-full border transition-all font-mono ${
              activePresetId === p.id
                ? "border-[var(--accent)] text-[var(--accent)] bg-[var(--accent)]10"
                : "border-[var(--border)] text-[var(--text-secondary)] hover:border-[var(--text-secondary)]"
            }`}
          >
            {p.label}
          </button>
        ))}
      </div>

      {/* Editor */}
      <div className="relative flex-shrink-0 p-3 border-b border-[var(--border)]">
        <div
          className={`relative rounded-lg border transition-all duration-300 ${
            running
              ? "border-[var(--accent)] shadow-[0_0_12px_var(--accent)]"
              : "border-[var(--border)]"
          }`}
        >
          <textarea
            ref={textareaRef}
            value={cypher}
            onChange={e => setCypher(e.target.value)}
            onKeyDown={e => { if (e.ctrlKey && e.key === "Enter") { e.preventDefault(); handleRun(); } }}
            rows={8}
            spellCheck={false}
            className="w-full bg-[var(--bg-elevated)] text-[var(--text-primary)] font-mono text-xs p-3 rounded-lg resize-none outline-none leading-relaxed"
            placeholder="Enter Cypher query or select a preset..."
          />
          {typingTarget !== null && (
            <span className="absolute bottom-2 right-2 w-0.5 h-3 bg-[var(--accent)] animate-pulse" />
          )}
        </div>

        {/* Run button */}
        <div className="flex items-center justify-between mt-2">
          <span className="text-[10px] text-[var(--text-secondary)] font-mono">Ctrl+Enter to run</span>
          <button
            onClick={handleRun}
            disabled={running}
            className={`flex items-center gap-1.5 text-xs font-semibold px-4 py-1.5 rounded-full transition-all ${
              running
                ? "bg-[var(--bg-elevated)] text-[var(--text-secondary)] cursor-not-allowed"
                : "bg-[var(--accent)] text-white hover:bg-[#d63a3a]"
            }`}
          >
            {running ? (
              <><span className="animate-spin">⟳</span> Executing...</>
            ) : (
              <>▶ Run</>
            )}
          </button>
        </div>
      </div>

      {/* Execution trace + results — scrollable */}
      <div className="flex-1 min-h-0 overflow-y-auto">
        <AnimatePresence mode="wait">
          {results && (
            <motion.div
              key="results"
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0 }}
              className="p-3 space-y-3"
            >
              {/* Exec header */}
              <div className="flex items-center justify-between">
                <span className="text-xs text-[var(--text-secondary)]">
                  Schema index hit → partitions {trace?.cgc.litPartitions.map(p => `#${p.id}`).join(", ") ?? "#7, #12"}
                </span>
                {executionMs !== null && (
                  <span className="text-xs font-mono text-[var(--accent-green)] bg-[var(--bg-elevated)] px-2 py-0.5 rounded">
                    ⚡ {executionMs}ms
                  </span>
                )}
              </div>

              {/* Results table */}
              <div className="rounded-lg border border-[var(--border)] overflow-hidden">
                <table className="w-full text-xs font-mono">
                  <thead>
                    <tr className="border-b border-[var(--border)] bg-[var(--bg-elevated)]">
                      {RESULT_COLS.map(col => (
                        <th key={col} className="text-left px-3 py-2 text-[var(--text-secondary)] font-medium">
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {results.map((row, i) => (
                      <motion.tr
                        key={i}
                        initial={{ opacity: 0, y: 4 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ delay: i * 0.06 }}
                        className="border-b border-[var(--border)] last:border-0 hover:bg-[var(--bg-elevated)] transition-colors"
                      >
                        {RESULT_COLS.map(col => (
                          <td key={col} className="px-3 py-2 text-[var(--text-primary)]">
                            {row[col] === "-" ? (
                              <span className="text-[var(--text-secondary)]">—</span>
                            ) : (
                              row[col]
                            )}
                          </td>
                        ))}
                      </motion.tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="text-[10px] text-[var(--text-secondary)] font-mono text-center">
                {results.length} rows · simulated
              </div>
            </motion.div>
          )}

          {!results && !running && (
            <div className="h-full flex items-center justify-center p-8">
              <div className="text-center">
                <div className="text-3xl mb-2 opacity-20">⚡</div>
                <p className="text-xs text-[var(--text-secondary)]">Select a preset or write a query and press Run</p>
              </div>
            </div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
