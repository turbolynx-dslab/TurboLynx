"use client";
import { motion } from "framer-motion";
import type { PhaseTrace } from "@/lib/pipeline-data";

interface BenchmarkPanelProps {
  speedup: number | null;
  turbolynxMs: number | null;
  naiveMs: number | null;
  active: boolean;
  presetLabel?: string;
  trace?: PhaseTrace | null;
}

export default function BenchmarkPanel({ speedup, turbolynxMs, naiveMs, active, presetLabel, trace }: BenchmarkPanelProps) {
  const displaySpeedup = speedup ?? 86;
  const displayMs = turbolynxMs ?? 14;
  const displayNaive = naiveMs ?? 1204;

  const partitionsHit = trace?.cgc.scannedPartitions ?? 2;
  const totalPartitions = trace?.cgc.totalPartitions ?? 34;
  const nullOpsB = trace ? Math.round(trace.cgc.nullOpsAvoided / 1e9) : 212;
  const planReduction = trace?.gem.reductionPct ?? 67;
  const memReduction = trace?.ssrf.nullPct ?? 72;

  return (
    <div className="h-full flex flex-col p-3 gap-3 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between flex-shrink-0">
        <span className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider">Performance</span>
        {presetLabel && (
          <span className="text-xs text-[var(--text-secondary)] font-mono">{presetLabel}</span>
        )}
      </div>

      {/* Big speedup number */}
      <motion.div
        initial={{ scale: 0.8, opacity: 0 }}
        animate={active ? { scale: 1, opacity: 1 } : { scale: 0.8, opacity: 0 }}
        transition={{ delay: 2.0, type: "spring", stiffness: 200 }}
        className="flex-shrink-0 text-center py-2"
      >
        <div className="text-5xl font-bold text-[var(--accent)] font-mono leading-none">
          {displaySpeedup}×
        </div>
        <div className="text-xs text-[var(--text-secondary)] mt-1">faster than naive</div>
      </motion.div>

      {/* Timing comparison */}
      <motion.div
        initial={{ opacity: 0, y: 8 }}
        animate={active ? { opacity: 1, y: 0 } : { opacity: 0, y: 8 }}
        transition={{ delay: 2.1, duration: 0.4 }}
        className="flex-shrink-0 space-y-2"
      >
        {/* TurboLynx bar */}
        <div className="space-y-1">
          <div className="flex items-center justify-between text-[10px] font-mono">
            <span className="text-[var(--accent)]">TurboLynx</span>
            <span className="text-[var(--accent)]">{displayMs}ms</span>
          </div>
          <div className="w-full bg-[var(--bg-elevated)] rounded-full h-2">
            <motion.div
              initial={{ width: 0 }}
              animate={active ? { width: `${(displayMs / displayNaive) * 100}%` } : { width: 0 }}
              transition={{ delay: 2.15, duration: 0.5 }}
              className="bg-[var(--accent)] h-2 rounded-full"
            />
          </div>
        </div>

        {/* Naive bar */}
        <div className="space-y-1">
          <div className="flex items-center justify-between text-[10px] font-mono">
            <span className="text-[var(--text-secondary)]">Naive scan</span>
            <span className="text-[var(--text-secondary)]">{displayNaive.toLocaleString()}ms</span>
          </div>
          <div className="w-full bg-[var(--bg-elevated)] rounded-full h-2">
            <motion.div
              initial={{ width: 0 }}
              animate={active ? { width: "100%" } : { width: 0 }}
              transition={{ delay: 2.2, duration: 0.5 }}
              className="bg-[var(--border)] h-2 rounded-full"
            />
          </div>
        </div>
      </motion.div>

      {/* Stats grid */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={active ? { opacity: 1 } : { opacity: 0 }}
        transition={{ delay: 2.3, duration: 0.4 }}
        className="flex-1 grid grid-cols-2 gap-2 content-start"
      >
        <div className="rounded-lg bg-[var(--bg-elevated)] p-2.5 text-center">
          <div className="text-sm font-bold font-mono text-[var(--accent-green)]">{partitionsHit}/{totalPartitions}</div>
          <div className="text-[10px] text-[var(--text-secondary)] mt-0.5">partitions hit</div>
        </div>
        <div className="rounded-lg bg-[var(--bg-elevated)] p-2.5 text-center">
          <div className="text-sm font-bold font-mono text-[var(--accent-blue)]">{nullOpsB}B</div>
          <div className="text-[10px] text-[var(--text-secondary)] mt-0.5">null-ops skipped</div>
        </div>
        <div className="rounded-lg bg-[var(--bg-elevated)] p-2.5 text-center">
          <div className="text-sm font-bold font-mono text-[var(--accent-orange)]">-{planReduction}%</div>
          <div className="text-[10px] text-[var(--text-secondary)] mt-0.5">plan space</div>
        </div>
        <div className="rounded-lg bg-[var(--bg-elevated)] p-2.5 text-center">
          <div className="text-sm font-bold font-mono text-[var(--accent-green)]">-{memReduction}%</div>
          <div className="text-[10px] text-[var(--text-secondary)] mt-0.5">memory (SSRF)</div>
        </div>
      </motion.div>

      {!active && (
        <div className="flex-1 flex items-center justify-center">
          <span className="text-xs text-[var(--border)] font-mono">run a query</span>
        </div>
      )}
    </div>
  );
}
