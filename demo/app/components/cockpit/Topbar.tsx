"use client";

interface TopbarProps {
  runCount: number;
}

const STATS = [
  { value: "77M",    label: "nodes" },
  { value: "282K",   label: "schemas" },
  { value: "1,917",  label: "edge types" },
  { value: "86×",    label: "faster" },
];

export default function Topbar({ runCount }: TopbarProps) {
  return (
    <div className="cockpit-topbar flex items-center justify-between px-4 h-full">
      {/* Brand */}
      <div className="flex items-center gap-3">
        <span className="text-sm font-bold tracking-tight text-[var(--text-primary)]">
          Turbo<span className="text-[var(--accent)]">Lynx</span>
        </span>
        <span className="text-xs text-[var(--text-secondary)] bg-[var(--bg-elevated)] px-2 py-0.5 rounded font-mono">
          DBpedia · live
        </span>
      </div>

      {/* Stats */}
      <div className="flex items-center gap-6">
        {STATS.map(s => (
          <div key={s.label} className="flex items-baseline gap-1">
            <span className="text-sm font-bold text-[var(--text-primary)] font-mono">{s.value}</span>
            <span className="text-xs text-[var(--text-secondary)]">{s.label}</span>
          </div>
        ))}
      </div>

      {/* Run indicator */}
      <div className="flex items-center gap-2 text-xs text-[var(--text-secondary)]">
        {runCount > 0 && (
          <span className="text-[var(--accent-green)]">✓ {runCount} {runCount === 1 ? "query" : "queries"} run</span>
        )}
        <span className="w-2 h-2 rounded-full bg-[var(--accent-green)] animate-pulse" />
        <span>connected</span>
      </div>
    </div>
  );
}
