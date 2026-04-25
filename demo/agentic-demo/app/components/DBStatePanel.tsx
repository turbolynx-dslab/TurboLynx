"use client";
import { useEffect, useState } from "react";
import { DBState } from "@/lib/scenario";

interface RecentQuery {
  label: string;  // single-line summary shown in the panel
  full: string;   // full Cypher body, shown in the click-through modal
}

function AnimatedValue({ value }: { value: number }) {
  const [display, setDisplay] = useState(value);
  useEffect(() => {
    if (display === value) return;
    const duration = 700;
    const start = display;
    const diff = value - start;
    const t0 = performance.now();
    let frame = 0;
    const tick = (now: number) => {
      const p = Math.min(1, (now - t0) / duration);
      setDisplay(Math.round(start + diff * p));
      if (p < 1) frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [value, display]);
  return <span className="tabular-nums">{display.toLocaleString()}</span>;
}

function Stat({
  label,
  value,
  delta,
}: {
  label: string;
  value: number;
  delta?: number;
}) {
  return (
    <div className="stat-card">
      <div className="flex items-center gap-1.5">
        <div className={`w-1.5 h-1.5 rounded-full bg-[var(--accent-db)] ${delta ? "pulse" : ""}`} />
        <div className="stat-label">{label}</div>
      </div>
      <div className="stat-value">
        <AnimatedValue value={value} />
      </div>
      {delta !== undefined && delta !== 0 && (
        <div className="stat-delta">
          {delta > 0 ? `+${delta.toLocaleString()}` : delta.toLocaleString()} this step
        </div>
      )}
    </div>
  );
}

export default function DBStatePanel({
  state,
  prevState,
  queries,
  onQueryClick,
}: {
  state: DBState;
  prevState: DBState | null;
  queries: RecentQuery[];
  onQueryClick: (full: string) => void;
}) {
  const deltaNodes = prevState ? state.nodes - prevState.nodes : 0;
  const deltaEdges = prevState ? state.edges - prevState.edges : 0;
  return (
    <div className="flex flex-col h-full min-h-0">
      <div className="px-4 py-3 border-b border-[var(--border)]">
        <div className="text-[10.5px] uppercase tracking-[0.08em] text-[var(--text-muted)] font-semibold">
          DB State
        </div>
        <div className="text-sm mt-0.5 text-[var(--text-primary)] font-medium">
          TurboLynx · dbpedia-cinema
        </div>
      </div>
      <Stat label="Nodes" value={state.nodes} delta={deltaNodes} />
      <Stat label="Edges" value={state.edges} delta={deltaEdges} />
      <div className="px-4 py-3 border-t border-[var(--border)] flex-1 overflow-y-auto min-h-0">
        <div className="text-[10.5px] uppercase tracking-[0.08em] text-[var(--text-muted)] font-semibold mb-2">
          Recent queries
        </div>
        <div className="flex flex-col gap-1.5">
          {queries.length === 0 && (
            <div className="text-[11px] text-[var(--text-muted)] italic">
              No queries yet.
            </div>
          )}
          {queries.map((q, i) => (
            <button
              key={i}
              className="recent-query"
              onClick={() => onQueryClick(q.full)}
              title="Click to view full query"
            >
              <span className="recent-query__text mono">{q.label}</span>
              <span className="recent-query__caret">↗</span>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
