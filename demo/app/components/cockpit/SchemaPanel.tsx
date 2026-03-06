"use client";
import { useRef, useEffect } from "react";
import { motion } from "framer-motion";
import type { PartitionInfo } from "@/lib/pipeline-data";

const TOTAL = 34;
const BLUE   = "#3b82f6";
const ORANGE = "#f97316";
const DIM    = "#27272a";

interface SchemaPanelProps {
  litPartitions: PartitionInfo[];
  active: boolean;
  delay: number;
}

function SchemaCanvas({ litPartitions, active }: { litPartitions: PartitionInfo[]; active: boolean }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const animRef   = useRef<number>(0);
  const timeRef   = useRef<number>(0);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d")!;
    const W = canvas.width;
    const H = canvas.height;

    // Deterministic dot positions (seeded by index)
    const dots = Array.from({ length: 120 }, (_, i) => {
      const a = (i * 137.508 * Math.PI) / 180;
      const r = 10 + (i / 120) * (Math.min(W, H) / 2 - 16);
      return {
        x: W / 2 + Math.cos(a) * r,
        y: H / 2 + Math.sin(a) * r,
        partition: i % TOTAL,
        baseR: 2 + (i % 3) * 0.8,
      };
    });

    const litIds = new Set(litPartitions.map(p => p.id));
    const colorMap = new Map(litPartitions.map(p => [p.id, p.color === "blue" ? BLUE : ORANGE]));

    const draw = (ts: number) => {
      ctx.clearRect(0, 0, W, H);
      const t = ts / 1000;

      dots.forEach(d => {
        const isLit = active && litIds.has(d.partition);
        const color = isLit ? colorMap.get(d.partition)! : DIM;
        const pulse = isLit ? 1 + 0.25 * Math.sin(t * 3 + d.partition) : 1;
        const r = d.baseR * pulse;

        if (isLit) {
          // Glow
          const grd = ctx.createRadialGradient(d.x, d.y, 0, d.x, d.y, r * 4);
          grd.addColorStop(0, color + "60");
          grd.addColorStop(1, "transparent");
          ctx.beginPath();
          ctx.arc(d.x, d.y, r * 4, 0, Math.PI * 2);
          ctx.fillStyle = grd;
          ctx.fill();
        }

        ctx.beginPath();
        ctx.arc(d.x, d.y, r, 0, Math.PI * 2);
        ctx.fillStyle = isLit ? color : color + "55";
        ctx.fill();
      });

      animRef.current = requestAnimationFrame(draw);
    };

    animRef.current = requestAnimationFrame(draw);
    return () => cancelAnimationFrame(animRef.current);
  }, [active, litPartitions]);

  return (
    <canvas
      ref={canvasRef}
      width={200} height={200}
      className="w-full h-full"
    />
  );
}

export default function SchemaPanel({ litPartitions, active, delay }: SchemaPanelProps) {
  return (
    <div className="h-full flex flex-col p-3 gap-3 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between flex-shrink-0">
        <span className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider">CGC</span>
        <motion.span
          initial={{ opacity: 0 }}
          animate={active ? { opacity: 1 } : { opacity: 0 }}
          transition={{ delay }}
          className="text-xs font-mono text-[var(--accent-green)]"
        >
          {litPartitions.length} / {TOTAL} partitions
        </motion.span>
      </div>

      {/* Canvas — schema dot cloud */}
      <div className="flex-1 min-h-0 relative rounded-lg overflow-hidden bg-[var(--bg-elevated)]">
        <SchemaCanvas litPartitions={litPartitions} active={active} />
        {!active && (
          <div className="absolute inset-0 flex items-center justify-center">
            <span className="text-xs text-[var(--text-secondary)] font-mono">282,764 schemas</span>
          </div>
        )}
      </div>

      {/* Partition cards */}
      <div className="flex-shrink-0 space-y-2">
        {litPartitions.map((p, i) => {
          const color = p.color === "blue" ? BLUE : ORANGE;
          return (
            <motion.div
              key={p.id}
              initial={{ opacity: 0, x: -8 }}
              animate={active ? { opacity: 1, x: 0 } : { opacity: 0, x: -8 }}
              transition={{ delay: delay + i * 0.1, duration: 0.3 }}
              className="rounded-lg p-2.5 border"
              style={{ borderColor: color + "40", background: color + "10" }}
            >
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs font-bold font-mono" style={{ color }}>
                  #{p.id} {p.label}
                </span>
                <span className="text-xs font-mono text-[var(--text-secondary)]">
                  {p.rowCount.toLocaleString()} rows
                </span>
              </div>
              <div className="flex flex-wrap gap-1">
                {p.attrs.map(a => (
                  <span key={a} className="text-[10px] font-mono text-[var(--text-secondary)] bg-[var(--bg-elevated)] px-1.5 py-0.5 rounded">
                    {a}
                  </span>
                ))}
              </div>
            </motion.div>
          );
        })}

        {/* Dim indicator */}
        {active && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: delay + 0.3 }}
            className="text-[10px] text-[var(--text-secondary)] font-mono text-center"
          >
            {TOTAL - litPartitions.length} partitions dimmed · 212B null-ops avoided
          </motion.div>
        )}
      </div>
    </div>
  );
}
