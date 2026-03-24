"use client";
import { useState, useRef, useEffect } from "react";
import { motion, useInView } from "framer-motion";
import SectionLabel from "@/components/ui/SectionLabel";

interface Dot { x: number; y: number; vx: number; vy: number; cluster: number; color: string; schema: number; }

const CLUSTER_COLORS = ["#e84545", "#3b82f6", "#22c55e", "#8b5cf6", "#f97316", "#06b6d4", "#ec4899", "#84cc16"];

function CGCCanvas({ animating, done }: { animating: boolean; done: boolean }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const dotsRef = useRef<Dot[]>([]);
  const rafRef = useRef<number>(0);
  const W = 400, H = 300;

  useEffect(() => {
    // Initialize dots
    dotsRef.current = Array.from({ length: 60 }, (_, i) => ({
      x: 20 + Math.random() * (W - 40),
      y: 20 + Math.random() * (H - 40),
      vx: (Math.random() - 0.5) * 0.8,
      vy: (Math.random() - 0.5) * 0.8,
      cluster: i % 8,
      color: CLUSTER_COLORS[i % 8],
      schema: i,
    }));
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d")!;

    // Cluster centers
    const centers = CLUSTER_COLORS.map((_, ci) => ({
      x: 60 + (ci % 4) * 80,
      y: 60 + Math.floor(ci / 4) * 120,
    }));

    const draw = () => {
      ctx.clearRect(0, 0, W, H);
      const dots = dotsRef.current;

      if (animating) {
        dots.forEach(d => {
          const target = centers[d.cluster];
          const strength = done ? 0.08 : 0.04;
          d.vx += (target.x - d.x) * strength;
          d.vy += (target.y - d.y) * strength;
          d.vx *= 0.85;
          d.vy *= 0.85;
        });
      } else {
        dots.forEach(d => {
          // random drift
          d.x += d.vx;
          d.y += d.vy;
          if (d.x < 10 || d.x > W - 10) d.vx *= -1;
          if (d.y < 10 || d.y > H - 10) d.vy *= -1;
        });
      }

      dots.forEach(d => {
        d.x += d.vx;
        d.y += d.vy;
      });

      // Draw cluster hulls when done
      if (done) {
        CLUSTER_COLORS.forEach((color, ci) => {
          const c = centers[ci];
          ctx.beginPath();
          ctx.arc(c.x, c.y, 40, 0, Math.PI * 2);
          ctx.fillStyle = color + "18";
          ctx.fill();
          ctx.strokeStyle = color + "60";
          ctx.lineWidth = 1.5;
          ctx.stroke();
          ctx.fillStyle = color;
          ctx.font = "bold 10px monospace";
          ctx.fillText(`gl${ci + 1}`, c.x - 8, c.y + 3);
        });
      }

      // Draw dots
      dots.forEach(d => {
        ctx.beginPath();
        ctx.fillStyle = d.color;
        ctx.arc(d.x, d.y, 3, 0, Math.PI * 2);
        ctx.fill();
      });

      rafRef.current = requestAnimationFrame(draw);
    };
    draw();
    return () => cancelAnimationFrame(rafRef.current);
  }, [animating, done]);

  return <canvas ref={canvasRef} width={W} height={H} className="rounded-xl bg-[var(--bg-surface)] border border-[var(--border)]" />;
}

export default function CGCClustering() {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true, margin: "-80px" });
  const [animating, setAnimating] = useState(false);
  const [done, setDone] = useState(false);
  const [tau, setTau] = useState(0.5);

  const graphletCount = Math.round(8 + (1 - tau) * 20);

  const run = () => {
    setAnimating(true);
    setTimeout(() => setDone(true), 1800);
  };
  const reset = () => { setAnimating(false); setDone(false); };

  return (
    <section ref={ref} className="py-24 px-6 max-w-7xl mx-auto">
      <SectionLabel number="02" label="CGC: Tame the Chaos" />

      <div className="grid lg:grid-cols-2 gap-16">
        <div>
          <motion.h2
            initial={{ opacity: 0, y: 16 }}
            animate={inView ? { opacity: 1, y: 0 } : {}}
            className="text-3xl md:text-4xl font-bold mb-3"
          >
            Cost-based Graphlet Chunking
          </motion.h2>
          <motion.p
            initial={{ opacity: 0 }}
            animate={inView ? { opacity: 1 } : {}}
            transition={{ delay: 0.2 }}
            className="text-[var(--text-secondary)] mb-8 text-lg"
          >
            Similar schemas cluster into graphlets. Each graphlet stores data in columnar format — zero nulls, full SIMD vectorization.
          </motion.p>

          {/* Canvas */}
          <div className="relative">
            <CGCCanvas animating={animating} done={done} />
            <div className="absolute top-3 left-3 text-xs font-mono text-[var(--text-secondary)] bg-[var(--bg-base)] px-2 py-1 rounded">
              {done
                ? <span className="text-[var(--accent-green)]">282,764 schemas → <strong>{graphletCount} graphlets</strong></span>
                : "282,764 unique attribute sets"}
            </div>
          </div>

          <div className="mt-4 flex gap-3">
            {!animating ? (
              <button onClick={run} className="flex items-center gap-2 bg-[var(--accent)] text-white text-sm font-semibold px-5 py-2 rounded-full hover:bg-[#d63a3a] transition-colors">
                ▶ Run CGC
              </button>
            ) : (
              <button onClick={reset} className="flex items-center gap-2 border border-[var(--border)] text-[var(--text-secondary)] text-sm px-5 py-2 rounded-full hover:border-[var(--text-secondary)] transition-colors">
                ↺ Reset
              </button>
            )}
          </div>
        </div>

        <div className="space-y-8">
          {/* Tau slider */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={inView ? { opacity: 1 } : {}}
            transition={{ delay: 0.3 }}
            className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl p-6"
          >
            <div className="flex justify-between items-center mb-3">
              <span className="text-sm font-medium">Merge threshold τ</span>
              <span className="text-sm font-mono text-[var(--accent)]">{tau.toFixed(2)}</span>
            </div>
            <input
              type="range" min="0" max="1" step="0.01"
              value={tau}
              onChange={e => setTau(parseFloat(e.target.value))}
              className="w-full accent-[var(--accent)]"
            />
            <div className="flex justify-between text-xs text-[var(--text-secondary)] mt-1">
              <span>All-in-one (many nulls)</span>
              <span className={tau > 0.3 && tau < 0.7 ? "text-[var(--accent-green)] font-semibold" : ""}>
                {tau > 0.3 && tau < 0.7 ? "✓ Sweet spot" : "Sweet spot"}
              </span>
              <span>All-separate (no SIMD)</span>
            </div>
            <div className="mt-3 text-center text-sm">
              Graphlets: <span className="font-bold text-[var(--text-primary)]">{graphletCount}</span>
            </div>
          </motion.div>

          {/* Comparison table */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            animate={inView ? { opacity: 1, y: 0 } : {}}
            transition={{ delay: 0.4 }}
            className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl overflow-hidden"
          >
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[var(--border)]">
                  <th className="text-left px-4 py-3 text-[var(--text-secondary)] font-medium text-xs uppercase tracking-wider"></th>
                  <th className="text-center px-3 py-3 text-[var(--text-secondary)] text-xs">Single Table</th>
                  <th className="text-center px-3 py-3 text-[var(--text-secondary)] text-xs">Separate All</th>
                  <th className="text-center px-3 py-3 text-[var(--accent)] text-xs font-semibold">TurboLynx CGC</th>
                </tr>
              </thead>
              <tbody>
                {[
                  ["Null entries", "212B", "0", "~0"],
                  ["Vectorization", "✗", "✗", "✓"],
                  ["Scan speed", "1×", "0.004×", "5319×"],
                ].map(([label, ma, sa, tl]) => (
                  <tr key={label} className="border-b border-[var(--border)] last:border-0">
                    <td className="px-4 py-3 text-[var(--text-secondary)] text-xs">{label}</td>
                    <td className="text-center px-3 py-3 text-[var(--text-secondary)]">{ma}</td>
                    <td className="text-center px-3 py-3 text-[var(--text-secondary)]">{sa}</td>
                    <td className="text-center px-3 py-3 text-[var(--accent-green)] font-bold">{tl}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </motion.div>
        </div>
      </div>
    </section>
  );
}
