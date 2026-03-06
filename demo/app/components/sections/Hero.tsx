"use client";
import { useEffect, useRef } from "react";
import { motion } from "framer-motion";
import AnimatedCounter from "@/components/ui/AnimatedCounter";

// Particle background using canvas
function ParticleCanvas() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d")!;

    const resize = () => {
      canvas.width = window.innerWidth;
      canvas.height = window.innerHeight;
    };
    resize();
    window.addEventListener("resize", resize);

    const N = 120;
    const pts = Array.from({ length: N }, () => ({
      x: Math.random() * canvas.width,
      y: Math.random() * canvas.height,
      vx: (Math.random() - 0.5) * 0.3,
      vy: (Math.random() - 0.5) * 0.3,
      r: Math.random() * 1.5 + 0.5,
    }));

    let raf: number;
    const draw = () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      // edges
      for (let i = 0; i < N; i++) {
        for (let j = i + 1; j < N; j++) {
          const dx = pts[i].x - pts[j].x;
          const dy = pts[i].y - pts[j].y;
          const d = Math.sqrt(dx * dx + dy * dy);
          if (d < 90) {
            ctx.beginPath();
            ctx.strokeStyle = `rgba(232,69,69,${0.12 * (1 - d / 90)})`;
            ctx.lineWidth = 0.5;
            ctx.moveTo(pts[i].x, pts[i].y);
            ctx.lineTo(pts[j].x, pts[j].y);
            ctx.stroke();
          }
        }
      }
      // nodes
      pts.forEach(p => {
        ctx.beginPath();
        ctx.fillStyle = "rgba(244,244,245,0.4)";
        ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
        ctx.fill();
        p.x += p.vx;
        p.y += p.vy;
        if (p.x < 0 || p.x > canvas.width) p.vx *= -1;
        if (p.y < 0 || p.y > canvas.height) p.vy *= -1;
      });
      raf = requestAnimationFrame(draw);
    };
    draw();
    return () => { cancelAnimationFrame(raf); window.removeEventListener("resize", resize); };
  }, []);

  return <canvas ref={canvasRef} className="absolute inset-0 pointer-events-none" />;
}

export default function Hero() {
  const words = ["Schemaless", "Graph", "Engine", "Strikes", "Back."];

  return (
    <section className="relative min-h-screen flex flex-col items-center justify-center overflow-hidden">
      <ParticleCanvas />

      {/* Radial gradient vignette */}
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,transparent_40%,var(--bg-base)_100%)]" />

      <div className="relative z-10 text-center px-6 max-w-4xl mx-auto">
        {/* VLDB chip */}
        <motion.div
          initial={{ opacity: 0, y: -12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          className="inline-flex items-center gap-2 border border-[var(--accent)] text-[var(--accent)] text-xs font-mono px-3 py-1 rounded-full mb-8 tracking-widest"
        >
          VLDB 2026
        </motion.div>

        {/* Title */}
        <div className="mb-6">
          <motion.h1
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.1 }}
            className="text-6xl md:text-8xl font-bold tracking-tight text-[var(--text-primary)] leading-none mb-2"
          >
            TurboLynx
          </motion.h1>
          <div className="flex flex-wrap justify-center gap-x-3 gap-y-0">
            {words.map((w, i) => (
              <motion.span
                key={w}
                initial={{ opacity: 0, y: 16 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.5, delay: 0.25 + i * 0.07 }}
                className={`text-3xl md:text-5xl font-semibold ${i === words.length - 1 ? "text-[var(--accent)]" : "text-[var(--text-primary)]"}`}
              >
                {w}
              </motion.span>
            ))}
          </div>
        </div>

        {/* Subtext */}
        <motion.p
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.6, delay: 0.7 }}
          className="text-[var(--text-secondary)] text-lg md:text-xl mb-10 leading-relaxed"
        >
          <AnimatedCounter target={77} duration={1800} suffix="M" /> nodes.{" "}
          <span className="text-[var(--text-primary)] font-semibold">
            <AnimatedCounter target={282764} duration={2200} />
          </span>{" "}
          unique schemas.
          <br />
          Every other engine chokes. We don&apos;t.
        </motion.p>

        {/* CTA */}
        <motion.a
          href="#schemaless"
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 1.0 }}
          className="inline-flex items-center gap-2 bg-[var(--accent)] hover:bg-[#d63a3a] text-white font-semibold px-8 py-3 rounded-full transition-colors"
        >
          See How ↓
        </motion.a>

        {/* Speed teaser */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.6, delay: 1.3 }}
          className="mt-16 flex items-center justify-center gap-8"
        >
          {[
            { val: "86×", label: "faster than best competitor" },
            { val: "5319×", label: "faster scan vs separate schemas" },
            { val: "183×", label: "faster than Neo4j on LDBC" },
          ].map(stat => (
            <div key={stat.val} className="text-center">
              <div className="text-2xl font-bold text-[var(--accent)]">{stat.val}</div>
              <div className="text-xs text-[var(--text-secondary)] mt-1 max-w-[120px]">{stat.label}</div>
            </div>
          ))}
        </motion.div>
      </div>
    </section>
  );
}
