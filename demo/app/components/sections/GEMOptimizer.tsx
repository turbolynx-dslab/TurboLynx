"use client";
import { useState, useRef } from "react";
import { motion, useInView, AnimatePresence } from "framer-motion";
import SectionLabel from "@/components/ui/SectionLabel";
import { Q13_GRAPHLETS, type Graphlet } from "@/lib/data";

type Step = "idle" | "analyzing" | "grouping" | "done";

function GraphletCard({ gl, step, index }: { gl: Graphlet; step: Step; index: number }) {
  const showOrder = step === "analyzing" || step === "grouping" || step === "done";
  const showGroup = step === "grouping" || step === "done";

  return (
    <motion.div
      layout
      initial={{ opacity: 0, scale: 0.9 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ delay: index * 0.04 }}
      className="bg-[var(--bg-surface)] rounded-lg border p-2.5 text-xs relative overflow-hidden"
      style={{ borderColor: showOrder ? gl.color + "80" : "var(--border)" }}
    >
      <div className="flex items-center justify-between mb-1.5">
        <span className="font-mono font-bold" style={{ color: showOrder ? gl.color : "var(--text-secondary)" }}>
          {gl.id}
        </span>
        <span className="text-[var(--text-secondary)] text-[10px]">{gl.entity}</span>
      </div>
      <div className="flex flex-wrap gap-0.5 mb-1.5">
        {gl.attrs.map(a => (
          <span key={a} className="px-1 py-0.5 rounded text-[10px] bg-[var(--bg-elevated)] text-[var(--text-secondary)]">{a}</span>
        ))}
      </div>
      <div className="text-[var(--text-secondary)] text-[10px]">{gl.rowCount.toLocaleString()} rows</div>

      {showOrder && (
        <motion.div
          initial={{ opacity: 0, y: 4 }}
          animate={{ opacity: 1, y: 0 }}
          className="mt-1.5 text-[10px] font-mono font-semibold"
          style={{ color: gl.color }}
        >
          → {gl.optimalOrder}
        </motion.div>
      )}

      {showGroup && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="absolute top-1 right-1 text-[10px] px-1 py-0.5 rounded-full"
          style={{ background: gl.color + "30", color: gl.color }}
        >
          {gl.optimalOrder === "Place-first" ? "vg₁" : "vg₂"}
        </motion.div>
      )}
    </motion.div>
  );
}

export default function GEMOptimizer() {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true, margin: "-80px" });
  const [step, setStep] = useState<Step>("idle");
  const [gemOn, setGemOn] = useState(true);

  const runStep = () => {
    if (step === "idle") {
      setStep("analyzing");
      setTimeout(() => setStep("grouping"), 2000);
      setTimeout(() => setStep("done"), 3500);
    } else {
      setStep("idle");
    }
  };

  const placefirstCount = Q13_GRAPHLETS.filter(g => g.optimalOrder === "Place-first").length;
  const filmfirstCount = Q13_GRAPHLETS.filter(g => g.optimalOrder === "Film-first").length;

  const naiveIntermediate = 1_820_000;
  const gemIntermediate   =    10_200;

  return (
    <section ref={ref} className="py-24 px-6 max-w-7xl mx-auto">
      <SectionLabel number="04" label="GEM: The Smart Optimizer" />

      <div className="grid lg:grid-cols-2 gap-16">
        <div>
          <motion.h2
            initial={{ opacity: 0, y: 16 }}
            animate={inView ? { opacity: 1, y: 0 } : {}}
            className="text-3xl md:text-4xl font-bold mb-3"
          >
            Graphlet Early Merge
          </motion.h2>
          <motion.p
            initial={{ opacity: 0 }}
            animate={inView ? { opacity: 1 } : {}}
            transition={{ delay: 0.2 }}
            className="text-[var(--text-secondary)] mb-6 text-lg"
          >
            Different graphlet schemas → different optimal join orders. GEM discovers this and groups same-order graphlets into virtual graphlets.
          </motion.p>

          {/* Query info */}
          <div className="bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl p-4 mb-6 font-mono text-xs text-[var(--text-secondary)]">
            <div className="text-[var(--accent-blue)] mb-1 text-[10px] uppercase tracking-wider">Query Q13</div>
            <div className="text-[var(--text-primary)]">MATCH (d:Person)-[:birthPlace]-&gt;(:Place &#123;country:&ldquo;USA&rdquo;&#125;),</div>
            <div className="text-[var(--text-primary)] ml-6">(d)-[:director]-&gt;(f:Film)</div>
            <div className="text-[var(--text-secondary)]">RETURN d.name, f.name</div>
          </div>

          {/* Step description */}
          <div className="space-y-3 mb-6">
            {[
              { label: "Step 1", desc: "Per-graphlet GOO finds optimal join order", active: step !== "idle" },
              { label: "Step 2", desc: `Same-order graphlets group → vg₁ (${placefirstCount} Place-first) + vg₂ (${filmfirstCount} Film-first)`, active: step === "grouping" || step === "done" },
              { label: "Step 3", desc: "One execution plan per virtual graphlet", active: step === "done" },
            ].map(s => (
              <div key={s.label} className={`flex items-start gap-3 transition-opacity ${s.active ? "opacity-100" : "opacity-30"}`}>
                <span className={`text-xs font-mono mt-0.5 ${s.active ? "text-[var(--accent)]" : "text-[var(--text-secondary)]"}`}>{s.label}</span>
                <span className="text-sm text-[var(--text-secondary)]">{s.desc}</span>
              </div>
            ))}
          </div>

          <div className="flex gap-3">
            <button onClick={runStep} className="flex items-center gap-2 bg-[var(--accent)] text-white font-semibold px-6 py-2.5 rounded-full hover:bg-[#d63a3a] transition-colors text-sm">
              {step === "idle" ? "▶ Run GEM" : "↺ Reset"}
            </button>
          </div>

          {/* GEM ON/OFF toggle & cost comparison */}
          <AnimatePresence>
            {step === "done" && (
              <motion.div
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0 }}
                className="mt-8 bg-[var(--bg-surface)] border border-[var(--border)] rounded-xl p-5"
              >
                {/* Toggle */}
                <div className="flex items-center gap-4 mb-5">
                  <span className={`text-sm ${!gemOn ? "text-[var(--accent)]" : "text-[var(--text-secondary)]"}`}>GEM OFF</span>
                  <button
                    onClick={() => setGemOn(v => !v)}
                    className={`relative w-12 h-6 rounded-full transition-colors ${gemOn ? "bg-[var(--accent-green)]" : "bg-[var(--border)]"}`}
                  >
                    <span className={`absolute top-1 w-4 h-4 rounded-full bg-white shadow transition-transform ${gemOn ? "translate-x-7" : "translate-x-1"}`} />
                  </button>
                  <span className={`text-sm ${gemOn ? "text-[var(--accent-green)]" : "text-[var(--text-secondary)]"}`}>GEM ON</span>
                </div>

                {/* Cost bars */}
                <div className="space-y-3">
                  <div>
                    <div className="flex justify-between text-xs mb-1">
                      <span className="text-[var(--text-secondary)]">Intermediate tuples</span>
                      <span className={gemOn ? "text-[var(--accent-green)] font-bold" : "text-[var(--accent)] font-bold"}>
                        {gemOn ? gemIntermediate.toLocaleString() : naiveIntermediate.toLocaleString()}
                      </span>
                    </div>
                    <div className="h-3 bg-[var(--bg-elevated)] rounded-full overflow-hidden">
                      <motion.div
                        animate={{ width: gemOn ? `${(gemIntermediate / naiveIntermediate) * 100}%` : "100%" }}
                        transition={{ duration: 0.6, ease: "easeInOut" }}
                        className="h-full rounded-full"
                        style={{ background: gemOn ? "var(--accent-green)" : "var(--accent)" }}
                      />
                    </div>
                  </div>

                  {gemOn && (
                    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="text-xs text-[var(--accent-green)] font-semibold">
                      ✓ {Math.round(naiveIntermediate / gemIntermediate)}× fewer intermediate results
                    </motion.div>
                  )}
                </div>

                <div className="mt-4 pt-4 border-t border-[var(--border)] text-xs text-[var(--text-secondary)] space-y-1">
                  <div>Compilation overhead: <span className="text-[var(--text-primary)]">−6.2%</span></div>
                  <div>Execution time: <span className="text-[var(--text-primary)]">−26.7%</span></div>
                  <div>Net speedup: <span className="text-[var(--accent)] font-bold">1.23×</span></div>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Right: graphlet grid */}
        <div>
          <div className="flex items-center justify-between mb-4">
            <span className="text-sm text-[var(--text-secondary)]">
              {Q13_GRAPHLETS.length} graphlets for Q13
            </span>
            {step !== "idle" && (
              <div className="flex gap-3 text-xs">
                <span className="flex items-center gap-1.5">
                  <span className="w-2.5 h-2.5 rounded-full bg-[#3b82f6] inline-block" />
                  Place-first ({placefirstCount})
                </span>
                <span className="flex items-center gap-1.5">
                  <span className="w-2.5 h-2.5 rounded-full bg-[#f97316] inline-block" />
                  Film-first ({filmfirstCount})
                </span>
              </div>
            )}
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
            {Q13_GRAPHLETS.map((gl, i) => (
              <GraphletCard key={gl.id} gl={gl} step={step} index={i} />
            ))}
          </div>

          {step === "done" && (
            <motion.div
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              className="mt-4 grid grid-cols-2 gap-3"
            >
              {[
                { name: "vg₁", label: "Place ⋈ Person ⋈ Film", count: placefirstCount, color: "#3b82f6" },
                { name: "vg₂", label: "Film ⋈ Person ⋈ Place", count: filmfirstCount, color: "#f97316" },
              ].map(vg => (
                <div key={vg.name} className="p-3 rounded-xl border text-sm" style={{ borderColor: vg.color + "60", background: vg.color + "10" }}>
                  <div className="font-bold" style={{ color: vg.color }}>{vg.name}</div>
                  <div className="text-xs text-[var(--text-secondary)] mt-0.5 font-mono">{vg.label}</div>
                  <div className="text-xs text-[var(--text-secondary)] mt-1">{vg.count} graphlets</div>
                </div>
              ))}
            </motion.div>
          )}
        </div>
      </div>
    </section>
  );
}
