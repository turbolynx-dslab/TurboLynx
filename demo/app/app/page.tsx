"use client";
import { useState, useEffect, useCallback } from "react";
import { AnimatePresence, motion } from "framer-motion";
import SceneNav from "@/components/scenes/SceneNav";
import S0_Problem from "@/components/scenes/S0_Problem";
import S1_Storage from "@/components/scenes/S1_Storage";
import S2_QuerySelect from "@/components/scenes/S2_QuerySelect";
import S3_Plan from "@/components/scenes/S3_Plan";
import S4_SSRF from "@/components/scenes/S4_SSRF";
import S5_Performance from "@/components/scenes/S5_Performance";
import { QState, INIT_QSTATE } from "@/lib/query-state";

//                    Data  Storage  Query  Plan  Intermediates  Results
const SCENE_STEPS = [  1,     1,      1,     1,       1,           1   ];

export default function Home() {
  const [scene, setScene] = useState(0);
  const [step, setStep] = useState(0);
  const [direction, setDirection] = useState(1);
  const [queryState, setQueryState] = useState<QState>(JSON.parse(JSON.stringify(INIT_QSTATE)));

  const handleScene = (n: number) => {
    setDirection(n > scene ? 1 : -1);
    setScene(n);
    setStep(0);
  };

  const handleStep = (n: number) => {
    setStep(n);
  };

  const goPrev = useCallback(() => {
    if (step > 0) { setStep(step - 1); return; }
    if (scene > 0) { setDirection(-1); setScene(scene - 1); setStep(SCENE_STEPS[scene - 1] - 1); }
  }, [scene, step]);

  const goNext = useCallback(() => {
    if (step < SCENE_STEPS[scene] - 1) { setStep(step + 1); return; }
    if (scene < SCENE_STEPS.length - 1) { setDirection(1); setScene(scene + 1); setStep(0); }
  }, [scene, step]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "ArrowLeft") { e.preventDefault(); goPrev(); }
      if (e.key === "ArrowRight") { e.preventDefault(); goNext(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [goPrev, goNext]);

  const totalSteps = SCENE_STEPS[scene];

  // Render the active scene with appropriate props
  const renderScene = () => {
    const base = { step, onStep: handleStep };
    switch (scene) {
      case 0: return <S0_Problem {...base} />;
      case 1: return <S1_Storage {...base} />;
      case 2: return <S2_QuerySelect {...base} queryState={queryState} onQueryChange={setQueryState} />;
      case 3: return <S3_Plan {...base} queryState={queryState} onGoToResults={() => handleScene(5)} />;
      case 4: return <S4_SSRF {...base} queryState={queryState} />;
      case 5: return <S5_Performance {...base} queryState={queryState} />;
      default: return null;
    }
  };

  return (
    <div style={{ height: "100dvh", display: "flex", flexDirection: "column", background: "#ffffff", overflow: "hidden" }}>
      <SceneNav
        scene={scene}
        step={step}
        totalSteps={totalSteps}
        onScene={handleScene}
        onStep={handleStep}
      />
      <div style={{ flex: 1, overflow: "hidden", position: "relative" }}>
        <AnimatePresence mode="wait" custom={direction}>
          <motion.div
            key={scene}
            custom={direction}
            initial={{ opacity: 0, x: direction * 40 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: direction * -40 }}
            transition={{ duration: 0.28, ease: "easeInOut" }}
            style={{ position: "absolute", inset: 0 }}
          >
            {renderScene()}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}
