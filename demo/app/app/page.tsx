"use client";
import { useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import SceneNav from "@/components/scenes/SceneNav";
import S0_Problem from "@/components/scenes/S0_Problem";
import S1_CGC from "@/components/scenes/S1_CGC";
import S2_Query from "@/components/scenes/S2_Query";
import S3_GEM from "@/components/scenes/S3_GEM";
import S4_SSRF from "@/components/scenes/S4_SSRF";
import S5_Performance from "@/components/scenes/S5_Performance";

const SCENE_STEPS = [3, 2, 2, 2, 2, 1];

const SCENES = [S0_Problem, S1_CGC, S2_Query, S3_GEM, S4_SSRF, S5_Performance];

export default function Home() {
  const [scene, setScene] = useState(0);
  const [step, setStep] = useState(0);
  const [direction, setDirection] = useState(1);

  const handleScene = (n: number) => {
    setDirection(n > scene ? 1 : -1);
    setScene(n);
    setStep(0);
  };

  const handleStep = (n: number) => {
    setStep(n);
  };

  const SceneComponent = SCENES[scene];
  const totalSteps = SCENE_STEPS[scene];

  return (
    <div style={{ height: "100dvh", display: "flex", flexDirection: "column", background: "#09090b", overflow: "hidden" }}>
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
            <SceneComponent step={step} onStep={handleStep} />
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}
