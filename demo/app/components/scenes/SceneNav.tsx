"use client";

const SCENES = [
  { id: 0, emoji: "⚡", label: "Problem"     },
  { id: 1, emoji: "🧩", label: "CGC"         },
  { id: 2, emoji: "🔍", label: "Query"       },
  { id: 3, emoji: "⚙️", label: "GEM"         },
  { id: 4, emoji: "🗜️", label: "SSRF"        },
  { id: 5, emoji: "🏆", label: "Performance" },
];

interface SceneNavProps {
  scene: number;
  step: number;
  totalSteps: number;
  onScene: (n: number) => void;
  onStep: (n: number) => void;
}

export default function SceneNav({ scene, step, totalSteps, onScene, onStep }: SceneNavProps) {
  const isFirst = step === 0;
  const isLast  = step === totalSteps - 1;

  const goPrev = () => {
    if (!isFirst) { onStep(step - 1); return; }
    if (scene > 0) { onScene(scene - 1); }
  };

  const goNext = () => {
    if (!isLast) { onStep(step + 1); return; }
    if (scene < SCENES.length - 1) { onScene(scene + 1); }
  };

  return (
    <div style={{
      height: "52px",
      borderBottom: "1px solid #27272a",
      background: "#131316",
      flexShrink: 0,
    }}>
      <div style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        width: "100%",
        maxWidth: 1440,
        margin: "0 auto",
        padding: "0 48px",
        height: "100%",
      }}>
        {/* Brand */}
        <button
          onClick={() => onScene(0)}
          style={{ fontWeight: 700, fontSize: 15, color: "#f4f4f5", letterSpacing: "-0.02em",
            background: "none", border: "none", cursor: "pointer", padding: 0 }}>
          Turbo<span style={{ color: "#e84545" }}>Lynx</span>
        </button>

        {/* Scene tabs */}
        <div style={{ display: "flex", gap: 4 }}>
          {SCENES.map(s => (
            <button
              key={s.id}
              onClick={() => onScene(s.id)}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 6,
                padding: "6px 14px",
                borderRadius: 8,
                border: "none",
                cursor: "pointer",
                fontSize: 13,
                fontWeight: 500,
                transition: "all 0.2s",
                background: s.id === scene ? "#e84545" : "transparent",
                color: s.id === scene ? "#fff" : "#71717a",
              }}
            >
              <span>{s.emoji}</span>
              <span>{s.label}</span>
            </button>
          ))}
        </div>

        {/* Prev / Step dots / Next */}
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <button
            onClick={goPrev}
            disabled={scene === 0 && isFirst}
            style={{
              padding: "6px 14px",
              borderRadius: 8,
              border: "1px solid #27272a",
              background: "transparent",
              color: scene === 0 && isFirst ? "#3f3f46" : "#a1a1aa",
              cursor: scene === 0 && isFirst ? "not-allowed" : "pointer",
              fontSize: 13,
              fontWeight: 500,
            }}
          >
            ← Back
          </button>

          {/* Step dots */}
          <div style={{ display: "flex", gap: 5 }}>
            {Array.from({ length: totalSteps }).map((_, i) => (
              <button
                key={i}
                onClick={() => onStep(i)}
                style={{
                  width: i === step ? 20 : 8,
                  height: 8,
                  borderRadius: 4,
                  border: "none",
                  cursor: "pointer",
                  transition: "all 0.25s",
                  background: i === step ? "#e84545" : i < step ? "#e8454566" : "#27272a",
                }}
              />
            ))}
          </div>

          <button
            onClick={goNext}
            disabled={scene === SCENES.length - 1 && isLast}
            style={{
              padding: "6px 14px",
              borderRadius: 8,
              border: "none",
              background: scene === SCENES.length - 1 && isLast ? "#27272a" : "#e84545",
              color: scene === SCENES.length - 1 && isLast ? "#3f3f46" : "#fff",
              cursor: scene === SCENES.length - 1 && isLast ? "not-allowed" : "pointer",
              fontSize: 13,
              fontWeight: 600,
            }}
          >
            Next →
          </button>
        </div>
      </div>
    </div>
  );
}
