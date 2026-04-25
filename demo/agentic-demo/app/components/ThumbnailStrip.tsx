"use client";
import { STEPS } from "@/lib/scenario";

export default function ThumbnailStrip({
  active,
  onSelect,
}: {
  active: number;
  onSelect: (id: number) => void;
}) {
  return (
    <div className="thumb-strip">
      {STEPS.map((s) => {
        const isActive = s.id === active;
        return (
          <button
            key={s.id}
            onClick={() => onSelect(s.id)}
            className={`thumb ${isActive ? "thumb--active" : ""}`}
          >
            <div className="thumb__step">Step {s.id}</div>
            <div className="thumb__title">{s.title}</div>
          </button>
        );
      })}
    </div>
  );
}
