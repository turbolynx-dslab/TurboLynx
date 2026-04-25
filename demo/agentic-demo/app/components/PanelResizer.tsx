"use client";
import { useEffect, useRef } from "react";

interface Props {
  /** "left"  — drags the divider between left and center zones.
   *  "right" — drags the divider between center and right zones. */
  side: "left" | "right";
  onDelta: (dx: number) => void;
  onCommit: () => void;
}

export default function PanelResizer({ side, onDelta, onCommit }: Props) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    let dragging = false;
    let startX = 0;

    const onDown = (e: PointerEvent) => {
      dragging = true;
      startX = e.clientX;
      el.setPointerCapture(e.pointerId);
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
    };
    const onMove = (e: PointerEvent) => {
      if (!dragging) return;
      const dx = e.clientX - startX;
      if (dx !== 0) {
        onDelta(dx);
        startX = e.clientX;
      }
    };
    const onUp = (e: PointerEvent) => {
      if (!dragging) return;
      dragging = false;
      el.releasePointerCapture(e.pointerId);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      onCommit();
    };
    el.addEventListener("pointerdown", onDown);
    el.addEventListener("pointermove", onMove);
    el.addEventListener("pointerup", onUp);
    el.addEventListener("pointercancel", onUp);
    return () => {
      el.removeEventListener("pointerdown", onDown);
      el.removeEventListener("pointermove", onMove);
      el.removeEventListener("pointerup", onUp);
      el.removeEventListener("pointercancel", onUp);
    };
  }, [onDelta, onCommit]);

  return (
    <div
      ref={ref}
      className={`resizer resizer--${side}`}
      role="separator"
      aria-orientation="vertical"
      title="Drag to resize"
    />
  );
}
