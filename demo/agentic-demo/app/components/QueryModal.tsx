"use client";
import { useEffect } from "react";

interface Props {
  text: string | null;
  onClose: () => void;
}

export default function QueryModal({ text, onClose }: Props) {
  useEffect(() => {
    if (!text) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [text, onClose]);

  if (!text) return null;

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
    } catch {}
  };

  return (
    <div className="query-modal-backdrop" onClick={onClose}>
      <div
        className="query-modal"
        role="dialog"
        aria-modal="true"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="query-modal__head">
          <div className="text-[11px] uppercase tracking-[0.08em] text-[var(--text-muted)] font-semibold">
            Executed query
          </div>
          <div className="ml-auto flex items-center gap-2">
            <button className="query-modal__btn" onClick={copy} title="Copy">Copy</button>
            <button className="query-modal__btn" onClick={onClose} title="Close">Close</button>
          </div>
        </div>
        <pre className="query-modal__body">{text}</pre>
      </div>
    </div>
  );
}
