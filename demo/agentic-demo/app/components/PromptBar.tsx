"use client";
import { useEffect, useRef, useState } from "react";
import { STEPS } from "@/lib/scenario";

interface Props {
  /** called when the user presses Enter on text that matches a scripted step */
  onJumpToStep: (id: number) => void;
  /** called when the user presses Enter on text that doesn't match anything */
  onFreeFormSubmit: (text: string) => void;
}

/** Match free-form input against scripted prompts so the demo still
 *  progresses without a real agent. Returns a step id or null. */
function matchScriptedStep(text: string): number | null {
  const q = text.trim().toLowerCase();
  if (!q) return null;
  // Exact-prefix fast-path — chip click fills the exact prompt.
  for (const s of STEPS) {
    if (q === s.userPrompt.toLowerCase().trim()) return s.id;
  }
  // Token overlap fallback so near-paraphrases still progress.
  let best: { id: number; score: number } | null = null;
  for (const s of STEPS) {
    const p = s.userPrompt.toLowerCase();
    const tokens = q.split(/[\s\.,?!]+/).filter((t) => t.length > 1);
    let hits = 0;
    for (const t of tokens) if (p.includes(t)) hits++;
    const score = tokens.length ? hits / tokens.length : 0;
    if (score >= 0.55 && (!best || score > best.score)) {
      best = { id: s.id, score };
    }
  }
  return best ? best.id : null;
}

export default function PromptBar({ onJumpToStep, onFreeFormSubmit }: Props) {
  const [value, setValue] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  // Drop synthetic scenario prompts that start with "(" — those represent
  // agent-initiated turns, not user questions.
  const suggestions = STEPS.filter((s) => s.userPrompt && !s.userPrompt.startsWith("("));

  const fillChip = (text: string) => {
    setValue(text);
    inputRef.current?.focus();
  };

  const submit = () => {
    const v = value.trim();
    if (!v) return;
    const hit = matchScriptedStep(v);
    if (hit) {
      onJumpToStep(hit);
    } else {
      onFreeFormSubmit(v);
    }
    setValue("");
  };

  return (
    <div className="prompt-wrap">
      <div className="suggest-row thin-scrollbar">
        {suggestions.map((s) => (
          <button
            key={s.id}
            className="suggest-chip"
            onClick={() => fillChip(s.userPrompt)}
            title="Click to load into prompt. Press Enter to send."
          >
            {s.userPrompt.length > 48 ? s.userPrompt.slice(0, 45) + "…" : s.userPrompt}
          </button>
        ))}
      </div>
      <div className="prompt-input">
        <input
          ref={inputRef}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") submit();
          }}
          placeholder="Ask anything about the cinema graph…"
        />
        <button
          className="prompt-send"
          onClick={submit}
          disabled={!value.trim()}
          aria-label="Send"
          title="Send"
        >
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M5 12h14M13 5l7 7-7 7" />
          </svg>
        </button>
      </div>
    </div>
  );
}
