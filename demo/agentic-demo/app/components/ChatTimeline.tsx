"use client";
import { useEffect, useRef, useState } from "react";
import { ChatTurn, ToolCall, ToolKind } from "@/lib/scenario";

const BADGE_CLASS: Record<ToolKind, string> = {
  list_types: "tool-badge--db",
  describe_type: "tool-badge--db",
  sample_type: "tool-badge--db",
  list_labels: "tool-badge--db",
  describe_label: "tool-badge--db",
  sample_label: "tool-badge--db",
  query_cypher: "tool-badge--db",
  mutate_cypher: "tool-badge--write",
  python: "tool-badge--py",
};
const BADGE_LABEL: Record<ToolKind, string> = {
  list_types: "DB",
  describe_type: "DB",
  sample_type: "DB",
  list_labels: "DB",
  describe_label: "DB",
  sample_label: "DB",
  query_cypher: "DB",
  mutate_cypher: "DB·WRITE",
  python: "PY",
};

function ToolCard({ t }: { t: ToolCall }) {
  const [open, setOpen] = useState(false);
  const hasBody = Boolean(t.payload || t.resultHint);
  return (
    <div className="tool-card">
      <div className="tool-head" onClick={() => hasBody && setOpen((v) => !v)}>
        <span className={`tool-badge ${BADGE_CLASS[t.kind]}`}>{BADGE_LABEL[t.kind]}</span>
        <span className="tool-title">{t.title}</span>
        {hasBody && <span className="tool-caret">{open ? "▾" : "▸"}</span>}
      </div>
      {open && (
        <div className="tool-body">
          {t.payload && <pre className="tool-code">{t.payload}</pre>}
          {t.resultHint && <div className="tool-result">→ {t.resultHint}</div>}
        </div>
      )}
    </div>
  );
}

function ThinkingBubble() {
  return (
    <div className="chat-turn chat-turn--agent">
      <div className="chat-avatar chat-avatar--agent" title="TurboLynx Agent">TL</div>
      <div className="chat-bubble chat-bubble--agent">
        <div className="chat-role">Agent</div>
        <div className="thinking-dots" aria-label="Agent is thinking">
          <span /><span /><span />
        </div>
      </div>
    </div>
  );
}

export default function ChatTimeline({ turns }: { turns: ChatTurn[] }) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const endRef = useRef<HTMLDivElement>(null);

  // Stick to the latest bubble whenever the number of turns changes AND
  // whenever the last turn's text changes (e.g. thinking dots appearing or
  // being replaced by a real agent response).
  const lastTurnId = turns.length
    ? `${turns.length}:${turns[turns.length - 1].role}:${turns[turns.length - 1].text.slice(0, 40)}`
    : "0";

  useEffect(() => {
    const el = endRef.current;
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "end" });
      return;
    }
    const scroll = scrollRef.current;
    if (scroll) scroll.scrollTo({ top: scroll.scrollHeight, behavior: "smooth" });
  }, [lastTurnId]);

  return (
    <div className="flex flex-col h-full min-h-0">
      <div className="px-4 py-3 border-b border-[var(--border)]">
        <div className="text-[10.5px] uppercase tracking-[0.08em] text-[var(--text-muted)] font-semibold">
          Conversation
        </div>
        <div className="text-sm mt-0.5 text-[var(--text-primary)] font-medium">
          Cinema graph · exploration
        </div>
      </div>
      <div ref={scrollRef} className="flex-1 overflow-y-auto min-h-0">
        {turns.length === 0 ? (
          <div className="chat-empty">
            <div className="chat-empty__title">No conversation yet.</div>
            <div className="chat-empty__body">
              Click a suggested question below to load it, then press{" "}
              <kbd className="chat-empty__kbd">Enter</kbd> to run it.
              Or type your own.
            </div>
          </div>
        ) : (
          <div className="chat-list">
            {turns.map((t, i) => {
              if (t.role === "agent" && t.text === "__THINKING__") {
                return <ThinkingBubble key={`think-${i}`} />;
              }
              return (
                <div
                  key={i}
                  className={`chat-turn ${t.role === "user" ? "chat-turn--user" : "chat-turn--agent"}`}
                >
                  {t.role === "agent" && (
                    <div className="chat-avatar chat-avatar--agent" title="TurboLynx Agent">
                      TL
                    </div>
                  )}
                  <div className={`chat-bubble ${t.role === "user" ? "chat-bubble--user" : "chat-bubble--agent"}`}>
                    <div className="chat-role">{t.role === "user" ? "You" : "Agent"}</div>
                    <div>{t.text}</div>
                    {t.tools && t.tools.length > 0 && (
                      <div className="tool-stack">
                        {t.tools.map((tool, j) => (
                          <ToolCard key={j} t={tool} />
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
            <div ref={endRef} />
          </div>
        )}
      </div>
    </div>
  );
}
