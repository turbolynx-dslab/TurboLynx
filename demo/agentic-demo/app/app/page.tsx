"use client";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { STEPS, ScenarioStep, ChatTurn, ToolKind } from "@/lib/scenario";
import ChatTimeline from "@/components/ChatTimeline";
import VizCanvas from "@/components/VizCanvas";
import DBStatePanel from "@/components/DBStatePanel";
import PromptBar from "@/components/PromptBar";
import PanelResizer from "@/components/PanelResizer";
import QueryModal from "@/components/QueryModal";

const LS_LEFT = "agentic.leftW";
const LS_RIGHT = "agentic.rightW";
const MIN_SIDE = 240;
const MAX_SIDE = 720;
const MIN_CENTER = 360;
// The DB state panel reflects the loaded workspace, not the conversation —
// it's always populated even before the user asks anything. Numbers match
// the small cinema subgraph at /data/dbpedia-cinema.
const BASE_DB = {
  nodes: 2_841,
  edges: 9_712,
  filmNodes: 420,
  recentQueries: [] as string[],
};

const DB_KINDS: ToolKind[] = [
  "query_cypher", "mutate_cypher",
  "list_types", "describe_type", "sample_type",
  "list_labels", "describe_label", "sample_label",
];
const firstLine = (s: string) => s.trim().split("\n")[0] || s;

export default function Home() {
  // `displayStep` is the step whose viz is currently rendered.
  const [displayStep, setDisplayStep] = useState(0);
  // `displayTurns` is the chat the user sees. The scripted turns from each
  // step are not dumped in — they're revealed one at a time with a thinking
  // bubble between, so the agent feels like it's reasoning through the
  // question in real time.
  const [displayTurns, setDisplayTurns] = useState<ChatTurn[]>([]);
  const [thinking, setThinking] = useState(false);
  const [vizLoading, setVizLoading] = useState(false);
  const [modalQuery, setModalQuery] = useState<string | null>(null);
  // Track whether a reveal sequence is running so chip clicks don't stack.
  const revealLock = useRef(false);
  const timerIds = useRef<number[]>([]);

  const [leftW, setLeftW]   = useState<number>(380);
  const [rightW, setRightW] = useState<number>(320);
  const rootRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    try {
      const l = parseInt(localStorage.getItem(LS_LEFT)  || "", 10);
      const r = parseInt(localStorage.getItem(LS_RIGHT) || "", 10);
      if (Number.isFinite(l)) setLeftW(Math.min(MAX_SIDE, Math.max(MIN_SIDE, l)));
      if (Number.isFinite(r)) setRightW(Math.min(MAX_SIDE, Math.max(MIN_SIDE, r)));
    } catch {}
  }, []);
  const persist = useCallback(() => {
    try {
      localStorage.setItem(LS_LEFT, String(leftW));
      localStorage.setItem(LS_RIGHT, String(rightW));
    } catch {}
  }, [leftW, rightW]);

  const current: ScenarioStep | null = useMemo(
    () => (displayStep > 0 ? STEPS.find((s) => s.id === displayStep) || null : null),
    [displayStep],
  );

  const canvasSubtitle = useMemo(() => {
    if (!current) return "Awaiting prompt";
    switch (current.viz) {
      case "bars+cards":                 return "Schema probe · list_types result";
      case "force-directed":              return "Force-directed network · written by agent";
      case "force-directed+community":    return "Louvain overlay · Python in sandbox";
      case "force-directed+centrality":   return "PageRank overlay · Python in sandbox";
      case "sankey+heatmap":              return "Sankey + heatmap · written by agent";
      case "report":                      return "Generated docx report";
      default:                            return "";
    }
  }, [current]);
  const prev: ScenarioStep | null = useMemo(() => {
    if (displayStep <= 1) return null;
    return STEPS.find((s) => s.id === displayStep - 1) || null;
  }, [displayStep]);

  const chatTurns: ChatTurn[] = useMemo(() => {
    const out = [...displayTurns];
    if (thinking) out.push({ role: "agent", text: "__THINKING__" });
    return out;
  }, [displayTurns, thinking]);

  const recentQueries = useMemo(() => {
    const acc: { label: string; full: string }[] = [];
    for (const t of displayTurns) {
      if (!t.tools) continue;
      for (const tool of t.tools) {
        if (!DB_KINDS.includes(tool.kind)) continue;
        const full = tool.payload && tool.payload.trim().length > 0 ? tool.payload : tool.title;
        acc.push({ label: firstLine(full), full });
      }
    }
    return acc.slice().reverse().slice(0, 8);
  }, [displayTurns]);

  const clearTimers = () => {
    timerIds.current.forEach((id) => clearTimeout(id));
    timerIds.current = [];
  };
  const schedule = (fn: () => void, delay: number) => {
    const id = window.setTimeout(fn, delay);
    timerIds.current.push(id);
  };
  useEffect(() => () => clearTimers(), []);

  /** Reveal a scripted step one turn at a time, with thinking bubbles between
   *  agent turns. Pre-populates any prior steps' chat so the user can see
   *  history, then animates the new step in. */
  const revealStep = useCallback((targetStep: number) => {
    if (revealLock.current) return;
    revealLock.current = true;
    clearTimers();

    const target = STEPS.find((s) => s.id === targetStep);
    if (!target) { revealLock.current = false; return; }

    // Prior steps appear in full, immediately. Only the target animates.
    const historical: ChatTurn[] = [];
    for (const s of STEPS) {
      if (s.id >= targetStep) break;
      historical.push(...s.chat);
    }
    setDisplayTurns(historical);
    // Keep viz on previous step's state until the reveal finishes.
    if (targetStep > 1) {
      setDisplayStep(targetStep - 1);
    } else {
      setDisplayStep(0);
    }
    setThinking(false);

    let t = 0;
    const revealed: ChatTurn[] = [...historical];
    // Track whether this is the first agent turn in the step — the very
    // first thinking spell after a user prompt is the longest, since the
    // model has to parse the question and decide on a plan. Subsequent
    // turns are already in flow and respond quicker.
    let firstAgentTurn = true;

    for (let i = 0; i < target.chat.length; i++) {
      const turn = target.chat[i];
      if (turn.role === "user") {
        // User turns land almost instantly after the keypress.
        t += 180;
        revealed.push(turn);
        const snapshot = revealed.slice();
        schedule(() => setDisplayTurns(snapshot), t);
      } else {
        // Pause after the previous bubble before the dots turn on.
        const pauseAfterPrev = firstAgentTurn ? 480 : 380;
        t += pauseAfterPrev;
        schedule(() => setThinking(true), t);

        // Latency model, tuned to feel deliberate but keep the recorded
        // demo tight:
        //   - 1600ms base per turn (plan + streaming a paragraph)
        //   - +750ms per tool call (invocation + result decode)
        //   - +500ms extra on the first agent turn of a step (planning cost)
        const toolCount = turn.tools?.length ?? 0;
        const workDelay = 1600 + toolCount * 750 + (firstAgentTurn ? 500 : 0);
        t += workDelay;
        revealed.push(turn);
        const snapshot = revealed.slice();
        schedule(() => {
          setThinking(false);
          setDisplayTurns(snapshot);
        }, t);

        firstAgentTurn = false;
      }
    }

    // After the last turn, a brief "rendering" overlay so the visualization
    // doesn't snap in abruptly.
    const finalRender = 650;
    schedule(() => setVizLoading(true), t);
    t += finalRender;
    schedule(() => setDisplayStep(targetStep), t);
    schedule(() => setVizLoading(false), t + 200);
    schedule(() => { revealLock.current = false; }, t + 250);
  }, []);

  const handleFreeForm = useCallback((text: string) => {
    // Free-form prompts don't advance the scripted flow — just acknowledge.
    if (revealLock.current) return;
    revealLock.current = true;
    clearTimers();
    setDisplayTurns((prev) => [...prev, { role: "user", text }]);
    schedule(() => setThinking(true), 140);
    schedule(() => {
      setThinking(false);
      setDisplayTurns((prev) => [
        ...prev,
        {
          role: "agent",
          text:
            "This preview runs a scripted scenario. Live-agent mode — real LLM + MCP + Python sandbox — will route your question through TurboLynx and return answers here.",
        },
      ]);
      revealLock.current = false;
    }, 1400);
  }, []);

  const resizeLeft = useCallback((dx: number) => {
    setLeftW((w) => {
      const rootW = rootRef.current ? rootRef.current.clientWidth : 1600;
      const next = Math.max(MIN_SIDE, Math.min(MAX_SIDE, w + dx));
      return Math.min(next, rootW - rightW - 12 - MIN_CENTER);
    });
  }, [rightW]);

  const resizeRight = useCallback((dx: number) => {
    setRightW((w) => {
      const rootW = rootRef.current ? rootRef.current.clientWidth : 1600;
      const next = Math.max(MIN_SIDE, Math.min(MAX_SIDE, w - dx));
      return Math.min(next, rootW - leftW - 12 - MIN_CENTER);
    });
  }, [leftW]);

  return (
    <div className="page-frame">
      <div className="topbar">
        <a
          href={process.env.NEXT_PUBLIC_BASE_PATH ? "/demo-video/" : "/"}
          className="brand-chip"
          title="Back to TurboLynx demos"
        >
          <span aria-hidden>←</span>
          <span>turbolynx.io</span>
        </a>
        <div className="font-semibold text-[13px] tracking-tight text-[var(--text-primary)]">
          TurboLynx <span className="text-[var(--text-muted)] font-normal">· Agentic DB Demo</span>
        </div>
        <a
          href={process.env.NEXT_PUBLIC_BASE_PATH ? "/demo-agentic/intro/" : "/intro/"}
          className="brand-chip ml-auto"
          title="Intro slides"
        >
          <span>Intro slides</span>
          <span aria-hidden>↗</span>
        </a>
      </div>

      <div
        ref={rootRef}
        className="zone-root"
        style={{
          gridTemplateColumns: `${leftW}px 6px 1fr 6px ${rightW}px`,
        }}
      >
        <div className="zone-left zone-card">
          <ChatTimeline turns={chatTurns} />
        </div>
        <div className="zone-left-rz">
          <PanelResizer side="left" onDelta={resizeLeft} onCommit={persist} />
        </div>

        <div className="zone-center zone-card">
          <div className="px-4 py-3 border-b border-[var(--border)]">
            <div className="text-[10.5px] uppercase tracking-[0.08em] text-[var(--text-muted)] font-semibold">
              Agent Canvas
            </div>
            <div className="text-sm mt-0.5 text-[var(--text-primary)] font-medium">
              {canvasSubtitle}
            </div>
          </div>
          <div className="flex-1 relative min-h-0">
            <VizCanvas step={current} loading={vizLoading} />
          </div>
        </div>

        <div className="zone-right-rz">
          <PanelResizer side="right" onDelta={resizeRight} onCommit={persist} />
        </div>
        <div className="zone-right zone-card">
          <DBStatePanel
            state={current ? current.dbState : BASE_DB}
            prevState={prev?.dbState ?? null}
            queries={recentQueries}
            onQueryClick={setModalQuery}
          />
        </div>

        <div className="zone-bottom zone-card">
          <PromptBar onJumpToStep={revealStep} onFreeFormSubmit={handleFreeForm} />
        </div>
      </div>

      <QueryModal text={modalQuery} onClose={() => setModalQuery(null)} />
    </div>
  );
}
