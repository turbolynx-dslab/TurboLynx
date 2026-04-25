"use client";
import { useCallback, useEffect, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";

/** Intro deck for the Agentic DB demo — navigable with arrow keys,
 *  Space, or the on-page buttons. Ends with a "Start demo" CTA. */

const HOME_HREF = process.env.NEXT_PUBLIC_BASE_PATH ? "/demo-agentic/" : "/";
const LANDING_HREF = process.env.NEXT_PUBLIC_BASE_PATH ? "/demo-video/" : "/";

interface Slide {
  key: string;
  eyebrow: string;
  render: () => React.ReactNode;
}

const SLIDES: Slide[] = [
  {
    key: "title",
    eyebrow: "TurboLynx · 2026",
    render: () => (
      <>
        <h1 className="slide-title">Agentic Database</h1>
        <p className="slide-lede">
          An LLM agent, a schemaless graph, and code generated on the fly.
        </p>
        <div className="slide-meta">
          Not demonstrating a database feature — a division of labour between
          the database and an autonomous agent.
        </div>
      </>
    ),
  },
  {
    key: "concept",
    eyebrow: "Concept",
    render: () => (
      <>
        <h2 className="slide-h2">What is an agentic database?</h2>
        <ul className="slide-list">
          <li>
            <strong>Not a passive store.</strong> It is the structure over
            which an agent plans, retrieves, and reasons.
          </li>
          <li>
            Plans, reasoning paths, tool routing — <strong>inherently
            graph-shaped.</strong>
          </li>
          <li>
            A schemaless graph engine is a natural fit: heterogeneous
            entities, free-form relations, no rigid schema to fight with.
          </li>
        </ul>
      </>
    ),
  },
  {
    key: "roles",
    eyebrow: "Roles",
    render: () => (
      <>
        <h2 className="slide-h2">Separation of roles</h2>
        <div className="slide-roles">
          <div className="role-card">
            <div className="role-card__eyebrow">User</div>
            <div className="role-card__title">Natural language</div>
            <div className="role-card__body">
              Asks questions. No Cypher, no Python, no schema knowledge.
            </div>
          </div>
          <div className="role-card role-card--accent">
            <div className="role-card__eyebrow">Agent</div>
            <div className="role-card__title">Plans, writes code</div>
            <div className="role-card__body">
              Decides the strategy. Writes Cypher. Writes Python. Picks the
              visualization.
            </div>
          </div>
          <div className="role-card">
            <div className="role-card__eyebrow">TurboLynx</div>
            <div className="role-card__title">Graph engine</div>
            <div className="role-card__body">
              Schema. Reads. Writes. That's it.
            </div>
          </div>
        </div>
        <p className="slide-footnote">
          The agent also has a Python sandbox with networkx, scikit-learn,
          plotly, and the usual visualization stack. Nothing of the analysis
          happens inside the database.
        </p>
      </>
    ),
  },
  {
    key: "dataset",
    eyebrow: "Dataset",
    render: () => (
      <>
        <h2 className="slide-h2">A schemaless cinema graph</h2>
        <div className="stat-strip">
          <div className="stat-strip__item">
            <div className="stat-strip__n">2,841</div>
            <div className="stat-strip__lbl">nodes</div>
          </div>
          <div className="stat-strip__item">
            <div className="stat-strip__n">9,712</div>
            <div className="stat-strip__lbl">edges</div>
          </div>
          <div className="stat-strip__item">
            <div className="stat-strip__n">26</div>
            <div className="stat-strip__lbl">types</div>
          </div>
        </div>
        <ul className="slide-list">
          <li>
            DBpedia movie subgraph, loaded under a <strong>single{" "}
            <code>NODE</code> label</strong>.
          </li>
          <li>
            No <code>Film</code> or <code>Person</code> label at the catalog
            level. Type membership is expressed as an edge:
            <code className="slide-inline-code">
              (:NODE)-[:type]-&gt;(:NODE &#123;uri:".../Film"&#125;)
            </code>.
          </li>
          <li>
            The agent discovers structure at runtime by walking those edges —
            and sees the schema heterogeneity head-on.
          </li>
        </ul>
      </>
    ),
  },
  {
    key: "ui",
    eyebrow: "UI",
    render: () => (
      <>
        <h2 className="slide-h2">Three panels · three roles</h2>
        <div className="slide-ui">
          <div className="slide-ui__zone">
            <div className="slide-ui__eyebrow">Left</div>
            <div className="slide-ui__title">Conversation</div>
            <div className="slide-ui__body">
              User turns, agent reasoning, tool calls inline.
            </div>
          </div>
          <div className="slide-ui__zone slide-ui__zone--center">
            <div className="slide-ui__eyebrow">Center</div>
            <div className="slide-ui__title">Agent Canvas</div>
            <div className="slide-ui__body">
              Everything the agent renders. Not provided by the DB.
            </div>
          </div>
          <div className="slide-ui__zone">
            <div className="slide-ui__eyebrow">Right</div>
            <div className="slide-ui__title">DB State</div>
            <div className="slide-ui__body">
              Node / edge counters. Stream of queries TurboLynx actually ran.
            </div>
          </div>
        </div>
        <p className="slide-footnote">
          If something appears in the center panel, the agent made it.
        </p>
      </>
    ),
  },
  {
    key: "watchfor",
    eyebrow: "What to watch for",
    render: () => (
      <>
        <h2 className="slide-h2">Keep count of two things.</h2>
        <div className="watch-table">
          <div className="watch-col">
            <div className="watch-col__head">Database does</div>
            <ol className="watch-list">
              <li>Report schema</li>
              <li>Read the graph</li>
              <li>Return structured results</li>
            </ol>
          </div>
          <div className="watch-col watch-col--accent">
            <div className="watch-col__head">Agent does</div>
            <ol className="watch-list">
              <li>Discover types</li>
              <li>Build a director-collaborator network</li>
              <li>Detect communities (Louvain)</li>
              <li>Rank centrality (PageRank)</li>
              <li>Sankey + temporal heatmap</li>
              <li>Compile an audit-trail report</li>
            </ol>
          </div>
        </div>
        <p className="slide-footnote slide-footnote--big">
          Three DB capabilities. <strong>Six analyses.</strong> Everything in
          the second column is code the agent wrote this session.
        </p>
      </>
    ),
  },
];

export default function Intro() {
  const [idx, setIdx] = useState(0);
  const [dir, setDir] = useState(1);

  const go = useCallback(
    (delta: number) => {
      setDir(delta);
      setIdx((i) => Math.max(0, Math.min(SLIDES.length - 1, i + delta)));
    },
    [],
  );

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "ArrowRight" || e.key === " ") { e.preventDefault(); go(1); }
      else if (e.key === "ArrowLeft") { e.preventDefault(); go(-1); }
      else if (e.key === "Home")    { setIdx(0); }
      else if (e.key === "End")     { setIdx(SLIDES.length - 1); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [go]);

  const slide = SLIDES[idx];
  const isLast = idx === SLIDES.length - 1;

  return (
    <div className="intro-root">
      {/* Topbar */}
      <div className="intro-top">
        <a href={LANDING_HREF} className="brand-chip" title="Back to TurboLynx demos">
          <span aria-hidden>←</span>
          <span>turbolynx.io</span>
        </a>
        <div className="intro-top__title">
          TurboLynx <span className="text-[var(--text-muted)] font-normal">· Agentic DB Demo · Intro</span>
        </div>
        <a className="intro-top__skip" href={HOME_HREF}>Skip to demo →</a>
      </div>

      {/* Stage */}
      <div className="intro-stage">
        <AnimatePresence mode="wait" initial={false} custom={dir}>
          <motion.div
            key={slide.key}
            custom={dir}
            initial={{ opacity: 0, x: dir * 24 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -dir * 24 }}
            transition={{ duration: 0.28, ease: [0.2, 0.6, 0.2, 1] }}
            className="slide"
          >
            <div className="slide-eyebrow">{slide.eyebrow}</div>
            {slide.render()}
          </motion.div>
        </AnimatePresence>
      </div>

      {/* Navigation */}
      <div className="intro-nav">
        <button
          className="intro-nav__btn"
          onClick={() => go(-1)}
          disabled={idx === 0}
          aria-label="Previous"
        >
          ←
        </button>

        <div className="intro-nav__dots">
          {SLIDES.map((s, i) => (
            <button
              key={s.key}
              onClick={() => { setDir(i > idx ? 1 : -1); setIdx(i); }}
              className={`intro-dot ${i === idx ? "intro-dot--active" : ""}`}
              aria-label={`Slide ${i + 1}`}
              title={s.eyebrow}
            />
          ))}
        </div>

        {!isLast ? (
          <button
            className="intro-nav__btn"
            onClick={() => go(1)}
            aria-label="Next"
          >
            →
          </button>
        ) : (
          <a href={HOME_HREF} className="intro-nav__cta">Start demo →</a>
        )}
      </div>
    </div>
  );
}
