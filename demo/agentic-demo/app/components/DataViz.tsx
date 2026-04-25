"use client";
import { useState } from "react";

/** Scene 1 — HTML replacement for the canvas "bars + cards" view so the
 *  sample-node cards can be clicked for a property detail popup. */

const CAT = [
  "#6d28d9", "#0891b2", "#d97706", "#15803d", "#b91c1c",
  "#9333ea", "#0d9488", "#b45309", "#be185d", "#4338ca",
];

const TYPE_ROWS: [string, number][] = [
  ["Person",        1_482],
  ["Film",            420],
  ["Place",           276],
  ["Organisation",    198],
  ["Writer",          152],
  ["City",            118],
  ["MusicalArtist",    67],
  ["Genre",            42],
  ["Award",            38],
  ["Country",          25],
];

interface SampleNode {
  type: string;
  title: string;
  uri: string;
  properties: Record<string, string>;
}

const SAMPLES: SampleNode[] = [
  {
    type: "Film",
    title: "Inception",
    uri: "http://dbpedia.org/resource/Inception",
    properties: {
      releaseYear: "2010",
      runtime:     "148 min",
      director:    "Christopher Nolan",
      starring:    "Leonardo DiCaprio, Joseph Gordon-Levitt",
      genre:       "Science fiction",
      budget:      "$160,000,000",
      gross:       "$836,800,000",
      country:     "United States",
    },
  },
  {
    type: "Person",
    title: "Christopher Nolan",
    uri: "http://dbpedia.org/resource/Christopher_Nolan",
    properties: {
      birthDate:   "1970-07-30",
      birthPlace:  "London, England",
      occupation:  "Film director, screenwriter, producer",
      spouse:      "Emma Thomas",
      award:       "Academy Award for Best Director (2024, Oppenheimer)",
      notableWork: "The Dark Knight · Inception · Interstellar · Oppenheimer",
    },
  },
  {
    type: "Award",
    title: "Academy Award for Best Director",
    uri: "http://dbpedia.org/resource/Academy_Award_for_Best_Director",
    properties: {
      presenter:    "Academy of Motion Picture Arts and Sciences",
      country:      "United States",
      firstAwarded: "1929",
      category:     "Direction of a theatrical feature film",
    },
  },
];

export default function DataViz() {
  const [detail, setDetail] = useState<SampleNode | null>(null);
  const maxV = Math.max(...TYPE_ROWS.map((r) => r[1]));

  return (
    <div className="data-viz">
      <div className="data-viz__grid">
        {/* Left: type distribution bars */}
        <div>
          <div className="data-viz__head">Type distribution · via rdf:type edges</div>
          <div>
            {TYPE_ROWS.map(([name, value], i) => (
              <div key={name} className="data-viz__row">
                <div className="data-viz__label">{name}</div>
                <div className="data-viz__bar-wrap">
                  <div
                    className="data-viz__bar"
                    style={{
                      width: `${(value / maxV) * 100}%`,
                      background: CAT[i % CAT.length],
                    }}
                  />
                </div>
                <div className="data-viz__val">{value.toLocaleString()}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Right: sample nodes — clickable */}
        <div>
          <div className="data-viz__head">Sample nodes · click to inspect</div>
          <div className="data-viz__cards">
            {SAMPLES.map((s) => (
              <button key={s.title} className="data-viz__card" onClick={() => setDetail(s)}>
                <div className="data-viz__card-type">{s.type}</div>
                <div className="data-viz__card-title">{s.title}</div>
                <div className="data-viz__card-sub">
                  {Object.entries(s.properties).slice(0, 2).map(([k, v]) => `${k}: ${v}`).join(" · ")}
                </div>
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Node detail modal */}
      {detail && (
        <div className="query-modal-backdrop" onClick={() => setDetail(null)}>
          <div className="query-modal" onClick={(e) => e.stopPropagation()}>
            <div className="query-modal__head">
              <div>
                <div className="text-[11px] uppercase tracking-[0.08em] text-[var(--text-muted)] font-semibold">
                  NODE · {detail.type}
                </div>
                <div className="text-[15px] font-semibold text-[var(--text-primary)] mt-0.5">
                  {detail.title}
                </div>
              </div>
              <div className="ml-auto">
                <button className="query-modal__btn" onClick={() => setDetail(null)}>Close</button>
              </div>
            </div>
            <div className="query-modal__body">
              <div className="mb-3 text-[12px] text-[var(--text-muted)]">
                uri: <span className="text-[var(--text-body)]">{detail.uri}</span>
              </div>
              <div className="grid grid-cols-[160px_1fr] gap-y-2 gap-x-4 text-[13px]">
                {Object.entries(detail.properties).map(([k, v]) => (
                  <div key={k} className="contents">
                    <div className="text-[var(--text-muted)] mono">{k}</div>
                    <div className="text-[var(--text-body)]">{v}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
