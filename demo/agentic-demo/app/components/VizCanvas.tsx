"use client";
import { useEffect, useRef, useState } from "react";
import { VizKind, ScenarioStep } from "@/lib/scenario";
import ForceNetwork from "./ForceNetwork";
import DataViz from "./DataViz";
import ReportViz from "./ReportViz";

/** Shared palette — matches globals.css + ForceNetwork. Light canvas,
 *  zinc greys, red accent, restrained categorical hues. */
const COLOR = {
  bg: "#f8f9fa",
  card: "#ffffff",
  border: "#d4d4d8",
  borderStrong: "#a1a1aa",
  title: "#52525b",
  text: "#18181b",
  muted: "#71717a",
  accent: "#e84545",
};
const CATEGORY = [
  "#6d28d9", "#0891b2", "#d97706", "#15803d", "#b91c1c",
  "#9333ea", "#0d9488", "#b45309", "#be185d", "#4338ca",
];

function roundRect(
  ctx: CanvasRenderingContext2D,
  x: number, y: number, w: number, h: number,
  r: number, fill: boolean, stroke: boolean,
) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
  if (fill) ctx.fill();
  if (stroke) ctx.stroke();
}

function clearBg(ctx: CanvasRenderingContext2D, w: number, h: number) {
  ctx.fillStyle = COLOR.bg;
  ctx.fillRect(0, 0, w, h);
}

function drawBarsCards(ctx: CanvasRenderingContext2D, w: number, h: number) {
  clearBg(ctx, w, h);
  // Every node lives under the single `NODE` label. Type membership comes
  // from rdf:type edges — so this chart counts `(n)-[:type]->(t)` hits per
  // type URI, discovered by the agent at runtime.
  const rows: [string, number, string][] = [
    ["Person",        1_482, CATEGORY[0]],
    ["Film",            420, CATEGORY[1]],
    ["Place",           276, CATEGORY[2]],
    ["Organisation",    198, CATEGORY[3]],
    ["Writer",          152, CATEGORY[4]],
    ["City",            118, CATEGORY[5]],
    ["MusicalArtist",    67, CATEGORY[6]],
    ["Genre",            42, CATEGORY[7]],
    ["Award",            38, CATEGORY[8]],
    ["Country",          25, CATEGORY[9]],
  ];
  const pad = 32;
  const barH = 22;
  const rowG = 8;
  const titleY = pad;
  const headerH = 22;
  const chartLeft = pad;

  // Stack vertically when the panel is narrow; split when it can breathe.
  // "Enough" = at least 680px so both charts can sit side-by-side without
  // cramming. Otherwise show only the bar chart.
  const showCards = w >= 680;
  const chartW = showCards ? Math.min(w * 0.50, 460) : (w - pad * 2);

  const labelColW = 108;
  const valueColW = 54;
  const maxBarW = Math.max(40, chartW - labelColW - valueColW);
  const maxV = Math.max(...rows.map((r) => r[1]));

  ctx.font = '500 12px "Inter", -apple-system, sans-serif';
  ctx.textBaseline = "alphabetic";
  ctx.fillStyle = COLOR.title;
  ctx.fillText("Type distribution · via rdf:type edges", chartLeft, titleY);

  rows.forEach((d, i) => {
    const y = titleY + headerH + i * (barH + rowG);
    const bw = ((d[1] as number) / maxV) * maxBarW;
    ctx.font = '500 12px "Inter", -apple-system, sans-serif';
    ctx.fillStyle = COLOR.text;
    ctx.fillText(d[0] as string, chartLeft, y + 16);
    ctx.fillStyle = d[2] as string;
    roundRect(ctx, chartLeft + labelColW, y, bw || 2, barH, 4, true, false);
    ctx.fillStyle = COLOR.muted;
    ctx.font = '400 11px "JetBrains Mono", ui-monospace, monospace';
    ctx.fillText(
      (d[1] as number).toLocaleString(),
      chartLeft + labelColW + bw + 6,
      y + 16,
    );
  });

  if (!showCards) return;

  // Sample cards on the right — allocate a column starting at chartLeft + chartW + gap.
  const gap = 32;
  const cardsX = chartLeft + chartW + gap;
  const cardW  = Math.max(220, w - cardsX - pad);
  ctx.font = '500 12px "Inter", -apple-system, sans-serif';
  ctx.fillStyle = COLOR.title;
  ctx.fillText("Sample nodes", cardsX, titleY);

  const cards: [string, string, string][] = [
    ["Film",   "Inception",                         "releaseYear: 2010 · runtime: 148 min"],
    ["Person", "Christopher Nolan",                 "birthDate: 1970-07-30"],
    ["Award",  "Academy Award for Best Director",   "—"],
  ];
  const cardH = 76;
  const cardG = 12;
  cards.forEach((c, i) => {
    const y = titleY + headerH + i * (cardH + cardG);
    ctx.fillStyle = COLOR.card;
    ctx.strokeStyle = COLOR.border;
    ctx.lineWidth = 1;
    roundRect(ctx, cardsX, y, cardW, cardH, 10, true, true);
    ctx.fillStyle = COLOR.muted;
    ctx.font = '500 10px "Inter", -apple-system, sans-serif';
    ctx.fillText(c[0].toUpperCase(), cardsX + 14, y + 20);
    ctx.fillStyle = COLOR.text;
    ctx.font = '600 14px "Inter", -apple-system, sans-serif';
    ctx.fillText(c[1], cardsX + 14, y + 42);
    ctx.font = '400 11px "JetBrains Mono", ui-monospace, monospace';
    ctx.fillStyle = COLOR.muted;
    ctx.fillText(c[2], cardsX + 14, y + 62);
  });
}

function drawBeforeAfter(ctx: CanvasRenderingContext2D, w: number, h: number) {
  clearBg(ctx, w, h);
  const pad = 48;
  ctx.font = '500 13px "Inter", -apple-system, sans-serif';
  ctx.fillStyle = COLOR.title;
  ctx.fillText("Film nodes — before vs. after dedup", pad, 32);

  const pairs: [string, number, number][] = [
    ["Film total",       62_728, 62_655],
    ["Duplicates",            0,     73],
  ];
  const labelCol = 160;
  const barMax = w - pad * 2 - labelCol - 120;
  const maxV = Math.max(...pairs.map((p) => Math.max(p[1], p[2])));
  pairs.forEach((p, i) => {
    const yTop = 64 + i * 92;
    ctx.font = '500 12px "Inter", -apple-system, sans-serif';
    ctx.fillStyle = COLOR.text;
    ctx.fillText(p[0] as string, pad, yTop + 18);

    const x = pad + labelCol;
    const beforeW = ((p[1] as number) / maxV) * barMax;
    const afterW  = ((p[2] as number) / maxV) * barMax;

    ctx.fillStyle = "#e4e4e7";
    roundRect(ctx, x, yTop, beforeW || 2, 22, 4, true, false);
    ctx.fillStyle = COLOR.accent;
    roundRect(ctx, x, yTop + 32, afterW || 2, 22, 4, true, false);

    ctx.font = '400 11px "JetBrains Mono", ui-monospace, monospace';
    ctx.fillStyle = COLOR.muted;
    ctx.fillText(`before  ${(p[1] as number).toLocaleString()}`, x + (beforeW || 0) + 10, yTop + 16);
    ctx.fillText(`after   ${(p[2] as number).toLocaleString()}`, x + (afterW  || 0) + 10, yTop + 48);
  });
}
function drawSankeyHeatmap(ctx: CanvasRenderingContext2D, w: number, h: number) {
  clearBg(ctx, w, h);

  // Stack vertically: Sankey on top, heatmap below.
  const pad = 32;
  const halfH = (h - pad * 3) / 2;

  // ── Top half: Sankey ─────────────────────────────────────────
  ctx.font = '500 12px "Inter", -apple-system, sans-serif';
  ctx.fillStyle = COLOR.title;
  ctx.fillText("Sankey · community-to-community collaboration", pad, pad);

  const topY = pad + 16;
  const topH = halfH - 16;
  const N = 8;
  const seed = 17;
  const rng = (n: number) => ((Math.sin(seed + n * 9.17) + 1) * 0.5);
  const bandH = Math.max(12, (topH - (N - 1) * 4) / N);
  const bandG = 4;
  const xL = pad + 40;
  const xR = w - pad - 40;
  for (let i = 0; i < N; i++) {
    for (let j = 0; j < N; j++) {
      const y1 = topY + i * (bandH + bandG);
      const y2 = topY + j * (bandH + bandG);
      const flow = 0.8 + rng(i * 13 + j * 7) * 6;
      ctx.strokeStyle = CATEGORY[i % CATEGORY.length] + "55";
      ctx.lineWidth = flow;
      ctx.beginPath();
      ctx.moveTo(xL + 2, y1 + bandH / 2);
      ctx.bezierCurveTo(
        (xL + xR) / 2, y1 + bandH / 2,
        (xL + xR) / 2, y2 + bandH / 2,
        xR - 2, y2 + bandH / 2,
      );
      ctx.stroke();
    }
  }
  for (let i = 0; i < N; i++) {
    const y = topY + i * (bandH + bandG);
    ctx.fillStyle = CATEGORY[i % CATEGORY.length];
    roundRect(ctx, xL - 10, y, 8, bandH, 2, true, false);
    roundRect(ctx, xR + 2,  y, 8, bandH, 2, true, false);
  }

  // ── Bottom half: heatmap ─────────────────────────────────────
  const botStartY = pad * 2 + halfH;
  ctx.fillStyle = COLOR.title;
  ctx.fillText("Year × community-pair — collaborations", pad, botStartY);
  const heatY = botStartY + 16;
  const heatAvailH = halfH - 32;
  const years = 20;
  const pairs = 12;
  const heatAvailW = w - pad * 2;
  const cellW = Math.max(8, (heatAvailW - 48) / years);
  const cellH = Math.max(8, heatAvailH / pairs);
  const heatX = pad + 40;
  // community-pair row labels
  ctx.font = '400 10px "JetBrains Mono", ui-monospace, monospace';
  ctx.fillStyle = COLOR.muted;
  for (let y = 0; y < pairs; y++) {
    const ya = y % N, yb = (y * 3) % N;
    ctx.fillText(`${ya}→${yb}`, pad, heatY + y * cellH + cellH - 3);
  }
  for (let y = 0; y < pairs; y++) {
    for (let x = 0; x < years; x++) {
      const v = (Math.sin(x * 0.31 + y * 1.1) + 1) / 2;
      const alpha = 0.12 + v * 0.72;
      ctx.fillStyle = `rgba(232, 69, 69, ${alpha.toFixed(2)})`;
      ctx.fillRect(heatX + x * cellW, heatY + y * cellH, cellW - 2, cellH - 2);
    }
  }
  ctx.fillStyle = COLOR.muted;
  ctx.fillText("2005", heatX,                       heatY + pairs * cellH + 14);
  ctx.fillText("2024", heatX + (years - 1) * cellW - 10, heatY + pairs * cellH + 14);
}

function drawReport(ctx: CanvasRenderingContext2D, w: number, h: number) {
  clearBg(ctx, w, h);
  const pagesX = 60;
  const pagesY = 44;
  const pw = 180;
  const ph = 236;
  ctx.font = '500 12px "Inter", -apple-system, sans-serif';
  ctx.fillStyle = COLOR.title;
  ctx.fillText("Generated report — agentic_report.docx", pagesX, pagesY - 16);
  // Stacked pages for depth
  for (let i = 0; i < 4; i++) {
    ctx.fillStyle = COLOR.card;
    ctx.strokeStyle = COLOR.border;
    ctx.lineWidth = 1;
    roundRect(ctx, pagesX + i * 26, pagesY + i * 10, pw, ph, 8, true, true);
  }
  const topX = pagesX + 3 * 26;
  const topY = pagesY + 3 * 10;
  ctx.font = '600 14px "Inter", -apple-system, sans-serif';
  ctx.fillStyle = COLOR.text;
  ctx.fillText("Cinema Graph", topX + 16, topY + 32);
  ctx.font = '400 11px "Inter", -apple-system, sans-serif';
  ctx.fillStyle = COLOR.muted;
  ctx.fillText("Session report · 24 pages", topX + 16, topY + 50);

  const lines = [
    "1. Data overview",
    "2. Director × collaborator network",
    "3. Community detection (Louvain)",
    "4. Centrality (PageRank)",
    "5. Community flow (Sankey + heatmap)",
    "6. Report",
  ];
  ctx.font = '400 11px "Inter", -apple-system, sans-serif';
  ctx.fillStyle = COLOR.text;
  lines.forEach((l, i) => {
    ctx.fillText(l, topX + 16, topY + 80 + i * 20);
  });
}

function paint(ctx: CanvasRenderingContext2D, w: number, h: number, kind: VizKind) {
  switch (kind) {
    case "bars+cards":     return drawBarsCards(ctx, w, h);
    case "sankey+heatmap": return drawSankeyHeatmap(ctx, w, h);
    case "report":         return drawReport(ctx, w, h);
    // force-directed variants are handled by <ForceNetwork> — skip here.
    default:
      clearBg(ctx, w, h);
  }
}

export default function VizCanvas({
  step,
  loading = false,
}: {
  step: ScenarioStep | null;
  loading?: boolean;
}) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [size, setSize] = useState<{ w: number; h: number }>({ w: 800, h: 540 });

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const obs = new ResizeObserver(() => {
      const r = el.getBoundingClientRect();
      setSize({ w: Math.max(320, r.width), h: Math.max(240, r.height) });
    });
    obs.observe(el);
    const r = el.getBoundingClientRect();
    setSize({ w: Math.max(320, r.width), h: Math.max(240, r.height) });
    return () => obs.disconnect();
  }, []);

  const viz = step?.viz;
  const usesForce =
    viz === "force-directed" ||
    viz === "force-directed+community" ||
    viz === "force-directed+centrality";

  useEffect(() => {
    if (!step || usesForce) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = size.w * dpr;
    canvas.height = size.h * dpr;
    canvas.style.width = `${size.w}px`;
    canvas.style.height = `${size.h}px`;
    const ctx = canvas.getContext("2d")!;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    paint(ctx, size.w, size.h, step.viz);
  }, [step, size, usesForce]);

  return (
    <div ref={wrapRef} className="absolute inset-0">
      {step == null ? (
        <div className="viz-empty">
          <div className="viz-empty__inner">
            <div className="viz-empty__eyebrow">Agentic DB Demo</div>
            <div className="viz-empty__title">
              Ask a question to begin.
            </div>
            <div className="viz-empty__body">
              Pick one of the suggested prompts below, or type your own. The
              agent will query TurboLynx, generate any analysis code it needs,
              and render the result here.
            </div>
          </div>
        </div>
      ) : viz === "bars+cards" ? (
        <DataViz />
      ) : viz === "report" ? (
        <ReportViz />
      ) : usesForce ? (
        <ForceNetwork
          mode={
            viz === "force-directed"
              ? "plain"
              : viz === "force-directed+community"
              ? "community"
              : "centrality"
          }
          width={size.w}
          height={size.h}
        />
      ) : (
        <canvas ref={canvasRef} className="w-full h-full block" />
      )}

      {loading && (
        <div className="viz-loading">
          <div className="viz-loading__inner">
            <div className="viz-loading__spin" />
            <div>Generating visualisation…</div>
          </div>
        </div>
      )}
    </div>
  );
}
