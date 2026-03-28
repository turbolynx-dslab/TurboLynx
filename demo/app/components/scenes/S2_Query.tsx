"use client";
import React, { useState, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";

interface Props { step: number; onStep: (n: number) => void; }

// Render "GL_{p}-1" as GL<sub>p</sub>-1 (HTML)
function GLLabel({ id, style }: { id: string; style?: React.CSSProperties }) {
  const m = id.match(/^GL_\{(.+)\}-(.+)$/);
  if (!m) return <span style={style}>{id}</span>;
  return <span style={style}>GL<sub style={{ fontSize: "0.75em" }}>{m[1]}</sub>-{m[2]}</span>;
}

// SVG version of GL subscript rendering
function SVGGLText({ id, ...props }: { id: string } & React.SVGProps<SVGTextElement>) {
  const m = id.match(/^GL_\{(.+)\}-(.+)$/);
  if (!m) return <text {...props}>{id}</text>;
  return <text {...props}>GL<tspan baselineShift="sub" fontSize="0.75em">{m[1]}</tspan>-{m[2]}</text>;
}

// ─── Data ──────────────────────────────────────────────────────────────────

interface PNode { name: string; city?: string; }

const PERSON_GLS = [
  { id: "GL_{p}-1", color: "#DC2626",
    schema: ["birthDate","birthPlace","team","award","height","weight"],
    nodes: [
      { name: "G. Buffon", city: "Carrara" },
      { name: "M. Jordan", city: "Brooklyn" },
      { name: "S. Williams", city: "Saginaw" },
      { name: "Usain Bolt", city: "Sherwood" },
      { name: "Pelé", city: "Três Corações" },
    ] as PNode[] },
  { id: "GL_{p}-2", color: "#2563EB",
    schema: ["birthDate","deathDate","birthPlace","award","occupation","almaMater"],
    nodes: [
      { name: "Marie Curie", city: "Warsaw" },
      { name: "A. Einstein", city: "Ulm" },
      { name: "F. Nightingale", city: "Florence" },
      { name: "Ada Lovelace", city: "London" },
      { name: "N. Tesla", city: "Smiljan" },
    ] as PNode[] },
  { id: "GL_{p}-3", color: "#7C3AED",
    schema: ["birthDate","birthPlace","team","award"],
    nodes: [
      { name: "Oliver Kahn", city: "Karlsruhe" },
      { name: "Petr Čech", city: "Plzeň" },
      { name: "M. Neuer", city: "Gelsenkirchen" },
      { name: "I. Casillas", city: "Madrid" },
    ] as PNode[] },
  { id: "GL_{p}-4", color: "#D97706",
    schema: ["birthDate","deathDate","occupation","spouse"],
    nodes: [
      { name: "Cato" }, { name: "Aristotle" }, { name: "Cleopatra" }, { name: "Da Vinci" },
    ] as PNode[] },
  { id: "GL_{p}-5", color: "#059669",
    schema: ["birthDate","deathDate","occupation","spouse","genre"],
    nodes: [
      { name: "Ferdinand I" }, { name: "Kate Forsyth" }, { name: "Napoleon" },
      { name: "Chopin" }, { name: "Genghis Khan" },
    ] as PNode[] },
];

const CITY_GLS = [
  { id: "GL_{c}-1", color: "#0891B2",
    schema: ["population","area","country","utcOffset"],
    nodes: ["Carrara","Warsaw","Ulm","Florence","London","Smiljan"] },
  { id: "GL_{c}-2", color: "#0E7490",
    schema: ["population","elevation","country","areaTotal"],
    nodes: ["Brooklyn","Saginaw","Três Corações","Karlsruhe","Plzeň","Gelsenkirchen","Madrid"] },
  { id: "GL_{c}-3", color: "#06B6D4",
    schema: ["area","utcOffset","foundingDate"],
    nodes: ["Sherwood"] },
];

const COUNTRY_GLS = [
  { id: "GL_{co}-1", color: "#B45309",
    schema: ["population","gdp","continent"],
    nodes: ["Italy","Poland","Germany","UK","Croatia","Czech Rep.","Spain"] },
  { id: "GL_{co}-2", color: "#166534",
    schema: ["population","area","continent"],
    nodes: ["USA","Brazil","Jamaica"] },
];

const CITY_TO_COUNTRY: Record<string, string> = {
  "Carrara": "Italy", "Warsaw": "Poland", "Ulm": "Germany",
  "Florence": "Italy", "London": "UK", "Smiljan": "Croatia",
  "Brooklyn": "USA", "Saginaw": "USA", "Três Corações": "Brazil",
  "Karlsruhe": "Germany", "Plzeň": "Czech Rep.", "Gelsenkirchen": "Germany",
  "Madrid": "Spain", "Sherwood": "Jamaica",
};

// Optional predicates per variable (rdf:type handles the "label")
const P_PREDS = [
  { value: "", label: "\u2014" },
  { value: "team", label: "p.team" },
  { value: "award", label: "p.award" },
  { value: "occupation", label: "p.occupation" },
  { value: "almaMater", label: "p.almaMater" },
  { value: "deathDate", label: "p.deathDate" },
  { value: "spouse", label: "p.spouse" },
];
const C_PREDS = [
  { value: "", label: "\u2014" },
  { value: "area", label: "c.area" },
  { value: "elevation", label: "c.elevation" },
  { value: "utcOffset", label: "c.utcOffset" },
];
const CO_PREDS = [
  { value: "", label: "\u2014" },
  { value: "population", label: "co.population" },
  { value: "gdp", label: "co.gdp" },
];

// ─── Compute active / pruned + result rows ────────────────────────────────

interface GLState { active: boolean; reason: string | null; }
interface ResultRow { person: string; city: string; country: string; pGL: string; pColor: string; }

function computeAll(pAttr: string, cAttr: string, coAttr: string) {
  // Person GL: must have :birthPlace edge + optional predicate
  const p = new Map<string, GLState>();
  for (const gl of PERSON_GLS) {
    if (!gl.schema.includes("birthPlace"))
      p.set(gl.id, { active: false, reason: "no :birthPlace" });
    else if (pAttr && !gl.schema.includes(pAttr))
      p.set(gl.id, { active: false, reason: `no ${pAttr}` });
    else p.set(gl.id, { active: true, reason: null });
  }

  // City GL: must have :country edge + optional predicate
  const c = new Map<string, GLState>();
  for (const gl of CITY_GLS) {
    if (!gl.schema.includes("country"))
      c.set(gl.id, { active: false, reason: "no :country" });
    else if (cAttr && !gl.schema.includes(cAttr))
      c.set(gl.id, { active: false, reason: `no ${cAttr}` });
    else c.set(gl.id, { active: true, reason: null });
  }

  // Country GL: optional predicate only
  const co = new Map<string, GLState>();
  for (const gl of COUNTRY_GLS) {
    if (coAttr && !gl.schema.includes(coAttr))
      co.set(gl.id, { active: false, reason: `no ${coAttr}` });
    else co.set(gl.id, { active: true, reason: null });
  }

  // Active cities & countries
  const activeCities = new Set<string>();
  for (const gl of CITY_GLS) {
    if (!c.get(gl.id)?.active) continue;
    gl.nodes.forEach(n => activeCities.add(n));
  }
  const activeCountries = new Set<string>();
  for (const gl of COUNTRY_GLS) {
    if (!co.get(gl.id)?.active) continue;
    gl.nodes.forEach(n => activeCountries.add(n));
  }

  // Result rows
  const rows: ResultRow[] = [];
  for (const gl of PERSON_GLS) {
    if (!p.get(gl.id)?.active) continue;
    for (const node of gl.nodes) {
      if (!node.city) continue;
      if (!activeCities.has(node.city)) continue;
      const country = CITY_TO_COUNTRY[node.city];
      if (!country || !activeCountries.has(country)) continue;
      rows.push({ person: node.name, city: node.city, country, pGL: gl.id, pColor: gl.color });
    }
  }

  const pruned = [...p.values(), ...c.values(), ...co.values()].filter(v => !v.active).length;
  const total = PERSON_GLS.length + CITY_GLS.length + COUNTRY_GLS.length;
  return { p, c, co, rows, pruned, active: total - pruned };
}

// ─── GL Card ──────────────────────────────────────────────────────────────

function GLCard({ id, color, schema, nodeNames, active, reason, hl, highlight }: {
  id: string; color: string; schema: string[]; nodeNames: string[];
  active: boolean; reason: string | null; hl: string[]; highlight?: boolean;
}) {
  return (
    <motion.div
      animate={{ opacity: active ? 1 : 0.3, scale: highlight ? 1.02 : 1 }}
      transition={{ duration: 0.25 }}
      style={{
        padding: "6px 10px", borderRadius: 8,
        border: highlight
          ? `2px solid ${color}`
          : `1.5px solid ${active ? color + "40" : "#e5e7eb"}`,
        background: highlight ? color + "12" : active ? color + "06" : "#fafafa",
        borderLeft: `4px solid ${active ? color : "#d4d4d8"}`,
        boxShadow: highlight ? `0 0 12px ${color}30` : "none",
      }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 2 }}>
        <span style={{
          fontSize: 13, fontWeight: 800, fontFamily: "monospace",
          color: active ? color : "#a1a1aa",
        }}>{id}</span>
        <span style={{ fontSize: 12, color: "#9ca3af", fontFamily: "monospace" }}>
          {nodeNames.length}
        </span>
        {reason && (
          <span style={{
            fontSize: 11, fontFamily: "monospace", color: "#ef4444",
            background: "#fef2f2", padding: "1px 5px", borderRadius: 3,
            border: "1px solid #fecaca",
          }}>{reason}</span>
        )}
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 2, marginBottom: 2 }}>
        {schema.map(attr => {
          const isHl = hl.includes(attr);
          return (
            <span key={attr} style={{
              fontSize: 10, fontFamily: "monospace", padding: "1px 4px", borderRadius: 2,
              background: isHl ? (active ? color + "18" : "#f3f4f6") : "#f3f4f6",
              color: isHl ? (active ? color : "#a1a1aa") : "#6b7280",
              fontWeight: isHl ? 700 : 400,
            }}>{attr}</span>
          );
        })}
      </div>
      <div style={{
        fontSize: 11, fontFamily: "monospace", color: active ? "#52525b" : "#c4c4c4",
        lineHeight: 1.3, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
      }}>
        {nodeNames.join(", ")}
      </div>
    </motion.div>
  );
}

// ─── Small components ─────────────────────────────────────────────────────

function PredSel({ value, opts, onChange, accent }: {
  value: string; opts: { value: string; label: string }[];
  onChange: (v: string) => void; accent: string;
}) {
  return (
    <select value={value} onChange={e => onChange(e.target.value)} style={{
      fontSize: 13, fontFamily: "monospace", fontWeight: 600,
      color: value ? accent : "#9ca3af",
      background: value ? accent + "0a" : "#f5f5f5",
      border: `1.5px solid ${value ? accent + "40" : "#d4d4d8"}`,
      borderRadius: 4, padding: "1px 5px", cursor: "pointer", outline: "none",
    }}>
      {opts.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
  );
}

// ─── Graph Layout (precomputed) ───────────────────────────────────────────

interface NPos {
  key: string; name: string; x: number; y: number;
  glId: string; glColor: string;
  city?: string; country?: string;
}
interface GLBound {
  id: string; color: string; x: number; y: number; w: number; h: number;
}

function buildLayout() {
  const H = 600, GAP = 5, LBL = 15;

  const pBounds: GLBound[] = [];
  const persons: NPos[] = [];
  const pGLH = (H - (PERSON_GLS.length - 1) * GAP) / PERSON_GLS.length;
  PERSON_GLS.forEach((gl, gi) => {
    const y0 = gi * (pGLH + GAP);
    pBounds.push({ id: gl.id, color: gl.color, x: 15, y: y0, w: 265, h: pGLH });
    const rows = Math.ceil(gl.nodes.length / 2);
    const rowH = (pGLH - LBL) / Math.max(rows, 1);
    gl.nodes.forEach((n, ni) => {
      persons.push({
        key: `p-${n.name}`, name: n.name,
        x: 55 + (ni % 2) * 120,
        y: y0 + LBL + Math.floor(ni / 2) * rowH + rowH / 2,
        glId: gl.id, glColor: gl.color, city: n.city,
      });
    });
  });

  const cBounds: GLBound[] = [];
  const cities: NPos[] = [];
  const cGLH = (H - (CITY_GLS.length - 1) * GAP) / CITY_GLS.length;
  CITY_GLS.forEach((gl, gi) => {
    const y0 = gi * (cGLH + GAP);
    cBounds.push({ id: gl.id, color: gl.color, x: 395, y: y0, w: 195, h: cGLH });
    const rowH = (cGLH - LBL) / Math.max(gl.nodes.length, 1);
    gl.nodes.forEach((city, ci) => {
      cities.push({
        key: `c-${city}`, name: city,
        x: 490, y: y0 + LBL + ci * rowH + rowH / 2,
        glId: gl.id, glColor: gl.color, country: CITY_TO_COUNTRY[city],
      });
    });
  });

  const coBounds: GLBound[] = [];
  const countries: NPos[] = [];
  const coGLH = (H - (COUNTRY_GLS.length - 1) * GAP) / COUNTRY_GLS.length;
  COUNTRY_GLS.forEach((gl, gi) => {
    const y0 = gi * (coGLH + GAP);
    coBounds.push({ id: gl.id, color: gl.color, x: 710, y: y0, w: 220, h: coGLH });
    const rowH = (coGLH - LBL) / Math.max(gl.nodes.length, 1);
    gl.nodes.forEach((cty, ci) => {
      countries.push({
        key: `co-${cty}`, name: cty,
        x: 820, y: y0 + LBL + ci * rowH + rowH / 2,
        glId: gl.id, glColor: gl.color,
      });
    });
  });

  return { persons, cities, countries, pBounds, cBounds, coBounds };
}

const LAYOUT = buildLayout();
const CITY_POS = new Map(LAYOUT.cities.map(c => [c.name, c]));
const COUNTRY_POS = new Map(LAYOUT.countries.map(c => [c.name, c]));

// ─── Query Visualizer (Step 0) ────────────────────────────────────────────

interface VisualizerProps {
  pAttr: string; cAttr: string; coAttr: string;
  setPAttr: (v: string) => void; setCAttr: (v: string) => void; setCoAttr: (v: string) => void;
}

function QueryVisualizer({ pAttr, cAttr, coAttr, setPAttr, setCAttr, setCoAttr }: VisualizerProps) {
  const [selRow, setSelRow] = useState<number | null>(null);

  const state = useMemo(() => computeAll(pAttr, cAttr, coAttr), [pAttr, cAttr, coAttr]);

  // Highlight: which GLs / cities / countries are selected
  const selData = selRow !== null ? state.rows[selRow] : null;
  const selGL = selData?.pGL ?? null;
  const selCity = selData?.city ?? null;
  const selCountry = selData?.country ?? null;

  // Find which city GL contains the selected city
  const selCityGL = selCity
    ? CITY_GLS.find(gl => gl.nodes.includes(selCity))?.id ?? null : null;
  // Find which country GL contains the selected country
  const selCountryGL = selCountry
    ? COUNTRY_GLS.find(gl => gl.nodes.includes(selCountry))?.id ?? null : null;

  const kw = { color: "#2563EB", fontWeight: 700 } as const;
  const fix = { color: "#52525b" } as const;
  const dim = { color: "#71717a", fontWeight: 600 } as const;
  const typ = { color: "#9333EA", fontWeight: 600 } as const;
  const typBg = { ...typ, background: "#9333EA10", padding: "1px 6px", borderRadius: 3 } as const;

  return (
    <div style={{ height: "100%", display: "flex", gap: 8, overflow: "hidden" }}>

      {/* ── Left: Query + Results (stacked) ── */}
      <div style={{
        width: "28%", flexShrink: 0,
        display: "flex", flexDirection: "column", gap: 8, overflow: "hidden",
      }}>
        {/* Query box */}
        <div style={{
          flexShrink: 0, background: "#f8f9fa", border: "1px solid #d4d4d8",
          borderRadius: 10, padding: "10px 16px",
          fontFamily: "monospace", fontSize: 15, lineHeight: 1.9,
        }}>
          <div>
            <span style={kw}>MATCH</span>{"  "}
            <span style={fix}>{"(p) "}</span>
            <span style={{ fontWeight: 600, color: "#7C3AED" }}>-[:birthPlace]-&gt;</span>
            <span style={fix}>{" (c)"}</span>
          </div>
          <div>
            {"      "}
            <span style={fix}>{"(c) "}</span>
            <span style={{ fontWeight: 600, color: "#B45309" }}>-[:country]-&gt;</span>
            <span style={fix}>{" (co)"}</span>
          </div>
          <div>
            <span style={kw}>WHERE</span>{"  "}
            <span style={typ}>p.type</span><span style={fix}> = </span>
            <span style={typBg}>{`"dbo:Person"`}</span>
          </div>
          <div>
            {"  "}<span style={dim}>AND</span>{"  "}
            <span style={typ}>c.type</span><span style={fix}> = </span>
            <span style={typBg}>{`"dbo:City"`}</span>
          </div>
          <div>
            {"  "}<span style={dim}>AND</span>{"  "}
            <span style={typ}>co.type</span><span style={fix}> = </span>
            <span style={typBg}>{`"dbo:Country"`}</span>
          </div>
          <div>
            {"  "}<span style={dim}>AND</span>{"  "}
            <PredSel value={pAttr} opts={P_PREDS} onChange={v => { setPAttr(v); setSelRow(null); }} accent="#DC2626" />
            <span style={{ ...fix, opacity: pAttr ? 1 : 0.3 }}> IS NOT NULL</span>
          </div>
          <div>
            {"  "}<span style={dim}>AND</span>{"  "}
            <PredSel value={cAttr} opts={C_PREDS} onChange={v => { setCAttr(v); setSelRow(null); }} accent="#0891B2" />
            <span style={{ ...fix, opacity: cAttr ? 1 : 0.3 }}> IS NOT NULL</span>
          </div>
          <div>
            {"  "}<span style={dim}>AND</span>{"  "}
            <PredSel value={coAttr} opts={CO_PREDS} onChange={v => { setCoAttr(v); setSelRow(null); }} accent="#B45309" />
            <span style={{ ...fix, opacity: coAttr ? 1 : 0.3 }}> IS NOT NULL</span>
          </div>
          <div>
            <span style={kw}>RETURN</span>{"  "}
            <span style={fix}>p.name, c.name, co.name</span>
          </div>
        </div>

        {/* Arrow between query and results */}
        <div style={{
          flexShrink: 0, display: "flex", alignItems: "center", justifyContent: "center",
          padding: "2px 0",
        }}>
          <svg width="24" height="24" viewBox="0 0 24 24">
            <path d="M12 4 L12 16" stroke="#3B82F6" strokeWidth="2" fill="none" />
            <polygon points="6,15 12,22 18,15" fill="#3B82F6" />
          </svg>
        </div>

        {/* Results table */}
        <div style={{
          flex: 1, minHeight: 0, background: "#fff", border: "1px solid #d4d4d8",
          borderRadius: 10, overflow: "hidden", display: "flex", flexDirection: "column",
        }}>
          <div style={{
            padding: "6px 14px", background: "#f4f5f7", borderBottom: "1px solid #e5e7eb",
            display: "flex", alignItems: "center", justifyContent: "space-between",
          }}>
            <span style={{ fontSize: 14, fontFamily: "monospace", fontWeight: 700, color: "#374151" }}>
              Results
            </span>
            <span style={{
              fontSize: 14, fontFamily: "monospace", fontWeight: 600,
              color: "#059669", background: "#ecfdf5", padding: "2px 8px", borderRadius: 4,
            }}>
              {state.rows.length} rows
            </span>
          </div>
          <div style={{
            display: "grid", gridTemplateColumns: "2fr 1.5fr 1.5fr",
            padding: "4px 14px", borderBottom: "1px solid #e5e7eb",
            fontSize: 13, fontFamily: "monospace", fontWeight: 700, color: "#9ca3af",
            textTransform: "uppercase", letterSpacing: "0.04em",
          }}>
            <span>p.name</span><span>c.name</span><span>co.name</span>
          </div>
          <div style={{ flex: 1, overflowY: "auto" }}>
            {state.rows.length === 0 && (
              <div style={{ padding: "14px", fontSize: 14, color: "#9ca3af", fontFamily: "monospace", textAlign: "center" }}>
                No results
              </div>
            )}
            {state.rows.map((r, i) => {
              const isSel = selRow === i;
              return (
                <div key={i}
                  onClick={() => setSelRow(isSel ? null : i)}
                  style={{
                    display: "grid", gridTemplateColumns: "2fr 1.5fr 1.5fr",
                    padding: "4px 14px",
                    fontSize: 14, fontFamily: "monospace",
                    color: isSel ? "#18181b" : "#374151",
                    background: isSel ? r.pColor + "14" : i % 2 === 0 ? "#fff" : "#fafbfc",
                    borderLeft: `3px solid ${isSel ? r.pColor : "transparent"}`,
                    cursor: "pointer",
                    transition: "background 0.15s",
                    fontWeight: isSel ? 600 : 400,
                  }}
                  onMouseEnter={e => { if (!isSel) e.currentTarget.style.background = "#f0f1f3"; }}
                  onMouseLeave={e => { if (!isSel) e.currentTarget.style.background = i % 2 === 0 ? "#fff" : "#fafbfc"; }}
                >
                  <span style={{ color: isSel ? r.pColor : undefined }}>{r.person}</span>
                  <span>{r.city}</span>
                  <span>{r.country}</span>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* ── Right: Graph Canvas (full height) ── */}
      <div style={{
        flex: 1, minWidth: 0, background: "#fafbfc",
        border: "1px solid #e5e7eb", borderRadius: 10, overflow: "hidden",
      }}>
        <svg viewBox="0 -20 950 620" preserveAspectRatio="xMidYMid meet"
          style={{ width: "100%", height: "100%" }}>
          <defs>
            <marker id="ah-bp" markerWidth="6" markerHeight="5" refX="5" refY="2.5" orient="auto">
              <polygon points="0,0 6,2.5 0,5" fill="#7C3AED" opacity="0.6" />
            </marker>
            <marker id="ah-ct" markerWidth="6" markerHeight="5" refX="5" refY="2.5" orient="auto">
              <polygon points="0,0 6,2.5 0,5" fill="#B45309" opacity="0.6" />
            </marker>
            <marker id="ah-sel" markerWidth="7" markerHeight="6" refX="6" refY="3" orient="auto">
              <polygon points="0,0 7,3 0,6" fill="#18181b" />
            </marker>
          </defs>

          {/* Column headers */}
          <text x={147} y={-6} fontSize={15} fontFamily="monospace" fontWeight={700}
            fill="#3B82F6" textAnchor="middle">p</text>
          <text x={492} y={-6} fontSize={15} fontFamily="monospace" fontWeight={700}
            fill="#0891B2" textAnchor="middle">c</text>
          <text x={820} y={-6} fontSize={15} fontFamily="monospace" fontWeight={700}
            fill="#B45309" textAnchor="middle">co</text>

          {/* Edge type labels — centered between columns */}
          <text x={337} y={-6} fontSize={13} fontFamily="monospace" fontWeight={600}
            fill="#7C3AED" textAnchor="middle">:birthPlace</text>
          <text x={652} y={-6} fontSize={13} fontFamily="monospace" fontWeight={600}
            fill="#B45309" textAnchor="middle">:country</text>

          {/* GL boundaries — person */}
          {LAYOUT.pBounds.map(b => {
            const s = state.p.get(b.id);
            const active = s?.active ?? false;
            const isSel = selGL === b.id;
            return (
              <g key={b.id}>
                <rect x={b.x} y={b.y} width={b.w} height={b.h} rx={6}
                  fill={isSel ? b.color + "10" : active ? b.color + "05" : "#f5f5f5"}
                  stroke={isSel ? b.color : active ? b.color + "30" : "#e0e0e0"}
                  strokeWidth={isSel ? 1.5 : 1} strokeDasharray={isSel ? "" : "4 3"} />
                <SVGGLText id={b.id} x={b.x + 6} y={b.y + 14} fontSize={14} fontFamily="monospace"
                  fontWeight={700} fill={active ? b.color : "#b4b4b4"} />
                {!active && s?.reason && (
                  <text x={b.x + b.w - 4} y={b.y + 14} fontSize={12} fontFamily="monospace"
                    fill="#ef4444" textAnchor="end">{s.reason}</text>
                )}
              </g>
            );
          })}
          {/* GL boundaries — city */}
          {LAYOUT.cBounds.map(b => {
            const s = state.c.get(b.id);
            const active = s?.active ?? false;
            const isSel = selCityGL === b.id;
            return (
              <g key={b.id}>
                <rect x={b.x} y={b.y} width={b.w} height={b.h} rx={6}
                  fill={isSel ? b.color + "10" : active ? b.color + "05" : "#f5f5f5"}
                  stroke={isSel ? b.color : active ? b.color + "30" : "#e0e0e0"}
                  strokeWidth={isSel ? 1.5 : 1} strokeDasharray={isSel ? "" : "4 3"} />
                <SVGGLText id={b.id} x={b.x + 6} y={b.y + 14} fontSize={14} fontFamily="monospace"
                  fontWeight={700} fill={active ? b.color : "#b4b4b4"} />
                {!active && s?.reason && (
                  <text x={b.x + b.w - 4} y={b.y + 14} fontSize={12} fontFamily="monospace"
                    fill="#ef4444" textAnchor="end">{s.reason}</text>
                )}
              </g>
            );
          })}
          {/* GL boundaries — country */}
          {LAYOUT.coBounds.map(b => {
            const s = state.co.get(b.id);
            const active = s?.active ?? false;
            const isSel = selCountryGL === b.id;
            return (
              <g key={b.id}>
                <rect x={b.x} y={b.y} width={b.w} height={b.h} rx={6}
                  fill={isSel ? b.color + "10" : active ? b.color + "05" : "#f5f5f5"}
                  stroke={isSel ? b.color : active ? b.color + "30" : "#e0e0e0"}
                  strokeWidth={isSel ? 1.5 : 1} strokeDasharray={isSel ? "" : "4 3"} />
                <SVGGLText id={b.id} x={b.x + 6} y={b.y + 14} fontSize={14} fontFamily="monospace"
                  fontWeight={700} fill={active ? b.color : "#b4b4b4"} />
                {!active && s?.reason && (
                  <text x={b.x + b.w - 4} y={b.y + 14} fontSize={12} fontFamily="monospace"
                    fill="#ef4444" textAnchor="end">{s.reason}</text>
                )}
              </g>
            );
          })}

          {/* Edges: person → city (:birthPlace) */}
          {LAYOUT.persons.map(pn => {
            if (!pn.city) return null;
            const cn = CITY_POS.get(pn.city);
            if (!cn) return null;
            const pOk = state.p.get(pn.glId)?.active ?? false;
            const cOk = state.c.get(cn.glId)?.active ?? false;
            const edgeOk = pOk && cOk;
            const isSel = selData?.person === pn.name;
            return (
              <path key={`e-${pn.key}`}
                d={`M${pn.x + 8},${pn.y} C${pn.x + 70},${pn.y} ${cn.x - 70},${cn.y} ${cn.x - 8},${cn.y}`}
                fill="none"
                stroke={isSel ? pn.glColor : edgeOk ? "#7C3AED" : "#e0e0e0"}
                strokeWidth={isSel ? 3 : 1.5}
                opacity={isSel ? 1 : edgeOk ? 0.3 : 0.1}
                markerEnd={isSel ? "url(#ah-sel)" : edgeOk ? "url(#ah-bp)" : undefined}
              />
            );
          })}

          {/* Edges: city → country (:country) */}
          {LAYOUT.cities.map(cn => {
            if (!cn.country) return null;
            const co = COUNTRY_POS.get(cn.country);
            if (!co) return null;
            const cOk = state.c.get(cn.glId)?.active ?? false;
            const coOk = state.co.get(co.glId)?.active ?? false;
            const edgeOk = cOk && coOk;
            const isSel = selData?.city === cn.name && selData?.country === cn.country;
            return (
              <path key={`e-${cn.key}`}
                d={`M${cn.x + 8},${cn.y} C${cn.x + 70},${cn.y} ${co.x - 70},${co.y} ${co.x - 8},${co.y}`}
                fill="none"
                stroke={isSel ? cn.glColor : edgeOk ? "#B45309" : "#e0e0e0"}
                strokeWidth={isSel ? 3 : 1.5}
                opacity={isSel ? 1 : edgeOk ? 0.3 : 0.1}
                markerEnd={isSel ? "url(#ah-sel)" : edgeOk ? "url(#ah-ct)" : undefined}
              />
            );
          })}

          {/* Person nodes */}
          {LAYOUT.persons.map(pn => {
            const active = state.p.get(pn.glId)?.active ?? false;
            const isSel = selData?.person === pn.name;
            const idx = isSel ? (selRow ?? -1) : state.rows.findIndex(r => r.person === pn.name);
            return (
              <g key={pn.key} style={{ cursor: active && idx >= 0 ? "pointer" : "default" }}
                onClick={() => { if (idx >= 0) setSelRow(selRow === idx ? null : idx); }}>
                {isSel && <circle cx={pn.x} cy={pn.y} r={10}
                  fill="none" stroke={pn.glColor} strokeWidth={1.5} opacity={0.35} />}
                <circle cx={pn.x} cy={pn.y} r={isSel ? 7 : 5}
                  fill={isSel ? pn.glColor : active ? pn.glColor : "#d4d4d8"}
                  opacity={active ? 1 : 0.3} />
                <text x={pn.x + 8} y={pn.y + 3.5}
                  fontSize={isSel ? 14 : 12} fontFamily="monospace"
                  fontWeight={isSel ? 700 : 500}
                  fill={isSel ? "#18181b" : active ? "#374151" : "#c4c4c4"}>
                  {pn.name}
                </text>
              </g>
            );
          })}

          {/* City nodes */}
          {LAYOUT.cities.map(cn => {
            const active = state.c.get(cn.glId)?.active ?? false;
            const isSel = selData?.city === cn.name;
            return (
              <g key={cn.key}>
                {isSel && <circle cx={cn.x} cy={cn.y} r={10}
                  fill="none" stroke={cn.glColor} strokeWidth={1.5} opacity={0.35} />}
                <circle cx={cn.x} cy={cn.y} r={isSel ? 7 : 5}
                  fill={isSel ? cn.glColor : active ? cn.glColor : "#d4d4d8"}
                  opacity={active ? 1 : 0.3} />
                <text x={cn.x + 8} y={cn.y + 3.5}
                  fontSize={isSel ? 14 : 12} fontFamily="monospace"
                  fontWeight={isSel ? 700 : 500}
                  fill={isSel ? "#18181b" : active ? "#374151" : "#c4c4c4"}>
                  {cn.name}
                </text>
              </g>
            );
          })}

          {/* Country nodes */}
          {LAYOUT.countries.map(co => {
            const active = state.co.get(co.glId)?.active ?? false;
            const isSel = selData?.country === co.name;
            return (
              <g key={co.key}>
                {isSel && <circle cx={co.x} cy={co.y} r={10}
                  fill="none" stroke={co.glColor} strokeWidth={1.5} opacity={0.35} />}
                <circle cx={co.x} cy={co.y} r={isSel ? 7 : 5}
                  fill={isSel ? co.glColor : active ? co.glColor : "#d4d4d8"}
                  opacity={active ? 1 : 0.3} />
                <text x={co.x + 8} y={co.y + 3.5}
                  fontSize={isSel ? 14 : 12} fontFamily="monospace"
                  fontWeight={isSel ? 700 : 500}
                  fill={isSel ? "#18181b" : active ? "#374151" : "#c4c4c4"}>
                  {co.name}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
    </div>
  );
}

// ─── Plan Diagram (Step 1) ────────────────────────────────────────────────

interface PlanNode {
  op: string; color: string; detail?: string; rows?: string; time?: string;
  children?: PlanNode[];
}

// SVG tree layout
const CW = 180, CH_PLAN = 62, HG = 16, VG = 32;

interface LNode {
  op: string; color: string; detail?: string; rows?: string; time?: string;
  x: number; y: number; parentIdx: number;
}

function layoutTree(root: PlanNode): LNode[] {
  const result: LNode[] = [];
  const wCache = new Map<PlanNode, number>();
  function getW(n: PlanNode): number {
    if (wCache.has(n)) return wCache.get(n)!;
    const w = !n.children?.length
      ? CW
      : Math.max(CW, n.children.map(getW).reduce((a, b) => a + b, 0) + (n.children.length - 1) * HG);
    wCache.set(n, w);
    return w;
  }
  function walk(n: PlanNode, left: number, top: number, pi: number) {
    const sw = getW(n);
    const idx = result.length;
    result.push({
      op: n.op, color: n.color, detail: n.detail, rows: n.rows, time: n.time,
      x: left + sw / 2, y: top, parentIdx: pi,
    });
    if (n.children?.length) {
      const cws = n.children.map(getW);
      const total = cws.reduce((a, b) => a + b, 0) + (cws.length - 1) * HG;
      let cx = left + (sw - total) / 2;
      n.children.forEach((c, i) => { walk(c, cx, top + CH_PLAN + VG, idx); cx += cws[i] + HG; });
    }
  }
  walk(root, 0, 0, -1);
  return result;
}

function PlanDiagram({ root }: { root: PlanNode }) {
  const nodes = useMemo(() => layoutTree(root), [root]);
  if (nodes.length === 0) return null;
  const w = Math.max(...nodes.map(n => n.x)) + CW / 2;
  const h = Math.max(...nodes.map(n => n.y)) + CH_PLAN;
  const pad = 20;
  return (
    <svg viewBox={`${-pad} ${-pad} ${w + pad * 2} ${h + pad * 2}`}
      preserveAspectRatio="xMidYMid meet"
      style={{ width: "100%", height: "100%", display: "block" }}>
      {/* Edges */}
      {nodes.map((n, i) => {
        if (n.parentIdx < 0) return null;
        const p = nodes[n.parentIdx];
        const x1 = p.x, y1 = p.y + CH_PLAN, x2 = n.x, y2 = n.y;
        const my = (y1 + y2) / 2;
        return (
          <path key={`e-${i}`}
            d={`M${x1},${y1} C${x1},${my} ${x2},${my} ${x2},${y2}`}
            fill="none" stroke="#d4d4d8" strokeWidth={2} />
        );
      })}
      {/* Cards */}
      {nodes.map((n, i) => {
        const lx = n.x - CW / 2;
        return (
          <g key={i}>
            <rect x={lx + 2} y={n.y + 2} width={CW} height={CH_PLAN} rx={8} fill="#00000006" />
            <rect x={lx} y={n.y} width={CW} height={CH_PLAN} rx={8}
              fill="white" stroke={n.color + "35"} strokeWidth={1.2} />
            <rect x={lx} y={n.y + 6} width={4} height={CH_PLAN - 12} rx={2} fill={n.color} />
            <text x={lx + 14} y={n.y + 24} fontSize={16} fontWeight={700}
              fontFamily="monospace" fill={n.color}>{n.op}</text>
            {n.time && (
              <text x={lx + CW - 8} y={n.y + 24} fontSize={13} fontWeight={600}
                fontFamily="monospace" fill="#9ca3af" textAnchor="end">{n.time}</text>
            )}
            {n.detail && (
              (() => {
                const mm = n.detail.match(/^GL_\{(.+)\}-(.+)$/);
                return mm
                  ? <text x={lx + 14} y={n.y + 46} fontSize={14} fontFamily="monospace" fill="#6b7280">
                      GL<tspan baselineShift="sub" fontSize="0.75em">{mm[1]}</tspan>-{mm[2]}
                    </text>
                  : <text x={lx + 14} y={n.y + 46} fontSize={14} fontFamily="monospace" fill="#6b7280">
                      {n.detail.length > 20 ? n.detail.slice(0, 18) + "\u2026" : n.detail}
                    </text>;
              })()
            )}
          </g>
        );
      })}
    </svg>
  );
}

// Plan data: MATCH (p)-[:birthPlace]->(c)-[:country]->(co)
// Linear pipeline: NodeScan → AdjIdxJoin → IdSeek → AdjIdxJoin → IdSeek → Projection
// UnionAll fans out at Person GL partitions (GL-4/5 pruned: no :birthPlace, GL-8: no :country)
// IdSeek sits above AdjIdxJoin (matches actual profiler output).
// IdSeek is binary: left = target GL partitions, right = AdjIdxJoin input.
// AdjIdxJoin has single child (source nodes).
const EXEC_PLAN: PlanNode = {
  op: "Projection", color: "#71717a", detail: "p.name, c.name, co.name",
  time: "0.5ms",
  children: [{
    op: "IdSeek", color: "#B45309", detail: "Country",
    time: "0.05ms",
    children: [
      {
        op: "AdjIdxJoin", color: "#B45309", detail: ":country",
        time: "3.2ms",
        children: [
          {
            op: "IdSeek", color: "#0891B2", detail: "City",
            time: "0.2ms",
            children: [
              {
                op: "AdjIdxJoin", color: "#7C3AED", detail: ":birthPlace",
                time: "8.3ms",
                children: [
                  {
                    op: "UnionAll", color: "#DC2626", detail: "Person",
                    time: "0.08ms",
                    children: [
                      { op: "NodeScan", color: "#DC2626", detail: "GL_{p}-1", time: "12.4ms" },
                      { op: "NodeScan", color: "#a78bfa", detail: "GL_{p}-2", time: "7.1ms" },
                      { op: "NodeScan", color: "#7C3AED", detail: "GL_{p}-3", time: "1.6ms" },
                    ],
                  },
                  { op: "IdxScan", color: "#7C3AED", detail: ":birthPlace idx", time: "0.01ms" },
                ],
              },
              {
                op: "UnionAll", color: "#0891B2", detail: "City targets",
                time: "0.01ms",
                children: [
                  { op: "NodeScan", color: "#0891B2", detail: "GL_{c}-1", time: "4.2ms" },
                  { op: "NodeScan", color: "#0891B2", detail: "GL_{c}-2", time: "2.4ms" },
                ],
              },
            ],
          },
          { op: "IdxScan", color: "#B45309", detail: ":country idx", time: "0.01ms" },
        ],
      },
      {
        op: "UnionAll", color: "#B45309", detail: "Country targets",
        time: "0.01ms",
        children: [
          { op: "NodeScan", color: "#B45309", detail: "GL_{co}-1", time: "0.02ms" },
          { op: "NodeScan", color: "#166534", detail: "GL_{co}-2", time: "0.01ms" },
        ],
      },
    ],
  }],
};

function PlanView({ pAttr, cAttr, coAttr }: { pAttr: string; cAttr: string; coAttr: string }) {
  const state = useMemo(() => computeAll(pAttr, cAttr, coAttr), [pAttr, cAttr, coAttr]);

  // Build dynamic plan: filter NodeScan children by active GLs
  const activePerson = PERSON_GLS.filter(gl => state.p.get(gl.id)?.active).map(gl => gl.id);
  const activeCity = CITY_GLS.filter(gl => state.c.get(gl.id)?.active).map(gl => gl.id);
  const activeCountry = COUNTRY_GLS.filter(gl => state.co.get(gl.id)?.active).map(gl => gl.id);

  const filterPlan = (node: PlanNode): PlanNode => {
    if (!node.children) return node;
    if (node.op === "UnionAll" && node.detail === "Person") {
      return { ...node, children: node.children.filter(c => c.detail && activePerson.includes(c.detail)).map(filterPlan) };
    }
    if (node.op === "UnionAll" && node.detail === "City targets") {
      return { ...node, children: node.children.filter(c => c.detail && activeCity.includes(c.detail)).map(filterPlan) };
    }
    if (node.op === "UnionAll" && node.detail === "Country targets") {
      return { ...node, children: node.children.filter(c => c.detail && activeCountry.includes(c.detail)).map(filterPlan) };
    }
    return { ...node, children: node.children.map(filterPlan) };
  };

  const dynamicPlan = filterPlan(EXEC_PLAN);
  const prunedGLs = [
    ...PERSON_GLS.filter(gl => !state.p.get(gl.id)?.active).map(gl => gl.id),
    ...CITY_GLS.filter(gl => !state.c.get(gl.id)?.active).map(gl => gl.id),
    ...COUNTRY_GLS.filter(gl => !state.co.get(gl.id)?.active).map(gl => gl.id),
  ];

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", gap: 10, overflow: "hidden" }}>
      {/* Tree diagram */}
      <div style={{
        flex: 1, minHeight: 0,
        background: "#fafbfc", border: "1px solid #e5e7eb", borderRadius: 10,
        overflow: "hidden", padding: 12, boxSizing: "border-box",
      }}>
        <PlanDiagram root={dynamicPlan} />
      </div>

      {/* Footer note */}
      <div style={{
        flexShrink: 0, fontSize: 16, fontFamily: "monospace", color: "#9ca3af",
        textAlign: "center", lineHeight: 1.5,
      }}>
        Pipeline: <span style={{ color: "#DC2626" }}>NodeScan</span> per graphlet partition
        {" \u2192 "}<span style={{ color: "#7C3AED" }}>AdjIdxJoin</span> (CSR index)
        {" \u2192 "}<span style={{ color: "#0891B2" }}>IdSeek</span> (O(1) target lookup)
        {prunedGLs.length > 0 && (
          <>{" \u2014 "}{prunedGLs.map((id, i) => (
            <React.Fragment key={id}>{i > 0 && ", "}<GLLabel id={id} /></React.Fragment>
          ))} pruned</>
        )}
      </div>
    </div>
  );
}

// ─── Main ────────────────────────────────────────────────────────────────

export default function S2_Query({ step }: Props) {
  const [pAttr, setPAttr] = useState("");
  const [cAttr, setCAttr] = useState("");
  const [coAttr, setCoAttr] = useState("");

  const TITLES = ["Cypher Query on Graphlets", "Actual Query Plan"];
  const SUBS = [
    "How rdf:type, edges, and predicates map to graphlet operations",
    "Physical execution plan with CGC graphlet partitions",
  ];

  return (
    <div style={{ height: "100%", overflow: "hidden" }}>
      <div style={{
        maxWidth: 1440, margin: "0 auto", padding: "20px 40px", height: "100%",
        display: "flex", flexDirection: "column", boxSizing: "border-box", gap: 8,
      }}>
        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }} transition={{ duration: 0.25 }}
            style={{ flexShrink: 0 }}>
            <h2 style={{ fontSize: 22, fontWeight: 700, color: "#18181b", margin: 0 }}>
              {TITLES[step]}
            </h2>
          </motion.div>
        </AnimatePresence>

        <AnimatePresence mode="wait">
          <motion.div key={step} initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }} transition={{ duration: 0.25 }}
            style={{ flex: 1, minHeight: 0 }}>
            {step === 0 && <QueryVisualizer pAttr={pAttr} cAttr={cAttr} coAttr={coAttr} setPAttr={setPAttr} setCAttr={setCAttr} setCoAttr={setCoAttr} />}
            {step === 1 && <PlanView pAttr={pAttr} cAttr={cAttr} coAttr={coAttr} />}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}
