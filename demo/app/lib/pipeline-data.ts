// ---------------------------------------------------------------------------
// Pipeline E2E trace data for Hero Query and presets
// ---------------------------------------------------------------------------

export type PartitionInfo = {
  id: number;
  color: "blue" | "orange";
  label: string;
  rowCount: number;
  attrs: string[];
};

export type VirtualGraphlet = {
  id: string;
  color: "blue" | "orange";
  graphlets: string[];
  joinOrder: string;
  rowCount: number;
};

export type PhaseTrace = {
  cgc: {
    totalPartitions: number;
    scannedPartitions: number;
    nullOpsAvoided: number;
    litPartitions: PartitionInfo[];
  };
  plan: {
    unionAllGroups: Array<{ label: string; partitions: string[] }>;
  };
  gem: {
    rawCombinations: number;
    virtualGraphlets: VirtualGraphlet[];
    reductionPct: number;
  };
  ssrf: {
    ssMemoryRatio: number;
    ssrfMemoryRatio: number;
    nullPct: number;
  };
};

export type ResultRow = {
  name: string;
  born: string;
  city: string;
  population: string;
  entity_type: string;
};

export type PresetId = "Q1" | "Q7" | "Q10" | "Q13";

export type Preset = {
  id: PresetId;
  label: string;
  isHero?: boolean;
  cypher: string;
  results: ResultRow[];
  phase: PhaseTrace;
  turbolynxMs: number;
  naiveMs: number;
  speedup: number;
};

// ---------------------------------------------------------------------------
// Hero Query — Q10 Goalkeeper (clean short form for display)
// ---------------------------------------------------------------------------
export const HERO_CYPHER =
`MATCH (p)-[:birthPlace]->(c),
      (p)-[:rdf:type]->(t)
WHERE p.position = 'Goalkeeper'
  AND c.populationTotal > 1000000
RETURN p.name        AS name,
       p.birthDate   AS born,
       c.name        AS city,
       c.population  AS population,
       t.type        AS entity_type`;

const HERO_PHASE: PhaseTrace = {
  cgc: {
    totalPartitions: 34,
    scannedPartitions: 2,
    nullOpsAvoided: 212_000_000_000,
    litPartitions: [
      {
        id: 7,
        color: "blue",
        label: "Goalkeeper-compatible",
        rowCount: 44_200,
        attrs: ["position", "birthDate", "label", "birthPlace"],
      },
      {
        id: 12,
        color: "orange",
        label: "City-compatible",
        rowCount: 5_700,
        attrs: ["populationTotal", "label", "country"],
      },
    ],
  },
  plan: {
    unionAllGroups: [
      { label: "UnionAll(gl_p1..p3)", partitions: ["p1", "p2", "p3"] },
      { label: "⋈ [:birthPlace]", partitions: [] },
      { label: "UnionAll(gl_c1..c2)", partitions: ["c1", "c2"] },
      { label: "⋈ [:rdf:type]", partitions: [] },
      { label: "gl_t1", partitions: ["t1"] },
    ],
  },
  gem: {
    rawCombinations: 6,
    virtualGraphlets: [
      {
        id: "α",
        color: "blue",
        graphlets: ["gl_p1 (44,200)", "gl_p2 (38,100)", "gl_c1 (5,700)"],
        joinOrder: "p→c",
        rowCount: 88_100,
      },
      {
        id: "β",
        color: "orange",
        graphlets: ["gl_p3 (12,000)", "gl_c2 (1,800)"],
        joinOrder: "c→p",
        rowCount: 13_800,
      },
    ],
    reductionPct: 67,
  },
  ssrf: {
    ssMemoryRatio: 1.0,
    ssrfMemoryRatio: 0.28,
    nullPct: 72,
  },
};

const HERO_RESULTS: ResultRow[] = [
  { name: "Gianluigi Buffon", born: "1978-01-28", city: "Carrara",       population: "65,497",    entity_type: "SoccerPlayer" },
  { name: "Oliver Kahn",      born: "1969-06-15", city: "Karlsruhe",    population: "308,436",   entity_type: "SoccerPlayer" },
  { name: "Petr Čech",        born: "1982-05-20", city: "Plzeň",        population: "170,548",   entity_type: "SoccerPlayer" },
  { name: "Manuel Neuer",     born: "1986-03-27", city: "Gelsenkirchen",population: "260,654",   entity_type: "SoccerPlayer" },
  { name: "Iker Casillas",    born: "1981-05-20", city: "Madrid",       population: "3,223,334", entity_type: "SoccerPlayer" },
];

// ---------------------------------------------------------------------------
// Other presets (generic traces)
// ---------------------------------------------------------------------------
const GENERIC_PHASE: PhaseTrace = {
  cgc: {
    totalPartitions: 34,
    scannedPartitions: 4,
    nullOpsAvoided: 48_000_000_000,
    litPartitions: [
      { id: 3,  color: "blue",   label: "Type-compatible",     rowCount: 12_400, attrs: ["type", "label"] },
      { id: 18, color: "orange", label: "Homepage-compatible", rowCount: 3_200,  attrs: ["homepage", "label"] },
    ],
  },
  plan: {
    unionAllGroups: [
      { label: "UnionAll(gl_n1..n4)", partitions: ["n1","n2","n3","n4"] },
      { label: "⋈ [:rdf:type]", partitions: [] },
      { label: "gl_t1", partitions: ["t1"] },
    ],
  },
  gem: {
    rawCombinations: 4,
    virtualGraphlets: [
      { id: "α", color: "blue",   graphlets: ["gl_n1 (12,400)", "gl_n2 (8,900)"], joinOrder: "n→t", rowCount: 21_300 },
      { id: "β", color: "orange", graphlets: ["gl_n3 (5,100)", "gl_n4 (2,800)"],  joinOrder: "t→n", rowCount: 7_900 },
    ],
    reductionPct: 50,
  },
  ssrf: { ssMemoryRatio: 1.0, ssrfMemoryRatio: 0.41, nullPct: 59 },
};

export const PRESETS: Preset[] = [
  {
    id: "Q1",
    label: "Redirects",
    cypher:
`MATCH (a)-[:wikiPageRedirects]->(b)
WHERE a.label IS NOT NULL
RETURN a.label AS name, b.label AS entity_type
LIMIT 10`,
    results: [
      { name: "NYC",     born: "-", city: "-", population: "-", entity_type: "New York City" },
      { name: "AI",      born: "-", city: "-", population: "-", entity_type: "Artificial intelligence" },
      { name: "Beatles", born: "-", city: "-", population: "-", entity_type: "The Beatles" },
    ],
    phase: { ...GENERIC_PHASE, cgc: { ...GENERIC_PHASE.cgc, scannedPartitions: 1, nullOpsAvoided: 5_000_000_000 } },
    turbolynxMs: 8,
    naiveMs: 312,
    speedup: 39,
  },
  {
    id: "Q7",
    label: "Type + Homepage",
    cypher:
`MATCH (p)-[:rdf:type]->(t),
      (p)-[:foaf:homepage]->(h)
WHERE p.numberOfEmployees > 1000
RETURN p.label AS name, t.label AS entity_type
LIMIT 10`,
    results: [
      { name: "Apple Inc.",  born: "-", city: "-", population: "164,000", entity_type: "Company" },
      { name: "Google LLC",  born: "-", city: "-", population: "135,301", entity_type: "Company" },
      { name: "Microsoft",   born: "-", city: "-", population: "181,000", entity_type: "Company" },
    ],
    phase: GENERIC_PHASE,
    turbolynxMs: 41,
    naiveMs: 2_140,
    speedup: 52,
  },
  {
    id: "Q10",
    label: "Goalkeeper ★",
    isHero: true,
    cypher: HERO_CYPHER,
    results: HERO_RESULTS,
    phase: HERO_PHASE,
    turbolynxMs: 14,
    naiveMs: 1_204,
    speedup: 86,
  },
  {
    id: "Q13",
    label: "Director · City",
    cypher:
`MATCH (d)-[:dct:subject]->(s)
WHERE d.label IS NOT NULL
RETURN d.label AS name, s.label AS entity_type
LIMIT 10`,
    results: [
      { name: "Steven Spielberg",  born: "-", city: "Cincinnati", population: "-", entity_type: "FilmDirector" },
      { name: "Christopher Nolan", born: "-", city: "London",     population: "-", entity_type: "FilmDirector" },
      { name: "Martin Scorsese",   born: "-", city: "New York",   population: "-", entity_type: "FilmDirector" },
    ],
    phase: { ...GENERIC_PHASE, gem: { ...GENERIC_PHASE.gem, reductionPct: 67 } },
    turbolynxMs: 156,
    naiveMs: 8_750,
    speedup: 56,
  },
];

export const PHASE_DELAYS = {
  cgc:  0.3,
  plan: 0.6,
  gem:  0.9,
  ssrf: 1.4,
} as const;
