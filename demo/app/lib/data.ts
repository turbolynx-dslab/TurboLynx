// ---------------------------------------------------------------------------
// Hardcoded demo data — all numbers from paper / real DBpedia
// ---------------------------------------------------------------------------

export const DBPEDIA_STATS = {
  nodes: 77_000_000,
  uniqueSchemas: 282_764,
  attributes: 2796,
  nullOpsWithoutCGC: 212_000_000_000,
};

export interface NodeSample {
  id: string;
  type: string;
  attrs: Record<string, string>;
}

export const NODE_SAMPLES: NodeSample[] = [
  { id: "Q64", type: "Place", attrs: { wikiPageID: "18404", name: "Berlin", country: "Germany", population: "3,677,472", area: "891.7 km²", foundingDate: "1237" } },
  { id: "Q937", type: "Person", attrs: { wikiPageID: "736", name: "Albert Einstein", birthDate: "1879-03-14", deathDate: "1955-04-18", birthPlace: "Ulm", nationality: "German-American", occupation: "Physicist" } },
  { id: "Q11660", type: "Work", attrs: { wikiPageID: "228444", name: "Artificial Intelligence", abstract: "AI is intelligence demonstrated by machines..." } },
  { id: "Q2831", type: "Person", attrs: { wikiPageID: "21454", name: "Michael Jackson", birthDate: "1958-08-29", deathDate: "2009-06-25", genre: "Pop, R&B", instrument: "Voice", activeYears: "1964–2009" } },
  { id: "Q5765", type: "Organization", attrs: { wikiPageID: "19001", name: "Apple Inc.", foundingDate: "1976-04-01", location: "Cupertino, CA", industry: "Technology", numberOfEmployees: "164,000" } },
  { id: "Q229", type: "Film", attrs: { wikiPageID: "51201", name: "The Godfather", releaseDate: "1972-03-24", budget: "$6M", director: "Francis Ford Coppola", starring: "Marlon Brando" } },
  { id: "Q9685", type: "Person", attrs: { wikiPageID: "4422", name: "Ludwig van Beethoven", birthDate: "1770-12-17", deathDate: "1827-03-26", genre: "Classical", instrument: "Piano" } },
  { id: "Q1930187", type: "Place", attrs: { wikiPageID: "15895", name: "New York City", country: "USA", population: "8,336,817" } },
  { id: "Q8027", type: "Person", attrs: { wikiPageID: "3114", name: "Isaac Newton", birthDate: "1643-01-04", deathDate: "1727-03-31", nationality: "British", occupation: "Mathematician, Physicist" } },
  { id: "Q3332719", type: "Work", attrs: { wikiPageID: "44213", name: "Hamlet", abstract: "Tragedy by William Shakespeare", genre: "Tragedy" } },
  { id: "Q2736", type: "Film", attrs: { wikiPageID: "29981", name: "Star Wars", releaseDate: "1977-05-25", director: "George Lucas" } },
  { id: "Q1299", type: "Person", attrs: { wikiPageID: "5189", name: "The Beatles", genre: "Rock, Pop", instrument: "Guitar, Bass, Drums", activeYears: "1960–1970" } },
];

export const ATTR_COLORS: Record<string, string> = {
  wikiPageID: "#3b82f6",
  name: "#f97316",
  birthDate: "#8b5cf6",
  deathDate: "#8b5cf6",
  birthPlace: "#22c55e",
  nationality: "#06b6d4",
  occupation: "#f59e0b",
  genre: "#ec4899",
  instrument: "#84cc16",
  abstract: "#a1a1aa",
  country: "#22c55e",
  population: "#06b6d4",
  area: "#a1a1aa",
  industry: "#f59e0b",
  numberOfEmployees: "#a1a1aa",
  foundingDate: "#8b5cf6",
  location: "#22c55e",
  releaseDate: "#8b5cf6",
  budget: "#f59e0b",
  director: "#ec4899",
  starring: "#ec4899",
  activeYears: "#a1a1aa",
};

// ---------------------------------------------------------------------------
// Graphlet definitions (pre-computed for Q13: Person-Director-Film)
// ---------------------------------------------------------------------------
export interface Graphlet {
  id: string;
  entity: "Person" | "Film" | "Place";
  attrs: string[];
  rowCount: number;
  optimalOrder: "Place-first" | "Film-first";
  color: string;
}

export const Q13_GRAPHLETS: Graphlet[] = [
  { id: "gl₁", entity: "Person", attrs: ["name"], rowCount: 44200, optimalOrder: "Place-first", color: "#3b82f6" },
  { id: "gl₂", entity: "Person", attrs: ["name", "birthDate"], rowCount: 31800, optimalOrder: "Place-first", color: "#3b82f6" },
  { id: "gl₃", entity: "Person", attrs: ["name", "birthDate", "birthPlace"], rowCount: 18500, optimalOrder: "Place-first", color: "#3b82f6" },
  { id: "gl₄", entity: "Person", attrs: ["name", "birthDate", "birthPlace", "nationality"], rowCount: 12700, optimalOrder: "Film-first", color: "#f97316" },
  { id: "gl₅", entity: "Person", attrs: ["name", "birthDate", "nationality"], rowCount: 9200, optimalOrder: "Film-first", color: "#f97316" },
  { id: "gl₆", entity: "Film", attrs: ["name"], rowCount: 312000, optimalOrder: "Place-first", color: "#3b82f6" },
  { id: "gl₇", entity: "Film", attrs: ["name", "releaseDate"], rowCount: 228000, optimalOrder: "Place-first", color: "#3b82f6" },
  { id: "gl₈", entity: "Film", attrs: ["name", "releaseDate", "director"], rowCount: 145000, optimalOrder: "Film-first", color: "#f97316" },
  { id: "gl₉", entity: "Film", attrs: ["name", "budget"], rowCount: 89000, optimalOrder: "Film-first", color: "#f97316" },
  { id: "gl₁₀", entity: "Film", attrs: ["name", "budget", "director"], rowCount: 67000, optimalOrder: "Film-first", color: "#f97316" },
  { id: "gl₁₁", entity: "Place", attrs: ["name"], rowCount: 1200, optimalOrder: "Place-first", color: "#3b82f6" },
  { id: "gl₁₂", entity: "Place", attrs: ["name", "country"], rowCount: 890, optimalOrder: "Place-first", color: "#3b82f6" },
  { id: "gl₁₃", entity: "Place", attrs: ["name", "country", "population"], rowCount: 650, optimalOrder: "Film-first", color: "#f97316" },
  { id: "gl₁₄", entity: "Place", attrs: ["name", "population"], rowCount: 420, optimalOrder: "Film-first", color: "#f97316" },
  { id: "gl₁₅", entity: "Person", attrs: ["name", "occupation"], rowCount: 8900, optimalOrder: "Place-first", color: "#3b82f6" },
  { id: "gl₁₆", entity: "Person", attrs: ["name", "birthPlace", "occupation"], rowCount: 5400, optimalOrder: "Film-first", color: "#f97316" },
];

// ---------------------------------------------------------------------------
// Preset queries
// ---------------------------------------------------------------------------
export interface PresetQuery {
  id: string;
  label: string;
  description: string;
  cypher: string;
  matchedGraphlets: number;
  totalGraphlets: number;
  planSummary: string;
  naiveMs: number;
  turbolynxMs: number;
  speedup: number;
  results: Array<Record<string, string>>;
}

export const PRESET_QUERIES: PresetQuery[] = [
  {
    id: "Q7",
    label: "Musicians",
    description: "Find musicians who influenced each other",
    cypher: `MATCH (a:Artist)-[:influenced]->(b:Artist)
WHERE a.instrument IS NOT NULL
RETURN a.name, a.instrument, b.name
LIMIT 20`,
    matchedGraphlets: 127,
    totalGraphlets: 4210,
    planSummary: "AdjIdxJoin(Artist→influenced→Artist)",
    naiveMs: 4820,
    turbolynxMs: 89,
    speedup: 54,
    results: [
      { "a.name": "Bob Dylan", "a.instrument": "Guitar", "b.name": "Tom Petty" },
      { "a.name": "Jimi Hendrix", "a.instrument": "Guitar", "b.name": "Kurt Cobain" },
      { "a.name": "The Beatles", "a.instrument": "Guitar, Bass", "b.name": "Oasis" },
      { "a.name": "Miles Davis", "a.instrument": "Trumpet", "b.name": "Herbie Hancock" },
      { "a.name": "Chuck Berry", "a.instrument": "Guitar", "b.name": "The Rolling Stones" },
    ],
  },
  {
    id: "Q10",
    label: "Artists & Genres",
    description: "Artists and the genres they belong to",
    cypher: `MATCH (a:Artist)-[:genre]->(g)
WHERE a.name IS NOT NULL
RETURN a.name, g.name
ORDER BY a.name
LIMIT 20`,
    matchedGraphlets: 203,
    totalGraphlets: 4210,
    planSummary: "SchemaIdxScan + AdjIdxJoin(Artist→genre→Genre)",
    naiveMs: 2140,
    turbolynxMs: 41,
    speedup: 52,
    results: [
      { "a.name": "Adele", "g.name": "Soul, Pop" },
      { "a.name": "Arcade Fire", "g.name": "Indie Rock" },
      { "a.name": "Beethoven", "g.name": "Classical" },
      { "a.name": "David Bowie", "g.name": "Rock, Glam" },
      { "a.name": "Eminem", "g.name": "Hip hop" },
    ],
  },
  {
    id: "Q13",
    label: "Director ↔ City",
    description: "Films directed by people born in USA",
    cypher: `MATCH (d:Person)-[:birthPlace]->(:Place {country: "USA"}),
      (d)-[:director]->(f:Film)
RETURN d.name, f.name
LIMIT 20`,
    matchedGraphlets: 341,
    totalGraphlets: 4210,
    planSummary: "GEM vg₁: Place⋈Person⋈Film, vg₂: Film⋈Person⋈Place",
    naiveMs: 8750,
    turbolynxMs: 156,
    speedup: 56,
    results: [
      { "d.name": "Steven Spielberg", "f.name": "Schindler's List" },
      { "d.name": "Christopher Nolan", "f.name": "The Dark Knight" },
      { "d.name": "Martin Scorsese", "f.name": "Goodfellas" },
      { "d.name": "James Cameron", "f.name": "Titanic" },
      { "d.name": "George Lucas", "f.name": "Star Wars" },
    ],
  },
  {
    id: "Q20",
    label: "Multi-hop",
    description: "Knowledge graph 2-hop traversal",
    cypher: `MATCH (a)-[:wikiPageRedirects]->(b)-[:subject]->(c)
RETURN a.name, c.name
LIMIT 10`,
    matchedGraphlets: 89,
    totalGraphlets: 4210,
    planSummary: "2-hop AdjIdxJoin",
    naiveMs: 12300,
    turbolynxMs: 203,
    speedup: 61,
    results: [
      { "a.name": "NYC", "c.name": "Cities in New York" },
      { "a.name": "AI", "c.name": "Computer Science" },
      { "a.name": "Beatles", "c.name": "Rock Music" },
    ],
  },
];

// ---------------------------------------------------------------------------
// Benchmark data (Table 4 from paper, DBpedia)
// ---------------------------------------------------------------------------
export const BENCHMARK_DATA = {
  maxSpeedup: 86.14,
  competitors: ["Neo4j", "Kuzu", "Umbra", "DuckDB", "GraphScope"],
  queries: ["Q7", "Q10", "Q13", "Q19", "Q20"],
  // relative slowdown vs TurboLynx (TurboLynx = 1×)
  slowdowns: {
    "Neo4j":      [24.3,  18.7,  42.1,  135.8, 19.2],
    "Kuzu":       [8.2,   12.4,  31.5,  2528,  45.3],
    "Umbra":      [4.1,   5.8,   7.74,  12.3,  8.9],
    "DuckDB":     [15.2,  22.8,  86.14, 44.2,  31.7],
    "GraphScope": [18.9,  27.3,  51.2,  88.4,  42.1],
  },
};

// SSRF benchmark (Fig 8a — hop count effect)
export const SSRF_HOP_DATA = [
  { hops: 1, SS: 1.2, SSRF: 0.8 },
  { hops: 2, SS: 2.1, SSRF: 1.0 },
  { hops: 3, SS: 3.5, SSRF: 1.4 },
  { hops: 4, SS: 4.8, SSRF: 1.9 },
  { hops: 5, SS: 5.2, SSRF: 2.5 },
];

// Fig 8b — returned column effect
export const SSRF_COL_DATA = [
  { cols: 1, SS: 1.1, SSRF: 0.9 },
  { cols: 2, SS: 1.8, SSRF: 1.0 },
  { cols: 3, SS: 2.6, SSRF: 1.1 },
  { cols: 4, SS: 3.5, SSRF: 1.2 },
  { cols: 5, SS: 4.4, SSRF: 1.7 },
];
