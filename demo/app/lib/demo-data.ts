// ─── Scene 0: DBpedia nodes ─────────────────────────────────────────────────
export const DBPEDIA_NODES = [
  // Person
  { id: "n1", name: "Tiger Woods",          type: "Person", color: "#3B82F6",
    schema: ["abstract","height","weight","birthDate","birthPlace","team","position",
             "graduationYear","college","profession","wikiPageID","wikiPageRevisionID",
             "thumb","caption","website"] },
  { id: "n2", name: "Ferdinand I",           type: "Person", color: "#3B82F6",
    schema: ["abstract","activeYearsEndYear","activeYearsStartYear","birthDate",
             "deathDate","orderInOffice","wikiPageID","wikiPageRevisionID",
             "predecessor","successor","spouse","parent","child","sibling","religion"] },
  { id: "n3", name: "Kate Forsyth",          type: "Person", color: "#3B82F6",
    schema: ["abstract","birthDate","birthName","birthYear","wikiPageID",
             "wikiPageRevisionID","genre","nationality","award","occupation",
             "activeYearsStart","language","movement","label","publisher",
             "almaMater","education"] },
  { id: "n4", name: "Cato the Elder",        type: "Person", color: "#3B82F6",
    schema: ["abstract","orderInOffice","allegiance","wikiPageID",
             "wikiPageRevisionID","birthDate","deathDate","rank","caption","era"] },
  // Film
  { id: "n5", name: "Sholay",                type: "Film",   color: "#8B5CF6",
    schema: ["abstract","runtime","wikiPageID","wikiPageRevisionID",
             "country","language","starring","director"] },
  { id: "n6", name: "Private Life of Henry", type: "Film",   color: "#8B5CF6",
    schema: ["abstract","runtime","budget","gross","imdbId","releaseDate",
             "director","producer","distributor","cinematography",
             "editing","starring","musicComposer","country","language",
             "wikiPageID","wikiPageRevisionID","thumbnail"] },
  { id: "n7", name: "One Night of Love",     type: "Film",   color: "#8B5CF6",
    schema: ["abstract","gross","imdbId","runtime","musicComposer",
             "wikiPageID","wikiPageRevisionID","releaseDate",
             "director","starring","country"] },
  // City
  { id: "n8",  name: "Lake City, FL",        type: "City",   color: "#F59E0B",
    schema: ["abstract","areaTotal","populationDensity","areaCode","areaLand",
             "areaWater","elevation","utcOffset","timezone","postalCode",
             "governmentType","leaderName","leaderTitle","population",
             "populationAsOf","areaMetro","areaUrban","subdivisionName",
             "isPartOf","country","longd","latd","wikiPageID","wikiPageRevisionID",
             "north","east","south","west","map","mapCaption","blank","blankName",
             "settlementType","populationTotal","areaTotalSqMi","areaLandSqMi"] },
  { id: "n9",  name: "Priolo Gargallo",      type: "City",   color: "#F59E0B",
    schema: ["abstract","areaCode","areaTotal","elevation","population",
             "wikiPageID","wikiPageRevisionID","country","isPartOf",
             "longd","latd","mayor","postalCode","istat","cap",
             "provinceCode","region","timezone","utcOffset","areaWater"] },
  { id: "n10", name: "Fisher, Arkansas",     type: "City",   color: "#F59E0B",
    schema: ["abstract","areaCode","areaLand","areaTotal","populationDensity",
             "elevation","wikiPageID","wikiPageRevisionID","postalCode",
             "isPartOf","country","longd","latd","timezone","utcOffset",
             "governmentType","population","populationAsOf",
             "blank","blankName","areaTotalSqMi","areaLandSqMi",
             "populationTotal","areaCode2","areaCode3"] },
  // Book
  { id: "n11", name: "Moon Goddess & Son",   type: "Book",   color: "#10B981",
    schema: ["abstract","dcc","isbn","lcc","numberOfPages","oclc",
             "publisher","author","wikiPageID","wikiPageRevisionID",
             "releaseDate","language","subject","dewey","mediaType",
             "country","genre","series","edition","coverArtist"] },
  { id: "n12", name: "Lycurgus of Thrace",   type: "Book",   color: "#10B981",
    schema: ["abstract","wikiPageID","wikiPageRevisionID","comment","label"] },
  { id: "n13", name: "The English Teacher",  type: "Book",   color: "#10B981",
    schema: ["abstract","dcc","lcc","numberOfPages","oclc",
             "wikiPageID","wikiPageRevisionID","author","publisher",
             "releaseDate","language","isbn","genre","country","mediaType"] },
] as const;

export type DbpediaNode = typeof DBPEDIA_NODES[number];

export const DBPEDIA_TYPE_STATS = [
  { type: "Person", total: 657,  uniqueSchemas: 508  },
  { type: "Film",   total: 421,  uniqueSchemas: 287  },
  { type: "City",   total: 4500, uniqueSchemas: 1298 },
  { type: "Book",   total: 3154, uniqueSchemas: 557  },
] as const;

export const DBPEDIA_SCALE = {
  totalNodes:    77_000_000,
  uniqueSchemas: 282_764,
  nullChecks:    212_000_000_000,
} as const;

// ─── Scene 1: Person nodes (used in CGC scene) ──────────────────────────────
export const PERSON_NODES = [
  { id: "v0", attrs: { age: 20,  FN: "John",  LN: "Doe"  } },
  { id: "v1", attrs: { FN: "Frank", LN: "Hill" } },
  { id: "v2", attrs: { FN: "Franz" } },
  { id: "v3", attrs: { age: 25,  gender: "F", major: "Math" } },
  { id: "v4", attrs: { gender: "M", major: "CS",  name: "Mike" } },
  { id: "v5", attrs: { FN: "Sara", LN: "Kim", age: 22 } },
  { id: "v6", attrs: { name: "Alex", major: "Physics", birthday: "95-03" } },
  { id: "v7", attrs: { FN: "Lee",  age: 30,  url: "dbp.org" } },
  { id: "v8", attrs: { name: "Yuna", age: 28 } },
  { id: "v9", attrs: { FN: "Tom",  LN: "Park", gender: "M" } },
] as const;

export type PersonNode = typeof PERSON_NODES[number];

export const ALL_ATTRS = ["age","FN","LN","gender","major","name","birthday","url"] as const;

export const GRAPHLETS = [
  { id: "gl₁", color: "#3B82F6", schema: ["age","FN","LN"],          nodes: ["v0","v5","v7"], desc: "age/FN/LN" },
  { id: "gl₂", color: "#8B5CF6", schema: ["FN","LN","gender"],       nodes: ["v1","v2","v9"], desc: "FN+gender" },
  { id: "gl₃", color: "#F59E0B", schema: ["age","gender","major"],   nodes: ["v3","v4","v6"], desc: "age+major" },
  { id: "gl₄", color: "#10B981", schema: ["name","age"],             nodes: ["v8"],           desc: "name+age"  },
] as const;

// ─── Scene 0 Steps 2–3: Split vs Merge demo ─────────────────────────────────
export const DEMO_PERSONS = [
  { id:"dp1",  name:"Tiger Woods",       schema:["birthDate","birthPlace","height","weight","occupation","team","nationality"] },
  { id:"dp2",  name:"Ferdinand I",       schema:["birthDate","deathDate","spouse","orderInOffice","nationality"] },
  { id:"dp3",  name:"Kate Forsyth",      schema:["birthDate","nationality","occupation","award","almaMater","genre"] },
  { id:"dp4",  name:"Cato the Elder",    schema:["birthDate","deathDate","occupation"] },
  { id:"dp5",  name:"Gianluigi Buffon",  schema:["birthDate","birthPlace","nationality","team","award","height","weight"] },
  { id:"dp6",  name:"Oliver Kahn",       schema:["birthDate","birthPlace","nationality","team"] },
  { id:"dp7",  name:"Petr Čech",         schema:["birthDate","birthPlace","nationality","team","height"] },
  { id:"dp8",  name:"Manuel Neuer",      schema:["birthDate","birthPlace","nationality","team","award"] },
  { id:"dp9",  name:"Iker Casillas",     schema:["birthDate","birthPlace","nationality","team","award","occupation"] },
  { id:"dp10", name:"Marie Curie",       schema:["birthDate","deathDate","birthPlace","award","nationality","occupation","almaMater"] },
  { id:"dp11", name:"Aristotle",         schema:["birthDate","deathDate","occupation","nationality"] },
  { id:"dp12", name:"Cleopatra VII",     schema:["birthDate","deathDate","spouse","nationality"] },
  { id:"dp13", name:"Leonardo da Vinci", schema:["birthDate","deathDate","occupation","nationality"] },
  { id:"dp14", name:"Michael Jordan",    schema:["birthDate","birthPlace","nationality","team","occupation","award","height","weight"] },
  { id:"dp15", name:"Ada Lovelace",      schema:["birthDate","deathDate","birthPlace","occupation","nationality","almaMater"] },
  { id:"dp16", name:"Napoleon Bonaparte",schema:["birthDate","deathDate","birthPlace","spouse","nationality","occupation"] },
  { id:"dp17", name:"Serena Williams",   schema:["birthDate","birthPlace","nationality","team","award","occupation","height","weight"] },
  { id:"dp18", name:"Albert Einstein",   schema:["birthDate","deathDate","birthPlace","award","nationality","occupation","almaMater"] },
  { id:"dp19", name:"Frédéric Chopin",   schema:["birthDate","deathDate","birthPlace","nationality","occupation","genre"] },
  { id:"dp20", name:"Usain Bolt",        schema:["birthDate","birthPlace","nationality","award","occupation","height","weight"] },
  { id:"dp21", name:"Simone de Beauvoir",schema:["birthDate","deathDate","birthPlace","nationality","occupation","almaMater","award"] },
  { id:"dp22", name:"Genghis Khan",      schema:["birthDate","deathDate","spouse","nationality","occupation"] },
  { id:"dp23", name:"Florence Nightingale",schema:["birthDate","deathDate","birthPlace","nationality","occupation","award","almaMater"] },
  { id:"dp24", name:"Nikola Tesla",      schema:["birthDate","deathDate","birthPlace","nationality","occupation","almaMater"] },
  { id:"dp25", name:"Pelé",              schema:["birthDate","birthPlace","nationality","team","award","occupation","height","weight"] },
] as const;

export const PERSON_TABLE_COLS = [
  "birthDate","deathDate","birthPlace","nationality",
  "occupation","award","almaMater","spouse",
  "team","height","weight","orderInOffice","genre",
] as const;

// Graphlets produced by Layered Agglomerative Clustering on DEMO_PERSONS
export const CGC_GRAPHLETS = [
  {
    id: "GL-1", label: "Athletes", color: "#e84545",
    nodeIds: ["dp1","dp5","dp14","dp17","dp20","dp25"],
    schema: ["birthDate","birthPlace","nationality","team","occupation","award","height","weight"],
  },
  {
    id: "GL-2", label: "Scholars", color: "#3B82F6",
    nodeIds: ["dp10","dp18","dp23","dp15","dp24","dp21"],
    schema: ["birthDate","deathDate","birthPlace","nationality","occupation","award","almaMater"],
  },
  {
    id: "GL-3", label: "Footballers", color: "#8B5CF6",
    nodeIds: ["dp6","dp7","dp8","dp9"],
    schema: ["birthDate","birthPlace","nationality","team","award"],
  },
  {
    id: "GL-4", label: "Ancients", color: "#F59E0B",
    nodeIds: ["dp4","dp11","dp12","dp13"],
    schema: ["birthDate","deathDate","occupation","nationality","spouse"],
  },
  {
    id: "GL-5", label: "Mixed", color: "#10B981",
    nodeIds: ["dp2","dp3","dp16","dp19","dp22"],
    schema: ["birthDate","deathDate","nationality","occupation","spouse","genre"],
  },
  {
    id: "GL-C", label: "Cities", color: "#F59E0B",
    nodeIds: [],
    schema: ["populationTotal","areaTotal","country","timezone","lat","long"],
  },
] as const;

export const QUERY_ATTRS = [
  { key:"occupation",  label:"occupation"  },
  { key:"award",       label:"award"       },
  { key:"almaMater",   label:"almaMater"   },
  { key:"spouse",      label:"spouse"      },
  { key:"birthDate",   label:"birthDate"   },
] as const;

// ─── Scene 2: Query results ─────────────────────────────────────────────────
export const HERO_RESULTS = [
  { name: "Gianluigi Buffon", born: "1978-01-28", city: "Carrara",        pop: "65,497" },
  { name: "Oliver Kahn",      born: "1969-06-15", city: "Karlsruhe",      pop: "308,436" },
  { name: "Petr Čech",        born: "1982-05-20", city: "Plzeň",          pop: "170,548" },
  { name: "Manuel Neuer",     born: "1986-03-27", city: "Gelsenkirchen",  pop: "260,654" },
  { name: "Iker Casillas",    born: "1981-05-20", city: "Madrid",         pop: "3,223,334" },
];

// ─── Scene 5: Benchmarks ────────────────────────────────────────────────────
export const BENCHMARKS = {
  dbpedia: {
    label: "DBpedia Q13",
    note: "86× faster than the best competitor",
    data: [
      { system: "TurboLynx", ratio: 1,     highlight: true  },
      { system: "Umbra",     ratio: 7.74,  highlight: false },
      { system: "Kuzu",      ratio: 18.88, highlight: false },
      { system: "DuckPGQ",   ratio: 20.23, highlight: false },
      { system: "DuckDB",    ratio: 20.22, highlight: false },
      { system: "Neo4j",     ratio: 86.14, highlight: false },
    ],
  },
  ldbc: {
    label: "LDBC SF10",
    note: "Consistent wins across all graph workloads",
    data: [
      { system: "TurboLynx", ratio: 1,      highlight: true  },
      { system: "Umbra",     ratio: 7.74,   highlight: false },
      { system: "Memgraph",  ratio: 11.74,  highlight: false },
      { system: "GraphScope", ratio: 26.92, highlight: false },
      { system: "Kuzu",      ratio: 106.89, highlight: false },
      { system: "DuckDB",    ratio: 41.27,  highlight: false },
    ],
  },
  tpch: {
    label: "TPC-H Q3",
    note: "Even on relational workloads, TurboLynx excels",
    data: [
      { system: "TurboLynx", ratio: 1,     highlight: true  },
      { system: "Umbra",     ratio: 0.58,  highlight: false },
      { system: "DuckDB",    ratio: 1.53,  highlight: false },
      { system: "Kuzu",      ratio: 14.34, highlight: false },
      { system: "Neo4j",     ratio: 15.73, highlight: false },
      { system: "DuckPGQ",   ratio: 18.88, highlight: false },
    ],
  },
} as const;
