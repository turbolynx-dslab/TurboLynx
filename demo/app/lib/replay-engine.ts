// ---------------------------------------------------------------------------
// Replay Engine — simulates IdSeek batch-by-batch execution for the Inspect tab
// ---------------------------------------------------------------------------

export interface GraphletInfo {
  id: number;
  rows: number;
  cols: number;
  schema: string[];
}

export interface BatchResult {
  batchIndex: number;
  tupleCount: number; // 1024 or less for last batch
  // Which graphlets were hit and how many tuples from each
  graphletHits: { graphletId: number; count: number; schema: string[] }[];
  // Wide table (UNIONALL) stats
  wide: {
    totalColumns: number; // union of all schemas in this batch
    nullCount: number;    // cells that are NULL
    totalCells: number;   // tupleCount * totalColumns
    nullPercent: number;
  };
  // SSRF stats
  ssrf: {
    sourceColumns: number;  // columnar source columns (fixed)
    schemaCount: number;    // number of distinct schemas
    nullCount: number;      // always 0 for target side
    totalCells: number;
  };
}

export interface ReplayResult {
  totalBatches: number;
  totalTuples: number;
  batches: BatchResult[];
  // Cumulative stats
  cumulativeWideNulls: number[];   // cumulative NULL count after each batch
  cumulativeSsrfNulls: number[];   // always 0
  cumulativeSchemas: number[];     // cumulative distinct schemas (wide: union grows, ssrf: additive)
}

// Source schema: representative top attributes from Person graphlets
export const DEFAULT_SOURCE_SCHEMA = ["name", "birthDate", "occupation", "abstract"];

/**
 * Build a CDF (cumulative distribution function) from graphlet row counts.
 * Returns an array of { graphletIndex, cumulativeRows } sorted by cumulative rows.
 */
function buildCDF(graphlets: GraphletInfo[]): { idx: number; cumRows: number }[] {
  const cdf: { idx: number; cumRows: number }[] = [];
  let cumulative = 0;
  for (let i = 0; i < graphlets.length; i++) {
    cumulative += graphlets[i].rows;
    cdf.push({ idx: i, cumRows: cumulative });
  }
  return cdf;
}

/**
 * Given a position in [0, totalRows), find which graphlet it maps to via the CDF.
 */
function lookupCDF(cdf: { idx: number; cumRows: number }[], position: number): number {
  // Binary search for first entry where cumRows > position
  let lo = 0;
  let hi = cdf.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (cdf[mid].cumRows <= position) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return cdf[lo].idx;
}

/**
 * Simulate IdSeek replay batch-by-batch.
 *
 * @param targetGraphlets - target graphlets (e.g., c's 1304 graphlets)
 * @param sourceSchema    - source node schema columns (e.g., Person attributes)
 * @param totalEdges      - total edge count (capped for demo)
 * @param batchSize       - tuples per batch (default 1024)
 */
export function simulateReplay(
  targetGraphlets: GraphletInfo[],
  sourceSchema: string[],
  totalEdges: number,
  batchSize: number = 1024,
): ReplayResult {
  // Cap total tuples for demo to keep it manageable (20 batches)
  const maxTuples = 20480;
  const totalTuples = Math.min(totalEdges, maxTuples);
  const totalBatches = Math.ceil(totalTuples / batchSize);

  const cdf = buildCDF(targetGraphlets);
  const totalRows = cdf.length > 0 ? cdf[cdf.length - 1].cumRows : 1;

  const batches: BatchResult[] = [];
  const cumulativeWideNulls: number[] = [];
  const cumulativeSsrfNulls: number[] = [];
  const cumulativeSchemas: number[] = [];

  // Track cumulative state
  let runningWideNulls = 0;
  const seenSchemaSets = new Set<string>();

  for (let b = 0; b < totalBatches; b++) {
    const startTuple = b * batchSize;
    const tupleCount = Math.min(batchSize, totalTuples - startTuple);

    // Count hits per graphlet in this batch
    const hitCounts = new Map<number, number>();
    for (let t = 0; t < tupleCount; t++) {
      const position = (startTuple + t) % totalRows;
      const gIdx = lookupCDF(cdf, position);
      hitCounts.set(gIdx, (hitCounts.get(gIdx) ?? 0) + 1);
    }

    // Build graphlet hits array
    const graphletHits: { graphletId: number; count: number; schema: string[] }[] = [];
    for (const [gIdx, count] of hitCounts) {
      const g = targetGraphlets[gIdx];
      graphletHits.push({ graphletId: g.id, count, schema: g.schema });
    }
    // Sort by graphlet ID for determinism
    graphletHits.sort((a, b) => a.graphletId - b.graphletId);

    // Wide table: union of all schemas in this batch
    const allCols = new Set<string>();
    for (const hit of graphletHits) {
      for (const col of hit.schema) {
        allCols.add(col);
      }
    }
    const totalColumns = allCols.size;

    // Count NULLs in wide table: for each tuple, NULLs = totalColumns - graphlet's col count
    let wideNullCount = 0;
    for (const hit of graphletHits) {
      const graphletColCount = hit.schema.length;
      wideNullCount += hit.count * (totalColumns - graphletColCount);
    }
    const wideTotalCells = tupleCount * totalColumns;
    const wideNullPercent = wideTotalCells > 0
      ? Math.round((wideNullCount / wideTotalCells) * 100)
      : 0;

    // SSRF: source columns fixed (columnar), target stored row-packed = 0 NULLs
    const ssrfSchemaCount = graphletHits.length; // distinct schemas in this batch
    const ssrfTotalCells = tupleCount * (sourceSchema.length + 1); // +1 for schema pointer col

    // Track cumulative distinct schemas for wide side
    // For wide: we track the union of all column sets seen so far (growing set)
    for (const hit of graphletHits) {
      const key = hit.schema.slice().sort().join("|");
      seenSchemaSets.add(key);
    }

    const batchResult: BatchResult = {
      batchIndex: b,
      tupleCount,
      graphletHits,
      wide: {
        totalColumns,
        nullCount: wideNullCount,
        totalCells: wideTotalCells,
        nullPercent: wideNullPercent,
      },
      ssrf: {
        sourceColumns: sourceSchema.length,
        schemaCount: ssrfSchemaCount,
        nullCount: 0,
        totalCells: ssrfTotalCells,
      },
    };

    batches.push(batchResult);

    // Cumulative stats
    runningWideNulls += wideNullCount;
    cumulativeWideNulls.push(runningWideNulls);
    cumulativeSsrfNulls.push(0);
    cumulativeSchemas.push(seenSchemaSets.size);
  }

  return {
    totalBatches,
    totalTuples,
    batches,
    cumulativeWideNulls,
    cumulativeSsrfNulls,
    cumulativeSchemas,
  };
}

/**
 * Generate sample rows for display (first N rows of a batch).
 * Returns rows with actual column values or null for missing columns.
 */
export function generateSampleRows(
  batch: BatchResult,
  allColumns: string[],
  maxRows: number = 8,
): (string | null)[][] {
  const rows: (string | null)[][] = [];
  let remaining = maxRows;

  for (const hit of batch.graphletHits) {
    if (remaining <= 0) break;
    const count = Math.min(hit.count, remaining);
    const schemaSet = new Set(hit.schema);

    for (let i = 0; i < count; i++) {
      const row: (string | null)[] = allColumns.map((col) => {
        if (schemaSet.has(col)) {
          // Generate a deterministic placeholder value
          return sampleValue(col, hit.graphletId, i);
        }
        return null;
      });
      rows.push(row);
    }
    remaining -= count;
  }

  return rows;
}

/**
 * Generate a deterministic sample value for a column in a graphlet.
 */
function sampleValue(col: string, graphletId: number, rowIndex: number): string {
  const seed = graphletId * 1000 + rowIndex;
  switch (col) {
    case "id":
    case "uri":
      return `dbr:${seed}`;
    case "name":
      return SAMPLE_NAMES[seed % SAMPLE_NAMES.length];
    case "birthDate":
      return `${1940 + (seed % 60)}-${String(1 + (seed % 12)).padStart(2, "0")}-${String(1 + (seed % 28)).padStart(2, "0")}`;
    case "occupation":
      return SAMPLE_OCCUPATIONS[seed % SAMPLE_OCCUPATIONS.length];
    case "abstract":
      return `Abstract #${seed}...`;
    case "birthPlace":
      return SAMPLE_CITIES[seed % SAMPLE_CITIES.length];
    case "nationality":
      return SAMPLE_COUNTRIES[seed % SAMPLE_COUNTRIES.length];
    case "deathDate":
      return `${1980 + (seed % 40)}-${String(1 + (seed % 12)).padStart(2, "0")}-${String(1 + (seed % 28)).padStart(2, "0")}`;
    case "population":
    case "populationTotal":
      return String(10000 + (seed * 7919) % 9990000);
    case "areaTotal":
      return `${100 + (seed * 31) % 9900} km2`;
    case "country":
      return SAMPLE_COUNTRIES[seed % SAMPLE_COUNTRIES.length];
    default:
      return `val_${seed % 100}`;
  }
}

const SAMPLE_NAMES = [
  "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank",
  "Iris", "Jack", "Kim", "Leo", "Mia", "Nate", "Olivia", "Pat",
];
const SAMPLE_OCCUPATIONS = [
  "Scientist", "Engineer", "Artist", "Writer", "Musician", "Actor",
  "Politician", "Athlete", "Professor", "Director",
];
const SAMPLE_CITIES = [
  "Berlin", "Paris", "Tokyo", "London", "Seoul", "Rome", "Vienna",
  "Madrid", "Prague", "Oslo",
];
const SAMPLE_COUNTRIES = [
  "Germany", "France", "Japan", "UK", "Korea", "Italy", "Austria",
  "Spain", "Czechia", "Norway",
];
