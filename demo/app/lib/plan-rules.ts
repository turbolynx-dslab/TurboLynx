// ---------------------------------------------------------------------------
// ORCA-style Plan Transformation Engine for TurboLynx Demo
// ---------------------------------------------------------------------------

// ---- Data Types -----------------------------------------------------------

export interface PlanNode {
  op: string;
  detail?: string;
  color?: string;
  rows?: number;
  cost?: number;
  children?: PlanNode[];
  tableId?: string;
  graphletIds?: number[];
  joinPred?: string;
}

export interface GraphletInfo {
  id: number;
  rows: number;
  cols: number;
  schema: string[];
}

export interface VertexPartition {
  label: string;
  numColumns: number;
  numGraphlets: number;
  totalRows: number;
  graphlets: GraphletInfo[];
}

export interface EdgePartition {
  type: string;
  totalRows: number;
}

export interface Catalog {
  summary: {
    totalNodes: number;
    totalEdges: number;
    vertexPartitions: number;
    edgePartitions: number;
    totalGraphlets: number;
  };
  vertexPartitions: VertexPartition[];
  edgePartitions: EdgePartition[];
}

export interface QueryNode {
  alias: string;
  graphletIds: number[];
}

export interface QueryEdge {
  type: string;
  srcAlias: string;
  dstAlias: string;
}

// ---- Color Constants ------------------------------------------------------

export const COLORS = {
  Get: "#3b82f6",
  GetEdge: "#60a5fa",
  NAryJoin: "#F59E0B",
  Join: "#F59E0B",
  Select: "#F59E0B",
  Filter: "#F59E0B",
  Project: "#71717a",
  UnionAll: "#ec4899",
} as const;

// ---- Helpers --------------------------------------------------------------

function fmt(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
  if (n >= 1_000) return (n / 1_000).toFixed(0) + "K";
  return String(n);
}

/**
 * Join selectivity: for edge joins, use edge.rows / (src.rows × dst.rows).
 * For non-edge joins, use a conservative constant.
 */
const DEFAULT_SELECTIVITY = 0.00001;

function cloneNode(node: PlanNode): PlanNode {
  return {
    ...node,
    children: node.children ? node.children.map(cloneNode) : undefined,
    graphletIds: node.graphletIds ? [...node.graphletIds] : undefined,
  };
}

function sumRows(graphlets: GraphletInfo[], ids: number[]): number {
  const idSet = new Set(ids);
  return graphlets.filter((g) => idSet.has(g.id)).reduce((s, g) => s + g.rows, 0);
}

// ---- 1. buildNAryJoin -----------------------------------------------------

export function buildNAryJoin(
  queryNodes: QueryNode[],
  queryEdges: QueryEdge[],
  catalog: Catalog
): PlanNode {
  const vp = catalog.vertexPartitions[0]; // single vertex partition

  // Build Get nodes for each query node variable
  const getNodes: PlanNode[] = queryNodes.map((qn) => {
    const rows = sumRows(vp.graphlets, qn.graphletIds);
    return {
      op: "Get",
      detail: `${qn.alias} (${qn.graphletIds.length} GLs, ${fmt(rows)})`,
      color: COLORS.Get,
      rows,
      cost: rows,
      tableId: qn.alias,
      graphletIds: [...qn.graphletIds],
    };
  });

  // Build Get nodes for each edge type
  const edgeGets: PlanNode[] = queryEdges.map((qe) => {
    const ep = catalog.edgePartitions.find((e) => e.type === qe.type || e.type.includes(qe.type));
    const rows = ep ? ep.totalRows : 0;
    return {
      op: "Get",
      detail: `:${qe.type} (${fmt(rows)})`,
      color: COLORS.GetEdge,
      rows,
      cost: rows,
      tableId: "edge:" + qe.type,
      joinPred: `${qe.srcAlias}._id = _sid AND ${qe.dstAlias}._id = _did`,
    };
  });

  const allChildren = [...getNodes, ...edgeGets];

  const naryJoin: PlanNode = {
    op: "NAryJoin",
    detail: `${allChildren.length}-way join`,
    color: COLORS.NAryJoin,
    rows: allChildren.reduce((min, c) => Math.min(min, c.rows ?? Infinity), Infinity),
    children: allChildren,
  };

  // Wrap with Select if there is a WHERE clause (birthDate IS NOT NULL)
  const select: PlanNode = {
    op: "Select",
    detail: "p.birthDate IS NOT NULL",
    color: COLORS.Select,
    rows: naryJoin.rows,
    children: [naryJoin],
  };

  const project: PlanNode = {
    op: "Project",
    detail: "p.*, c.*",
    color: COLORS.Project,
    rows: select.rows,
    children: [select],
  };

  return project;
}

// ---- 2. expandNAryJoinDP --------------------------------------------------

/**
 * Enumerate all left-deep binary join orderings for the tables in an NAryJoin.
 * Returns top-K orderings sorted by estimated cost.
 */
export function expandNAryJoinDP(naryJoin: PlanNode, k = 6): PlanNode[] {
  // Extract the NAryJoin (may be wrapped in Project/Select)
  const nar = findNode(naryJoin, "NAryJoin");
  if (!nar || !nar.children) return [];

  const tables = nar.children;
  const perms = permutations(tables);
  const plans: PlanNode[] = [];

  for (const perm of perms) {
    const tree = buildLeftDeepJoin(perm);
    tree.cost = computeCost(tree);
    plans.push(tree);
  }

  plans.sort((a, b) => (a.cost ?? 0) - (b.cost ?? 0));
  return plans.slice(0, k);
}

function findNode(tree: PlanNode, op: string): PlanNode | null {
  if (tree.op === op) return tree;
  if (tree.children) {
    for (const c of tree.children) {
      const found = findNode(c, op);
      if (found) return found;
    }
  }
  return null;
}

function permutations<T>(arr: T[]): T[][] {
  if (arr.length <= 1) return [arr];
  const result: T[][] = [];
  for (let i = 0; i < arr.length; i++) {
    const rest = [...arr.slice(0, i), ...arr.slice(i + 1)];
    for (const p of permutations(rest)) {
      result.push([arr[i], ...p]);
    }
  }
  return result;
}

function getSelectivity(left: PlanNode, right: PlanNode): number {
  // If one side is an edge, selectivity = edge.rows / (node.rows × other_node.rows)
  // Approximate: edge join is selective, node-node join is not
  const leftIsEdge = left.tableId?.startsWith("edge:");
  const rightIsEdge = right.tableId?.startsWith("edge:");
  if (leftIsEdge || rightIsEdge) {
    const edgeRows = (leftIsEdge ? left.rows : right.rows) ?? 1;
    const nodeRows = (leftIsEdge ? right.rows : left.rows) ?? 1;
    // selectivity ≈ edge.rows / (nodeRows × universe)
    // simplified: edge join reduces to ~edgeRows output
    return Math.min(1, edgeRows / Math.max(1, nodeRows * nodeRows));
  }
  return DEFAULT_SELECTIVITY;
}

function buildLeftDeepJoin(tables: PlanNode[]): PlanNode {
  let current = cloneNode(tables[0]);
  for (let i = 1; i < tables.length; i++) {
    const right = cloneNode(tables[i]);
    const sel = getSelectivity(current, right);
    const leftRows = current.rows ?? 0;
    const rightRows = right.rows ?? 0;
    const joinRows = Math.max(1, Math.round(leftRows * rightRows * sel));
    const rightLabel = (right.tableId ?? right.detail ?? "?").replace("edge:", ":");
    current = {
      op: "Join",
      detail: `\u22C8 ${rightLabel}`,
      color: COLORS.Join,
      rows: joinRows,
      children: [current, right],
      joinPred: right.joinPred,
    };
  }
  return current;
}

// ---- 3. pushJoinBelowUnionAll ---------------------------------------------

/**
 * PushJoinBelowUnionAll: take a binary join tree, find ALL multi-graphlet Gets,
 * and produce a flat UnionAll over the cross-product of all graphlet combinations.
 *
 * Example: Join(Join(Get(p, [GL1..GL358]), Get(edge)), Get(c, [GL1..GL1304]))
 * →  UnionAll(
 *      Join(Join(Get(GL-p1), Get(edge)), Get(GL-c1)),
 *      Join(Join(Get(GL-p1), Get(edge)), Get(GL-c2)),
 *      ...
 *    )
 *
 * For display: shows first few concrete sub-trees + overflow.
 */
export function pushJoinBelowUnionAll(
  joinTree: PlanNode,
  graphletData: GraphletInfo[]
): PlanNode {
  // Collect all multi-graphlet leaf Gets in the tree
  const multiGets: PlanNode[] = [];
  collectMultiGets(joinTree, multiGets);

  if (multiGets.length === 0) return cloneNode(joinTree);

  // Build cross-product of graphlet IDs
  // Each entry: Map<tableId, singleGraphletId>
  const combos = crossProductGraphlets(multiGets);
  const totalCombos = combos.totalCount;

  // Generate sample sub-trees (first N concrete combos)
  const MAX_DISPLAY = 4;
  const sampleCombos = combos.samples.slice(0, MAX_DISPLAY);

  const unionChildren: PlanNode[] = sampleCombos.map((combo) => {
    return instantiateTree(joinTree, combo, graphletData);
  });

  if (totalCombos > unionChildren.length) {
    unionChildren.push({
      op: "...",
      detail: `+${(totalCombos - unionChildren.length).toLocaleString()} more sub-trees`,
      color: "#a1a1aa",
      rows: 0,
    });
  }

  const unionAll: PlanNode = {
    op: "UnionAll",
    detail: `${totalCombos.toLocaleString()} sub-trees`,
    color: COLORS.UnionAll,
    children: unionChildren,
  };
  unionAll.cost = computeCost(unionAll);
  return unionAll;
}

function collectMultiGets(node: PlanNode, result: PlanNode[]) {
  if (isMultiGraphletGet(node)) { result.push(node); return; }
  if (node.children) node.children.forEach(c => collectMultiGets(c, result));
}

function crossProductGraphlets(multiGets: PlanNode[]): { samples: Map<string, number>[]; totalCount: number } {
  // Each multiGet has tableId and graphletIds
  // Cross product: for 2 tables with [a,b] and [c,d] → [{t1:a,t2:c}, {t1:a,t2:d}, {t1:b,t2:c}, {t1:b,t2:d}]
  let totalCount = 1;
  const idSets: { tableId: string; ids: number[] }[] = [];
  for (const mg of multiGets) {
    const tid = mg.tableId ?? "?";
    const ids = mg.graphletIds ?? [];
    totalCount *= ids.length;
    idSets.push({ tableId: tid, ids: ids.slice(0, 8) }); // sample top 8 per table
  }

  // Generate sample combos (limited)
  let combos: Map<string, number>[] = [new Map()];
  for (const { tableId, ids } of idSets) {
    const next: Map<string, number>[] = [];
    for (const combo of combos) {
      for (const id of ids.slice(0, 4)) { // limit to 4 per table for samples
        const m = new Map(combo);
        m.set(tableId, id);
        next.push(m);
        if (next.length >= 20) break;
      }
      if (next.length >= 20) break;
    }
    combos = next;
  }

  return { samples: combos.slice(0, 10), totalCount };
}

/** Create a concrete sub-tree by replacing each multi-graphlet Get with a single-graphlet Get */
function instantiateTree(tree: PlanNode, combo: Map<string, number>, graphletData: GraphletInfo[]): PlanNode {
  if (isMultiGraphletGet(tree) && tree.tableId && combo.has(tree.tableId)) {
    const gid = combo.get(tree.tableId)!;
    const glInfo = graphletData.find(g => g.id === gid);
    const rows = glInfo ? glInfo.rows : 0;
    return {
      op: "Get", detail: `GL-${tree.tableId ?? "?"}-${gid} (${fmt(rows)})`, color: tree.color,
      rows, cost: rows, tableId: tree.tableId, graphletIds: [gid],
    };
  }
  return {
    ...tree,
    children: tree.children?.map(c => instantiateTree(c, combo, graphletData)),
    graphletIds: tree.graphletIds ? [...tree.graphletIds] : undefined,
  };
}

function isMultiGraphletGet(node: PlanNode): boolean {
  return node.op === "Get" && !!node.graphletIds && node.graphletIds.length > 1;
}

function findMultiGraphletGet(node: PlanNode): PlanNode | null {
  if (isMultiGraphletGet(node)) return node;
  if (node.children) {
    for (const c of node.children) {
      const found = findMultiGraphletGet(c);
      if (found) return found;
    }
  }
  return null;
}

// ---- 4. joinAssociativity -------------------------------------------------

export function joinAssociativity(joinTree: PlanNode): PlanNode | null {
  if (joinTree.op !== "Join" || !joinTree.children || joinTree.children.length < 2) {
    return null;
  }

  const [leftChild, cNode] = joinTree.children;

  // Pattern: Join(Join(A, B), C)
  if (leftChild.op !== "Join" || !leftChild.children || leftChild.children.length < 2) {
    return null;
  }

  const [aNode, bNode] = leftChild.children;

  // Check right child depth <= 1
  if (cNode.children && cNode.children.length > 0) {
    const maxDepth = treeDepth(cNode);
    if (maxDepth > 1) return null;
  }

  // Result: Join(Join(A, C), B)
  const aClone = cloneNode(aNode);
  const bClone = cloneNode(bNode);
  const cClone = cloneNode(cNode);

  const innerRows = Math.max(
    1,
    Math.round((aClone.rows ?? 0) * (cClone.rows ?? 0) * getSelectivity(aClone, cClone))
  );

  const innerJoin: PlanNode = {
    op: "Join",
    detail: `\u22C8 ${cClone.tableId ?? cClone.detail ?? "?"}`,
    color: COLORS.Join,
    rows: innerRows,
    children: [aClone, cClone],
    joinPred: joinTree.joinPred,
  };
  innerJoin.cost = computeCost(innerJoin);

  const outerRows = Math.max(
    1,
    Math.round(innerRows * (bClone.rows ?? 0) * getSelectivity(innerJoin, bClone))
  );

  const outerJoin: PlanNode = {
    op: "Join",
    detail: `\u22C8 ${bClone.tableId ?? bClone.detail ?? "?"}`,
    color: COLORS.Join,
    rows: outerRows,
    children: [innerJoin, bClone],
    joinPred: leftChild.joinPred,
  };
  outerJoin.cost = computeCost(outerJoin);

  return outerJoin;
}

function treeDepth(node: PlanNode): number {
  if (!node.children || node.children.length === 0) return 0;
  return 1 + Math.max(...node.children.map(treeDepth));
}

// ---- 5. joinCommutativity -------------------------------------------------

export function joinCommutativity(joinTree: PlanNode): PlanNode {
  if (joinTree.op !== "Join" || !joinTree.children || joinTree.children.length < 2) {
    return cloneNode(joinTree);
  }

  const [left, right] = joinTree.children;
  return {
    ...cloneNode(joinTree),
    children: [cloneNode(right), cloneNode(left)],
  };
}

// ---- 6. expandGEM ---------------------------------------------------------

export function expandGEM(
  naryJoin: PlanNode,
  graphletData: GraphletInfo[],
  numGroups = 2
): PlanNode[] {
  const nar = findNode(naryJoin, "NAryJoin");
  if (!nar || !nar.children) return [];

  const tables = nar.children;

  // Find multi-graphlet nodes
  const multiNodes = tables.filter(isMultiGraphletGet);
  if (multiNodes.length === 0) return [];

  // Build edge-respecting chain: interleave nodes and edges in query order
  // e.g. (p)-[:birthPlace]->(c)-[:country]->(co) → [p, edge:bP, c, edge:co, co]
  const nodeTables = tables.filter(t => !t.tableId?.startsWith("edge:"));
  const edgeTables = tables.filter(t => t.tableId?.startsWith("edge:"));
  const forwardChain: PlanNode[] = [];
  for (let i = 0; i < nodeTables.length; i++) {
    forwardChain.push(nodeTables[i]);
    if (i < edgeTables.length) forwardChain.push(edgeTables[i]);
  }
  const reverseChain = [...forwardChain].reverse();

  const results: PlanNode[] = [];

  for (const multiNode of multiNodes) {
    const ids = multiNode.graphletIds!;
    const groups = splitIntoGroups(ids, numGroups);

    const groupPlans: PlanNode[] = groups.map((groupIds, gi) => {
      const groupRows = sumRows(graphletData, groupIds);
      const vgGet: PlanNode = {
        op: "Get",
        detail: `VG-${multiNode.tableId ?? "?"}-${gi + 1} (${groupIds.length} GLs, ${fmt(groupRows)})`,
        color: COLORS.Get,
        rows: groupRows,
        cost: groupRows,
        tableId: multiNode.tableId,
        graphletIds: groupIds,
      };

      // Group 0: forward (left-deep), Group 1: reverse (right-deep order)
      const chain = gi % 2 === 0 ? forwardChain : reverseChain;
      const ordered = chain.map(t => t === multiNode ? vgGet : cloneNode(t));
      return buildLeftDeepJoin(ordered);
    });

    const unionAll: PlanNode = {
      op: "UnionAll",
      detail: `GEM: ${numGroups} virtual graphlets for ${multiNode.tableId}`,
      color: COLORS.UnionAll,
      children: groupPlans,
    };
    unionAll.cost = computeCost(unionAll);
    results.push(unionAll);
  }

  return results;
}

function splitIntoGroups(ids: number[], numGroups: number): number[][] {
  // Deterministic split (round-robin) for reproducibility
  const groups: number[][] = Array.from({ length: numGroups }, () => []);
  ids.forEach((id, i) => groups[i % numGroups].push(id));
  return groups;
}

/**
 * Greedy Operator Ordering (GOO):
 * Start with all tables as separate components.
 * Greedily pick the lowest-cost join pair, merge, repeat.
 */
function greedyOperatorOrdering(tables: PlanNode[]): PlanNode {
  const components = tables.map(cloneNode);

  while (components.length > 1) {
    let bestCost = Infinity;
    let bestI = 0;
    let bestJ = 1;

    for (let i = 0; i < components.length; i++) {
      for (let j = i + 1; j < components.length; j++) {
        const lRows = components[i].rows ?? 0;
        const rRows = components[j].rows ?? 0;
        const joinCost = lRows * rRows * getSelectivity(components[i], components[j]);
        if (joinCost < bestCost) {
          bestCost = joinCost;
          bestI = i;
          bestJ = j;
        }
      }
    }

    const left = components[bestI];
    const right = components[bestJ];
    const joinRows = Math.max(
      1,
      Math.round((left.rows ?? 0) * (right.rows ?? 0) * getSelectivity(left, right))
    );

    const joined: PlanNode = {
      op: "Join",
      detail: `\u22C8 ${(right.tableId ?? right.detail ?? "").replace("edge:", ":")}`,
      color: COLORS.Join,
      rows: joinRows,
      children: [left, right],
    };
    joined.cost = computeCost(joined);

    // Remove bestJ first (higher index) then bestI
    components.splice(bestJ, 1);
    components.splice(bestI, 1);
    components.push(joined);
  }

  return components[0];
}

// ---- 7. computeCost -------------------------------------------------------

export function computeCost(tree: PlanNode): number {
  switch (tree.op) {
    case "Get": {
      return tree.rows ?? 0;
    }
    case "Join": {
      if (!tree.children || tree.children.length < 2) return 0;
      const leftCost = computeCost(tree.children[0]);
      const rightCost = computeCost(tree.children[1]);
      const leftRows = tree.children[0].rows ?? 0;
      const rightRows = tree.children[1].rows ?? 0;
      const sel = getSelectivity(tree.children[0], tree.children[1]);
      return leftCost + rightCost + leftRows * rightRows * sel;
    }
    case "UnionAll": {
      if (!tree.children) return 0;
      return tree.children.reduce((s, c) => s + computeCost(c), 0);
    }
    case "NAryJoin": {
      // Cost of an un-optimized NAryJoin is the product of all child rows * selectivity^(n-1)
      if (!tree.children) return 0;
      return tree.children.reduce((s, c) => s + computeCost(c), 0);
    }
    case "Select":
    case "Filter":
    case "Project":
    default: {
      if (!tree.children || tree.children.length === 0) return tree.rows ?? 0;
      return computeCost(tree.children[0]);
    }
  }
}

// ---- 8. implementPhysical --------------------------------------------------

/**
 * Convert a logical binary join tree into a physical plan.
 * For graph queries:
 *   Join(Get(node), Get(edge)) → AdjIdxJoin(NodeScan, IndexScan(adj_fwd))
 *   Then the target node is looked up via IdSeek(IndexScan(node_id))
 *
 * Simplified: detects the pattern and produces the TurboLynx-style plan:
 *   NodeScan → AdjIdxJoin → IdSeek → Projection
 */
export function implementPhysical(logicalTree: PlanNode): PlanNode {
  return implRec(logicalTree);
}

function implRec(node: PlanNode): PlanNode {
  if (node.op === "UnionAll") {
    return { ...node, children: node.children?.map(implRec), color: COLORS.UnionAll };
  }
  if (node.op === "Join" && node.children && node.children.length === 2) {
    const [left, right] = node.children;

    const leftIsEdge = left.tableId?.startsWith("edge:") || left.detail?.startsWith(":");
    const rightIsEdge = right.tableId?.startsWith("edge:") || right.detail?.startsWith(":");
    const leftIsNode = left.op === "Get" && !leftIsEdge;
    const rightIsNode = right.op === "Get" && !rightIsEdge;

    // Pattern: Get(node) ⋈ Get(edge) → AdjIdxJoin(NodeScan, IndexScan(adj_fwd))
    if (leftIsNode && rightIsEdge) {
      const edgeName = right.tableId?.replace("edge:", "") ?? right.detail ?? "edge";
      return {
        op: "AdjIdxJoin", detail: `:${edgeName}`,
        color: "#8B5CF6", rows: node.rows, cost: node.cost,
        children: [
          { op: "NodeScan", detail: left.detail, color: COLORS.Get, rows: left.rows, tableId: left.tableId, graphletIds: left.graphletIds },
          { op: "IndexScan", detail: `${edgeName}_fwd`, color: "#0891B2", rows: right.rows },
        ],
      };
    }
    if (rightIsNode && leftIsEdge) {
      const edgeName = left.tableId?.replace("edge:", "") ?? left.detail ?? "edge";
      return {
        op: "AdjIdxJoin", detail: `:${edgeName}`,
        color: "#8B5CF6", rows: node.rows, cost: node.cost,
        children: [
          { op: "NodeScan", detail: right.detail, color: COLORS.Get, rows: right.rows, tableId: right.tableId, graphletIds: right.graphletIds },
          { op: "IndexScan", detail: `${edgeName}_fwd`, color: "#0891B2", rows: left.rows },
        ],
      };
    }

    // Pattern: AdjIdxJoin_result ⋈ Get(node) → IdSeek(AdjIdxJoin_result, IndexScan(node_id))
    // i.e. left child is a join (which becomes AdjIdxJoin) and right child is a node Get
    const implLeft = implRec(left);
    const implRight = implRec(right);

    const leftIsAdjResult = implLeft.op === "AdjIdxJoin" || implLeft.op === "IdSeek";
    const rightIsAdjResult = implRight.op === "AdjIdxJoin" || implRight.op === "IdSeek";

    if (leftIsAdjResult && (rightIsNode || right.op === "Get")) {
      return {
        op: "IdSeek", detail: right.detail ?? "lookup",
        color: "#0891B2", rows: node.rows, cost: node.cost,
        children: [
          implLeft,
          { op: "IndexScan", detail: "node_id", color: "#0891B2", rows: right.rows },
        ],
      };
    }
    if (rightIsAdjResult && (leftIsNode || left.op === "Get")) {
      return {
        op: "IdSeek", detail: left.detail ?? "lookup",
        color: "#0891B2", rows: node.rows, cost: node.cost,
        children: [
          implRight,
          { op: "IndexScan", detail: "node_id", color: "#0891B2", rows: left.rows },
        ],
      };
    }

    // Default: HashJoin (node ⋈ node without edge)
    return {
      op: "HashJoin", detail: node.detail ?? "hash",
      color: "#e84545", rows: node.rows, cost: node.cost,
      children: [implLeft, implRight],
    };
  }
  if (node.op === "Get") {
    return {
      op: "NodeScan", detail: node.detail,
      color: COLORS.Get, rows: node.rows, cost: node.cost,
      tableId: node.tableId, graphletIds: node.graphletIds,
    };
  }
  return { ...node, children: node.children?.map(implRec) };
}
