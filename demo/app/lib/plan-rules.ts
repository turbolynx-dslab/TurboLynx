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

/** Default join selectivity constant. */
const JOIN_SELECTIVITY = 0.00001;

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
      detail: `${qn.alias} (${qn.graphletIds.length} GLs, ${fmt(rows)} rows)`,
      color: COLORS.Get,
      rows,
      cost: rows,
      tableId: qn.alias,
      graphletIds: [...qn.graphletIds],
    };
  });

  // Build Get nodes for each edge type
  const edgeGets: PlanNode[] = queryEdges.map((qe) => {
    const ep = catalog.edgePartitions.find((e) => e.type === qe.type);
    const rows = ep ? ep.totalRows : 0;
    return {
      op: "Get",
      detail: `[:${qe.type}] (${fmt(rows)} rows)`,
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

function buildLeftDeepJoin(tables: PlanNode[]): PlanNode {
  let current = cloneNode(tables[0]);
  for (let i = 1; i < tables.length; i++) {
    const right = cloneNode(tables[i]);
    const leftRows = current.rows ?? 0;
    const rightRows = right.rows ?? 0;
    const joinRows = Math.max(1, Math.round(leftRows * rightRows * JOIN_SELECTIVITY));
    const rightLabel = right.tableId ?? right.detail ?? "?";
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

export function pushJoinBelowUnionAll(
  joinTree: PlanNode,
  graphletData: GraphletInfo[]
): PlanNode {
  return pushJoinBelowUnionAllRec(joinTree, graphletData);
}

function pushJoinBelowUnionAllRec(
  node: PlanNode,
  graphletData: GraphletInfo[]
): PlanNode {
  if (node.op !== "Join" || !node.children || node.children.length < 2) {
    return cloneNode(node);
  }

  const [left, right] = node.children;

  // Recurse into children first (deepest-first transformation)
  const newLeft = pushJoinBelowUnionAllRec(left, graphletData);
  const newRight = pushJoinBelowUnionAllRec(right, graphletData);

  // Check if either child is a multi-graphlet Get
  const multiGet = findMultiGraphletGet(newLeft) ?? findMultiGraphletGet(newRight);
  if (!multiGet) {
    return { ...node, children: [newLeft, newRight] };
  }

  // Determine which side is the multi-graphlet Get
  const isLeftMulti = isMultiGraphletGet(newLeft);
  const multiSide = isLeftMulti ? newLeft : newRight;
  const otherSide = isLeftMulti ? newRight : newLeft;

  if (!multiSide.graphletIds || multiSide.graphletIds.length <= 1) {
    return { ...node, children: [newLeft, newRight] };
  }

  const MAX_DISPLAY = 4;
  const allIds = multiSide.graphletIds;
  const displayIds = allIds.slice(0, MAX_DISPLAY);
  const overflow = allIds.length - MAX_DISPLAY;

  const unionChildren: PlanNode[] = displayIds.map((gid) => {
    const glInfo = graphletData.find((g) => g.id === gid);
    const glRows = glInfo ? glInfo.rows : 0;
    const getNode: PlanNode = {
      op: "Get",
      detail: `GL-${gid} (${fmt(glRows)} rows)`,
      color: multiSide.color,
      rows: glRows,
      cost: glRows,
      tableId: multiSide.tableId,
      graphletIds: [gid],
    };
    const otherClone = cloneNode(otherSide);
    const joinRows = Math.max(
      1,
      Math.round(glRows * (otherClone.rows ?? 0) * JOIN_SELECTIVITY)
    );
    return {
      op: "Join",
      detail: node.detail,
      color: COLORS.Join,
      rows: joinRows,
      children: isLeftMulti ? [getNode, otherClone] : [otherClone, getNode],
      joinPred: node.joinPred,
    };
  });

  if (overflow > 0) {
    unionChildren.push({
      op: "Join",
      detail: `... +${overflow} more`,
      color: "#a1a1aa",
      rows: 0,
    });
  }

  const unionAll: PlanNode = {
    op: "UnionAll",
    detail: `${allIds.length} sub-plans`,
    color: COLORS.UnionAll,
    rows: unionChildren.reduce((s, c) => s + (c.rows ?? 0), 0),
    children: unionChildren,
  };

  unionAll.cost = computeCost(unionAll);
  return unionAll;
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
    Math.round((aClone.rows ?? 0) * (cClone.rows ?? 0) * JOIN_SELECTIVITY)
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
    Math.round(innerRows * (bClone.rows ?? 0) * JOIN_SELECTIVITY)
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
  const results: PlanNode[] = [];

  // Find multi-graphlet nodes
  const multiNodes = tables.filter(isMultiGraphletGet);
  if (multiNodes.length === 0) return [];

  // For each multi-graphlet node, split into groups
  for (const multiNode of multiNodes) {
    const ids = multiNode.graphletIds!;
    const groups = splitIntoGroups(ids, numGroups);

    const groupPlans: PlanNode[] = groups.map((groupIds, gi) => {
      const groupRows = sumRows(graphletData, groupIds);
      const vgGet: PlanNode = {
        op: "Get",
        detail: `VG-${gi + 1} (${groupIds.length} GLs, ${fmt(groupRows)} rows)`,
        color: COLORS.Get,
        rows: groupRows,
        cost: groupRows,
        tableId: multiNode.tableId,
        graphletIds: groupIds,
      };

      // Build table set for this group's sub-problem:
      // replace the multi-node with the VG, keep everything else
      const subTables = tables.map((t) =>
        t === multiNode ? vgGet : cloneNode(t)
      );

      return greedyOperatorOrdering(subTables);
    });

    const unionAll: PlanNode = {
      op: "UnionAll",
      detail: `GEM: ${numGroups} virtual graphlets for ${multiNode.tableId}`,
      color: COLORS.UnionAll,
      rows: groupPlans.reduce((s, c) => s + (c.rows ?? 0), 0),
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
        const joinCost = lRows * rRows * JOIN_SELECTIVITY;
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
      Math.round((left.rows ?? 0) * (right.rows ?? 0) * JOIN_SELECTIVITY)
    );

    const joined: PlanNode = {
      op: "Join",
      detail: `\u22C8 GOO`,
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
      return leftCost + rightCost + leftRows * rightRows * JOIN_SELECTIVITY;
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
