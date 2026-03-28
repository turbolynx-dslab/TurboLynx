// Shared query builder state types — used by S2_QuerySelect and S3_Plan

export type EdgeDirection = "right" | "left" | "undirected";
export type WhereOp = "=" | "!=" | ">" | "<" | ">=" | "<=" | "IS NOT NULL" | "IS NULL" | "CONTAINS" | "STARTS WITH";
export type AggFn = "" | "COUNT" | "SUM" | "AVG" | "MIN" | "MAX";

export interface MatchPattern {
  id: string; sourceVar: string; edgeType: string;
  direction: EdgeDirection; targetVar: string; optional: boolean;
}
export interface WhereFilter {
  id: string; variable: string; property: string; operator: WhereOp; value: string;
}
export interface ReturnItem {
  id: string; variable: string; property: string; alias: string; aggregate: AggFn;
}
export interface OrderByItem { expr: string; desc: boolean; }

export interface QState {
  matches: MatchPattern[];
  wheres: WhereFilter[];
  returns: ReturnItem[];
  orderBy: OrderByItem[];
  limit: number | null;
}

// Default initial state
let _id = 0;
export const uid = () => `_${++_id}`;

export const mkMatch = (s = "", e = "", d: EdgeDirection = "right", t = "", opt = false): MatchPattern =>
  ({ id: uid(), sourceVar: s, edgeType: e, direction: d, targetVar: t, optional: opt });
export const mkReturn = (v = "", p = "", agg: AggFn = "", alias = ""): ReturnItem =>
  ({ id: uid(), variable: v, property: p, aggregate: agg, alias });

export const INIT_QSTATE: QState = {
  matches: [mkMatch()],
  wheres: [],
  returns: [mkReturn()],
  orderBy: [],
  limit: 20,
};

// Cypher generation
export function generateCypher(s: QState): string {
  const lines: string[] = [];
  const used = new Set<number>();
  for (let i = 0; i < s.matches.length; i++) {
    if (used.has(i)) continue;
    const m = s.matches[i];
    if (!m.sourceVar && !m.edgeType && !m.targetVar) continue;
    const kw = m.optional ? "OPTIONAL MATCH" : "MATCH";
    let chain = fmtArrow(m);
    let lastTarget = m.targetVar;
    used.add(i);
    for (let j = i + 1; j < s.matches.length; j++) {
      if (used.has(j)) continue;
      const n = s.matches[j];
      if (n.sourceVar === lastTarget && n.sourceVar && !n.optional === !m.optional) {
        chain += fmtArrowCont(n);
        lastTarget = n.targetVar;
        used.add(j);
      }
    }
    lines.push(`${kw} ${chain}`);
  }
  const conds = s.wheres.filter(w => w.variable && w.property).map(w => {
    if (w.operator === "IS NOT NULL" || w.operator === "IS NULL") return `${w.variable}.${w.property} ${w.operator}`;
    const val = /^\d+(\.\d+)?$/.test(w.value) ? w.value : `"${w.value}"`;
    return `${w.variable}.${w.property} ${w.operator} ${val}`;
  });
  if (conds.length) lines.push(`WHERE ${conds.join("\n  AND ")}`);
  const rets = s.returns.filter(r => r.variable).map(r => {
    let expr = r.property ? `${r.variable}.${r.property}` : r.variable;
    if (r.aggregate) expr = `${r.aggregate}(${expr})`;
    if (r.alias) expr += ` AS ${r.alias}`;
    return expr;
  });
  if (rets.length) lines.push(`RETURN ${rets.join(", ")}`);
  const ords = s.orderBy.filter(o => o.expr).map(o => o.desc ? `${o.expr} DESC` : o.expr);
  if (ords.length) lines.push(`ORDER BY ${ords.join(", ")}`);
  if (s.limit !== null && s.limit > 0) lines.push(`LIMIT ${s.limit}`);
  return lines.join("\n");
}

function fmtArrow(m: MatchPattern): string {
  const e = m.edgeType ? `[:${m.edgeType}]` : "";
  if (m.direction === "right") return `(${m.sourceVar})-${e}->(${m.targetVar})`;
  if (m.direction === "left") return `(${m.sourceVar})<-${e}-(${m.targetVar})`;
  return `(${m.sourceVar})-${e}-(${m.targetVar})`;
}
function fmtArrowCont(m: MatchPattern): string {
  const e = m.edgeType ? `[:${m.edgeType}]` : "";
  if (m.direction === "right") return `-${e}->(${m.targetVar})`;
  if (m.direction === "left") return `<-${e}-(${m.targetVar})`;
  return `-${e}-(${m.targetVar})`;
}
