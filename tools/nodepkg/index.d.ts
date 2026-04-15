/**
 * TurboLynx — Node.js bindings (WASM).
 */

export interface QueryResult {
  columns: string[];
  types: string[];
  rows: unknown[][];
}

export interface LabelInfo {
  name: string;
  type: 'node' | 'edge';
}

export class TurboLynxError extends Error {}

export type ParamValue = null | boolean | number | bigint | string | unknown;
export type ParamMap = Record<string, ParamValue>;

export interface ExplainResult {
  plan: string;
}

export class TurboLynx {
  /** Open a workspace directory (read-only). */
  static open(workspacePath: string): Promise<TurboLynx>;

  /** TurboLynx library version. */
  static version(): Promise<string>;

  /**
   * Run a Cypher query. Optional `params` are substituted into `$name`
   * placeholders using the same escaping rules as the Python binding.
   */
  query(cypher: string, params?: ParamMap): Promise<QueryResult>;

  /** Return the query plan (EXPLAIN) without executing. */
  explain(cypher: string, params?: ParamMap): Promise<ExplainResult>;

  /** List all node and edge labels. */
  labels(): Promise<LabelInfo[]>;

  /** Inspect properties for a label. */
  schema(label: string, isEdge?: boolean): Promise<Record<string, string>>;

  /** Close the connection. Idempotent. */
  close(): void;
}
