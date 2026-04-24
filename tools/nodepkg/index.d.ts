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

export interface OpenOptions {
  /**
   * When true, open the workspace via the writable connect path — WAL
   * writer active, CREATE / MERGE / SET / DELETE / REMOVE / DROP accepted.
   * Defaults to false (read-only).
   */
  writable?: boolean;
}

export class TurboLynx {
  /**
   * Open a workspace directory. Defaults to read-only; pass
   * `{ writable: true }` to enable mutations.
   */
  static open(workspacePath: string, options?: OpenOptions): Promise<TurboLynx>;

  /** True if the connection was opened in writable mode. */
  readonly writable: boolean;

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
