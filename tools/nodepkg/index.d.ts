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

export class TurboLynx {
  /** Open a workspace directory (read-only). */
  static open(workspacePath: string): Promise<TurboLynx>;

  /** TurboLynx library version. */
  static version(): Promise<string>;

  /** Run a Cypher query. */
  query(cypher: string): Promise<QueryResult>;

  /** List all node and edge labels. */
  labels(): Promise<LabelInfo[]>;

  /** Inspect properties for a label. */
  schema(label: string, isEdge?: boolean): Promise<Record<string, string>>;

  /** Close the connection. Idempotent. */
  close(): void;
}
