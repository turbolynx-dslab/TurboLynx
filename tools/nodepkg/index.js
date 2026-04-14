/**
 * TurboLynx — Node.js bindings (WASM).
 *
 * Loads the Emscripten-built WASM module and exposes a small async API:
 *   const { TurboLynx } = require('turbolynx');
 *   const db = await TurboLynx.open('/path/to/workspace');
 *   const r  = await db.query('MATCH (n:Person) RETURN n.firstName LIMIT 5');
 *   db.close();
 */
'use strict';

const path = require('path');

let _modulePromise = null;
function loadModule() {
  if (_modulePromise) return _modulePromise;
  // The Emscripten module exports a factory function (MODULARIZE=1).
  const factory = require(path.join(__dirname, 'wasm', 'turbolynx.js'));
  _modulePromise = factory({
    locateFile: (file) => path.join(__dirname, 'wasm', file),
  });
  return _modulePromise;
}

class TurboLynxError extends Error {
  constructor(message) {
    super(message);
    this.name = 'TurboLynxError';
  }
}

class TurboLynx {
  constructor(mod, connId, mountPoint) {
    this._mod = mod;
    this._connId = connId;
    this._mountPoint = mountPoint;
    this._closed = false;
  }

  /**
   * Open a workspace directory (read-only).
   *
   * @param {string} workspacePath  Absolute path to a TurboLynx workspace
   *                                (must contain catalog.bin, store.db,
   *                                .store_meta, catalog_version).
   * @returns {Promise<TurboLynx>}
   */
  static async open(workspacePath) {
    if (!workspacePath || typeof workspacePath !== 'string') {
      throw new TurboLynxError('open(): workspacePath must be a non-empty string');
    }
    const mod = await loadModule();
    const abs = path.resolve(workspacePath);

    // Mount the host directory into MEMFS via NODEFS so the WASM side
    // can read catalog/store files directly without copying.
    const mountPoint = '/workspace_' + Math.random().toString(36).slice(2, 10);
    try {
      mod.FS.mkdir(mountPoint);
      mod.FS.mount(mod.NODEFS, { root: abs }, mountPoint);
    } catch (e) {
      throw new TurboLynxError('Failed to mount workspace: ' + e.message);
    }

    const connId = mod.ccall('turbolynx_wasm_open', 'number', ['string'], [mountPoint]);
    if (connId < 0) {
      try { mod.FS.unmount(mountPoint); mod.FS.rmdir(mountPoint); } catch (_) {}
      throw new TurboLynxError('Failed to open workspace at ' + abs);
    }
    return new TurboLynx(mod, connId, mountPoint);
  }

  /**
   * Run a Cypher query and return the result as a plain JS object.
   *
   * @param {string} cypher
   * @returns {Promise<{columns: string[], types: string[], rows: any[][]}>}
   */
  async query(cypher) {
    this._assertOpen();
    if (typeof cypher !== 'string') {
      throw new TurboLynxError('query(): cypher must be a string');
    }
    const json = this._mod.ccall(
      'turbolynx_wasm_query',
      'string',
      ['number', 'string'],
      [this._connId, cypher],
    );
    if (!json) throw new TurboLynxError('query() returned no result');
    let parsed;
    try { parsed = JSON.parse(json); }
    catch (e) { throw new TurboLynxError('Failed to parse result JSON: ' + e.message); }
    if (parsed && parsed.error) throw new TurboLynxError(parsed.error);
    return parsed;
  }

  /**
   * List all node and edge labels in the database.
   *
   * @returns {Promise<Array<{name: string, type: 'node'|'edge'}>>}
   */
  async labels() {
    this._assertOpen();
    const json = this._mod.ccall(
      'turbolynx_wasm_get_labels', 'string', ['number'], [this._connId],
    );
    return JSON.parse(json);
  }

  /**
   * Inspect properties for a label.
   *
   * @param {string}  label   Label name (case-sensitive).
   * @param {boolean} isEdge  True if this label is an edge/relationship type.
   * @returns {Promise<Record<string, string>>}  Map of propertyName -> typeName.
   */
  async schema(label, isEdge = false) {
    this._assertOpen();
    const json = this._mod.ccall(
      'turbolynx_wasm_get_schema',
      'string',
      ['number', 'string', 'number'],
      [this._connId, label, isEdge ? 1 : 0],
    );
    return JSON.parse(json);
  }

  /**
   * Library version string (matches the C library).
   * @returns {string}
   */
  static async version() {
    const mod = await loadModule();
    return mod.ccall('turbolynx_wasm_get_version', 'string', [], []);
  }

  /**
   * Close the connection and unmount the workspace. Idempotent.
   */
  close() {
    if (this._closed) return;
    try { this._mod.ccall('turbolynx_wasm_close', null, ['number'], [this._connId]); } catch (_) {}
    try { this._mod.FS.unmount(this._mountPoint); } catch (_) {}
    try { this._mod.FS.rmdir(this._mountPoint); } catch (_) {}
    this._closed = true;
  }

  _assertOpen() {
    if (this._closed) throw new TurboLynxError('Connection is closed');
  }
}

module.exports = { TurboLynx, TurboLynxError };
