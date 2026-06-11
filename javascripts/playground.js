/* ── TurboLynx WASM Playground — Terminal UI ───────────────────── */
(function () {
  'use strict';

  var WASM_BASE = (window.TL_WASM_BASE || 'assets/wasm');
  var WORKSPACE_BASE = (window.TL_WORKSPACE_BASE || 'assets/workspace');
  // [remoteName, localName] — remote uses non-dot names so MkDocs/Jekyll serve them.
  var WORKSPACE_FILES = [
    ['catalog.bin', 'catalog.bin'],
    ['catalog_version', 'catalog_version'],
    ['store_meta', '.store_meta'],
    ['store.db', 'store.db']
  ];

  var statusEl    = document.getElementById('tl-wasm-status');
  var termBody    = document.getElementById('tl-terminal-body');
  var historyEl   = document.getElementById('tl-terminal-history');
  var inputEl     = document.getElementById('tl-terminal-input');
  var inputLineEl = document.getElementById('tl-terminal-input-line');

  if (!statusEl || !inputEl) return;

  var WasmModule = null;
  var connId = -1;
  var isRunning = false;

  /* ── Tab-completion state ──────────────────────────────────── */
  var CYPHER_KEYWORDS = [
    'MATCH', 'OPTIONAL', 'WHERE', 'RETURN', 'WITH', 'UNWIND', 'ORDER', 'BY',
    'SKIP', 'LIMIT', 'CREATE', 'MERGE', 'DELETE', 'DETACH', 'SET', 'REMOVE',
    'AS', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'TRUE', 'FALSE',
    'DISTINCT', 'COUNT', 'COLLECT', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
    'ASC', 'DESC', 'CONTAINS', 'STARTS', 'ENDS', 'UNION', 'ALL'
  ];
  var dbLabels = [];       // [{name, type}]
  var dbProperties = {};   // label -> [propName, ...]
  var completionItems = []; // all completable strings
  function loadCompletionData() {
    if (connId < 0 || !WasmModule) return;
    try {
      var json = WasmModule.ccall('turbolynx_wasm_get_labels', 'string', ['number'], [connId]);
      dbLabels = JSON.parse(json);

      for (var i = 0; i < dbLabels.length; i++) {
        var lbl = dbLabels[i];
        var isEdge = lbl.type === 'edge' ? 1 : 0;
        var sJson = WasmModule.ccall('turbolynx_wasm_get_schema', 'string',
          ['number', 'string', 'number'], [connId, lbl.name, isEdge]);
        var schema = JSON.parse(sJson);
        dbProperties[lbl.name] = Object.keys(schema);
      }
    } catch (e) { /* non-critical */ }

    completionItems = CYPHER_KEYWORDS.slice();
    for (var i = 0; i < dbLabels.length; i++) {
      completionItems.push(dbLabels[i].name);
    }
    var seen = {};
    for (var label in dbProperties) {
      var props = dbProperties[label];
      for (var j = 0; j < props.length; j++) {
        if (!seen[props[j]]) {
          completionItems.push(props[j]);
          seen[props[j]] = true;
        }
      }
    }
  }

  function getWordBeforeCursor() {
    var sel = window.getSelection();
    if (!sel.rangeCount) return { word: '', start: 0, end: 0, fullText: '' };
    var range = sel.getRangeAt(0);
    var text = inputEl.textContent || '';
    var pos = range.startOffset;
    var start = pos;
    while (start > 0 && /[a-zA-Z0-9_]/.test(text[start - 1])) start--;
    return { word: text.slice(start, pos), start: start, end: pos, fullText: text };
  }

  function replaceWordBeforeCursor(info, replacement) {
    var newText = info.fullText.slice(0, info.start) + replacement + info.fullText.slice(info.end);
    inputEl.textContent = newText;
    var newPos = info.start + replacement.length;
    try {
      var textNode = inputEl.firstChild;
      if (textNode) {
        var sel = window.getSelection();
        var range = document.createRange();
        range.setStart(textNode, Math.min(newPos, textNode.length));
        range.collapse(true);
        sel.removeAllRanges();
        sel.addRange(range);
      }
    } catch (e) { /* best-effort */ }
  }

  function longestCommonPrefix(arr) {
    if (arr.length === 0) return '';
    var lcp = arr[0];
    for (var i = 1; i < arr.length; i++) {
      while (arr[i].toLowerCase().indexOf(lcp.toLowerCase()) !== 0) {
        lcp = lcp.slice(0, -1);
        if (!lcp) return '';
      }
    }
    return lcp;
  }

  function handleTab(e) {
    if (connId < 0) return;
    e.preventDefault();

    var info = getWordBeforeCursor();
    var prefix = info.word;
    if (!prefix) {
      // No prefix: show all completions
      showCompletionGrid(completionItems);
      return;
    }

    var lower = prefix.toLowerCase();
    var matches = completionItems.filter(function (item) {
      return item.toLowerCase().indexOf(lower) === 0;
    });

    if (matches.length === 0) return;

    if (matches.length === 1) {
      // Single match: complete it
      replaceWordBeforeCursor(info, matches[0]);
    } else {
      // Multiple matches: complete to longest common prefix, show grid
      var lcp = longestCommonPrefix(matches);
      if (lcp.length > prefix.length) {
        replaceWordBeforeCursor(info, lcp);
      }
      showCompletionGrid(matches);
    }
  }

  function showCompletionGrid(items) {
    // Remove previous completion grid
    var prev = document.getElementById('tl-completion-grid');
    if (prev) prev.remove();

    // Compute column layout (like DuckDB shell)
    var maxLen = 0;
    for (var i = 0; i < items.length; i++) {
      if (items[i].length > maxLen) maxLen = items[i].length;
    }
    var colWidth = maxLen + 2;
    // Estimate terminal char width: ~80 cols
    var termCols = 72;
    var numCols = Math.max(1, Math.floor(termCols / colWidth));

    var html = '';
    for (var i = 0; i < items.length; i++) {
      html += escapeHtml(pad(items[i], colWidth));
      if ((i + 1) % numCols === 0) html += '\n';
    }

    appendToHistory('<pre class="tl-term-completions" id="tl-completion-grid">' + html + '</pre>');
  }

  function setStatus(text, cls) {
    statusEl.textContent = text;
    statusEl.className = 'tl-playground-badge' + (cls ? ' ' + cls : '');
  }

  /* ── Append content to terminal history ────────────────────── */
  function appendToHistory(html) {
    var div = document.createElement('div');
    div.innerHTML = html;
    historyEl.appendChild(div);
    termBody.scrollTop = termBody.scrollHeight;
  }

  function appendQueryEcho(query) {
    var lines = query.split('\n');
    var html = '<div class="tl-term-echo"><span class="tl-prompt">turbolynx&gt;&nbsp;</span>' +
      escapeHtml(lines[0]);
    for (var i = 1; i < lines.length; i++) {
      html += '\n<span class="tl-prompt-cont">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>' + escapeHtml(lines[i]);
    }
    html += '</div>';
    appendToHistory(html);
  }

  /* ── ASCII table rendering ─────────────────────────────────── */
  function renderAsciiTable(result) {
    if (!result.columns || result.columns.length === 0) return '';

    var cols = result.columns;
    var rows = result.rows || [];

    var widths = [];
    for (var c = 0; c < cols.length; c++) {
      widths[c] = cols[c].length;
    }
    for (var r = 0; r < rows.length; r++) {
      for (var c = 0; c < cols.length; c++) {
        var val = rows[r][c];
        var str = val === null ? 'null' : String(val);
        if (str.length > 30) str = str.slice(0, 28) + '..';
        rows[r][c] = str;
        if (str.length > widths[c]) widths[c] = str.length;
      }
    }

    for (var c = 0; c < widths.length; c++) {
      if (widths[c] > 30) widths[c] = 30;
    }

    var sep = '+';
    for (var c = 0; c < cols.length; c++) {
      sep += '-' + repeat('-', widths[c]) + '-+';
    }

    var header = '|';
    for (var c = 0; c < cols.length; c++) {
      header += ' ' + pad(cols[c], widths[c]) + ' |';
    }

    var body = '';
    for (var r = 0; r < rows.length; r++) {
      var line = '|';
      for (var c = 0; c < cols.length; c++) {
        line += ' ' + pad(rows[r][c], widths[c]) + ' |';
      }
      body += escapeHtml(line) + '\n';
    }

    return escapeHtml(sep) + '\n' +
           escapeHtml(header) + '\n' +
           escapeHtml(sep) + '\n' +
           body +
           escapeHtml(sep);
  }

  function pad(str, len) {
    while (str.length < len) str += ' ';
    return str;
  }

  function repeat(ch, n) {
    var s = '';
    for (var i = 0; i < n; i++) s += ch;
    return s;
  }

  /* ── Load WASM + workspace ──────────────────────────────────── */
  async function init() {
    try {
      setStatus('Loading WASM...', '');

      var resp = await fetch(WASM_BASE + '/turbolynx.js');
      if (!resp.ok) throw new Error('Failed to fetch turbolynx.js: ' + resp.status);
      var src = await resp.text();

      var createModule = new Function(src + '\nreturn Module;')();

      setStatus('Initializing...', '');
      WasmModule = await createModule({
        locateFile: function (file) { return WASM_BASE + '/' + file; }
      });

      setStatus('Loading data...', '');
      WasmModule.FS.mkdir('/workspace');

      for (var i = 0; i < WORKSPACE_FILES.length; i++) {
        var remoteName = WORKSPACE_FILES[i][0];
        var localName  = WORKSPACE_FILES[i][1];
        setStatus('Loading ' + localName + '...', '');
        try {
          var fresp = await fetch(WORKSPACE_BASE + '/' + remoteName);
          if (!fresp.ok) throw new Error(remoteName + ': ' + fresp.status + ' ' + fresp.statusText);

          var contentLength = parseInt(fresp.headers.get('Content-Length') || '0', 10);

          if (contentLength > 100 * 1024 * 1024) {
            var reader = fresp.body.getReader();
            var chunks = [];
            var received = 0;
            while (true) {
              var result = await reader.read();
              if (result.done) break;
              chunks.push(result.value);
              received += result.value.length;
              var pct = contentLength ? Math.round(received / contentLength * 100) : '?';
              setStatus('Loading ' + localName + ' (' + pct + '%)...', '');
            }
            var buf = new Uint8Array(received);
            var offset = 0;
            for (var j = 0; j < chunks.length; j++) {
              buf.set(chunks[j], offset);
              offset += chunks[j].length;
            }
          } else {
            var buf = new Uint8Array(await fresp.arrayBuffer());
          }

          WasmModule.FS.writeFile('/workspace/' + localName, buf);
          buf = null;
        } catch (e) {
          if (localName === 'store.db') {
            setStatus('Error', 'error');
            appendToHistory('<div class="tl-term-error">Failed to load store.db: ' + escapeHtml(e.message) + '</div>');
            return;
          }
          throw e;
        }
      }

      setStatus('Connecting...', '');
      connId = WasmModule.ccall('turbolynx_wasm_open', 'number', ['string'], ['/workspace']);
      if (connId < 0) throw new Error('Failed to connect (conn_id=' + connId + ')');

      // Load completion data from catalog
      loadCompletionData();

      setStatus('Ready', 'ready');
      appendToHistory('<div class="tl-term-hint">Enter ".help" for usage hints.</div>');
      inputEl.focus();

    } catch (e) {
      setStatus('Error', 'error');
      appendToHistory('<div class="tl-term-error">' + escapeHtml(e.message || String(e)) + '</div>');
    }
  }

  /* ── Dot commands ────────────────────────────────────────────── */
  function handleDotCommand(cmd) {
    var parts = cmd.trim().toLowerCase().split(/\s+/);
    var name = parts[0];

    if (name === '.help') {
      appendToHistory('<pre class="tl-term-table">' +
        '.help          Show this message\n' +
        '.labels        List node and edge labels\n' +
        '.schema LABEL  Show properties of a label\n' +
        '.clear         Clear terminal\n' +
        '\n' +
        'Tab            Autocomplete keywords and labels\n' +
        'Shift+Enter    Newline in query\n' +
        '</pre>');
      return true;
    }
    if (name === '.labels') {
      if (dbLabels.length === 0) {
        appendToHistory('<div class="tl-term-info">No labels found.</div>');
        return true;
      }
      var nodes = dbLabels.filter(function(l) { return l.type === 'node'; }).map(function(l) { return l.name; });
      var edges = dbLabels.filter(function(l) { return l.type === 'edge'; }).map(function(l) { return l.name; });
      var out = '';
      if (nodes.length) out += 'Nodes: ' + nodes.join(', ') + '\n';
      if (edges.length) out += 'Edges: ' + edges.join(', ');
      appendToHistory('<pre class="tl-term-table">' + escapeHtml(out) + '</pre>');
      return true;
    }
    if (name === '.schema') {
      var label = parts[1];
      if (!label) {
        appendToHistory('<div class="tl-term-error">Usage: .schema LABEL</div>');
        return true;
      }
      // Find matching label (case-insensitive)
      var match = null;
      for (var i = 0; i < dbLabels.length; i++) {
        if (dbLabels[i].name.toLowerCase() === label.toLowerCase()) {
          match = dbLabels[i]; break;
        }
      }
      if (!match) {
        appendToHistory('<div class="tl-term-error">Unknown label: ' + escapeHtml(label) + '</div>');
        return true;
      }
      try {
        var sJson = WasmModule.ccall('turbolynx_wasm_get_schema', 'string',
          ['number', 'string', 'number'], [connId, match.name, match.type === 'edge' ? 1 : 0]);
        var schema = JSON.parse(sJson);
        var out = match.name + ' (' + match.type + ')\n';
        for (var prop in schema) {
          out += '  ' + prop + ' : ' + schema[prop] + '\n';
        }
        appendToHistory('<pre class="tl-term-table">' + escapeHtml(out) + '</pre>');
      } catch (e) {
        appendToHistory('<div class="tl-term-error">' + escapeHtml(e.message) + '</div>');
      }
      return true;
    }
    if (name === '.clear') {
      historyEl.innerHTML = '';
      return true;
    }
    return false;
  }

  /* ── Execute query ──────────────────────────────────────────── */
  function runQuery(cypher) {
    if (connId < 0 || !WasmModule || isRunning) return;
    if (!cypher) return;

    // Handle dot commands
    if (cypher.charAt(0) === '.') {
      appendQueryEcho(cypher);
      inputEl.textContent = '';
      if (!handleDotCommand(cypher)) {
        appendToHistory('<div class="tl-term-error">Unknown command: ' + escapeHtml(cypher) + '. Enter ".help" for usage hints.</div>');
      }
      return;
    }

    isRunning = true;
    appendQueryEcho(cypher);
    inputEl.textContent = '';

    appendToHistory('<div class="tl-term-running" id="tl-running-indicator"><span class="tl-spinner"></span> Running...</div>');
    termBody.scrollTop = termBody.scrollHeight;

    setTimeout(function () {
      try {
        var t0 = performance.now();
        var json = WasmModule.ccall('turbolynx_wasm_query', 'string', ['number', 'string'], [connId, cypher]);
        var elapsed = (performance.now() - t0).toFixed(1);
        var result = JSON.parse(json);

        var spinner = document.getElementById('tl-running-indicator');
        if (spinner) spinner.remove();

        if (result.error) {
          appendToHistory('<div class="tl-term-error">Error: ' + escapeHtml(result.error) + '</div>');
        } else {
          var table = renderAsciiTable(result);
          var summary = result.rowCount + ' row' + (result.rowCount !== 1 ? 's' : '') +
                        ' \u00b7 ' + elapsed + ' ms';
          appendToHistory(
            '<pre class="tl-term-table">' + table + '</pre>' +
            '<div class="tl-term-summary">' + escapeHtml(summary) + '</div>'
          );
        }
      } catch (e) {
        var spinner = document.getElementById('tl-running-indicator');
        if (spinner) spinner.remove();
        appendToHistory('<div class="tl-term-error">Exception: ' + escapeHtml(e.message || String(e)) + '</div>');
      }
      isRunning = false;
      termBody.scrollTop = termBody.scrollHeight;
    }, 20);
  }

  function escapeHtml(str) {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  /* ── Event bindings ─────────────────────────────────────────── */
  inputEl.addEventListener('keydown', function (e) {
    if (e.key === 'Tab') {
      handleTab(e);
      return;
    }

    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      var text = inputEl.textContent.trim();
      if (text) runQuery(text);
    }
  });

  termBody.addEventListener('click', function (e) {
    if (e.target === termBody || e.target === historyEl || e.target === inputLineEl) {
      inputEl.focus();
    }
  });

  inputEl.addEventListener('paste', function (e) {
    e.preventDefault();
    var text = (e.clipboardData || window.clipboardData).getData('text/plain');
    document.execCommand('insertText', false, text);
  });

  document.querySelectorAll('.tl-preset-btn').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var query = btn.dataset.query;
      if (connId >= 0) {
        runQuery(query);
      } else {
        inputEl.textContent = query;
        inputEl.focus();
      }
    });
  });

  init();
})();
