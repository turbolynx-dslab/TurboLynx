#pragma once

namespace duckdb { class ClientContext; }

namespace turbolynx {

// Register linenoise completion and hints callbacks.
// Call once before entering the REPL loop.
void SetupCompletion();

// Populate label/type completion lists from the catalog.
// Call after DB is initialized; safe to call again after schema changes.
void PopulateCompletions(duckdb::ClientContext& client);

} // namespace turbolynx
