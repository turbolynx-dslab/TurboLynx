#pragma once

#include "replxx.hxx"

namespace duckdb { class ClientContext; }

namespace turbolynx {

// Register replxx completion, highlight, and hint callbacks.
void SetupCompletion(replxx::Replxx& rx);

// Populate label/type lists from the catalog (call after DB init).
void PopulateCompletions(replxx::Replxx& rx, duckdb::ClientContext& client);

} // namespace turbolynx
