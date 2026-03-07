#pragma once

#include "loader/bulkload_options.hpp"
#include <memory>

namespace duckdb {

class DuckDB;
struct BulkloadContext; // internal detail — defined in bulkload_pipeline.cpp

class BulkloadPipeline {
public:
    explicit BulkloadPipeline(BulkloadOptions opts);
    ~BulkloadPipeline();

    // vertices → edges (fwd+bwd interleaved per file) → histogram → persist
    void Run();

private:
    void InitializeWorkspace();
    void LoadVertices();
    void LoadEdges();       // fwd+bwd interleaved per file — replaces separate LoadForwardEdges/LoadBackwardEdges
    void RunPostProcessing();

    BulkloadOptions              opts_;
    std::unique_ptr<DuckDB>      database_;
    std::unique_ptr<BulkloadContext> ctx_; // must be declared after database_
};

} // namespace duckdb
