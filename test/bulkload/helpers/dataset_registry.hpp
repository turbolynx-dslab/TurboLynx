#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <stdexcept>
#include <cstring>

extern "C" {
#include "yyjson.h"
}

namespace bulktest {

struct VertexConfig {
    std::string label;       // catalog label to verify
    std::string label_arg;   // argument to --nodes (e.g. "Comment:Message")
    std::vector<std::string> files;
    uint64_t expected_count = 0;  // 0 = skip count check
};

struct EdgeConfig {
    std::string type;
    std::vector<std::string> fwd_files;
    std::vector<std::string> bwd_files;
    uint64_t expected_fwd_count = 0;  // 0 = skip count check
};

struct DatasetConfig {
    std::string name;
    std::vector<std::string> tags;
    std::string hf_repo;
    std::string local_path;
    std::vector<VertexConfig> vertices;
    std::vector<EdgeConfig>   edges;
};

class DatasetRegistry {
public:
    static void load(const char* json_path) {
        yyjson_doc* doc = yyjson_read_file(json_path, 0, nullptr, nullptr);
        if (!doc) throw std::runtime_error(std::string("Cannot read datasets.json: ") + json_path);

        yyjson_val* root     = yyjson_doc_get_root(doc);
        yyjson_val* datasets = yyjson_obj_get(root, "datasets");

        size_t idx, max;
        yyjson_val* ds;
        yyjson_arr_foreach(datasets, idx, max, ds) {
            DatasetConfig cfg;
            cfg.name       = yyjson_get_str(yyjson_obj_get(ds, "name"));
            cfg.hf_repo    = yyjson_get_str(yyjson_obj_get(ds, "hf_repo"));
            cfg.local_path = yyjson_get_str(yyjson_obj_get(ds, "local_path"));

            // tags
            yyjson_val* tags = yyjson_obj_get(ds, "tags");
            size_t ti, tmax; yyjson_val* tv;
            yyjson_arr_foreach(tags, ti, tmax, tv)
                cfg.tags.push_back(yyjson_get_str(tv));

            // vertices
            yyjson_val* verts = yyjson_obj_get(ds, "vertices");
            size_t vi, vmax; yyjson_val* vv;
            yyjson_arr_foreach(verts, vi, vmax, vv) {
                VertexConfig vc;
                vc.label          = yyjson_get_str(yyjson_obj_get(vv, "label"));
                yyjson_val* la = yyjson_obj_get(vv, "label_arg");
                vc.label_arg = la ? yyjson_get_str(la) : vc.label;
                yyjson_val* ec = yyjson_obj_get(vv, "expected_count");
                vc.expected_count = ec ? (uint64_t)yyjson_get_uint(ec) : 0;
                yyjson_val* files = yyjson_obj_get(vv, "files");
                size_t fi, fmax; yyjson_val* fv;
                yyjson_arr_foreach(files, fi, fmax, fv)
                    vc.files.push_back(yyjson_get_str(fv));
                cfg.vertices.push_back(std::move(vc));
            }

            // edges
            yyjson_val* edges = yyjson_obj_get(ds, "edges");
            size_t ei, emax; yyjson_val* ev;
            yyjson_arr_foreach(edges, ei, emax, ev) {
                EdgeConfig ec;
                ec.type = yyjson_get_str(yyjson_obj_get(ev, "type"));
                yyjson_val* efc = yyjson_obj_get(ev, "expected_fwd_count");
                ec.expected_fwd_count = efc ? (uint64_t)yyjson_get_uint(efc) : 0;
                yyjson_val* ff = yyjson_obj_get(ev, "fwd_files");
                size_t ffi, ffmax; yyjson_val* ffv;
                yyjson_arr_foreach(ff, ffi, ffmax, ffv)
                    ec.fwd_files.push_back(yyjson_get_str(ffv));
                yyjson_val* bf = yyjson_obj_get(ev, "bwd_files");
                size_t bfi, bfmax; yyjson_val* bfv;
                yyjson_arr_foreach(bf, bfi, bfmax, bfv)
                    ec.bwd_files.push_back(yyjson_get_str(bfv));
                cfg.edges.push_back(std::move(ec));
            }

            instance().map_[cfg.name] = std::move(cfg);
        }
        yyjson_doc_free(doc);
    }

    static const DatasetConfig& get(const std::string& name) {
        auto it = instance().map_.find(name);
        if (it == instance().map_.end())
            throw std::runtime_error("Dataset not found in registry: " + name);
        return it->second;
    }

    // Write updated expected_count values back to json_path
    static void write_counts(const std::string& name,
                              const std::vector<uint64_t>& vertex_counts,
                              const char* json_path);

private:
    static DatasetRegistry& instance() { static DatasetRegistry r; return r; }
    std::unordered_map<std::string, DatasetConfig> map_;
};

// Write-back implementation (--generate mode)
inline void DatasetRegistry::write_counts(const std::string& name,
                                           const std::vector<uint64_t>& vertex_counts,
                                           const char* json_path) {
    yyjson_doc* doc = yyjson_read_file(json_path, 0, nullptr, nullptr);
    if (!doc) return;
    yyjson_mut_doc* mdoc = yyjson_doc_mut_copy(doc, nullptr);
    yyjson_doc_free(doc);

    yyjson_mut_val* root     = yyjson_mut_doc_get_root(mdoc);
    yyjson_mut_val* datasets = yyjson_mut_obj_get(root, "datasets");

    size_t idx, max; yyjson_mut_val* ds;
    yyjson_mut_arr_foreach(datasets, idx, max, ds) {
        yyjson_mut_val* n = yyjson_mut_obj_get(ds, "name");
        if (!n || std::string(yyjson_mut_get_str(n)) != name) continue;

        yyjson_mut_val* verts = yyjson_mut_obj_get(ds, "vertices");
        size_t vi, vmax; yyjson_mut_val* vv;
        size_t ci = 0;
        yyjson_mut_arr_foreach(verts, vi, vmax, vv) {
            if (ci >= vertex_counts.size()) break;
            // Replace expected_count with new uint value using put (remove + add)
            yyjson_mut_val* key = yyjson_mut_str(mdoc, "expected_count");
            yyjson_mut_val* val = yyjson_mut_uint(mdoc, vertex_counts[ci]);
            yyjson_mut_obj_put(vv, key, val);
            ci++;
        }
        break;
    }

    yyjson_write_err err;
    yyjson_mut_write_file(json_path, mdoc, YYJSON_WRITE_PRETTY, nullptr, &err);
    if (err.code) fprintf(stderr, "[generate] write error: %s\n", err.msg);
    yyjson_mut_doc_free(mdoc);
}

} // namespace bulktest
