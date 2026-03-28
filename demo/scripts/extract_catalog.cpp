// Extract TurboLynx catalog to JSON using turbolynx_dump_catalog_json C API.
// Usage: ./extract_catalog --db-path /data/dbpedia -o catalog.json
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include "main/capi/turbolynx.h"

int main(int argc, char* argv[]) {
    const char* db_path = nullptr;
    const char* out_path = nullptr;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--db-path") == 0 && i + 1 < argc) db_path = argv[++i];
        else if (strcmp(argv[i], "-o") == 0 && i + 1 < argc) out_path = argv[++i];
    }
    if (!db_path) {
        fprintf(stderr, "Usage: %s --db-path <path> [-o output.json]\n", argv[0]);
        return 1;
    }

    int64_t conn = turbolynx_connect(db_path);
    if (conn < 0) { fprintf(stderr, "Failed to connect to %s\n", db_path); return 1; }

    char* json = turbolynx_dump_catalog_json(conn);
    if (!json) { fprintf(stderr, "Failed to dump catalog\n"); turbolynx_disconnect(conn); return 1; }

    if (out_path) {
        FILE* f = fopen(out_path, "w");
        if (!f) { fprintf(stderr, "Cannot open %s\n", out_path); free(json); turbolynx_disconnect(conn); return 1; }
        fputs(json, f);
        fclose(f);
    } else {
        fputs(json, stdout);
    }

    free(json);
    turbolynx_disconnect(conn);
    return 0;
}
