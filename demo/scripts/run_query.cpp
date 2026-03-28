// Run a single query on TurboLynx and print results.
// Usage: ./run_query --db-path /data/dbpedia -q "MATCH ..."
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include "main/capi/turbolynx.h"

int main(int argc, char* argv[]) {
    const char* db_path = nullptr;
    const char* query = nullptr;
    bool explain = false;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--db-path") == 0 && i + 1 < argc) db_path = argv[++i];
        else if (strcmp(argv[i], "-q") == 0 && i + 1 < argc) query = argv[++i];
        else if (strcmp(argv[i], "--explain") == 0) explain = true;
    }
    if (!db_path || !query) {
        fprintf(stderr, "Usage: %s --db-path <path> -q <cypher> [--explain]\n", argv[0]);
        return 1;
    }

    int64_t conn = turbolynx_connect(db_path);
    if (conn < 0) { fprintf(stderr, "Failed to connect\n"); return 1; }

    auto* prep = turbolynx_prepare(conn, const_cast<char*>(query));
    if (!prep) { fprintf(stderr, "Prepare failed\n"); turbolynx_disconnect(conn); return 1; }

    if (prep->plan) {
        printf("=== PLAN ===\n%s\n", prep->plan);
    }

    turbolynx_resultset_wrapper* rw = nullptr;
    turbolynx_num_rows total = turbolynx_execute(conn, prep, &rw);
    printf("=== RESULT: %zu rows ===\n", total);

    if (rw) {
        // Print column info
        if (rw->result_set) {
            printf("Columns: %zu\n", rw->result_set->num_properties);
        }

        int row = 0;
        while (turbolynx_fetch_next(rw) != TURBOLYNX_END_OF_RESULT && row < 5) {
            size_t ncols = rw->result_set ? rw->result_set->num_properties : 0;
            for (size_t c = 0; c < ncols && c < 10; c++) {
                turbolynx_string sv = turbolynx_get_varchar(rw, (idx_t)c);
                if (sv.data) printf("[%zu]=%.*s  ", c, (int)sv.size, sv.data);
                else printf("[%zu]=NULL  ", c);
            }
            printf("\n");
            row++;
        }
        turbolynx_close_resultset(rw);
    }

    turbolynx_close_prepared_statement(prep);
    turbolynx_disconnect(conn);
    return 0;
}
