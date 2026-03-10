// =============================================================================
// [storage][multiproc] Multi-process file locking tests
// =============================================================================
// Validates the Single Writer + Multiple Readers invariant introduced in M16.
//
// Advisory fcntl locks are per-process, so conflict testing requires separate
// processes. Pattern: fork() child, child exits 0 on expected behavior.
// _exit() is used in children to skip C++ destructors.
//
// CCM tests use g_test_settings.test_workspace (supports O_DIRECT).
// Raw fcntl tests use ScopedTempDir (no O_DIRECT required).
// =============================================================================

#include "catch.hpp"
#include "test_config.hpp"
#include "test_helper.hpp"

#include "storage/cache/chunk_cache_manager.h"
#include "common/exception.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>
#include <string>

namespace fs = std::filesystem;

using namespace duckdb;
using namespace turbolynxtest;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Create an empty file (no-op if already exists).
static void touch_file(const std::string &path) {
    int fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd >= 0) close(fd);
}

// Open path and non-blockingly try to set an advisory lock.
// lock_type: F_RDLCK or F_WRLCK.
// Returns: fd >= 0 on success, -1 if lock conflict (EAGAIN), -2 if open failed.
static int try_lock(const std::string &path, short lock_type) {
    int flags = (lock_type == F_RDLCK) ? O_RDONLY : O_RDWR;
    int fd = open(path.c_str(), flags);
    if (fd < 0) return -2;
    struct flock fl = {};
    fl.l_type   = lock_type;
    fl.l_whence = SEEK_SET;
    fl.l_start  = 0;
    fl.l_len    = 0;
    if (fcntl(fd, F_SETLK, &fl) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// Wait for forked child and return its exit code, or -1 if killed by signal.
static int wait_child(pid_t pid) {
    int status = 0;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

// Create a unique subdir inside the test workspace (O_DIRECT-compatible fs).
static std::string workspace_subdir(const std::string &tag) {
    std::string dir = g_test_settings.test_workspace
                      + "/mp_" + tag + "_" + std::to_string(getpid());
    fs::create_directories(dir);
    return dir;
}

// ===========================================================================
// Raw fcntl lock semantics
// ===========================================================================

TEST_CASE("Multi-process: F_WRLCK excludes another writer", "[storage][multiproc]") {
    ScopedTempDir tmp;
    std::string store = tmp.path() + "/store.db";
    touch_file(store);

    int pfd = try_lock(store, F_WRLCK);
    REQUIRE(pfd >= 0);

    pid_t pid = fork();
    if (pid == 0) {
        int cfd = try_lock(store, F_WRLCK);
        _exit(cfd == -1 ? 0 : 1);  // 0 = lock correctly refused
    }

    int rc = wait_child(pid);
    close(pfd);
    REQUIRE(rc == 0);
}

TEST_CASE("Multi-process: F_WRLCK excludes a reader", "[storage][multiproc]") {
    ScopedTempDir tmp;
    std::string store = tmp.path() + "/store.db";
    touch_file(store);

    int pfd = try_lock(store, F_WRLCK);
    REQUIRE(pfd >= 0);

    pid_t pid = fork();
    if (pid == 0) {
        int cfd = try_lock(store, F_RDLCK);
        _exit(cfd == -1 ? 0 : 1);
    }

    int rc = wait_child(pid);
    close(pfd);
    REQUIRE(rc == 0);
}

TEST_CASE("Multi-process: multiple F_RDLCK can coexist", "[storage][multiproc]") {
    ScopedTempDir tmp;
    std::string store = tmp.path() + "/store.db";
    touch_file(store);

    int pfd = try_lock(store, F_RDLCK);
    REQUIRE(pfd >= 0);

    pid_t pid = fork();
    if (pid == 0) {
        // Shared read lock must be granted when parent holds F_RDLCK
        int cfd = try_lock(store, F_RDLCK);
        _exit(cfd >= 0 ? 0 : 1);
    }

    int rc = wait_child(pid);
    close(pfd);
    REQUIRE(rc == 0);
}

TEST_CASE("Multi-process: F_RDLCK excludes a writer", "[storage][multiproc]") {
    ScopedTempDir tmp;
    std::string store = tmp.path() + "/store.db";
    touch_file(store);

    int pfd = try_lock(store, F_RDLCK);
    REQUIRE(pfd >= 0);

    pid_t pid = fork();
    if (pid == 0) {
        int cfd = try_lock(store, F_WRLCK);
        _exit(cfd == -1 ? 0 : 1);
    }

    int rc = wait_child(pid);
    close(pfd);
    REQUIRE(rc == 0);
}

TEST_CASE("Multi-process: lock released when process exits", "[storage][multiproc]") {
    ScopedTempDir tmp;
    std::string store = tmp.path() + "/store.db";
    touch_file(store);

    // Child acquires lock and exits — lock must be auto-released
    pid_t pid = fork();
    if (pid == 0) {
        int cfd = try_lock(store, F_WRLCK);
        _exit(cfd >= 0 ? 0 : 1);
    }
    REQUIRE(wait_child(pid) == 0);

    // Parent can now acquire the same lock
    int pfd = try_lock(store, F_WRLCK);
    REQUIRE(pfd >= 0);
    close(pfd);
}

// ===========================================================================
// ChunkCacheManager — lock conflict detection
// ===========================================================================

TEST_CASE("Multi-process: CCM write mode throws when writer lock is held",
          "[storage][multiproc]") {
    std::string dir   = workspace_subdir("ccm_ww");
    std::string store = dir + "/store.db";
    touch_file(store);

    // Parent holds raw F_WRLCK — avoids triggering the 1 GB fallocate in CCM
    int pfd = open(store.c_str(), O_RDWR);
    REQUIRE(pfd >= 0);
    struct flock fl = {};
    fl.l_type = F_WRLCK; fl.l_whence = SEEK_SET; fl.l_len = 0;
    REQUIRE(fcntl(pfd, F_SETLK, &fl) == 0);

    pid_t pid = fork();
    if (pid == 0) {
        bool ok = false;
        try {
            auto *ccm = new ChunkCacheManager(dir.c_str(), false, /*read_only=*/false);
            delete ccm;  // must not reach here
        } catch (const duckdb::IOException &e) {
            ok = (std::string(e.what()).find("store.db is locked") != std::string::npos);
        } catch (...) {}
        _exit(ok ? 0 : 1);
    }

    int rc = wait_child(pid);
    close(pfd);
    fs::remove_all(dir);

    REQUIRE(rc == 0);
}

TEST_CASE("Multi-process: CCM read-only throws when writer lock is held",
          "[storage][multiproc]") {
    std::string dir   = workspace_subdir("ccm_rw");
    std::string store = dir + "/store.db";
    touch_file(store);

    int pfd = open(store.c_str(), O_RDWR);
    REQUIRE(pfd >= 0);
    struct flock fl = {};
    fl.l_type = F_WRLCK; fl.l_whence = SEEK_SET; fl.l_len = 0;
    REQUIRE(fcntl(pfd, F_SETLK, &fl) == 0);

    pid_t pid = fork();
    if (pid == 0) {
        bool ok = false;
        try {
            auto *ccm = new ChunkCacheManager(dir.c_str(), false, /*read_only=*/true);
            delete ccm;
        } catch (const duckdb::IOException &e) {
            ok = (std::string(e.what()).find("locked by a writer") != std::string::npos);
        } catch (...) {}
        _exit(ok ? 0 : 1);
    }

    int rc = wait_child(pid);
    close(pfd);
    fs::remove_all(dir);

    REQUIRE(rc == 0);
}

TEST_CASE("Multi-process: CCM read-only succeeds with no lock contention",
          "[storage][multiproc]") {
    std::string dir   = workspace_subdir("ccm_ro");
    std::string store = dir + "/store.db";
    touch_file(store);

    ChunkCacheManager *ccm = nullptr;
    REQUIRE_NOTHROW(ccm = new ChunkCacheManager(dir.c_str(), false, /*read_only=*/true));
    REQUIRE(ccm != nullptr);
    REQUIRE(ccm->read_only_ == true);

    delete ccm;
    fs::remove_all(dir);
}
