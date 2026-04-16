#include "catch.hpp"

#include "../query/helpers/query_runner.hpp"
#include "test_helper.hpp"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <sys/wait.h>

namespace fs = std::filesystem;

namespace {

std::string ShellQuote(const std::string& value) {
    std::string quoted = "'";
    for (char c : value) {
        if (c == '\'') {
            quoted += "'\\''";
        } else {
            quoted += c;
        }
    }
    quoted += "'";
    return quoted;
}

int RunCommand(const std::string& command) {
    int rc = std::system(command.c_str());
    if (rc == -1) return -1;
    if (WIFEXITED(rc)) return WEXITSTATUS(rc);
    if (WIFSIGNALED(rc)) return 128 + WTERMSIG(rc);
    return rc;
}

fs::path WriteTinyPersonCsv(const fs::path& dir) {
    fs::path csv = dir / "person.csv";
    std::ofstream out(csv);
    REQUIRE(out.good());
    out << "id:ID(Person)|firstName:STRING\n";
    out << "1|Alice\n";
    out << "2|Bob\n";
    out.close();
    return csv;
}

void VerifyImportedWorkspace(const fs::path& workspace) {
    qtest::QueryRunner qr(workspace.string());
    REQUIRE(qr.count("MATCH (n:Person) RETURN count(n) AS cnt") == 2);
}

void RunImportSmoke(bool documented_syntax) {
    turbolynxtest::ScopedTempDir temp_dir;
    fs::path workspace = fs::path(temp_dir.path()) / "workspace";
    fs::path csv = WriteTinyPersonCsv(temp_dir.path());

    std::ostringstream cmd;
    cmd << ShellQuote(TEST_BULKLOAD_BIN) << " import"
        << " --workspace " << ShellQuote(workspace.string());
    if (documented_syntax) {
        cmd << " --nodes Person " << ShellQuote(csv.string());
    } else {
        cmd << " --nodes Person --nodes " << ShellQuote(csv.string());
    }
    cmd << " --skip-histogram";

    INFO(cmd.str());
    REQUIRE(RunCommand(cmd.str()) == 0);
    REQUIRE(fs::exists(workspace / "catalog.bin"));

    VerifyImportedWorkspace(workspace);
}

} // namespace

TEST_CASE("CLI import accepts documented node syntax and reopens cleanly",
          "[bulkload][smoke][cli]") {
    RunImportSmoke(true);
}

TEST_CASE("CLI import keeps repeated flag compatibility and reopens cleanly",
          "[bulkload][smoke][cli]") {
    RunImportSmoke(false);
}
