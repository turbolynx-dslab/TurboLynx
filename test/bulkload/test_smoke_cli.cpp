#include "catch.hpp"

#include "../query/helpers/query_runner.hpp"
#include "test_helper.hpp"

#include <array>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
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

std::string RunCommandCapture(const std::string& command, int& exit_code) {
    std::array<char, 512> buf;
    std::string output;
    std::unique_ptr<FILE, int(*)(FILE*)> pipe(popen(command.c_str(), "r"), pclose);
    if (!pipe) {
        exit_code = -1;
        return output;
    }
    while (fgets(buf.data(), buf.size(), pipe.get()) != nullptr) {
        output.append(buf.data());
    }
    int rc = pclose(pipe.release());
    if (rc == -1) {
        exit_code = -1;
    } else if (WIFEXITED(rc)) {
        exit_code = WEXITSTATUS(rc);
    } else if (WIFSIGNALED(rc)) {
        exit_code = 128 + WTERMSIG(rc);
    } else {
        exit_code = rc;
    }
    return output;
}

fs::path WriteTinyPersonCsv(const fs::path& dir) {
    fs::path csv = dir / "person.csv";
    std::ofstream out(csv);
    REQUIRE(out.good());
    out << "id:ID(Person)|firstName:STRING|city:STRING\n";
    out << "1|Alice|Seoul\n";
    out << "2|Bob|Seoul\n";
    out << "3|Carol|Busan\n";
    out.close();
    return csv;
}

fs::path WriteTinyKnowsCsv(const fs::path& dir) {
    fs::path csv = dir / "knows.csv";
    std::ofstream out(csv);
    REQUIRE(out.good());
    out << ":START_ID(Person)|:END_ID(Person)|since:INT\n";
    out << "1|2|2010\n";
    out << "2|3|2012\n";
    out.close();
    return csv;
}

std::string RunShellQuery(const fs::path& workspace, const std::string& query) {
    std::ostringstream cmd;
    cmd << ShellQuote(TEST_BULKLOAD_BIN)
        << " --ws " << ShellQuote(workspace.string())
        << " --mode csv --q " << ShellQuote(query)
        << " 2>&1";
    int rc = 0;
    auto out = RunCommandCapture(cmd.str(), rc);
    INFO(cmd.str());
    INFO(out);
    REQUIRE(rc == 0);
    return out;
}

void VerifyImportedWorkspace(const fs::path& workspace) {
    qtest::QueryRunner qr(workspace.string());
    REQUIRE(qr.count("MATCH (n:Person) RETURN count(n) AS cnt") == 3);
}

void RunImportSmoke(bool documented_syntax) {
    turbolynxtest::ScopedTempDir temp_dir;
    fs::path workspace = fs::path(temp_dir.path()) / "workspace";
    fs::path csv = WriteTinyPersonCsv(temp_dir.path());
    fs::path knows = WriteTinyKnowsCsv(temp_dir.path());

    std::ostringstream cmd;
    cmd << ShellQuote(TEST_BULKLOAD_BIN) << " import"
        << " --workspace " << ShellQuote(workspace.string());
    if (documented_syntax) {
        cmd << " --nodes Person " << ShellQuote(csv.string());
    } else {
        cmd << " --nodes Person --nodes " << ShellQuote(csv.string());
    }
    cmd << " --relationships KNOWS " << ShellQuote(knows.string());
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

TEST_CASE("Shell one-shot CRUD persists cleanly across reopen",
          "[bulkload][smoke][cli][shell]") {
    turbolynxtest::ScopedTempDir temp_dir;
    fs::path workspace = fs::path(temp_dir.path()) / "workspace";
    fs::path person = WriteTinyPersonCsv(temp_dir.path());
    fs::path knows = WriteTinyKnowsCsv(temp_dir.path());

    std::ostringstream import_cmd;
    import_cmd << ShellQuote(TEST_BULKLOAD_BIN) << " import"
               << " --workspace " << ShellQuote(workspace.string())
               << " --nodes Person " << ShellQuote(person.string())
               << " --relationships KNOWS " << ShellQuote(knows.string())
               << " --skip-histogram";
    REQUIRE(RunCommand(import_cmd.str()) == 0);

    auto set_out = RunShellQuery(workspace, "MATCH (n:Person {id: 1}) SET n.city = 'Daegu'");
    REQUIRE(set_out.find("Nodes created / updated / deleted successfully.") != std::string::npos);
    REQUIRE(RunShellQuery(workspace, "MATCH (n:Person {id: 1}) RETURN n.firstName, n.city;")
                .find("Alice,Daegu") != std::string::npos);

    auto create_out = RunShellQuery(workspace,
        "CREATE (n:Person {id: 4, firstName: 'Dave', city: 'Incheon'})");
    REQUIRE(create_out.find("Nodes created / updated / deleted successfully.") != std::string::npos);
    REQUIRE(RunShellQuery(workspace, "MATCH (n:Person) RETURN count(n);")
                .find("\n4\n") != std::string::npos);

    auto delete_out = RunShellQuery(workspace, "MATCH (n:Person {id: 4}) DELETE n");
    REQUIRE(delete_out.find("Nodes created / updated / deleted successfully.") != std::string::npos);
    REQUIRE(RunShellQuery(workspace, "MATCH (n:Person) RETURN count(n);")
                .find("\n3\n") != std::string::npos);

    auto detach_out = RunShellQuery(workspace, "MATCH (n:Person {id: 2}) DETACH DELETE n");
    REQUIRE(detach_out.find("Nodes created / updated / deleted successfully.") != std::string::npos);
    REQUIRE(RunShellQuery(workspace, "MATCH (n:Person) RETURN count(n);")
                .find("\n2\n") != std::string::npos);

    auto merge_out = RunShellQuery(workspace, "MERGE (n:Person {id: 5, firstName: 'Eve'})");
    REQUIRE(merge_out.find("Nodes created / updated / deleted successfully.") != std::string::npos);
    REQUIRE(merge_out.find("NodeScan(") == std::string::npos);
    REQUIRE(RunShellQuery(workspace, "MATCH (n:Person {id: 5}) RETURN n.firstName;")
                .find("\nEve\n") != std::string::npos);

    auto remove_out = RunShellQuery(workspace, "MATCH (n:Person {id: 1}) REMOVE n.city");
    REQUIRE(remove_out.find("Nodes created / updated / deleted successfully.") != std::string::npos);
    REQUIRE(RunShellQuery(workspace, "MATCH (n:Person {id: 1}) RETURN n.firstName, n.city;")
                .find("Alice,") != std::string::npos);
}
