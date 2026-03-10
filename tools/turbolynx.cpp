#include <iostream>
#include <string>

int RunShell(int argc, char** argv);
int RunImport(int argc, char** argv);

static void PrintUsage() {
    std::cout << "Usage: turbolynx [subcommand] [options]\n"
              << "\n"
              << "Subcommands:\n"
              << "  shell    Launch interactive query shell (default)\n"
              << "  import   Bulk-load graph data from CSV files\n"
              << "\n"
              << "Examples:\n"
              << "  turbolynx --workspace /path/to/db\n"
              << "  turbolynx shell --workspace /path/to/db --query \"MATCH (n:Person) RETURN count(*)\"\n"
              << "  turbolynx import --workspace /path/to/db \\\n"
              << "      --nodes Person dynamic/Person.csv \\\n"
              << "      --relationships KNOWS dynamic/Person_knows_Person.csv\n"
              << "\n"
              << "Run 'turbolynx <subcommand> --help' for subcommand-specific options.\n";
}

int main(int argc, char** argv) {
    // If first argument is not a flag, treat it as a subcommand
    std::string subcmd = "shell";
    if (argc > 1 && argv[1][0] != '-') {
        subcmd = argv[1];
        argc--;
        argv++;
    }

    if (subcmd == "shell") {
        return RunShell(argc, argv);
    } else if (subcmd == "import") {
        return RunImport(argc, argv);
    } else {
        std::cerr << "turbolynx: unknown subcommand '" << subcmd << "'\n\n";
        PrintUsage();
        return 1;
    }
}
