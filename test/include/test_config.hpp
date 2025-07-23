#pragma once
#include <string>

struct TestSettings {
    std::string test_workspace = "test_workspace"; // default
};

extern TestSettings g_test_settings;

void ParseArguments(int argc, char* argv[]);
