#define CATCH_CONFIG_RUNNER

#include "catch.hpp"

#include <string>
#include <iostream>
#include <filesystem>

namespace fs = std::filesystem;

// 전역 테스트 설정
struct TestSettings {
    std::string test_workspace = "test_workspace"; // 기본값
};

TestSettings g_test_settings;

// 커맨드라인 인자 파싱
void ParseArguments(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--test-workspace" && i + 1 < argc) {
            g_test_settings.test_workspace = argv[++i];
        } else {
            // 무시
        }
    }
}

int main(int argc, char* argv[]) {
    // 커맨드라인 인자 처리
    ParseArguments(argc, argv);

    // 테스트용 디렉토리 존재 확인 및 생성
    fs::path workspace_path(g_test_settings.test_workspace);
    if (!fs::exists(workspace_path)) {
        try {
            fs::create_directories(workspace_path);
            std::cout << "[INFO] Created test workspace at: " << workspace_path << std::endl;
        } catch (const std::exception& ex) {
            std::cerr << "[ERROR] Failed to create test workspace: " << ex.what() << std::endl;
            return 1;
        }
    }

    // 테스트 실행
    int result = Catch::Session().run(argc, argv);

    return result;
}
