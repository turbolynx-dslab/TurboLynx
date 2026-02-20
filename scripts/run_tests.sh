#!/usr/bin/env bash
# =============================================================================
# S62GDB Test Runner
# =============================================================================
# Usage:
#   ./scripts/run_tests.sh              # build + run all tests
#   ./scripts/run_tests.sh common       # run [common] module
#   ./scripts/run_tests.sh catalog      # run [catalog] module
#   ./scripts/run_tests.sh storage      # run [storage] module
#   ./scripts/run_tests.sh execution    # run [execution] module
#   ./scripts/run_tests.sh build        # build only
#   ./scripts/run_tests.sh rebuild      # clean + build + run all
#   ./scripts/run_tests.sh "histogram"  # raw Catch2 filter (partial match)
#
# Environment:
#   BUILD_DIR   — build directory (default: build-release)
#   JOBS        — parallel build jobs (default: nproc)
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET} $*"; }
success() { echo -e "${GREEN}[PASS]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET} $*"; }
error()   { echo -e "${RED}[FAIL]${RESET} $*"; }
header()  { echo -e "\n${BOLD}${CYAN}━━━ $* ━━━${RESET}\n"; }

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${REPO_ROOT}/build-release}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"
UNITTEST="${BUILD_DIR}/test/unittest"
MODULE="${1:-all}"

build() {
    header "Building unittest"
    cd "${BUILD_DIR}"
    cmake -GNinja -DCMAKE_BUILD_TYPE=Release \
          -DENABLE_TCMALLOC=OFF \
          -DBUILD_UNITTESTS=ON \
          "${REPO_ROOT}" -q 2>&1 | grep -v '^--' || true
    ninja -j"${JOBS}" unittest
    success "Build OK  →  ${UNITTEST}"
}

run_ctest() {
    local filter="$1"
    header "CTest: ${filter}"
    cd "${BUILD_DIR}"
    if [ "$filter" = "all" ]; then
        ctest --output-on-failure --timeout 60
    else
        ctest -R "s62_${filter}" --output-on-failure --timeout 60
    fi
}

run_catch() {
    local tag="$1"
    header "Catch2: ${tag}"
    mkdir -p "${BUILD_DIR}/test_workspace"
    if [ "$tag" = "all" ]; then
        "${UNITTEST}" --test-workspace "${BUILD_DIR}/test_workspace" -r console
    else
        "${UNITTEST}" "[${tag}]" --test-workspace "${BUILD_DIR}/test_workspace" -r console
    fi
}

case "$MODULE" in
    build)
        build
        ;;
    rebuild)
        info "Cleaning test objects..."
        rm -rf "${BUILD_DIR}/test"
        build
        run_catch all
        ;;
    all)
        build
        run_catch all
        ;;
    common|catalog|storage|execution)
        build
        run_catch "$MODULE"
        ;;
    *)
        # Raw Catch2 name/tag filter (e.g. "histogram", "DataChunk*")
        build
        header "Catch2 filter: ${MODULE}"
        mkdir -p "${BUILD_DIR}/test_workspace"
        "${UNITTEST}" "${MODULE}" --test-workspace "${BUILD_DIR}/test_workspace" -r console
        ;;
esac
