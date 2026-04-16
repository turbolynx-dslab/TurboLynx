#!/bin/bash
# Build a pip-installable wheel for TurboLynx Python API.
#
# Prerequisites:
#   1. TurboLynx must be built with CMake first (ninja in build dir)
#   2. Linux wheels: patchelf is recommended
#   3. macOS wheels: install_name_tool must be available (Xcode command line tools)
#
# Usage:
#   ./scripts/build_wheel.sh [BUILD_DIR]
#
# Examples:
#   ./scripts/build_wheel.sh                          # auto-detect build dir
#   ./scripts/build_wheel.sh /path/to/build-release   # specify build dir

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${1:-}"
UNAME_S="$(uname -s)"

find_shared_lib() {
    local root="$1"
    for lib in libturbolynx.so libturbolynx.dylib; do
        if [ -f "$root/src/$lib" ]; then
            printf '%s\n' "$root/src/$lib"
            return 0
        fi
    done
    return 1
}

# Auto-detect build directory
if [ -z "$BUILD_DIR" ]; then
    REPO_ROOT="$(cd "$PKG_DIR/../.." && pwd)"
    for candidate in build-portable build-release build-lwtest build; do
        if find_shared_lib "$REPO_ROOT/$candidate" >/dev/null 2>&1; then
            BUILD_DIR="$REPO_ROOT/$candidate"
            break
        fi
    done
fi

if [ -z "$BUILD_DIR" ] || [ ! -d "$BUILD_DIR" ]; then
    echo "ERROR: Build directory not found."
    echo "  Build TurboLynx first, then run:"
    echo "  $0 /path/to/build-dir"
    exit 1
fi

echo "Build directory: $BUILD_DIR"
echo "Package directory: $PKG_DIR"

# Check prerequisites
if [ "$UNAME_S" = "Darwin" ]; then
    if ! command -v install_name_tool >/dev/null 2>&1; then
        echo "WARNING: install_name_tool not found. Install Xcode command line tools."
    fi
elif ! command -v patchelf &> /dev/null; then
    echo "WARNING: patchelf not found. Install with: apt-get install patchelf"
fi

# Clean previous builds
rm -rf "$PKG_DIR/dist" "$PKG_DIR/build" "$PKG_DIR/*.egg-info"

# Build wheel
cd "$PKG_DIR"
TURBOLYNX_BUILD_DIR="$BUILD_DIR" pip wheel . -w dist/ --no-deps

echo ""
echo "Wheel built successfully:"
ls -lh dist/*.whl

echo ""
echo "Install with:"
echo "  pip install dist/$(ls dist/*.whl | head -1 | xargs basename)"
