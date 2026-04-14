"""TurboLynx Python package — pre-built binary packaging.

This setup.py packages pre-built shared libraries from a CMake build directory
into a pip-installable wheel. It does NOT compile TurboLynx from source.

Usage:
    # 1. Build TurboLynx with CMake first
    cd /path/to/turbograph-v3/build-lwtest && ninja

    # 2. Create wheel (auto-detects build dir)
    cd /path/to/turbograph-v3/tools/pythonpkg
    pip wheel . -w dist/

    # Or specify build dir explicitly:
    TURBOLYNX_BUILD_DIR=/path/to/build pip wheel . -w dist/

    # 3. Install
    pip install dist/turbolynx-*.whl
"""

import os
import sys
import glob
import shutil
import subprocess
from pathlib import Path

from setuptools import setup, find_packages
from setuptools.command.build_ext import build_ext
from setuptools.dist import Distribution


class BinaryDistribution(Distribution):
    """Mark distribution as containing platform-specific binaries."""
    def has_ext_modules(self):
        return True


def find_build_dir():
    """Locate the CMake build directory."""
    # 1. Environment variable
    env_dir = os.environ.get('TURBOLYNX_BUILD_DIR')
    if env_dir and os.path.isdir(env_dir):
        return env_dir

    # 2. Common locations relative to this file
    pkg_dir = Path(__file__).parent.resolve()
    candidates = [
        pkg_dir / '../../build-lwtest',
        pkg_dir / '../../build-release',
        pkg_dir / '../../build',
    ]
    for c in candidates:
        c = c.resolve()
        # Check if the build has the compiled module
        so_pattern = str(c / 'tools/pythonpkg/turbolynx/turbolynx_core*.so')
        if glob.glob(so_pattern):
            return str(c)

    return None


def patch_rpath(so_path):
    """Set RPATH to $ORIGIN so the .so finds bundled libturbolynx.so."""
    try:
        subprocess.run(
            ['patchelf', '--set-rpath', '$ORIGIN', str(so_path)],
            check=True, capture_output=True
        )
    except FileNotFoundError:
        print("WARNING: patchelf not found. RPATH not patched.")
        print("  Install with: apt-get install patchelf")
        print("  The wheel may not work on other machines without libturbolynx.so in LD_LIBRARY_PATH.")
    except subprocess.CalledProcessError as e:
        print(f"WARNING: patchelf failed: {e.stderr.decode()}")


class PrebuiltBuildExt(build_ext):
    """Custom build_ext that copies pre-built .so files instead of compiling."""

    def run(self):
        build_dir = find_build_dir()
        if build_dir is None:
            print("ERROR: Cannot find TurboLynx build directory.")
            print("  Build TurboLynx first: cd build-lwtest && ninja")
            print("  Or set TURBOLYNX_BUILD_DIR environment variable.")
            sys.exit(1)

        build_dir = Path(build_dir)
        print(f"Using build directory: {build_dir}")

        # Destination: the turbolynx package in the build output
        pkg_dir = Path(self.build_lib) / 'turbolynx'
        pkg_dir.mkdir(parents=True, exist_ok=True)

        # 1. Copy turbolynx_core*.so
        core_pattern = str(build_dir / 'tools/pythonpkg/turbolynx/turbolynx_core*.so')
        core_files = glob.glob(core_pattern)
        if not core_files:
            print(f"ERROR: turbolynx_core*.so not found in {build_dir}/tools/pythonpkg/turbolynx/")
            sys.exit(1)

        for f in core_files:
            dst = pkg_dir / Path(f).name
            print(f"  Copying {f} -> {dst}")
            shutil.copy2(f, dst)
            patch_rpath(dst)

        # 2. Copy libturbolynx.so
        lib_path = build_dir / 'src' / 'libturbolynx.so'
        if lib_path.exists():
            dst = pkg_dir / 'libturbolynx.so'
            print(f"  Copying {lib_path} -> {dst}")
            shutil.copy2(lib_path, dst)
        else:
            print(f"WARNING: libturbolynx.so not found at {lib_path}")
            print("  The wheel will require libturbolynx.so in LD_LIBRARY_PATH at runtime.")

        # 3. Copy Python source files (in case they weren't included by setuptools)
        src_pkg = Path(__file__).parent / 'turbolynx'
        for py_file in src_pkg.glob('*.py'):
            dst = pkg_dir / py_file.name
            if not dst.exists():
                shutil.copy2(py_file, dst)


setup(
    packages=find_packages(),
    package_data={
        'turbolynx': ['*.so', '*.so.*'],
    },
    cmdclass={
        'build_ext': PrebuiltBuildExt,
    },
    distclass=BinaryDistribution,
)
