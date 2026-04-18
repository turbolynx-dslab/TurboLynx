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
import sysconfig
from pathlib import Path

from setuptools import setup, find_packages
from setuptools.command.build_ext import build_ext
from setuptools.dist import Distribution


def sync_license_bundle():
    """Generate package-local license files before metadata/build steps."""
    script = Path(__file__).resolve().parents[1] / 'sync_package_licenses.py'
    package_dir = Path(__file__).resolve().parent
    subprocess.run([sys.executable, str(script), str(package_dir)], check=True)


sync_license_bundle()


def is_macos():
    return sys.platform == 'darwin'


def shared_lib_name():
    return 'libturbolynx.dylib' if is_macos() else 'libturbolynx.so'


def extension_patterns(base_dir):
    ext_suffix = sysconfig.get_config_var('EXT_SUFFIX')
    patterns = []
    if ext_suffix:
        patterns.append(str(base_dir / f'turbolynx_core*{ext_suffix}'))
    patterns.extend([
        str(base_dir / 'turbolynx_core*.so'),
        str(base_dir / 'turbolynx_core*.pyd'),
    ])
    return patterns


def glob_first(base_dir):
    seen = set()
    for pattern in extension_patterns(base_dir):
        for match in glob.glob(pattern):
            if match not in seen:
                seen.add(match)
                yield match


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
        pkg_dir / '../../build-portable',
        pkg_dir / '../../build-lwtest',
        pkg_dir / '../../build-release',
        pkg_dir / '../../build',
    ]
    for c in candidates:
        c = c.resolve()
        for _ in glob_first(c / 'tools/pythonpkg/turbolynx'):
            return str(c)

    return None


def patch_linux_rpath(binary_path):
    """Set RPATH to $ORIGIN so the extension finds bundled libturbolynx.so."""
    try:
        subprocess.run(
            ['patchelf', '--set-rpath', '$ORIGIN', str(binary_path)],
            check=True, capture_output=True
        )
    except FileNotFoundError:
        print("WARNING: patchelf not found. RPATH not patched.")
        print("  Install with: apt-get install patchelf")
        print("  The wheel may not work on other machines without libturbolynx.so in LD_LIBRARY_PATH.")
    except subprocess.CalledProcessError as e:
        print(f"WARNING: patchelf failed: {e.stderr.decode()}")


def patch_macos_loader_path(module_path, dylib_path):
    """Point the extension at the bundled libturbolynx.dylib."""
    try:
        subprocess.run(
            ['install_name_tool', '-id', '@rpath/libturbolynx.dylib', str(dylib_path)],
            check=True,
            capture_output=True,
        )
    except FileNotFoundError:
        print("WARNING: install_name_tool not found. macOS wheel may need manual fixups.")
        return
    except subprocess.CalledProcessError as e:
        print(f"WARNING: install_name_tool -id failed: {e.stderr.decode().strip()}")

    try:
        deps = subprocess.run(
            ['otool', '-L', str(module_path)],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()[1:]
    except FileNotFoundError:
        print("WARNING: otool not found. macOS wheel may need manual dependency fixups.")
        return
    except subprocess.CalledProcessError as e:
        print(f"WARNING: otool failed: {e.stderr.strip()}")
        return

    replacement = '@loader_path/libturbolynx.dylib'
    current_names = []
    for line in deps:
        dep = line.strip().split(' ', 1)[0]
        if dep.endswith('/libturbolynx.dylib') or dep == 'libturbolynx.dylib' or dep.endswith('libturbolynx.dylib'):
            current_names.append(dep)

    if not current_names:
        return

    for dep in current_names:
        try:
            subprocess.run(
                ['install_name_tool', '-change', dep, replacement, str(module_path)],
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"WARNING: install_name_tool -change failed: {e.stderr.decode().strip()}")


def patch_runtime_linking(module_path, library_path):
    if is_macos():
        patch_macos_loader_path(module_path, library_path)
    else:
        patch_linux_rpath(module_path)


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

        # 1. Copy turbolynx_core extension module
        core_files = list(glob_first(build_dir / 'tools/pythonpkg/turbolynx'))
        if not core_files:
            print(f"ERROR: turbolynx_core extension not found in {build_dir}/tools/pythonpkg/turbolynx/")
            sys.exit(1)

        # 2. Copy libturbolynx shared library
        lib_name = shared_lib_name()
        lib_path = build_dir / 'src' / lib_name
        bundled_lib = None
        if lib_path.exists():
            bundled_lib = pkg_dir / lib_name
            print(f"  Copying {lib_path} -> {bundled_lib}")
            shutil.copy2(lib_path, bundled_lib)
        else:
            print(f"WARNING: {lib_name} not found at {lib_path}")
            if is_macos():
                print(f"  The wheel will require {lib_name} to be discoverable by dyld at runtime.")
            else:
                print(f"  The wheel will require {lib_name} in LD_LIBRARY_PATH at runtime.")

        for f in core_files:
            dst = pkg_dir / Path(f).name
            print(f"  Copying {f} -> {dst}")
            shutil.copy2(f, dst)
            if bundled_lib is not None:
                patch_runtime_linking(dst, bundled_lib)

        # 3. Copy Python source files (in case they weren't included by setuptools)
        src_pkg = Path(__file__).parent / 'turbolynx'
        for py_file in src_pkg.glob('*.py'):
            dst = pkg_dir / py_file.name
            if not dst.exists():
                shutil.copy2(py_file, dst)

        # 4. Ship license and notice files inside the package as well, so the
        # wheel remains self-contained even when only the installed artifact is
        # available.
        package_root = Path(__file__).parent
        for doc_name in ('LICENSE', 'THIRD-PARTY-NOTICES.md'):
            doc_src = package_root / doc_name
            if doc_src.exists():
                shutil.copy2(doc_src, pkg_dir / doc_name)

        licenses_src = package_root / 'licenses'
        licenses_dst = pkg_dir / 'licenses'
        if licenses_src.exists():
            if licenses_dst.exists():
                shutil.rmtree(licenses_dst)
            shutil.copytree(licenses_src, licenses_dst)


setup(
    packages=find_packages(),
    package_data={
        'turbolynx': [
            '*.so',
            '*.so.*',
            '*.dylib',
            '*.pyd',
            'LICENSE',
            'THIRD-PARTY-NOTICES.md',
            'licenses/*',
            'licenses/**/*',
        ],
    },
    cmdclass={
        'build_ext': PrebuiltBuildExt,
    },
    distclass=BinaryDistribution,
)
