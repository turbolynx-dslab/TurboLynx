"""OSS supply-chain graph application harness for TurboLynx.

Importing this package transparently preloads `libtbb.12.dylib` on macOS so
that the `turbolynx` Python extension can resolve it. This shim compensates
for an engine-side gap where `tools/pythonpkg/scripts/build_wheel.sh` does
not bundle the TBB runtime in macOS wheels; it walks up from this file to
find the TurboLynx `build-portable/` tree and preloads the dylib via ctypes.
Revisit if the wheel starts vendoring TBB itself.
"""
from __future__ import annotations

import ctypes
import sys
from pathlib import Path

__version__ = "0.0.1"


def _preload_tbb() -> None:
    if sys.platform != "darwin":
        return
    here = Path(__file__).resolve()
    for parent in here.parents:
        build_dir = parent / "build-portable"
        if not build_dir.is_dir():
            continue
        candidates = sorted(build_dir.rglob("libtbb.12.dylib"))
        for candidate in candidates:
            try:
                ctypes.CDLL(str(candidate), mode=ctypes.RTLD_GLOBAL)
                return
            except OSError:
                continue
        return


_preload_tbb()
