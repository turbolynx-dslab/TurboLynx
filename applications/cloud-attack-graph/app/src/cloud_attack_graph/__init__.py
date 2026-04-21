"""Cloud attack-graph harness for TurboLynx."""

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
