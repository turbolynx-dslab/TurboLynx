"""Fixture loading and workspace connection helpers."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import turbolynx


VERTEX_LABELS: tuple[str, ...] = (
    "Identity",
    "Workload",
    "Secret",
    "Resource",
)

EDGE_TYPES: tuple[str, ...] = (
    "RUNS_AS",
    "USES_SECRET",
    "CAN_ASSUME",
    "CAN_ACCESS",
)


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "applications").is_dir() and (parent / "src").is_dir():
            return parent
    raise RuntimeError(f"could not locate TurboLynx repo root from {here}")


def _default_build_dir() -> Path:
    override = os.environ.get("TLX_BUILD_DIR")
    if override:
        return Path(override)
    root = _repo_root()
    for candidate in ("build-release", "build-portable", "build"):
        build_dir = root / candidate
        if build_dir.is_dir():
            return build_dir
    return root / "build-release"


def turbolynx_binary() -> Path:
    binary = _default_build_dir() / "tools" / "turbolynx"
    if not binary.exists():
        raise FileNotFoundError(
            f"turbolynx binary not found at {binary}. "
            "Set TLX_BUILD_DIR or build TurboLynx first."
        )
    return binary


def _vertex_file(fixture_dir: Path, label: str) -> Path:
    return fixture_dir / f"nodes_{label.lower()}.csv"


def _edge_file(fixture_dir: Path, rel: str) -> Path:
    return fixture_dir / f"edges_{rel.lower()}.csv"


def load_fixture(workspace: Path, fixture_dir: Path, *, log_level: str = "warn") -> None:
    workspace = Path(workspace)
    fixture_dir = Path(fixture_dir)

    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.parent.mkdir(parents=True, exist_ok=True)

    args: list[str] = [
        str(turbolynx_binary()),
        "import",
        "--workspace", str(workspace),
        "--log-level", log_level,
    ]
    for label in VERTEX_LABELS:
        csv = _vertex_file(fixture_dir, label)
        if not csv.exists():
            raise FileNotFoundError(f"missing vertex fixture: {csv}")
        args += ["--nodes", label, str(csv)]
    for rel in EDGE_TYPES:
        csv = _edge_file(fixture_dir, rel)
        backward = csv.with_suffix(".csv.backward")
        if not csv.exists():
            raise FileNotFoundError(f"missing edge fixture: {csv}")
        if not backward.exists():
            raise FileNotFoundError(f"missing backward edge fixture: {backward}")
        args += ["--relationships", rel, str(csv)]

    completed = subprocess.run(args, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(
            "turbolynx import failed "
            f"(exit {completed.returncode})\n"
            f"command: {' '.join(args)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )


def connect(workspace: Path):
    return turbolynx.connect(str(Path(workspace)))
