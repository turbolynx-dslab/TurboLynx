#!/usr/bin/env python3
"""Stage license and notice files into package directories.

This keeps npm/pip artifacts self-contained without requiring callers to ship
the full repository source tree alongside the packaged artifact.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIRS = (
    REPO_ROOT / "tools" / "nodepkg",
    REPO_ROOT / "tools" / "mcp",
    REPO_ROOT / "tools" / "pythonpkg",
)
PACKAGE_ROOT_MARKERS = {pkg.resolve() for pkg in PACKAGE_DIRS}
SPECIAL_COPIES = (
    (REPO_ROOT / "LICENSE", Path("licenses/TurboLynx-MIT.txt")),
    (REPO_ROOT / "licenses" / "duckdb-MIT.txt", Path("licenses/duckdb-MIT.txt")),
    (REPO_ROOT / "src" / "include" / "optimizer" / "orca" / "LICENSE",
     Path("licenses/src/include/optimizer/orca/LICENSE")),
    (REPO_ROOT / "src" / "optimizer" / "orca" / "gporca" / "LICENSE",
     Path("licenses/src/optimizer/orca/gporca/LICENSE")),
    (REPO_ROOT / "src" / "optimizer" / "orca" / "gporca" / "COPYRIGHT",
     Path("licenses/src/optimizer/orca/gporca/COPYRIGHT")),
)


def is_notice_file(path: Path) -> bool:
    name = path.name.upper()
    prefixes = ("LICENSE", "NOTICE", "COPYRIGHT")
    return any(name == prefix or name.startswith(f"{prefix}.") or name.startswith(f"{prefix}-")
               or name.startswith(f"{prefix}_") for prefix in prefixes)


def iter_vendor_notice_files() -> list[Path]:
    files: list[Path] = []
    for path in sorted((REPO_ROOT / "third_party").rglob("*")):
        if path.is_file() and is_notice_file(path):
            files.append(path)
    return files


def write_package_license(package_dir: Path) -> None:
    apache_text = (REPO_ROOT / "src" / "include" / "optimizer" / "orca" / "LICENSE").read_text()
    package_license = (
        "TurboLynx packaged distribution notice\n"
        "=====================================\n\n"
        "The package-level metadata for this artifact declares Apache-2.0.\n"
        "This artifact also includes TurboLynx-originated and third-party code under\n"
        "their original permissive licenses. See `THIRD-PARTY-NOTICES.md` and the\n"
        "`licenses/` directory included with this package for the preserved texts.\n\n"
        "Apache License 2.0\n"
        "------------------\n\n"
    )
    (package_dir / "LICENSE").write_text(package_license + apache_text)


def clean_generated_tree(package_dir: Path) -> None:
    for relative in (
        Path("LICENSE"),
        Path("THIRD-PARTY-NOTICES.md"),
        Path("licenses/duckdb-MIT.txt"),
        Path("licenses/TurboLynx-MIT.txt"),
        Path("licenses/third_party"),
        Path("licenses/src"),
    ):
        target = package_dir / relative
        if target.is_dir():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()


def copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def sync_package_dir(package_dir: Path) -> None:
    package_dir = package_dir.resolve()
    if package_dir not in PACKAGE_ROOT_MARKERS:
        raise SystemExit(f"unsupported package dir: {package_dir}")

    clean_generated_tree(package_dir)
    write_package_license(package_dir)
    copy_file(REPO_ROOT / "THIRD-PARTY-NOTICES.md", package_dir / "THIRD-PARTY-NOTICES.md")

    for src, relative_dst in SPECIAL_COPIES:
        copy_file(src, package_dir / relative_dst)

    for src in iter_vendor_notice_files():
        relative = src.relative_to(REPO_ROOT)
        copy_file(src, package_dir / "licenses" / relative)


def main(argv: list[str]) -> int:
    if len(argv) == 1:
        targets = PACKAGE_DIRS
    else:
        targets = [Path(arg) if Path(arg).is_absolute() else (REPO_ROOT / arg) for arg in argv[1:]]

    for target in targets:
        sync_package_dir(target)
        print(f"synced license bundle for {target.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
