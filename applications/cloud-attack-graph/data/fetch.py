"""Placeholder for future public-cloud fixture acquisition.

The M1 application slice uses only committed tiny CSV fixtures under
`tests/fixtures/`, so no network fetch step is required yet.
"""

from __future__ import annotations


def main() -> None:
    raise SystemExit(
        "No remote dataset fetch is implemented yet. "
        "Use the committed tiny fixture under tests/fixtures/."
    )


if __name__ == "__main__":
    main()
