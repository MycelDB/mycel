#!/usr/bin/env python3
"""Lightweight documentation hygiene checks."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

REQUIRED_CLI_DOCS = [
    "admin",
    "auth",
    "automation",
    "blob",
    "change-stream",
    "cluster",
    "domain",
    "export",
    "graph",
    "import",
    "inference",
    "metadata",
    "node",
    "principal",
    "query",
    "schema",
    "semantic",
    "session",
    "space",
    "transaction",
    "user",
]

LINK_RE = re.compile(r"\]\(([^)#][^)]*\.md)(?:#[^)]*)?\)")


def check_links() -> list[str]:
    errors: list[str] = []
    for path in DOCS.rglob("*.md"):
        text = path.read_text(encoding="utf-8")
        for match in LINK_RE.finditer(text):
            target = match.group(1)
            if "://" in target or target.startswith("mailto:"):
                continue
            resolved = (path.parent / target).resolve()
            try:
                resolved.relative_to(ROOT)
            except ValueError:
                errors.append(f"{path.relative_to(ROOT)}: link escapes repo: {target}")
                continue
            if not resolved.exists():
                errors.append(f"{path.relative_to(ROOT)}: missing link target: {target}")
    return errors


def check_cli_docs() -> list[str]:
    errors: list[str] = []
    cli_dir = DOCS / "operations" / "cli"
    for command in REQUIRED_CLI_DOCS:
        if not (cli_dir / f"{command}.md").exists():
            errors.append(f"missing CLI doc for top-level command: {command}")
    return errors


def main() -> int:
    errors = check_links() + check_cli_docs()
    if errors:
        for err in errors:
            print(err, file=sys.stderr)
        return 1
    print("docs checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
