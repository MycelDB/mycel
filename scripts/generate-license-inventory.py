#!/usr/bin/env python3
"""Generate docs/legal/dependency-license-inventory.tsv for Go modules."""
from __future__ import annotations

import csv
import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "legal" / "dependency-license-inventory.tsv"


def infer_license(text: str) -> str:
    sample = text[:40000].lower()
    if "apache license" in sample and "version 2.0" in sample:
        return "Apache-2.0"
    if "mit license" in sample or ("permission is hereby granted, free of charge" in sample and "the software" in sample):
        return "MIT"
    if "redistribution and use in source and binary forms" in sample and "neither the name" in sample:
        return "BSD-3-Clause"
    if "redistribution and use in source and binary forms" in sample:
        return "BSD-2-Clause"
    if "isc license" in sample or "permission to use, copy, modify, and/or distribute this software" in sample:
        return "ISC"
    if "mozilla public license version 2.0" in sample:
        return "MPL-2.0"
    if "creative commons" in sample and "cc0" in sample:
        return "CC0-1.0"
    return "UNKNOWN"


def run(args: list[str]) -> str:
    return subprocess.check_output(args, cwd=ROOT, text=True)



def go_cache_escape(value: str) -> str:
    # Match Go module cache escaping for uppercase letters, e.g. Azure -> !azure.
    return "".join(f"!{char.lower()}" if "A" <= char <= "Z" else char for char in value)


def module_cache_dir(module_path: str, version: str) -> pathlib.Path | None:
    try:
        gomodcache = pathlib.Path(run(["go", "env", "GOMODCACHE"]).strip())
    except Exception:
        return None
    return gomodcache / f"{go_cache_escape(module_path)}@{go_cache_escape(version)}"

def candidate_dirs(module_path: str, version: str, listed_dir: str) -> list[pathlib.Path]:
    dirs: list[pathlib.Path] = []
    if listed_dir:
        dirs.append(pathlib.Path(listed_dir))
    cached = module_cache_dir(module_path, version)
    if cached is not None:
        dirs.append(cached)

    with_parents: list[pathlib.Path] = []
    for directory in dirs:
        current = directory
        for _ in range(4):
            with_parents.append(current)
            if current.name == "mod" or str(current).endswith("/pkg/mod"):
                break
            current = current.parent

    seen: set[str] = set()
    unique: list[pathlib.Path] = []
    for directory in dirs + with_parents:
        key = str(directory)
        if key not in seen:
            seen.add(key)
            unique.append(directory)
    return unique


def license_files(directory: pathlib.Path) -> list[pathlib.Path]:
    if not directory.exists():
        return []
    matches: list[pathlib.Path] = []
    for child in sorted(directory.iterdir()):
        name = child.name.lower()
        if child.is_file() and (
            name == "license"
            or name == "licence"
            or name.startswith("license.")
            or name.startswith("licence.")
            or name == "copying"
            or name.startswith("copying.")
        ):
            matches.append(child)
    return matches


def main() -> None:
    modules = run(["go", "list", "-mod=readonly", "-m", "-f", "{{.Path}}\t{{.Version}}\t{{.Dir}}", "all"])
    rows: list[list[str]] = []
    for line in modules.splitlines():
        parts = (line.split("\t") + ["", "", ""])[:3]
        module_path, version, listed_dir = parts
        if not version:
            continue  # first-party main module

        files: list[pathlib.Path] = []
        licenses: list[str] = []
        seen_files: set[str] = set()
        for directory in candidate_dirs(module_path, version, listed_dir):
            for file in license_files(directory):
                if str(file) in seen_files:
                    continue
                seen_files.add(str(file))
                files.append(file)
                try:
                    licenses.append(infer_license(file.read_text(errors="ignore")))
                except OSError:
                    pass

        license_expr = " OR ".join(sorted({license for license in licenses if license != "UNKNOWN"})) or "UNKNOWN"
        rows.append([
            "go",
            module_path,
            version,
            "module",
            license_expr,
            ", ".join(file.name for file in files) or "not found",
            f"https://{module_path}",
        ])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(["Ecosystem", "Package", "Version", "Scope", "License", "License files", "Source"])
        writer.writerows(rows)


if __name__ == "__main__":
    main()
