# Third-party notices and dependency license inventory

This repository is licensed under the Apache License, Version 2.0. The project also uses third-party open source dependencies. This document records the current license inventory and the release expectations for preserving upstream notices.

Tracked issue: MycelDB/mycel#24

## Inventory

The generated inventory is committed at:

```text
docs/legal/dependency-license-inventory.tsv
```

It includes package name, resolved version, dependency ecosystem, scope, declared or detected license, detected license files when available, and upstream source/resolution metadata.

Current detected license summary:

- `Apache-2.0`: 49
- `BSD-2-Clause`: 5
- `BSD-3-Clause`: 20
- `ISC`: 1
- `MIT`: 21

## Covered ecosystems

- Go module dependencies from `go.mod` / `go.sum`.

## Notice handling

- Keep upstream copyright, license, and NOTICE files intact in source checkouts and vendored/generated material.
- Do not remove license headers from generated code or copied third-party source.
- Binary, Docker, SDK, and desktop distributions should include this repository's `LICENSE` and link to or include this third-party notice document.
- If a dependency declares a license outside the existing allowlist, review it before release and update this document with any required attribution or redistribution notes.
- This inventory is a release-hygiene aid and is not legal advice.

## Regeneration

Run `python3 scripts/generate-license-inventory.py` from the repository root. The script uses `go list -m -mod=readonly` and module-cache license files; it does not require external Python packages and is designed not to modify `go.sum`.

After regenerating, review changes to `docs/legal/dependency-license-inventory.tsv`, update the summary above if needed, and run the normal repository validation before opening a release PR.

## Distribution guidance

Docker images and binary releases should include or link to `docs/legal/third-party-notices.md` and `docs/legal/dependency-license-inventory.tsv`.
