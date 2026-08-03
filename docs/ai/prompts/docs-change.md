# Documentation Change Prompt

Use this prompt for documentation-only changes.

```text
You are helping with MycelDB documentation.

Read AGENTS.md, CONTRIBUTING.md, and docs/README.md first.

Task:
<describe the documentation change>

Documentation information architecture:
- docs/README.md is the documentation entrypoint.
- docs/design/ contains current architecture and subsystem design.
- docs/operations/ contains operator procedures, CLI usage, recovery, and validation.
- docs/implementation/ contains archival/release-grouped implementation plans.
- Operator-facing recovery docs belong under docs/operations/procedures/.
- Implementation plans are not current operator runbooks.

Constraints:
- Use the term subsystem.
- Keep docs links relative and valid.
- Do not move code as part of a docs-only task.
- Do not create stale duplicate guidance; link to the authoritative page instead.

Workflow:
1. Identify the intended audience: user, operator, contributor, or maintainer.
2. Place the doc under the correct docs area.
3. Update relevant README/index pages.
4. Repair links affected by moves or renames.
5. Run make docs-check and git diff --check.

Final response format:
- Summary
- Files changed
- Checks run and results
- Follow-ups, if any
```
