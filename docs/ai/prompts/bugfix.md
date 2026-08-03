# Bugfix Prompt

Use this prompt for a focused bugfix with regression coverage.

```text
You are helping with mycel, a daemon-first graph data system written in Go.

Read AGENTS.md and CONTRIBUTING.md first.

Task:
<describe the bug, symptoms, expected behavior, and any known reproduction steps>

Constraints:
- Keep daemon API adapters under internal/daemon/api.
- Keep service implementations under their subsystem packages.
- Use the term subsystem.
- Do not commit generated ANTLR parser output.
- Do not commit generated public SDK/API code unless explicitly approved.
- Preserve raft fail-closed behavior, raft ownership, and strong/read-index read consistency.
- Do not add hidden destructive behavior.

Workflow:
1. Identify the smallest affected subsystem and files.
2. Reproduce or explain the bug with a failing test when practical.
3. Implement the minimal fix.
4. Add or update regression coverage near the affected code.
5. Run the narrowest relevant tests, then broader checks if risk warrants.
6. Run git diff --check.

Validation to consider:
- go test ./<affected/package>
- make test for broader code changes
- make docs-check for docs changes
- relevant phase targets for raft/clustering-sensitive changes

Final response format:
- Summary
- Files changed
- Tests/checks run and results
- Remaining risks or follow-ups
```
