# Small Feature Prompt

Use this prompt for a small, reviewable feature or behavior addition.

```text
You are helping with mycel, a daemon-first graph data system written in Go.

Read AGENTS.md and CONTRIBUTING.md first.

Task:
<describe the feature, user/operator value, and intended scope>

Constraints:
- Keep the change small and reviewable.
- Keep daemon API adapters under internal/daemon/api.
- Keep service implementations under their subsystem packages.
- Use the term subsystem.
- Do not commit generated ANTLR parser output.
- Do not commit generated public SDK/API code unless explicitly approved.
- Do not change public API, protobuf, SDK, CLI, storage format, or migration behavior without calling it out.
- Preserve raft fail-closed behavior, raft ownership, and strong/read-index read consistency.
- Keep backup/restore and divergence workflows explicit and operator-selected.
- Avoid hidden destructive behavior; require explicit flags for risky operations.

Workflow:
1. Produce a brief implementation plan before editing.
2. Identify affected subsystem(s), API surfaces, tests, and docs.
3. Implement the smallest useful slice that leaves the system functional.
4. Add tests for new behavior.
5. Update docs or CLI help if user/operator behavior changes.
6. Run targeted checks and git diff --check.

Validation to consider:
- go test ./<affected/package>
- make test
- make docs-check
- relevant phase targets for raft/clustering-sensitive changes

Final response format:
- Summary
- Files changed
- Tests/checks run and results
- Compatibility notes
- Remaining risks or follow-ups
```
