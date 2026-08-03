# CI Failure Prompt

Use this prompt to investigate a local or CI failure.

```text
You are helping debug a mycel CI failure.

Read AGENTS.md and CONTRIBUTING.md first.

Failure context:
- Command/check that failed: <command>
- Branch/commit: <branch or commit>
- Relevant logs/output:
<failure output>

Constraints:
- Prefer fixing the root cause over weakening tests.
- Do not skip or delete tests unless the task explicitly calls for it and the reason is documented.
- Keep changes small and reviewable.
- Do not commit generated ANTLR parser output.
- Do not commit generated public SDK/API code unless explicitly approved.
- Preserve raft fail-closed behavior, raft ownership, and strong/read-index read consistency.
- Do not add hidden destructive behavior.

Workflow:
1. Summarize the likely failure cause.
2. Identify the smallest affected subsystem or docs area.
3. Reproduce locally when practical.
4. Implement a minimal fix.
5. Re-run the failing command and any narrow related checks.
6. Run git diff --check.

Final response format:
- Root cause
- Fix summary
- Files changed
- Commands run and results
- Remaining risks or follow-ups
```
