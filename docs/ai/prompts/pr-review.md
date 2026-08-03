# PR Review Prompt

Use this prompt to ask an AI assistant for a pre-review of a diff. This does not
replace maintainer review.

```text
You are reviewing a MycelDB pull request.

Read AGENTS.md and CONTRIBUTING.md first.

Review target:
<paste or attach the PR summary, diff, and relevant test output>

Review priorities:
1. Correctness and edge cases.
2. Tests: coverage of new behavior and regressions.
3. Documentation: operator, CLI, API, or design docs updated as needed.
4. Safety: no hidden destructive behavior, no leaked secrets, no unsafe backup/restore or divergence automation.
5. Raft/clustering: preserve system raft metadata authority, fail-closed behavior, raft ownership, and strong/read-index reads.
6. Generated artifacts: no unintended generated ANTLR/parser, SDK/API, or protobuf output.
7. Maintainability: small scope, clear subsystem boundaries, actionable errors.
8. Licensing/provenance: no incompatible copied code or unreviewed model output concerns.

Output format:
- Blocking issues
- Non-blocking suggestions
- Missing tests or docs
- Commands/checks that should be run
- Questions for the author

Be specific. Cite files and explain why each issue matters.
```
