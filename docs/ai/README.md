# AI Prompt Templates

These prompt templates are optional helpers for contributors who use AI coding
assistants. They are not a substitute for review, tests, or project judgment.
Contributors remain responsible for every submitted change.

Before using any template, read:

- [AGENTS.md](../../AGENTS.md)
- [CONTRIBUTING.md](../../CONTRIBUTING.md)

Do not paste secrets, credentials, tokens, private user data, proprietary
third-party code, or confidential operational details into AI tools.

## Available prompts

| Prompt | Use when |
| --- | --- |
| [Bugfix](prompts/bugfix.md) | Fixing a focused defect with regression coverage. |
| [Documentation change](prompts/docs-change.md) | Updating docs, navigation, runbooks, or examples. |
| [Small feature](prompts/feature-small.md) | Implementing a small scoped feature or behavior addition. |
| [PR review](prompts/pr-review.md) | Asking an AI assistant to review a diff before human review. |
| [CI failure](prompts/ci-failure.md) | Investigating and fixing a failing local or CI check. |

## How to use

1. Copy the relevant prompt into your AI assistant.
2. Replace placeholder text such as `<task>` or `<failure output>`.
3. Add any relevant issue, design document, command output, or file paths.
4. Ask for a small, reviewable plan before allowing broad edits.
5. Review every changed line yourself.
6. Run the required checks and include the results in your PR.

## Prompt conventions

The templates intentionally repeat key project rules so they work in tools that
do not automatically read repository context. Prefer small diffs, explicit test
commands, and final summaries that include changed files, validation, and risks.
