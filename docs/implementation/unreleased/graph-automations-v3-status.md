# Graph automations V3 implementation status

V3 foundation work is implemented behind the existing automation subsystem.

## Implemented

- Workflow definition model and validation.
- Durable workflow instances and step-run records.
- Initial workflow instance bootstrap from pending invocations.
- Tool registry foundation with explicit allowlist checks.
- Schedule trigger model and checkpoint-backed scheduled invocation creation.
- Bounded scan trigger validation requiring `LIMIT`.
- Proposal records with approve/reject lifecycle.
- Domain policy records and policy enforcement during definition validation.
- CLI `mycel automation validate` validates V3 workflow definitions locally.
- Example workflow: `examples/automations/research_note_workflow.json`.

## Not yet exposed over public API

The V3 internal records are not yet exposed in `mycel-api` RPCs. Follow-up API work should add workflow instance, step run, proposal, and policy RPCs before SDK/admin UI exposure.

Suggested RPC groups:

- list/get workflow instances
- list/get workflow step runs
- list/get/approve/reject proposals
- get/update automation policy
- scheduler status

## Not yet in admin UI

`mycel-admin` currently exposes V2 automation definition and run inspection. V3 workflow timeline, proposal queue, and policy/budget editors still need API support before UI implementation.
