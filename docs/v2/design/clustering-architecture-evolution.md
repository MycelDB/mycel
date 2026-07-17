# Clustering Architecture Evolution

## Status

Forward-looking architecture note.

This document describes how Mycel clustering should evolve from the current static-primary foundation into a more complete distributed architecture. It is not a single implementation plan; it is a staged direction of travel.

## Current foundation

Mycel currently has the following clustering foundation:

- stable node identity
- explicit bootstrap and join admission
- per-node one-time join tokens
- topology and membership metadata
- persisted authority metadata with static primary and epoch
- public admin cluster API
- CLI and `mycel-admin` cluster management
- WAL-first mutation foundation
- initial primary-only guards for membership and space writes

The current cluster is not yet a replicated database. It is a primary-authority foundation that prevents obvious split-brain write entry points while preparing for WAL replication.

## Target model

The long-term target is:

```text
clients may connect to any daemon
any daemon can authenticate cluster tokens
followers serve reads
writes are routed to the primary
primary commits writes to WAL
followers replicate and apply WAL
clients can request appropriate read consistency
leadership can be changed safely
```

## Design principles

- WAL remains the source of durable mutation order.
- Clustered client writes are accepted by exactly one authority at a time.
- Followers do not independently create authoritative cluster state.
- Node lifecycle and authority role remain separate.
- Clients should not need to know all internals, but SDKs should be cluster-aware.
- Failover must be explicit and fenced before it becomes automatic.
- Secrets must not be exposed in WAL or cluster status responses.

## Stage 1: Static primary with guarded writes

This is the current stage.

Properties:

- bootstrap node becomes primary
- authority is persisted as primary + epoch
- followers derive role locally
- membership mutation requires primary
- cluster data writes are progressively guarded to require standalone or primary
- followers are read/query endpoints and local operational participants

Remaining work in this stage:

- complete write-authority guard coverage across durable mutation modules
- add primary hints to follower write rejection errors
- classify local-only versus cluster-authoritative mutations
- document short-term follower session behavior

## Stage 2: Cluster-aware clients with redirect/hint

Before transparent forwarding, clients should learn how to route writes.

Follower write rejection should include primary information:

- primary node ID
- primary node name
- primary admin/client endpoint, if known
- authority epoch

SDKs, CLI, and `mycel-admin` should use this to:

- discover cluster status from any node
- cache the current primary endpoint
- retry writes against the primary after `FailedPrecondition`
- refresh topology when authority epoch changes
- prefer local/follower nodes for reads where acceptable

This keeps daemon complexity low while improving user experience.

## Stage 3: Cluster-wide authentication

Node-local sessions are not ideal for a cluster.

Introduce cluster-wide token verification through a replicated keyring or equivalent mechanism.

Desired properties:

- login to any node works
- any node can verify tokens minted by any admitted node
- tokens include cluster ID and issuer node ID
- token validation can reject wrong-cluster tokens
- signing keys rotate safely
- signing secrets are never exposed in plaintext WAL records or status APIs

Possible implementation direction:

- primary manages auth signing key metadata
- secrets are stored using local secure storage or envelope encryption
- public verification keys or key IDs replicate through WAL
- short-lived access tokens reduce revocation pressure
- refresh/session state can remain primary-owned or explicitly replicated

This stage enables clients to move between nodes without re-authenticating per daemon.

## Stage 4: WAL replication MVP

Implemented as an MVP: primary-to-follower WAL propagation uses an internal `StreamWal` RPC, follower receive log, progress store, WAL applier replay, and role-aware status in CLI/UI. Remaining work includes snapshot transfer, retention coordination, stronger read consistency, and promotion/fencing.

Minimum viable behavior:

1. primary exposes a WAL tail stream from a requested LSN
2. follower connects to primary and requests records after its applied/received LSN
3. follower writes received records to local WAL or a receive log
4. follower applies records through existing WAL appliers
5. follower advances replicated apply progress
6. status APIs expose replication lag and health

Important decisions:

- whether followers append replicated records to the same WAL format
- how to mark records as primary-originated versus locally-originated
- how to prevent follower client writes from entering replicated WAL
- how to resume after disconnect
- how to catch up from checkpoints/snapshots

Read safety at this stage should be explicit. Follower reads may be stale unless a caller asks for a specific LSN or reads from primary.

## Stage 5: Read consistency controls

Once followers replicate WAL, clients need read consistency options.

Possible API concepts:

```text
read_preference: primary | follower | nearest
read_consistency: stale_ok | after_lsn | primary_linearizable
```

Examples:

- dashboards use `follower + stale_ok`
- after a write returns LSN `N`, a client may request `after_lsn=N`
- critical admin operations use primary reads

This lets followers provide useful scale-out reads without hiding consistency semantics.

## Stage 6: Optional write forwarding

After redirect/hint and shared auth are stable, followers can optionally forward writes to primary.

Benefits:

- clients can connect to any daemon for both reads and writes
- `mycel-admin` and SDK routing become simpler
- load balancers can target all nodes

Costs:

- followers become authenticated request proxies
- request identity and authorization context must be preserved
- retries need idempotency
- errors must distinguish follower/proxy/primary failures
- tracing and audit logs must show original client and forwarding node

Write forwarding should not bypass primary authority. The primary still performs authorization, WAL append/sync/apply, and response generation.

## Stage 7: Manual promotion and fencing

Before automatic election, support explicit operator-driven promotion.

Manual promotion should include:

- promote a selected admitted follower
- increment authority epoch
- fence the old primary when possible
- require proof that the promoted node is sufficiently caught up
- persist authority change safely
- make clients refresh primary on epoch change

Open design questions:

- what lag threshold is acceptable for promotion
- how to handle unreachable old primary
- whether promotion requires an operator quorum or recovery token
- where authority changes are recorded before consensus exists

This stage provides operational recovery without pretending automatic election is solved.

## Stage 8: Leases, quorum, and election

Automatic leader election should come after the WAL and authority model are mature.

Likely additions:

- heartbeat and health tracking
- primary lease or term
- voting membership
- quorum rules
- election protocol
- log matching and commit index semantics, if adopting a Raft-like model
- learners/observers for non-voting replicas

At this point, authority epoch may evolve into or be paired with election term.

## Stage 9: Advanced replication and placement

Future improvements may include:

- snapshot transfer
- checkpoint-based catch-up
- replica lag admission gates
- read replicas/observers
- region-aware routing
- per-space placement policies
- background cache warming
- semantic work scheduling across nodes

Semantic analysis should remain primary-scheduled for authoritative state, though followers may perform disposable compute and cache warming.

## Operational UX evolution

`mycel-admin`, CLI, and SDKs should evolve with the cluster:

1. show primary/follower role everywhere relevant
2. show primary hints on write failures
3. route writes to primary automatically
4. show replication lag and follower health
5. expose read consistency choices where useful
6. support manual promotion workflows
7. later, visualize election/quorum state

## Summary roadmap

```text
1. static primary + complete write guards
2. primary hints + cluster-aware clients
3. cluster-wide auth/token verification
4. WAL primary-to-follower replication
5. read consistency controls
6. optional follower write forwarding
7. manual promotion + fencing
8. lease/quorum/election
9. advanced placement and replica management
```

The immediate next engineering focus should be completing write-authority coverage and primary hints. WAL replication should follow once the write surface is clearly guarded.
