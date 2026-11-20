# Performance baseline

Status: draft  
Last updated: 2026-11-19

## Production requirement

CommonFolio does not need every performance optimization before first production,
but it does need a measured baseline and a safe operating envelope. Launching
without performance data risks discovering collapse points only after customer
traffic arrives.

The first requirement is characterization: know what the initial three-pod AWS
cluster can safely handle and which operations are most likely to become
bottlenecks.

## Mandatory first-cut capabilities

Before production, Mycel should have baseline measurements for expected
CommonFolio workloads, including:

- graph insert/write latency;
- graph read latency;
- lexical search latency;
- semantic search latency when semantic search is used;
- hybrid search latency when hybrid search is used;
- backup duration;
- restore duration;
- mixed read/write/search behavior under expected early production traffic.

The benchmark dataset shape should roughly match early CommonFolio production
workspaces rather than synthetic-only tiny fixtures.

## Initial safe operating envelope

The production launch plan should state:

- expected request rate;
- expected data size per workspace;
- expected attachment volume if attachments are used;
- expected search mix;
- expected backup window;
- measured headroom on the initial three-pod cluster;
- stop-onboarding or scale-up thresholds.

This envelope can be conservative. It is acceptable for first production to say
"do not onboard beyond X until scaling work is complete," as long as that limit
is measured and operationally visible.

## Launch gate

CommonFolio production should require:

1. a repeatable benchmark/load-test harness;
2. a representative initial dataset profile;
3. measured baseline latency/throughput for key operations;
4. an initial capacity envelope for the three-pod AWS deployment;
5. at least one load test showing the system does not collapse under expected
   early production traffic;
6. a known list of bottlenecks or risks accepted for first production.

## Follow-up design work

- Define release-regression performance gates.
- Store benchmark history for trend analysis.
- Add profiling hooks for CPU, memory, lock contention, Raft proposal latency,
  storage latency, and index/projection lag.
- Define SLOs for the first user-facing CommonFolio paths.
- Extend Mycel Lab with performance qualification scenarios.
