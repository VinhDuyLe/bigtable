# Implement

## Bigtable: A Distributed Storage System for Structured Data

Paper: https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf

Implement Bigtable from scratch.

This is a phased implementation. Each phase includes design, code, unit & integration tests, and a reproducible local run setup.

Phase A — Storage core (SSTable, Bloom, Block cache) 

Phase B — MemTable + WAL (multi-segment WAL manager, replay, retention)

Phase C — Compaction pipeline (minor, merge, major, discard) + compaction schedulers, throttling

Phase D — Manifest, reference-count SSTable lifecycle, safe file GC

Phase E — Ratis integration (per-tablet Raft groups, state machine, snapshots, dynamic membership)

Phase F — Master, two-level METADATA table, tablet assignment & rebalancer

Phase G — Client SDK (tablet lookup, cache, watchers), admin CLI 

Phase H — Coprocessor sandbox (WASM runner + out-of-process runner)

Phase I — Object-store integration (SStable, multipart upload, atomic rename semantics)

Phase J — Security & ops (mTLS for all RPCs, KMS envelope encryption, cert rotation, Prometheus metrics, tracing)

Phase K — Deployment artifacts (Docker images, Helm charts, k8s operator patterns, PVs)

Phase L — Testing & validation (YCSB harness, chaos testing, cluster tests on kind, resource exhaustion tests)



