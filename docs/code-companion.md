# Code Companion

This file maps the future Complete Engineering Guide sections to code locations.

| Guide Section | Code Location | Status |
|---|---|---|
| Foundation: API and data model | `kv-common`, `kv-storage-api` | Skeleton |
| Foundation: token ring and vnodes | `kv-partitioning` | Skeleton |
| Foundation: write/read path | `kv-replication` | Skeleton |
| Going Deeper: hinted handoff | `kv-repair` | Planned |
| Going Deeper: digest reads and read repair | `kv-replication`, `kv-repair` | Planned |
| Going Deeper: Merkle anti-entropy | `kv-repair` | Planned |
| Going Deeper: tombstones and TTL | `kv-storage-api`, storage modules | Planned |
| At Scale: compaction debt | `kv-storage-rocksdb`, `kv-storage-toy-lsm`, `kv-bench` | Planned |
| At Scale: deterministic simulation | `kv-simulator` | Planned |
| At Scale: local 5-node ring | `deploy/compose` | Planned |
| At Scale: GCP deployment | `deploy/gke` | Planned |

## Sync Rule

When the guide claims a mechanism exists, this companion must point to the file or test that implements it. If the code only simulates a production behavior locally, say so here and in the guide.
