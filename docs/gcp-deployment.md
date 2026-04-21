# GCP Deployment Plan

## Target Shape

- Regional GKE cluster.
- One `StatefulSet` for KV nodes.
- Headless Service for stable internode DNS.
- One PersistentVolumeClaim per pod.
- Zone-aware node labels used as rack metadata.
- Region-aware metadata reserved for future multi-region placement.

## Design Rules

- PVC durability is local-node restart durability, not the full data safety story.
- System durability comes from replication factor, repair, and topology management.
- Do not stretch one naive global ring across continents.
- Prefer explicit region-local placement and asynchronous cross-region convergence for the first multi-region model.

## Managed Service Contrast

Cloud Bigtable is the managed comparison point. It offers managed replication, app-profile routing, and row-level atomicity, but still pushes row-key design, hot tablets, and routing semantics onto the application owner.
