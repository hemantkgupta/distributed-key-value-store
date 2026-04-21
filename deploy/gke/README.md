# GKE Plan

The GKE deployment will use:
- Regional GKE cluster.
- `StatefulSet` for KV nodes.
- Headless Service for stable peer identity.
- PVC template for local persistent state.
- Zone labels mapped to rack metadata.

Manifests will be added after the node runtime and configuration model exist.
