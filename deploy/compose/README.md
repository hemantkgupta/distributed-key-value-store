# Docker Compose Plan

The Compose runtime will model a 5-node ring on one machine.

Planned services:
- `kv-node-1` through `kv-node-5`
- `kv-admin`
- `kv-bench`
- optional `toxiproxy` or equivalent failure-injection proxy

Local Compose can validate protocol behavior and failure handling, but it cannot prove true rack, zone, or region blast-radius behavior.
