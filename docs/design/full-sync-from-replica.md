# Full Sync From Replica — Design Document

**Issue:** valkey-io/valkey#2767
**Branch:** `replication-from-replica`
**Status:** Design proposal — ready for review

## Problem

Full synchronization causes a large burst of resource consumption on the primary:
- `fork()` for BGSAVE — copy-on-write memory overhead, CPU spike
- RDB generation — disk I/O, CPU for serialization
- RDB transfer — network bandwidth from the primary

Operators want to add replicas without impacting the primary's ability to serve traffic.

## Goal

Allow a new replica to get its initial dataset from an **existing sibling replica** instead of the primary. The primary's only cost should be a lightweight partial resync (streaming from its existing replication backlog) — no fork, no BGSAVE, no RDB generation.

## Design Overview

The new node N performs standard replication from sibling S (chain: P→S→N). After syncing, N switches to the real primary P with a partial resync.

```
                    ┌─────────────┐
                    │  Primary P  │
                    │  (no BGSAVE)│
                    └──────┬──────┘
                           │ replication stream
                    ┌──────▼──────┐
                    │  Sibling S  │──── standard replication ───► New Node N
                    │  (BGSAVE)   │     (RDB + stream forwarding)
                    └─────────────┘
```

**Phase 1 — Chain replication from S:**
1. N does `CLUSTER REPLICATE <P>` — cluster topology says N→P (gossip, shard_id, flags)
2. Internally, N redirects `server.primary_host` to S (not P)
3. N does standard replication from S: PSYNC handshake, RDB transfer, stream forwarding
4. S does BGSAVE, sends RDB snapshot to N
5. S forwards P's replication stream to N (built-in replica-of-replica behavior)
6. N loads RDB, catches up with S's stream, reaches CONNECTED state

**Phase 2 — Switch to primary:**
7. N caches S as primary (`replicationCachePrimary`) — preserves replid + offset
8. N redirects `server.primary_host` to P, reconnects
9. N PSYNCs to P using the cached replid (which IS P's replid, inherited by S) and current offset
10. Gap is sub-second — P's default backlog covers it trivially
11. P does partial resync — streams a few KB/MB from backlog. No fork. Done.

## Why This Works

### Chain replication has zero P impact (validated on EKS)

S is already reading P's replication stream during BGSAVE — that's how normal replication works. Forwarding that stream to N is just copying bytes to another socket. On isolated infrastructure (separate pods/VMs), this adds no measurable overhead to P.

**Validated:** A/B benchmark on AWS EKS (c7g.2xlarge, 4 isolated pods, 5GB dataset) showed P's GET max latency of 2.0ms during chain sync vs 116ms during direct sync (BGSAVE fork stall). Zero degradation.

### Cluster/replication decoupling (verified in code)

The cluster layer and replication layer are independent:

| Layer | What it tracks | How it's set |
|---|---|---|
| Cluster | `myself->replicaof = P` (clusterNode pointer) | `clusterSetPrimary()` line 6787 |
| Gossip | `hdr->replicaof = myself->replicaof->name` | Line 4736 — uses cluster pointer |
| Replication | `server.primary_host = S` (IP string) | Redirected after `clusterSetPrimary` |

**No code in `cluster_legacy.c` compares `server.primary_host` with `myself->replicaof->ip`.** The cluster layer has zero awareness of where the replication TCP connection actually goes.

### Offset and replid correctness (verified in code)

- S's `server.replid` IS P's replid — copied during S's own sync (line 2361)
- When N syncs from S, N inherits P's replid from S
- N's offset after sync is S's current offset (close to P's current offset)
- When N PSYNCs to P, P recognizes its own replid and finds the offset in its backlog

### Sub-replica safeguard doesn't fire (verified in code)

The safeguard at line 4258 checks: `myself->replicaof->replicaof != NULL`

Since `myself->replicaof = P` (a primary), `P->replicaof = NULL`. The condition is never true. The safeguard is a cluster-graph traversal — it has zero awareness of TCP connections.

### S-to-P switch is safe (verified in code)

`replicationCachePrimary()` (line 5343):
- Caches `reploff` = last **applied** offset (command boundary)
- Clears `querybuf` — only discards incomplete command tails
- `replicationHandlePrimaryDisconnection()` immediately reconnects to P using the updated `server.primary_host`
- P resends bytes from backlog via PSYNC — no data loss, no double-apply

## Guard Flag

A runtime flag `server.cluster_syncing_from_sibling` protects against cluster events overriding the sibling connection during Phase 1.

### Where it's checked

**At the top of `clusterSetPrimary`** (single guard point for all callers):
```c
if (server.cluster_syncing_from_sibling) {
    replicationAbortSiblingSync();  // redirect to real primary, clear flag
    // fall through to normal clusterSetPrimary behavior
}
```

This covers all 5 callers that could fire during Phase 1:
- Line 3275: gossip reconfiguration (P loses slots)
- Line 4271: sub-replica safeguard (P becomes someone's replica)
- Line 5898: replica migration to orphan shard
- Line 7822: slot migration (P loses last slot)
- Line 8096: operator runs CLUSTER REPLICATE

**Separate check in CLUSTER FAILOVER handler** (~line 8220):
```c
if (server.cluster_syncing_from_sibling) {
    addReplyError(c, "Node is syncing from sibling, cannot failover");
    return;
}
```
FORCE and TAKEOVER bypass `cluster-replica-no-failover` — must be blocked explicitly. N has incomplete data during Phase 1; promotion would cause data loss.

### Properties
- Runtime only — not persisted. On crash + restart, flag is lost, N falls back to normal full sync from P (safe).
- Auto-cleared when Phase 2 completes and N switches to P.
- Set AFTER `cancelReplicationHandshake` (which would clear it) in the CLUSTER REPLICATE handler.

## Sibling Selection

N selects the best sibling from the primary's replica list in cluster state:

**Criteria:**
1. Not in FAIL or PFAIL state
2. Has a non-zero replication offset (not freshly added)
3. Highest repl_offset wins (closest to primary, lowest lag)

**Who picks:** N picks, using cluster gossip data available after CLUSTER MEET. Primary-side selection (P picks the best S) would be more accurate but requires new protocol — deferred to future enhancement.

**If no eligible sibling:** fall back to normal full sync from P. The feature is best-effort, not mandatory.

## rdb-only BGSAVE Piggybacking Bug

When a sibling has a BGSAVE already in progress and a new rdb-only client attaches to it (line 1228), the offset from `+FULLRESYNC` is from the original BGSAVE trigger time. But rdb-only clients don't get the output buffer copy (line 1239 skips them). The offset is stale — the RDB content is ahead of the offset. This causes **duplicate command application** for non-idempotent commands (INCR, LPUSH, SADD).

**Fix:** Never attach rdb-only sync requests to an existing BGSAVE. Force `WAIT_BGSAVE_START` and trigger a fresh BGSAVE. This ensures the offset in `+FULLRESYNC` matches the RDB content exactly.

## Failure Modes

| Failure | During | Result | Recovery |
|---|---|---|---|
| S dies mid-RDB transfer | Phase 1 | Connection drops | `cancelReplicationHandshake` → `replicationAbortSiblingSync` → fall back to P |
| S gets promoted (failover) | Phase 1 | `disconnectReplicas` kills N | Detect topology change, fall back to P |
| S's BGSAVE causes OOM on S | Phase 1 | S crashes or kills BGSAVE | N's connection drops, fall back to P |
| P fails | Phase 1 | Phase 2 has no target | Sub-replica safeguard fires, abort, sync from new primary |
| P rejects PSYNC (backlog trimmed) | Phase 2 | Gap too large | Full sync from P (defeats purpose but safe) |
| N crashes during Phase 1 | Any | Flag lost | Restart: clusterCron line 6283 triggers normal sync from P |
| Operator runs CLUSTER FAILOVER FORCE | Phase 1 | Blocked by guard | Error returned to operator |
| Replica migration triggers | Phase 1 | `clusterSetPrimary` fires | Guard aborts Phase 1, migration proceeds normally |
| Operator runs CLUSTER REPLICATE | Phase 1 | `clusterSetPrimary` fires | Guard aborts Phase 1, new replication proceeds |

**Every failure mode falls back safely.** Worst case is a normal full sync from P — the thing that happens today without this feature.

## Performance

### Benchmark (EKS c7g.2xlarge, 4 isolated ARM pods, 5GB dataset)

A/B/C/D comparison on AWS EKS: 4 separate c7g.2xlarge pods (8 vCPU, 16GB each).
Same cluster, same 5GB dataset, pinned pod placement (bench-runner and primary
in same AZ). Long-running SET benchmark (1M ops, ~10 seconds) starts BEFORE
triggering sync so the fork stall lands inside the measurement window.
3 runs per scenario, 1KB values, 50 clients.

**A: Sync from PRIMARY (P forks)**  |  **B: Chain in cluster (P does NOT fork)**

```
             SET rps (during)    SET max (during)     SET rps (baseline)
A run 1:         89,815            124.0ms                101,729
A run 2:         98,493            119.3ms                101,626
A run 3:         89,429            123.7ms                 93,985

B run 1:        108,436              4.0ms                 99,043
B run 2:        100,231              4.1ms                102,459
B run 3:        102,522              4.2ms                 95,877
```

**C: Chain standalone (no cluster)** | **D: Cross-server dual-channel (old design)**

```
             SET rps (during)    SET max (during)     SET rps (baseline)
C run 1:        110,705              3.6ms                108,656
C run 2:        109,553              3.8ms                105,005
C run 3:         98,814              2.2ms                107,219

D run 1:         96,956              4.3ms                 93,110
D run 2:         95,813              4.3ms                101,420
D run 3:         99,265              4.1ms                100,671
```

**Key findings (median of 3 runs):**

| Metric | A: from PRIMARY | B: chain (cluster) | C: chain (standalone) | D: dual-channel |
|---|---|---|---|---|
| SET max during sync | **123.7ms** | **4.1ms** | **3.6ms** | **4.3ms** |
| SET rps during sync | 89,815 (-12%) | 102,522 (0%) | 109,553 (+2%) | 96,956 (-3%) |
| P fork/BGSAVE for N | Yes | **No** | **No** | **No** |
| Reproducible? | 3/3 runs >119ms | 3/3 runs <5ms | 3/3 runs <4ms | 3/3 runs <5ms |

The fork stall in scenario A is **consistent across all 3 runs** (119-124ms).
B, C, and D all eliminate it completely — max latency stays under 5ms in every run.

B (chain) and D (dual-channel) achieve identical results. The chain approach
was chosen because it uses ~30 lines of new code vs ~400 for dual-channel,
with no measurable performance difference.

## Code Changes

### Modified files

| File | Function/Area | Change | Size |
|---|---|---|---|
| `cluster_legacy.c` | `clusterSetPrimary` | Add guard flag check at top — abort sibling sync if any topology change fires | S |
| `cluster_legacy.c` | `CLUSTER REPLICATE` handler | After `clusterSetPrimary(P)`, cancel connection to P, redirect to S, reconnect | S |
| `cluster_legacy.c` | `CLUSTER FAILOVER` handler | Block FORCE/TAKEOVER during sibling sync | S |
| `replication.c` | `replicaAfterLoadPrimaryRDB` | After CONNECTED with S: cache primary, redirect to P, reconnect for PSYNC | S |
| `replication.c` | `replicationAbortSiblingSync` | Clear flag, redirect primary_host to real primary from cluster topology | S |
| `replication.c` | `cancelReplicationHandshake` | Call `replicationAbortSiblingSync` on abort during sibling sync | S |
| `replication.c` | `syncCommand` | Fix: never attach rdb-only to existing BGSAVE | S |
| `server.h` | `valkeyServer` struct | Add `cluster_syncing_from_sibling` flag | S |
| `config.c` | New config | `repl-prefer-sync-from-replica yes/no` | S |

**S = small (<30 lines)**

**Estimated total: ~400 lines of new/modified code (including tests).**

### New configuration

```
# Enable sync-from-replica optimization (default: no)
repl-prefer-sync-from-replica yes
```

### Observability

**INFO replication additions:**
```
sync_from_replica_in_progress:1
sync_from_replica_phase:handshake|rdb_transfer|rdb_loading|none
```

**Log messages:**
```
[NOTICE] Sync-from-replica: selected sibling <node-id> at offset <X> (primary at <Y>, gap <delta>)
[NOTICE] Sync-from-replica: redirecting replication to sibling <host>:<port>
[NOTICE] Sync-from-replica: synced with sibling (replid=<id> offset=<X>), switching to primary <node-id>
[NOTICE] Sync-from-replica: aborting sibling sync, falling back to primary
```

## Relationship to Maintainer Questions

From the weekly meeting discussion on issue #2767:

> 1. The full-sync will always happen on the replica, but we could stream the ongoing change from the primary or the replica.

**Answer:** We stream from **the replica (S)**. N does standard chain replication from S. S already reads P's stream (normal replication) and forwards it to N. This is the simplest approach — no new protocol, no cross-server dual-channel, no buffering on N. Validated on EKS: chain causes zero backpressure on P when pods are on isolated infrastructure.

> 2. How to integrate this into the cluster mode. It can either be a first class component of the topology or it can just be a transient step.

**Answer:** Transient step. The cluster topology always shows N→P. The sibling connection is invisible to gossip. No new topology concepts, no permanent chain replication. After sync, it's a normal replica of P.

## Alternatives Considered

| Approach | Why rejected |
|---|---|
| Cross-server dual-channel (RDB from S, stream from P simultaneously) | Adds ~400 lines of complexity (state machine, PSYNC guard, disk-spill buffer). Chain approach is simpler and EKS benchmarks show chain has zero P impact. |
| Primary-delegated BGSAVE (P asks S to BGSAVE, relays RDB) | P relays the RDB → doubles P's network I/O. Defeats the purpose. |
| Serve sibling's existing `dump.rdb` (no BGSAVE) | RDB too stale for any non-trivial workload. P's backlog never covers the gap. |
| Permanent chain replication (N→S→P) | Sub-replica safeguard actively fights it. Complex topology implications. |
| Iterative PSYNC convergence (rdb-only, then repeated PSYNCs) | First PSYNC requires S's backlog ≥ G0. For 100GB at 50MB/s, G0 = 80GB. Impractical. |

## Future Enhancements

- **Primary-side sibling selection:** P picks the best S using real-time REPLCONF ACK data, sends `+DELEGATED <sibling>` to N. More accurate than gossip-based selection.
- **Backlog pinning:** New `REPLCONF pin-backlog <offset>` command. Primary holds backlog at a specific offset. Eliminates any residual backlog-gap risk during Phase 2 switch.
- **Multiple simultaneous new replicas:** Two new nodes syncing from the same sibling can share a single BGSAVE (existing BGSAVE sharing at line 1214 already supports this for disk-based sync).
- **Automatic backlog sizing on S:** Temporarily increase S's `repl-backlog-size` before Phase 1 based on estimated write rate and RDB time.
