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

The new node N performs a dual-channel sync from sibling replica S, with a disk-spill buffer on N for the replication stream. After syncing from S, N switches to the real primary P with a partial resync.

```
                    ┌─────────────┐
                    │  Primary P  │
                    │  (no BGSAVE)│
                    └──────┬──────┘
                           │ replication stream
                    ┌──────▼──────┐
                    │  Sibling S  │──── RDB channel ────► New Node N
                    │  (BGSAVE)   │──── Main channel ───► (buffer on N)
                    └─────────────┘
```

**Phase 1 — Dual-channel sync from S:**
1. N does `CLUSTER REPLICATE <P>` — cluster topology says N→P (gossip, shard_id, flags)
2. Internally, N connects to S (not P) using dual-channel replication
3. RDB channel: S does BGSAVE, sends RDB snapshot to N
4. Main channel: S proxies P's replication stream to N
5. N buffers the main channel stream locally — in memory up to 64MB, then spills to a temp file on N's disk

**Phase 2 — RDB load + buffer drain:**
6. N loads the RDB (dataset at offset X)
7. N drains the buffered stream (disk file → memory → live), applying commands from X+1 onward
8. N is now caught up with S (and transitively close to P)

**Phase 3 — Switch to primary:**
9. N disconnects from S, caches replid + offset
10. N PSYNCs to P using the cached replid (which IS P's replid, inherited by S) and current offset
11. Gap is sub-second — P's default 10MB backlog covers it trivially
12. P does partial resync — streams a few KB/MB from backlog. No fork. Done.

## Why This Works

### Cluster/replication decoupling (verified in code)

The cluster layer and replication layer are independent:

| Layer | What it tracks | How it's set |
|---|---|---|
| Cluster | `myself->replicaof = P` (clusterNode pointer) | `clusterSetPrimary()` line 6787 |
| Gossip | `hdr->replicaof = myself->replicaof->name` | Line 4736 — uses cluster pointer |
| Replication | `server.primary_host = S` (IP string) | `replicationSetPrimary()` line 4441 |

**No code in `cluster_legacy.c` compares `server.primary_host` with `myself->replicaof->ip`.** Only 2 references to `server.primary_host` in the entire file — both are NULL checks. The cluster layer has zero awareness of where the replication TCP connection actually goes.

### Offset and replid correctness (verified in code)

- S's `server.replid` IS P's replid — copied during S's own sync (line 2361)
- S's `+FULLRESYNC` sends P's replid + S's `primary_repl_offset` at fork time (line 838)
- The RDB snapshot and the offset are consistent — both captured in the same call stack before `fork()` returns (line 1066), with no event loop iteration between them
- When N PSYNCs to P, P recognizes its own replid and finds the offset in its backlog

### Sub-replica safeguard doesn't fire (verified in code)

The safeguard at line 4258 checks: `myself->replicaof->replicaof != NULL`

Since `myself->replicaof = P` (a primary), `P->replicaof = NULL`. The condition is never true. The safeguard is a cluster-graph traversal — it has zero awareness of TCP connections.

### S-to-P switch is safe (verified in code)

`replicationCachePrimary()` (line 4722):
- Caches `reploff` = last **applied** offset (command boundary)
- Clears `querybuf` — only discards incomplete command tails
- P resends those bytes via PSYNC — no data loss, no double-apply

The switch (freeClient → connectWithPrimary) is synchronous — no event loop iteration, no race.

### S cannot silently drop data (verified in code)

The shared replication buffer delivers bytes in-order (TCP + sequential buffer). If N falls behind, S hits the COB limit and **disconnects** N — never partial delivery. N's `reploff` is always accurate.

## Disk-Spill Buffer

### Problem it solves

During RDB load (which can take minutes for large datasets), the main channel stream accumulates. The existing `pending_repl_data` memory buffer has a 256MB hard limit. At 50MB/s writes × 90s RDB load = 4.5GB needed — buffer overflows.

### Design

Hybrid buffer: keep first 64MB in memory, spill overflow to a temp file on N's disk.

N is a brand-new empty node — it has abundant free disk space and no competing I/O workload.

```
Incoming stream from S:
  ├── First 64MB → pending_repl_data in memory (existing linked list)
  └── Overflow  → temp file: temp-<pid>-repl.buf (sequential append)
                   fsync every 8MB (same pattern as RDB-to-disk, line 2792)

After RDB load — drain in order:
  1. Memory blocks (existing streamReplDataBufToDb logic)
  2. Disk file in 16MB chunks → append to querybuf → processInputBuffer
  3. Live stream (switch to normal replication read handler)
```

### Scale

| Dataset | Write rate | RDB time | Buffer size | Memory-only? | Disk-spill? |
|---|---|---|---|---|---|
| 1 GB | 5 MB/s | 18s | 90 MB | ✓ | ✓ |
| 10 GB | 50 MB/s | 90s | 4.5 GB | ✗ | ✓ |
| 100 GB | 50 MB/s | 1600s | 80 GB | ✗ | ✓ (80GB disk) |

Memory during drain: dataset + 16MB chunk. Fits on any machine that can hold the dataset.

## Guard Flag

A runtime flag `server.cluster_syncing_from_sibling` protects against cluster events overriding the sibling connection during Phase 1.

### Where it's checked

**At the top of `clusterSetPrimary`** (single guard point for all callers):
```c
if (server.cluster_syncing_from_sibling) {
    abortSiblingSync();  // close S connection, clear flag, cleanup
    // fall through to normal clusterSetPrimary behavior
}
```

This covers all 5 callers that could fire during Phase 1:
- Line 3275: gossip reconfiguration (P loses slots)
- Line 4271: sub-replica safeguard (P becomes someone's replica)
- Line 5898: replica migration to orphan shard
- Line 7822: slot migration (P loses last slot)
- Line 8096: operator runs CLUSTER REPLICATE

**Separate check in CLUSTER FAILOVER handler** (~line 8140):
```c
if (server.cluster_syncing_from_sibling) {
    addReplyError(c, "Node is syncing from sibling, cannot failover");
    return;
}
```
FORCE and TAKEOVER bypass `cluster-replica-no-failover` — must be blocked explicitly. N has incomplete data during Phase 1; promotion would cause data loss.

### Properties
- Runtime only — not persisted. On crash + restart, flag is lost, N falls back to normal full sync from P (safe).
- Auto-cleared when Phase 2 (buffer drain) completes and N switches to P.

## Sibling Selection

N selects the best sibling from `CLUSTER REPLICAS <primary-id>`:

**Criteria:**
1. Not in FAIL or PFAIL state
2. Connected (cluster bus link active)
3. Lowest replication lag (highest `repl_offset` in gossip)
4. Pre-flight check: `P_gossip_offset - S_offset < S.repl_backlog_size × 0.8`

**Who picks:** N picks, using cluster gossip data available after CLUSTER MEET. Primary-side selection (P picks the best S) would be more accurate but requires new protocol — deferred to future enhancement.

**If no eligible sibling:** fall back to normal full sync from P. The feature is best-effort, not mandatory.

## rdb-only BGSAVE Piggybacking Bug

When a sibling has a BGSAVE already in progress and a new rdb-only client attaches to it (line 1228), the offset from `+FULLRESYNC` is from the original BGSAVE trigger time. But rdb-only clients don't get the output buffer copy (line 1239 skips them). The offset is stale — the RDB content is ahead of the offset. This causes **duplicate command application** for non-idempotent commands (INCR, LPUSH, SADD).

**Fix:** Never attach rdb-only sync requests to an existing BGSAVE. Force `WAIT_BGSAVE_START` and trigger a fresh BGSAVE. This ensures the offset in `+FULLRESYNC` matches the RDB content exactly.

## Failure Modes

| Failure | During | Result | Recovery |
|---|---|---|---|
| S dies mid-RDB transfer | Phase 1 | RDB incomplete | Abort, retry with different S or fall back to P |
| S gets promoted (failover) | Phase 1 | `disconnectReplicas` kills N | Detect topology change, abort, fall back |
| S's BGSAVE causes OOM on S | Phase 1 | S crashes or kills BGSAVE | N's connection drops, fall back to P |
| N's disk fills during spill | Phase 1 | `write()` returns ENOSPC | Abort sync, fall back to P |
| P fails | Phase 1 | Phase 3 has no target | Sub-replica safeguard fires, abort, sync from new primary |
| P rejects PSYNC (backlog trimmed) | Phase 3 | Gap too large | Full sync from P (defeats purpose but safe) |
| N crashes during Phase 1 | Any | Flag lost, temp files orphaned | Restart: clusterCron line 6283 triggers normal sync from P |
| Operator runs CLUSTER FAILOVER FORCE | Phase 1 | Blocked by guard | Error returned to operator |
| Replica migration triggers | Phase 1 | `clusterSetPrimary` fires | Guard aborts Phase 1, migration proceeds normally |

**Every failure mode falls back safely.** Worst case is a normal full sync from P — the thing that happens today without this feature.

## Performance

For 100GB dataset at 50MB/s writes:

| Metric | Normal full sync (from P) | Sync from replica |
|---|---|---|
| Total time | ~27 min | ~40 min |
| Primary fork/BGSAVE | Yes (300s, COW memory spike) | **No** |
| Primary RDB transfer | Yes (1000s, saturates network) | **No** |
| Primary cost | High (fork + COW + RDB I/O + network) | **Near zero** (partial resync of a few MB) |
| Replica S cost | None | BGSAVE + RDB transfer (acceptable) |
| New node N cost | RDB load | RDB load + buffer drain |

The tradeoff: ~50% longer total sync time, but **zero impact on primary**.

### Benchmark (EKS c5.2xlarge, isolated pods, 5GB dataset, A/B comparison)

A/B comparison on AWS EKS: 4 separate c5.2xlarge pods (8 vCPU, 16GB each).
Same cluster, same 5GB dataset, back to back. 300k ops per measurement,
1KB values, 50 clients.

**Scenario A: Sync from PRIMARY (P forks for BGSAVE)**

```
                   SET rps   SET p50   SET p99   SET max   GET rps   GET p50   GET p99   GET max
A1 BASELINE:       58,525    0.799ms   1.415ms   2.735ms   60,374    0.767ms   1.127ms   1.935ms
A2 DURING SYNC:    54,210    0.839ms   1.407ms  55.423ms   59,382    0.799ms   1.167ms   5.407ms
A3 AFTER SYNC:     55,669    0.847ms   1.271ms   3.031ms   61,538    0.759ms   1.087ms   3.327ms
```

**Scenario B: Sync from SIBLING (P does NOT fork)**

```
                   SET rps   SET p50   SET p99   SET max   GET rps   GET p50   GET p99   GET max
B1 BASELINE:       58,766    0.807ms   1.167ms   2.879ms   62,176    0.751ms   1.103ms   2.943ms
B2 DURING SYNC:    57,615    0.823ms   1.183ms   2.535ms   60,520    0.767ms   1.135ms   2.847ms
B3 AFTER SYNC:     55,289    0.863ms   1.223ms   2.471ms   61,450    0.759ms   1.127ms   4.863ms
```

**Key findings:**

| Metric | Sync from PRIMARY | Sync from SIBLING |
|---|---|---|
| SET max during sync | **55.4ms** | **2.5ms** (22x better) |
| SET rps during sync | 54,210 (-7.4%) | 57,615 (-2.0%) |
| P sync_full increase | +1 (forked) | **+0** (never forked) |

The fork stall is visible in the tail latency: a 55ms spike on the
primary during BGSAVE (A2 SET max). With sync-from-sibling, max stays
at normal baseline noise (2.5ms). On larger datasets or tighter memory,
these stalls grow to multi-second range.

## Code Changes

### Modified files

| File | Function/Area | Change | Size |
|---|---|---|---|
| `cluster_legacy.c` | `clusterSetPrimary` | Add guard flag check at top — abort sibling sync if any topology change fires | S |
| `cluster_legacy.c` | `CLUSTER REPLICATE` handler | After `clusterSetPrimary(P)`, check config and override replication target to S | M |
| `cluster_legacy.c` | `CLUSTER FAILOVER` handler | Block FORCE/TAKEOVER during sibling sync | S |
| `replication.c` | `bufferReplData` | Add disk-spill path when memory exceeds threshold | M |
| `replication.c` | `streamReplDataBufToDb` | Add disk-file drain path (read chunks, feed to processInputBuffer) | M |
| `replication.c` | `freePendingReplDataBuf` | Cleanup temp file on abort | S |
| `replication.c` | `siblingRdbChannelHandler` | Async state machine for sibling handshake (TLS-safe) | M |
| `replication.c` | `siblingFallbackToPrimary` | Centralised abort + redirect to primary | S |
| `replication.c` | `syncCommand` | Fix: never attach rdb-only to existing BGSAVE | S |
| `replication.c` | `cancelReplicationHandshake` | Centralised sibling fallback on any abort/timeout | S |
| `replication.c` | `dualChannelSyncSuccess` | Clear sibling state after steady-state reached | S |
| `server.h` | `replDataBuf` struct | Add disk-spill fields (fd, path, offsets, mem_limit) | S |
| `server.h` | `valkeyServer` struct | Add `cluster_syncing_from_sibling` flag, sibling state machine | S |
| `config.c` | New configs | `repl-prefer-sync-from-replica yes/no`, `repl-sync-buffer-mem-limit 64mb` | S |

**S = small (<30 lines), M = medium (30-100 lines)**

**Estimated total: ~1600 lines of new/modified code (including tests).**

### New configuration

```
# Enable sync-from-replica optimization (default: no)
repl-prefer-sync-from-replica yes

# Memory threshold before disk spill for replication buffer (default: 64mb)
repl-sync-buffer-mem-limit 64mb
```

### Observability

**INFO replication additions:**
```
sync_from_replica_in_progress:1
sync_from_replica_source:<sibling-node-id>
sync_from_replica_phase:rdb_transfer|rdb_loading|buffer_drain|psync_switch|done
sync_from_replica_buffer_mem:67108864
sync_from_replica_buffer_disk:4831838208
sync_from_replica_offset_gap:12345678
```

**Log messages:**
```
[NOTICE] Sync-from-replica: selected sibling <node-id> at offset <X> (primary at <Y>, gap <delta>)
[NOTICE] Sync-from-replica: RDB transfer from sibling started (<N> bytes)
[NOTICE] Sync-from-replica: RDB loaded, draining buffer (<N> bytes memory + <M> bytes disk)
[NOTICE] Sync-from-replica: buffer drained, switching to primary PSYNC at offset <X>
[NOTICE] Sync-from-replica: PSYNC to primary succeeded, steady-state replication active
[WARNING] Sync-from-replica: failed at <phase> (<reason>), falling back to full sync from primary
```

## Relationship to Maintainer Questions

From the weekly meeting discussion on issue #2767:

> 1. The full-sync will always happen on the replica, but we could stream the ongoing change from the primary or the replica.

**Answer:** We stream from the **primary directly**. N opens two connections: an rdb-only channel to sibling S (for the RDB snapshot), and a main PSYNC channel to P (for the ongoing replication stream). Streaming through S causes backpressure on P when S forks for BGSAVE. Direct P→N eliminates this — N is an empty/fast node that doesn't slow P down.

> 2. How to integrate this into the cluster mode. It can either be a first class component of the topology or it can just be a transient step.

**Answer:** Transient step. The cluster topology always shows N→P. The sibling connection is invisible to gossip. No new topology concepts, no permanent chain replication. After sync, it's a normal replica.

## Alternatives Considered

| Approach | Why rejected |
|---|---|
| Cross-server dual-channel (RDB from S, stream from P simultaneously) | Breaks all dual-channel same-server assumptions ($ENDOFF, set-rdb-client-id, provisional_primary). Too complex. |
| Primary-delegated BGSAVE (P asks S to BGSAVE, relays RDB) | P relays the RDB → doubles P's network I/O. Defeats the purpose. |
| Serve sibling's existing `dump.rdb` (no BGSAVE) | RDB too stale for any non-trivial workload. P's backlog never covers the gap. |
| rdb-only from S + PSYNC to P (no buffering) | Gap = write_rate × RDB_time. Always exceeds P's backlog for production workloads. |
| Permanent chain replication (N→S→P) | Sub-replica safeguard actively fights it. Source switching on catchup risks corruption. Complex topology implications. |
| Iterative PSYNC convergence (rdb-only, then repeated PSYNCs) | First PSYNC requires S's backlog ≥ G0. For 100GB at 50MB/s, G0 = 80GB. Impractical. |

## Future Enhancements

- **Primary-side sibling selection:** P picks the best S using real-time REPLCONF ACK data, sends `+DELEGATED <sibling>` to N. More accurate than gossip-based selection.
- **Backlog pinning:** New `REPLCONF pin-backlog <offset>` command. Primary holds backlog at a specific offset. Eliminates any residual backlog-gap risk during Phase 3 switch.
- **Multiple simultaneous new replicas:** Two new nodes syncing from the same sibling can share a single BGSAVE (existing BGSAVE sharing at line 1214 already supports this for disk-based sync).
- **Automatic backlog sizing on S:** Temporarily increase S's `repl-backlog-size` before Phase 1 based on estimated write rate and RDB time.
