# CAP Theorem Demo — Consistency vs Availability

A dependency-free simulation (standard library + `asyncio` only) that
demonstrates the CAP theorem trade-off using two **separate, independent**
5-node clusters:

- **`ConsistentSystem` (CP)** — during a network partition, it refuses to
  read or write unless it can reach a quorum (majority) of its nodes. It
  never returns a wrong answer, but it can become unavailable.
- **`AvailableSystem` (AP)** — during a partition, it always answers using
  whatever node the client reaches, replicating best-effort to whoever
  else is reachable. It's always available, but its nodes can drift out
  of sync with each other.

Each system owns its **own** `Network` and its **own** set of `Node`
objects — `cp` and `ap` never share state. That separation matters: it's
what lets the demo prove, at the end, that CP's cluster stayed perfectly
consistent while AP's cluster physically diverged.

## Files

| File         | Purpose                                                                                                                            |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------- |
| `Network.py` | Simulates the network; lets us "partition" a set of nodes into two groups                                                          |
| `Node.py`    | A single in-memory key/value replica, identified by `node_id`                                                                      |
| `Systems.py` | The `ConsistentSystem` (CP) and `AvailableSystem` (AP) logic, plus the shared `Result` type                                        |
| `demo.py`    | `testSystems()` runs the full CAP scenario; `testNetwork()` is a smaller, standalone sanity check of `Network`/`Node` in isolation |

## Run it

```bash
python3 main.py
```

This runs `testSystems()`. `testNetwork()` is left in the file as a
lower-level scratch test of partition/heal/connectivity logic — call it
from `main()` instead if you want to see just the network mechanics on
their own, without the CP/AP systems layered on top.

## What you'll see

1. **Normal operation** — CP's cluster and AP's cluster are both
   initialized to `balance = 100`. They're separate clusters, but they
   agree because nothing's partitioned yet.
2. **A partition is introduced** _identically_ on both clusters' networks:
   nodes `{0, 1}` are cut off from `{2, 3, 4}`.
3. A client sends a write **and** a read to node `0` (the cut-off side)
   through **both** systems at once:
   - CP **rejects both** — it can only see 2/5 nodes, short of the
     quorum of 3, so it refuses to write _or_ read rather than risk
     serving a stale/unconfirmed value.
   - AP **accepts the write immediately** on node 0 (and replicates it
     to node 1, the only other node it can reach) and **answers the
     read instantly** from node 0's local store.
4. Reading `balance` from a majority node (`2`) on both systems shows
   `100` either way — but for very different reasons: CP's `100` is a
   quorum-confirmed, guaranteed-correct value; AP's `100` is just
   whatever node 2 happens to have locally, while node 0 in that same
   AP cluster is silently sitting on `250`.
5. **The partition heals** on both networks.
6. **Final state comparison** — because CP and AP are genuinely separate
   clusters, this is where the divergence becomes visible:
   - CP's cluster: every node still reads `100`. It never accepted the
     conflicting write, so there was never anything to reconcile.
   - AP's cluster: nodes `0` and `1` read `250`; nodes `2`, `3`, `4` read
     `100`. Healing the network doesn't fix this by itself — an AP
     system needs an explicit reconciliation strategy (e.g.
     last-write-wins, vector clocks, CRDTs, read-repair) to converge.
     That step is intentionally left out here so the divergence stays
     visible.

## Design notes

- **Quorum lives only on `ConsistentSystem`.** `DistributedSystem` (the
  shared base class) deliberately has no quorum concept — quorum-gating
  and unconditional availability are mutually exclusive by definition,
  so giving `AvailableSystem` a quorum would just make it CP.
- **Two `Network` instances, two node sets.** Earlier versions of this
  demo shared one `Network`/node set between both systems, which meant
  a write through AP silently mutated data that CP's dict of nodes also
  pointed at — making it look like CP had "changed its mind" when it
  hadn't touched anything. Giving each system its own physical cluster
  fixes that and makes Step 6's comparison meaningful.

## Extending it

- Add a third system for eventual consistency + read-repair on heal.
- Add more keys / concurrent random writes from multiple clients.
- Try a 3-way partition instead of a 2-way split.
- Swap which side (majority/minority) the client's writes target.
