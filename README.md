\$proto = "https" + "://"
\$hostName = "github" + ".com"
\$repoUrl = proto + hostName + "/gautam1921-batman/distributed-raft-engine"

\$readme = @"
# Distributed Raft Consensus Engine (Zero-Dependency Python)

A fault-tolerant **Distributed Raft Consensus Engine** and **Replicated Key-Value Store** built from scratch in Python using only the standard library (`asyncio`, `socket`, `json`, `struct`, `random`, `time`).

---

## System Architecture

```text
+-------------------------------------------------------------------+

|                        Client / CLI Layer                         |
|                       (cluster_runner.py)                         |
+---------------------------------+---------------------------------+
                                  | Propose (SET / DEL) / Read (GET)
                                  v
+-------------------------------------------------------------------+

|                  Replicated Key-Value State Machine               |
|                          (kv_store.py)                            |
+---------------------------------+---------------------------------+
                                  | Apply Committed Log Entries
                                  v
+-------------------------------------------------------------------+

|                       Raft Consensus Engine                       |
|                          (raft_node.py)                           |
|  - Leader Election & Randomized Timeouts (RequestVote RPC)        |
|  - Log Replication & Quorum Commits (AppendEntries RPC)           |
|  - Term Discovery, Election Restriction & Log Conflict Truncation |
+---------------------------------+---------------------------------+
                                  | 4-Byte Length-Prefixed JSON
                                  v
+-------------------------------------------------------------------+

|                      Async TCP Transport Layer                    |
|                          (transport.py)                           |
|  - Non-blocking asyncio TCP streams                               |
|  - Chaos Hooks: Packet Loss Simulation & Network Partitions       |
+-------------------------------------------------------------------+
```

---

## Key Engineering Highlights

* **Zero External Dependencies**: Built purely with Python's standard library (`asyncio`, `socket`, `struct`, `json`), eliminating third-party framework abstraction so every socket frame, timer, and state transition is transparent.
* **Custom Length-Prefixed TCP Framing (`transport.py`)**: Uses 4-byte big-endian headers (`struct.pack('!I', len)`) over `asyncio` streams to prevent TCP packet coalescing/fragmentation issues, paired with chaos hooks (`drop_rate` and `blocked_peers`) for fault injection.
* **Split-Vote Prevention & Election Safety (`raft_node.py`)**:
  * Randomized election timeouts prevent synchronized candidate collisions.
  * Voters enforce the Raft election restriction—denying votes to candidates whose logs lag behind in `last_log_term` or `last_log_index`.
* **Log Matching & Majority Quorum Commits (`raft_node.py`)**:
  * Leaders replicate entries via `AppendEntries` heartbeats, verifying `prev_log_index` and `prev_log_term` and automatically backtracking `next_index` to overwrite inconsistent follower logs.
  * `commit_index` advances only after a strict majority `(N // 2) + 1` replicates an entry from the leader's current term.
* **Deterministic State Machine Replication (`kv_store.py`)**: Applies committed log entries in strict index order across all surviving nodes, guaranteeing linearizable state convergence after leader crashes or network partitions.

---

## Project Structure

| File | Responsibility |
| :--- | :--- |
| `transport.py` | Async TCP server/client with 4-byte length-prefixed JSON framing, packet drop simulation, and bidirectional partition toggles. |
| `raft_node.py` | Core Raft state machine (`FOLLOWER`, `CANDIDATE`, `LEADER`), election timers, `RequestVote`, `AppendEntries`, and commit index advancement. |
| `kv_store.py` | Replicated Key-Value store wrapper that proposes commands to Raft and applies committed entries deterministically. |
| `cluster_runner.py` | Interactive 5-node cluster orchestrator and chaos-testing CLI (`status`, `set`, `get`, `del`, `kill`, `revive`, `partition`, `heal`, `drop`). |

---

## Quick Start

### 1. Clone & Launch Cluster
```powershell
git clone $repoUrl.git
cd distributed-raft-engine
python cluster_runner.py
```

### 2. Interactive Chaos & Failover Walkthrough
Once the 5-node cluster elects a leader in **Term 1**, test fault tolerance live in the `raft>` prompt:

```text
# 1. Inspect initial cluster roles and terms
raft> status

# 2. Write keys through the active leader and replicate across all 5 nodes
raft> set engine raft
raft> set status fault_tolerant
raft> status

# 3. Kill the active leader mid-flight (e.g., Node 2)
raft> kill 2

# 4. Verify a follower automatically wins Term 2 with zero committed data loss
raft> status

# 5. Commit new data under the new leader, then revive the old leader
raft> set failover verified
raft> revive 2
raft> status
```

---

## Future Roadmap
* **Write-Ahead Log (WAL) Persistence**: Persist `current_term`, `voted_for`, and `log` entries to disk with `fsync` for full process crash recovery.
* **Log Compaction & Snapshotting**: Bound memory growth by snapshotting applied KV state and truncating historical log prefixes.
"@

[System.IO.File]::WriteAllText("$PWD\README.md", $readme, [System.Text.UTF8Encoding]::new($false))
git add README.md
git commit -m "Add detailed architecture and walkthrough README"
git push origin main
