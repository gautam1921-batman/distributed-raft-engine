# Distributed Raft Consensus Engine (Zero-Dependency Python)

A fault-tolerant **Distributed Raft Consensus Engine** and **Replicated Key-Value Store** built from scratch in Python using only the standard library (`asyncio`, `socket`, `json`, `struct`, `random`, `time`).

## Architecture & Core Components
* **Async TCP RPC Transport (`transport.py`)**: 4-byte big-endian length-prefixed JSON framing over `asyncio` streams, with built-in chaos hooks for packet loss (`drop`) and bidirectional network partitions (`partition`).
* **Leader Election (`raft_node.py`)**: `RequestVote` RPCs, monotonic term discovery, election log up-to-date checks, and randomized election timeouts to prevent split votes.
* **Log Replication & Commit Safety (`raft_node.py`)**: `AppendEntries` heartbeats, `prevLogIndex`/`prevLogTerm` log matching, conflict truncation, and majority quorum commit advancement.
* **Replicated Key-Value State Machine (`kv_store.py` & `cluster_runner.py`)**: Applies committed entries deterministically across a 5-node cluster with an interactive CLI for live leader failover (`kill`), recovery (`revive`), and network partitioning.

## Quick Start

```powershell
git clone https://github.com
cd distributed-raft-engine
python cluster_runner.py
```

## Interactive Chaos Test Demo
```text
raft> status
raft> set project raft
raft> kill 2
raft> status
raft> revive 2
```
