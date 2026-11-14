# Distributed Raft Consensus Engine (Zero-Dependency Python)

A fault-tolerant **Distributed Raft Consensus Engine** and **Replicated Key-Value Store** built from scratch in Python using only the standard library (`asyncio`, `socket`, `json`, `struct`, `random`, `time`).

## Key Features
* **Custom Async TCP Transport (`transport.py`)**: Implements 4-byte length-prefixed JSON framing over `asyncio` streams, with built-in chaos hooks for packet loss and network partitions.
* **Leader Election & Split-Vote Prevention (`raft_node.py`)**: Implements `RequestVote` RPCs, term discovery, log up-to-date checks, and randomized election timeouts.
* **Log Replication & Quorum Safety (`raft_node.py`)**: Implements `AppendEntries` heartbeats, `prevLogIndex`/`prevLogTerm` consistency checks, conflict truncation, and majority quorum commits.
* **Replicated Key-Value State Machine (`kv_store.py` & `cluster_runner.py`)**: Applies committed log entries deterministically across a 5-node cluster with an interactive CLI for live leader failover (`kill`), recovery (`revive`), and partitioning (`partition`).

## Quick Start
```powershell
python cluster_runner.py
```
