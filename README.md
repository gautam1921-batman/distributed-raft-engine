# Distributed Raft Consensus Engine

**Repository:** https://github.com  
**GitHub Profile:** https://github.com

A fault-tolerant Distributed Raft Consensus Engine and Replicated Key-Value Store built from scratch in Python using zero external dependencies (syncio, socket, json, struct, andom, 	ime).

## Features
- **Async TCP Transport (	ransport.py)**: 4-byte length-prefixed JSON framing over syncio streams with packet loss and network partition simulation.
- **Leader Election (aft_node.py)**: RequestVote RPCs, term discovery, log up-to-date verification, and randomized election timeouts.
- **Log Replication & Safety (aft_node.py)**: AppendEntries heartbeats, log consistency checks, conflict truncation, and majority quorum commits.
- **Replicated KV State Machine (kv_store.py, cluster_runner.py)**: 5-node interactive cluster supporting live leader failover (kill), recovery (evive), and network partitions (partition).

## Run Locally
`powershell
git clone https://github.com.git
cd distributed-raft-engine
python cluster_runner.py
`
"@ -Encoding UTF8

Set-Content -Path ".gitignore" -Value @"
__pycache__/
*.py[cod]
.vscode/
