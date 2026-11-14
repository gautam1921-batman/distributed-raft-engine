import asyncio
from typing import Dict, Tuple

from kv_store import KVServer
from raft_node import Role

BASE_PORT = 9200
NODE_IDS = (1, 2, 3, 4, 5)


def build_peers(node_id: int) -> Dict[int, Tuple[str, int]]:
    return {
        nid: ("127.0.0.1", BASE_PORT + nid)
        for nid in NODE_IDS
        if nid != node_id
    }


async def async_input(prompt: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: input(prompt))


def print_help() -> None:
    print(
        """
=== Raft KV Cluster Commands ===
  status                  - Show role, term, commit index, and KV store for every node
  set <key> <value>       - Write a key-value pair via the current leader
  get <key> [node_id]     - Read a key from the leader (or a specific follower)
  del <key>               - Delete a key via the current leader
  kill <node_id>          - Kill a node (e.g. kill the active leader!)
  revive <node_id>        - Restart a killed node and watch its log catch up
  partition <id1> <id2>   - Sever network link between two nodes in both directions
  heal                    - Heal all network partitions and reset packet loss
  drop <rate>             - Set packet drop probability (0.0 to 1.0) on all live nodes
  help                    - Show this command menu
  exit                    - Shut down cluster and exit
"""
    )


async def main() -> None:
    cluster: Dict[int, KVServer] = {}
    alive: Dict[int, bool] = {}

    print("Starting 5-node Raft KV cluster on ports 9201-9205...")
    for nid in NODE_IDS:
        srv = KVServer(nid, "127.0.0.1", BASE_PORT + nid, build_peers(nid))
        cluster[nid] = srv
        alive[nid] = True
        await srv.start()

    await asyncio.sleep(2.5)
    print_help()

    try:
        while True:
            raw = (await async_input("raft> ")).strip()
            if not raw:
                continue

            parts = raw.split()
            cmd = parts.pop(0).lower()

            if cmd == "exit":
                break

            elif cmd == "help":
                print_help()

            elif cmd == "status":
                print("\n--- Cluster State ---")
                for nid in NODE_IDS:
                    if not alive[nid]:
                        print(f"Node {nid}: [DEAD / OFFLINE]")
                        continue
                    r = cluster[nid].raft
                    print(
                        f"Node {nid}: role={r.role.value:<9} | term={r.current_term:<2} | "
                        f"commit={r.commit_index:<2} | log_len={len(r.log)-1:<2} | "
                        f"store={cluster[nid].store}"
                    )
                print("---------------------\n")

            elif cmd == "set" and len(parts) >= 2:
                key = parts.pop(0)
                value = " ".join(parts)
                leader = next(
                    (
                        cluster[nid]
                        for nid in NODE_IDS
                        if alive[nid] and cluster[nid].raft.role == Role.LEADER
                    ),
                    None,
                )
                if not leader:
                    print("No active leader right now (election in progress). Try again.")
                    continue
                res = await leader.client_set(key, value)
                print("SET result:", res)

            elif cmd == "del" and len(parts) == 1:
                key = parts.pop(0)
                leader = next(
                    (
                        cluster[nid]
                        for nid in NODE_IDS
                        if alive[nid] and cluster[nid].raft.role == Role.LEADER
                    ),
                    None,
                )
                if not leader:
                    print("No active leader right now. Try again.")
                    continue
                res = await leader.client_del(key)
                print("DEL result:", res)

            elif cmd == "get" and len(parts) >= 1:
                key = parts.pop(0)
                if len(parts) == 1 and parts.pop(0).isdigit():
                    target_id = int(raw.split().pop())
                    if not alive.get(target_id):
                        print(f"Node {target_id} is dead.")
                        continue
                    print("GET result:", cluster[target_id].client_get(key))
                else:
                    leader = next(
                        (
                            cluster[nid]
                            for nid in NODE_IDS
                            if alive[nid] and cluster[nid].raft.role == Role.LEADER
                        ),
                        None,
                    )
                    if not leader:
                        print("No active leader; specify a live node_id: get <key> <id>")
                        continue
                    print("GET result:", leader.client_get(key))

            elif cmd == "kill" and len(parts) == 1:
                arg = parts.pop(0)
                if not arg.isdigit():
                    print("Usage: kill <node_id>")
                    continue
                nid = int(arg)
                if nid not in cluster or not alive[nid]:
                    print(f"Node {nid} is already dead or invalid.")
                    continue
                await cluster[nid].stop()
                alive[nid] = False
                print(f"Killed Node {nid}.")

            elif cmd == "revive" and len(parts) == 1:
                arg = parts.pop(0)
                if not arg.isdigit():
                    print("Usage: revive <node_id>")
                    continue
                nid = int(arg)
                if nid not in cluster or alive[nid]:
                    print(f"Node {nid} is already alive or invalid.")
                    continue
                srv = KVServer(nid, "127.0.0.1", BASE_PORT + nid, build_peers(nid))
                cluster[nid] = srv
                alive[nid] = True
                await srv.start()
                print(f"Revived Node {nid}.")

            elif cmd == "partition" and len(parts) == 2:
                arg1, arg2 = parts.pop(0), parts.pop(0)
                if arg1.isdigit() and arg2.isdigit():
                    a, b = int(arg1), int(arg2)
                    if a in cluster and b in cluster:
                        cluster[a].raft.transport.blocked_peers.add(b)
                        cluster[b].raft.transport.blocked_peers.add(a)
                        print(f"Partitioned Node {a} <-> Node {b}")

            elif cmd == "heal":
                for nid in NODE_IDS:
                    cluster[nid].raft.transport.blocked_peers.clear()
                    cluster[nid].raft.transport.drop_rate = 0.0
                print("Healed all network partitions and reset packet drop rate to 0.0.")

            elif cmd == "drop" and len(parts) == 1:
                rate = float(parts.pop(0))
                for nid in NODE_IDS:
                    cluster[nid].raft.transport.drop_rate = rate
                print(f"Set packet drop rate to {rate:.2f} across all nodes.")

            else:
                print("Invalid command. Type 'help' for usage.")

    finally:
        print("Shutting down cluster...")
        for nid in NODE_IDS:
            if alive.get(nid):
                await cluster[nid].stop()


if __name__ == "__main__":
    asyncio.run(main())
