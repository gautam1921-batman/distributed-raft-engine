import asyncio
from typing import Any, Dict, Tuple

from raft_node import RaftNode, Role


class KVServer:
    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        peers: Dict[int, Tuple[str, int]],
    ):
        self.node_id = node_id
        self.store: Dict[str, Any] = {}
        self.raft = RaftNode(
            node_id=node_id,
            host=host,
            port=port,
            peers=peers,
            apply_callback=self._apply_command,
        )

    def _apply_command(self, index: int, command: dict) -> None:
        op = command.get("op")
        key = command.get("key")
        if op == "SET":
            self.store[key] = command.get("value")
        elif op == "DEL":
            self.store.pop(key, None)

    async def start(self) -> None:
        await self.raft.start()

    async def stop(self) -> None:
        await self.raft.stop()

    async def client_set(self, key: str, value: Any) -> dict:
        if self.raft.role != Role.LEADER:
            return {
                "ok": False,
                "error": "NOT_LEADER",
                "leader_hint": self.raft.leader_id,
            }

        committed, log_idx = await self.raft.propose(
            {"op": "SET", "key": key, "value": value}
        )
        if not committed:
            return {
                "ok": False,
                "error": "COMMIT_TIMEOUT_OR_STEPDOWN",
                "leader_hint": self.raft.leader_id,
            }

        while self.raft.last_applied < log_idx:
            await asyncio.sleep(0.01)

        return {"ok": True, "index": log_idx, "key": key, "value": value}

    async def client_del(self, key: str) -> dict:
        if self.raft.role != Role.LEADER:
            return {
                "ok": False,
                "error": "NOT_LEADER",
                "leader_hint": self.raft.leader_id,
            }

        committed, log_idx = await self.raft.propose({"op": "DEL", "key": key})
        if not committed:
            return {
                "ok": False,
                "error": "COMMIT_TIMEOUT_OR_STEPDOWN",
                "leader_hint": self.raft.leader_id,
            }

        while self.raft.last_applied < log_idx:
            await asyncio.sleep(0.01)

        return {"ok": True, "index": log_idx, "deleted": key}

    def client_get(self, key: str) -> dict:
        return {
            "ok": True,
            "node_id": self.node_id,
            "role": self.raft.role.value,
            "key": key,
            "value": self.store.get(key),
        }