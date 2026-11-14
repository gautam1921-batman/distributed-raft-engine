import asyncio
import random
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from transport import AsyncTCPTransport


class Role(str, Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class RaftNode:
    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        peers: Dict[int, Tuple[str, int]],
        apply_callback: Optional[Callable[[int, Any], None]] = None,
    ):
        self.node_id = node_id
        self.peers = peers
        self.all_node_ids = [node_id] + list(peers.keys())
        self.quorum_size = (len(self.all_node_ids) // 2) + 1
        self.apply_callback = apply_callback

        self.current_term: int = 0
        self.voted_for: Optional[int] = None
        self.log: List[dict] = [{"term": 0, "command": None}]

        self.commit_index: int = 0
        self.last_applied: int = 0
        self.role: Role = Role.FOLLOWER
        self.leader_id: Optional[int] = None

        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        self.heartbeat_interval: float = 0.20
        self.election_timeout_range: Tuple[float, float] = (1.50, 3.00)
        self._last_heartbeat_time: float = time.monotonic()
        self._election_timeout: float = self._random_timeout()

        self.transport = AsyncTCPTransport(node_id, host, port, peers, self._on_rpc)
        self._running: bool = False
        self._tasks: List[asyncio.Task] = []

    def _random_timeout(self) -> float:
        return random.uniform(*self.election_timeout_range)

    def _reset_election_timer(self) -> None:
        self._last_heartbeat_time = time.monotonic()
        self._election_timeout = self._random_timeout()

    @property
    def last_log_index(self) -> int:
        return len(self.log) - 1

    @property
    def last_log_term(self) -> int:
        return self.log[-1]["term"]

    async def start(self) -> None:
        self._running = True
        self._reset_election_timer()
        await self.transport.start()
        self._tasks = [
            asyncio.create_task(self._election_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._apply_loop()),
        ]

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.transport.stop()

    def _become_follower(self, term: int, leader_id: Optional[int] = None) -> None:
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.role = Role.FOLLOWER
        if leader_id is not None:
            self.leader_id = leader_id
        self._reset_election_timer()

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.node_id
        next_idx = self.last_log_index + 1
        for peer_id in self.peers:
            self.next_index[peer_id] = next_idx
            self.match_index[peer_id] = 0
        print(f"[Node {self.node_id}] Became LEADER for Term {self.current_term}")

    async def _election_loop(self) -> None:
        while self._running:
            await asyncio.sleep(0.05)
            if self.role == Role.LEADER:
                continue
            elapsed = time.monotonic() - self._last_heartbeat_time
            if elapsed >= self._election_timeout:
                await self._start_election()

    async def _start_election(self) -> None:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self._reset_election_timer()

        election_term = self.current_term
        votes_received = 1
        print(f"[Node {self.node_id}] Timeout -> Starting election for Term {election_term}")

        request = {
            "type": "RequestVote",
            "term": election_term,
            "candidate_id": self.node_id,
            "last_log_index": self.last_log_index,
            "last_log_term": self.last_log_term,
        }

        tasks = [
            asyncio.create_task(self.transport.send_rpc(peer_id, request))
            for peer_id in self.peers
        ]

        for coro in asyncio.as_completed(tasks):
            resp = await coro
            if not resp or self.role != Role.CANDIDATE or self.current_term != election_term:
                continue
            if resp["term"] > self.current_term:
                self._become_follower(resp["term"])
                return
            if resp.get("vote_granted"):
                votes_received += 1
                if votes_received >= self.quorum_size:
                    self._become_leader()
                    await self._broadcast_append_entries()
                    return

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.heartbeat_interval)
            if self.role == Role.LEADER:
                await self._broadcast_append_entries()

    async def _broadcast_append_entries(self) -> None:
        if self.role != Role.LEADER:
            return
        tasks = [
            asyncio.create_task(self._replicate_to_peer(peer_id))
            for peer_id in self.peers
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
        self._advance_commit_index()

    async def _replicate_to_peer(self, peer_id: int) -> None:
        if self.role != Role.LEADER:
            return

        next_idx = self.next_index.get(peer_id, self.last_log_index + 1)
        prev_log_index = next_idx - 1
        prev_log_term = self.log[prev_log_index]["term"]
        entries = self.log[next_idx:]

        request = {
            "type": "AppendEntries",
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries,
            "leader_commit": self.commit_index,
        }

        resp = await self.transport.send_rpc(peer_id, request)
        if not resp or self.role != Role.LEADER:
            return
        if resp["term"] > self.current_term:
            self._become_follower(resp["term"])
            return

        if resp.get("success"):
            new_match = prev_log_index + len(entries)
            self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), new_match)
            self.next_index[peer_id] = self.match_index[peer_id] + 1
        else:
            self.next_index[peer_id] = max(1, next_idx - 1)

    def _advance_commit_index(self) -> None:
        if self.role != Role.LEADER:
            return
        for n in range(self.last_log_index, self.commit_index, -1):
            if self.log[n]["term"] != self.current_term:
                continue
            replicated_count = 1
            for peer_id in self.peers:
                if self.match_index.get(peer_id, 0) >= n:
                    replicated_count += 1
            if replicated_count >= self.quorum_size:
                self.commit_index = n
                break

    async def _apply_loop(self) -> None:
        while self._running:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied]
                if self.apply_callback and entry["command"] is not None:
                    self.apply_callback(self.last_applied, entry["command"])
            await asyncio.sleep(0.02)

    async def propose(self, command: Any, timeout: float = 3.0) -> Tuple[bool, Optional[int]]:
        if self.role != Role.LEADER:
            return False, self.leader_id
        entry = {"term": self.current_term, "command": command}
        self.log.append(entry)
        target_index = self.last_log_index
        asyncio.create_task(self._broadcast_append_entries())

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.role != Role.LEADER:
                return False, self.leader_id
            if self.commit_index >= target_index:
                return True, target_index
            await asyncio.sleep(0.02)
        return False, self.leader_id

    async def _on_rpc(self, msg: dict) -> dict:
        msg_type = msg.get("type")
        if msg_type == "RequestVote":
            return self._handle_request_vote(msg)
        elif msg_type == "AppendEntries":
            return self._handle_append_entries(msg)
        return {"error": f"Unknown RPC type: {msg_type}"}

    def _handle_request_vote(self, req: dict) -> dict:
        term = req["term"]
        candidate_id = req["candidate_id"]
        last_log_index = req["last_log_index"]
        last_log_term = req["last_log_term"]

        if term > self.current_term:
            self._become_follower(term)
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        log_ok = (last_log_term > self.last_log_term) or (
            last_log_term == self.last_log_term and last_log_index >= self.last_log_index
        )
        if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
            self.voted_for = candidate_id
            self._reset_election_timer()
            return {"term": self.current_term, "vote_granted": True}
        return {"term": self.current_term, "vote_granted": False}

    def _handle_append_entries(self, req: dict) -> dict:
        term = req["term"]
        leader_id = req["leader_id"]
        prev_log_index = req["prev_log_index"]
        prev_log_term = req["prev_log_term"]
        entries = req["entries"]
        leader_commit = req["leader_commit"]

        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        self._become_follower(term, leader_id=leader_id)

