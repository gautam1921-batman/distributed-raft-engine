import asyncio
import json
import random
import struct
from typing import Callable, Awaitable, Dict, Optional, Set, Tuple


class AsyncTCPTransport:
    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        peers: Dict[int, Tuple[str, int]],
        rpc_handler: Callable[[dict], Awaitable[dict]],
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.rpc_handler = rpc_handler

        self.drop_rate: float = 0.0
        self.blocked_peers: Set[int] = set()
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_connection, self.host, self.port
        )

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            header = await reader.readexactly(4)
            (msg_len,) = struct.unpack("!I", header)
            payload = await reader.readexactly(msg_len)
            request = json.loads(payload.decode("utf-8"))

            response = await self.rpc_handler(request)

            resp_bytes = json.dumps(response).encode("utf-8")
            writer.write(struct.pack("!I", len(resp_bytes)) + resp_bytes)
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def send_rpc(
        self, target_id: int, message: dict, timeout: float = 1.0
    ) -> Optional[dict]:
        if target_id in self.blocked_peers:
            return None
        if self.drop_rate > 0.0 and random.random() < self.drop_rate:
            return None
        if target_id not in self.peers:
            return None

        target_host, target_port = self.peers[target_id]
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(target_host, target_port), timeout=timeout
            )
            try:
                req_bytes = json.dumps(message).encode("utf-8")
                writer.write(struct.pack("!I", len(req_bytes)) + req_bytes)
                await writer.drain()

                header = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
                (resp_len,) = struct.unpack("!I", header)
                resp_payload = await asyncio.wait_for(
                    reader.readexactly(resp_len), timeout=timeout
                )
                return json.loads(resp_payload.decode("utf-8"))
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
        except (asyncio.TimeoutError, OSError, asyncio.IncompleteReadError):
            return None
