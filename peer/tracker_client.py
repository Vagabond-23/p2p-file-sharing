"""
TrackerClient: manages the persistent connection from a peer to the tracker.
"""

import socket
import logging
import threading
from typing import Optional

from common.protocol import Message, MsgType, send_msg, recv_msg
from common.constants import TRACKER_HOST, TRACKER_PORT, CONNECT_TIMEOUT, RECV_TIMEOUT
from peer.chunk_manager import FileManifest

logger = logging.getLogger(__name__)


class TrackerClient:
    def __init__(self, peer_id: str, peer_host: str, peer_port: int,
                 tracker_host: str = TRACKER_HOST, tracker_port: int = TRACKER_PORT):
        self.peer_id = peer_id
        self.peer_host = peer_host
        self.peer_port = peer_port
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()

    def connect(self) -> None:
        self._sock = socket.create_connection(
            (self.tracker_host, self.tracker_port), timeout=CONNECT_TIMEOUT
        )
        self._sock.settimeout(None)  # blocking — no timeout on tracker ops
        logger.info(f"Connected to tracker at {self.tracker_host}:{self.tracker_port}")

    def disconnect(self) -> None:
        if self._sock:
            try:
                send_msg(self._sock, Message(
                    type=MsgType.UNREGISTER,
                    payload={"peer_id": self.peer_id}
                ))
            except OSError:
                pass
            try:
                self._sock.close()
            except OSError:
                pass

    def register(self) -> None:
        """Tell the tracker we exist and what port we're listening on."""
        with self._lock:
            send_msg(self._sock, Message(
                type=MsgType.REGISTER,
                payload={
                    "peer_id": self.peer_id,
                    "host": self.peer_host,
                    "port": self.peer_port,
                }
            ))
            resp = recv_msg(self._sock)
            if resp is None or resp.type != MsgType.TRACKER_OK:
                raise ConnectionError(f"Registration failed: {resp}")
            logger.info(f"Registered with tracker as '{self.peer_id}'")

    def announce_file(self, manifest: FileManifest) -> None:
        """Tell the tracker we have all chunks of a file."""
        with self._lock:
            send_msg(self._sock, Message(
                type=MsgType.ANNOUNCE_CHUNK,
                payload={
                    "peer_id": self.peer_id,
                    "filename": manifest.filename,
                    "chunks": list(range(manifest.total_chunks)),
                    "manifest": manifest.to_dict(),
                }
            ))
            resp = recv_msg(self._sock)
            if resp is None or resp.type != MsgType.TRACKER_OK:
                raise ConnectionError(f"Announce failed: {resp}")
            logger.info(f"Announced '{manifest.filename}' ({manifest.total_chunks} chunks)")

    def announce_chunk(self, filename: str, chunk_index: int) -> None:
        """Tell the tracker we now have one specific chunk (during download)."""
        with self._lock:
            send_msg(self._sock, Message(
                type=MsgType.ANNOUNCE_CHUNK,
                payload={
                    "peer_id": self.peer_id,
                    "filename": filename,
                    "chunks": [chunk_index],
                    "manifest": None,
                }
            ))
            recv_msg(self._sock)  # consume ACK, don't block on failure

    def get_peers(self, filename: str) -> dict:
        """
        Ask the tracker for peers that have chunks of `filename`.
        Returns {"manifest": {...}, "peers": [{"peer_id", "host", "port", "chunks": [...]}]}
        """
        with self._lock:
            send_msg(self._sock, Message(
                type=MsgType.GET_PEERS,
                payload={"filename": filename}
            ))
            resp = recv_msg(self._sock)
            if resp is None or resp.type != MsgType.PEERS_RESPONSE:
                raise ConnectionError(f"GET_PEERS failed: {resp}")
            return resp.payload