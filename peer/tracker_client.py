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
        if self.tracker_host == "auto":
            self._auto_discover_tracker()

        self._sock = socket.create_connection(
            (self.tracker_host, self.tracker_port), timeout=CONNECT_TIMEOUT
        )
        self._sock.settimeout(None)  # blocking — no timeout on tracker ops
        logger.info(f"Connected to tracker at {self.tracker_host}:{self.tracker_port}")

    def _auto_discover_tracker(self) -> None:
        """Finds the tracker via UDP broadcast on the local network."""
        logger.info("Looking for tracker on local network (UDP broadcast)...")
        udp_port = 5001
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(2.0)

        for attempt in range(3):
            try:
                # Send broadcast
                sock.sendto(b"TRACKER_DISCOVERY", ("<broadcast>", udp_port))
                
                # Wait for reply
                data, addr = sock.recvfrom(1024)
                if data.startswith(b"TRACKER_HERE:"):
                    self.tracker_host = addr[0]
                    self.tracker_port = int(data.split(b":")[1])
                    logger.info(f"Auto-discovery successful. Found tracker at {self.tracker_host}:{self.tracker_port}")
                    sock.close()
                    return
            except TimeoutError:
                logger.debug(f"Auto-discovery attempt {attempt + 1} timed out...")
            except OSError as e:
                logger.debug(f"Auto-discovery broadcast error: {e}")
                
        sock.close()
        raise ConnectionError("Auto-discovery failed: No tracker found on LAN after 3 attempts.")

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