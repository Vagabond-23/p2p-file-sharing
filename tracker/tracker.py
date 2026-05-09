#!/usr/bin/env python3
"""
Tracker server — run exactly one instance.

Usage:
  python -m tracker.tracker
  python -m tracker.tracker --host 0.0.0.0 --port 5000

The tracker maintains:
  - peer registry: peer_id -> {host, port}
  - file index:    filename -> {manifest, peer_id -> set of chunk indices}
"""

import argparse
import logging
import socket
import sys
import os
import threading
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol import Message, MsgType, send_msg, recv_msg
from common.constants import TRACKER_HOST, TRACKER_PORT, RECV_TIMEOUT
from peer.chunk_manager import FileManifest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("tracker")


class TrackerState:
    """All mutable tracker state — protected by a single lock for simplicity."""

    def __init__(self):
        self._lock = threading.Lock()
        # peer_id -> {"host": str, "port": int}
        self._peers: dict[str, dict] = {}
        # filename -> FileManifest
        self._manifests: dict[str, FileManifest] = {}
        # filename -> {peer_id -> set[int]}  (which chunks each peer has)
        self._file_peers: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))

    def register(self, peer_id: str, host: str, port: int) -> None:
        with self._lock:
            self._peers[peer_id] = {"host": host, "port": port}
            logger.info(f"Registered: {peer_id} @ {host}:{port}")

    def unregister(self, peer_id: str) -> None:
        with self._lock:
            self._peers.pop(peer_id, None)
            for fname in self._file_peers:
                self._file_peers[fname].pop(peer_id, None)
            logger.info(f"Unregistered: {peer_id}")

    def announce_chunks(self, peer_id: str, filename: str,
                        chunks: list[int], manifest_dict: Optional[dict]) -> None:
        with self._lock:
            if manifest_dict and filename not in self._manifests:
                self._manifests[filename] = FileManifest.from_dict(manifest_dict)
            self._file_peers[filename][peer_id].update(chunks)
            logger.info(f"Announce: {peer_id} has {len(chunks)} chunk(s) of '{filename}'")

    def get_peers_for_file(self, filename: str) -> dict:
        with self._lock:
            manifest = self._manifests.get(filename)
            if manifest is None:
                return {"manifest": None, "peers": []}

            peers = []
            for peer_id, chunks in self._file_peers[filename].items():
                info = self._peers.get(peer_id)
                if info:
                    peers.append({
                        "peer_id": peer_id,
                        "host": info["host"],
                        "port": info["port"],
                        "chunks": sorted(chunks),
                    })
            return {"manifest": manifest.to_dict(), "peers": peers}

    def list_files(self) -> list[dict]:
        with self._lock:
            result = []
            for fname, manifest in self._manifests.items():
                peer_count = sum(
                    1 for p, chunks in self._file_peers[fname].items()
                    if chunks and p in self._peers
                )
                result.append({
                    "filename": fname,
                    "total_size": manifest.total_size,
                    "total_chunks": manifest.total_chunks,
                    "peer_count": peer_count,
                })
            return result


class TrackerServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.state = TrackerState()

    def serve_forever(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(64)
        logger.info(f"Tracker listening on {self.host}:{self.port} (TCP)")

        # Start UDP broadcast listener for auto-discovery
        udp_thread = threading.Thread(target=self._handle_udp_broadcast, daemon=True)
        udp_thread.start()

        try:
            while True:
                conn, addr = server.accept()
                conn.settimeout(None)  # blocking — large manifests need no timeout
                t = threading.Thread(
                    target=self._handle_peer,
                    args=(conn, addr),
                    daemon=True,
                )
                t.start()
        except KeyboardInterrupt:
            logger.info("Tracker shutting down.")
        finally:
            server.close()

    def _handle_udp_broadcast(self) -> None:
        """Listens for UDP broadcasts on port 5001 to support peer auto-discovery."""
        udp_port = 5001
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", udp_port))
            logger.info(f"Tracker auto-discovery listening on UDP port {udp_port}")
            
            while True:
                data, addr = sock.recvfrom(1024)
                if data == b"TRACKER_DISCOVERY":
                    # Reply with our TCP port
                    reply = f"TRACKER_HERE:{self.port}".encode('utf-8')
                    sock.sendto(reply, addr)
        except Exception as e:
            logger.error(f"UDP listener error: {e}")

    def _handle_peer(self, conn: socket.socket, addr) -> None:
        peer_id: Optional[str] = None
        logger.debug(f"New connection from {addr}")

        try:
            while True:
                msg = recv_msg(conn)
                if msg is None:
                    break

                if msg.type == MsgType.REGISTER:
                    peer_id = msg.payload["peer_id"]
                    self.state.register(
                        peer_id=peer_id,
                        host=msg.payload["host"],
                        port=msg.payload["port"],
                    )
                    send_msg(conn, Message(type=MsgType.TRACKER_OK))

                elif msg.type == MsgType.UNREGISTER:
                    pid = msg.payload.get("peer_id", peer_id)
                    if pid:
                        self.state.unregister(pid)
                    break

                elif msg.type == MsgType.ANNOUNCE_CHUNK:
                    self.state.announce_chunks(
                        peer_id=msg.payload["peer_id"],
                        filename=msg.payload["filename"],
                        chunks=msg.payload["chunks"],
                        manifest_dict=msg.payload.get("manifest"),
                    )
                    send_msg(conn, Message(type=MsgType.TRACKER_OK))

                elif msg.type == MsgType.GET_PEERS:
                    filename = msg.payload.get("filename")
                    if filename == "__list__":
                        send_msg(conn, Message(
                            type=MsgType.PEERS_RESPONSE,
                            payload={"files": self.state.list_files()},
                        ))
                    else:
                        result = self.state.get_peers_for_file(filename)
                        send_msg(conn, Message(type=MsgType.PEERS_RESPONSE, payload=result))

                else:
                    logger.warning(f"Unknown message type: {msg.type}")
                    send_msg(conn, Message(
                        type=MsgType.TRACKER_ERR,
                        payload={"reason": f"unknown message type: {msg.type}"}
                    ))
        except (OSError, KeyError, TimeoutError) as e:
            logger.debug(f"Peer {addr} error: {e}")
        finally:
            if peer_id:
                self.state.unregister(peer_id)
            conn.close()


def main():
    parser = argparse.ArgumentParser(description="P2P File Sharing — Tracker")
    parser.add_argument("--host", default="0.0.0.0",
                        help="Interface to bind on. 0.0.0.0 = accept from all machines.")
    parser.add_argument("--port", type=int, default=TRACKER_PORT)
    args = parser.parse_args()

    TrackerServer(host=args.host, port=args.port).serve_forever()


if __name__ == "__main__":
    main()