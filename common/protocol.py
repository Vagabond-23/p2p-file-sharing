"""
Defines the P2P protocol: message types, serialization, and framing.
All messages are length-prefixed JSON over TCP.
"""

import json
import struct
from dataclasses import dataclass, asdict, field
from typing import Optional, List
from enum import Enum


CHUNK_SIZE = 512 * 1024  # 512 KB
HEADER_SIZE = 4           # 4-byte big-endian uint32 for message length


class MsgType(str, Enum):
    # Tracker messages
    REGISTER        = "REGISTER"
    UNREGISTER      = "UNREGISTER"
    GET_PEERS       = "GET_PEERS"
    PEERS_RESPONSE  = "PEERS_RESPONSE"
    ANNOUNCE_CHUNK  = "ANNOUNCE_CHUNK"
    TRACKER_OK      = "TRACKER_OK"
    TRACKER_ERR     = "TRACKER_ERR"

    # Peer messages
    REQUEST_CHUNK   = "REQUEST_CHUNK"
    CHUNK_RESPONSE  = "CHUNK_RESPONSE"
    CHUNK_NOT_FOUND = "CHUNK_NOT_FOUND"
    HANDSHAKE       = "HANDSHAKE"
    HANDSHAKE_ACK   = "HANDSHAKE_ACK"


@dataclass
class Message:
    type: str
    payload: dict = field(default_factory=dict)

    def encode(self) -> bytes:
        body = json.dumps({"type": self.type, "payload": self.payload}).encode()
        header = struct.pack(">I", len(body))
        return header + body

    @staticmethod
    def decode(data: bytes) -> "Message":
        obj = json.loads(data.decode())
        return Message(type=obj["type"], payload=obj.get("payload", {}))


def send_msg(sock, msg: Message) -> None:
    """Send a framed message over a socket."""
    sock.sendall(msg.encode())


def recv_msg(sock) -> Optional[Message]:
    """Receive a framed message from a socket. Returns None on disconnect."""
    header = _recv_exact(sock, HEADER_SIZE)
    if header is None:
        return None
    length = struct.unpack(">I", header)[0]
    body = _recv_exact(sock, length)
    if body is None:
        return None
    return Message.decode(body)


def _recv_exact(sock, n: int) -> Optional[bytes]:
    """Read exactly n bytes from socket, or return None on EOF."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf