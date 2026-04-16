"""
Uploader: TCP server that serves chunks to other peers.
Supports upload bandwidth throttling via a shared TokenBucket.
"""

import socket
import threading
import logging
from common.protocol import Message, MsgType, send_msg, recv_msg
from common.constants import RECV_TIMEOUT
from common.throttle import TokenBucket
from peer.chunk_manager import read_chunk, FileManifest

logger = logging.getLogger(__name__)

# Send in 64 KB sub-chunks so the token bucket stays responsive
# even when serving large 512 KB chunks.
_SEND_BLOCK = 64 * 1024


class UploaderServer:
    """
    Runs in its own thread. Accepts peer connections and serves chunks.
    shared_files: dict[filename -> (filepath, FileManifest)]
    upload_limiter: shared TokenBucket (None = unlimited)
    """

    def __init__(self, host: str, port: int, shared_files: dict,
                 upload_limiter: TokenBucket | None = None):
        self.host = host
        self.port = port
        self.shared_files = shared_files
        self.upload_limiter = upload_limiter or TokenBucket(0)
        self._server_sock: socket.socket | None = None
        self._stop_event = threading.Event()

    def set_upload_limit(self, rate: float) -> None:
        """Change upload rate at runtime. rate=0 means unlimited."""
        self.upload_limiter.set_rate(rate)
        label = f"{rate/1024/1024:.1f} MB/s" if rate > 0 else "unlimited"
        logger.info(f"Upload limit set to {label}")

    def start(self) -> threading.Thread:
        t = threading.Thread(target=self._serve, daemon=True, name="UploaderServer")
        t.start()
        return t

    def stop(self) -> None:
        self._stop_event.set()
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass

    def _serve(self) -> None:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(32)
        logger.info(f"Uploader listening on {self.host}:{self.port}")

        while not self._stop_event.is_set():
            try:
                conn, addr = self._server_sock.accept()
                conn.settimeout(RECV_TIMEOUT)
                threading.Thread(
                    target=self._handle_peer,
                    args=(conn, addr),
                    daemon=True,
                ).start()
            except OSError:
                break

    def _handle_peer(self, conn: socket.socket, addr) -> None:
        logger.debug(f"Uploader: connection from {addr}")
        try:
            while True:
                msg = recv_msg(conn)
                if msg is None:
                    break
                if msg.type == MsgType.REQUEST_CHUNK:
                    self._serve_chunk(conn, msg.payload)
                else:
                    logger.warning(f"Uploader: unexpected msg type {msg.type}")
                    break
        except (OSError, TimeoutError) as e:
            logger.debug(f"Uploader: peer {addr} disconnected: {e}")
        finally:
            conn.close()

    def _serve_chunk(self, conn: socket.socket, payload: dict) -> None:
        filename = payload.get("filename")
        index = payload.get("chunk_index")

        entry = self.shared_files.get(filename)
        if entry is None:
            send_msg(conn, Message(type=MsgType.CHUNK_NOT_FOUND,
                                   payload={"reason": "file not found"}))
            return

        filepath, manifest = entry
        if index < 0 or index >= manifest.total_chunks:
            send_msg(conn, Message(type=MsgType.CHUNK_NOT_FOUND,
                                   payload={"reason": "invalid chunk index"}))
            return

        try:
            data = read_chunk(filepath, index)
        except OSError as e:
            send_msg(conn, Message(type=MsgType.CHUNK_NOT_FOUND,
                                   payload={"reason": str(e)}))
            return

        # Send metadata header first
        send_msg(conn, Message(
            type=MsgType.CHUNK_RESPONSE,
            payload={
                "filename": filename,
                "chunk_index": index,
                "size": len(data),
                "sha256": manifest.chunks[index].sha256,
            }
        ))

        # Send data in blocks, throttling each block
        offset = 0
        while offset < len(data):
            block = data[offset: offset + _SEND_BLOCK]
            self.upload_limiter.consume(len(block))
            conn.sendall(block)
            offset += len(block)

        logger.debug(f"Uploader: served chunk {index} of '{filename}' ({len(data):,} bytes)")