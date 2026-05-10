
import json
import os
import socket
import threading
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from queue import PriorityQueue, Empty
from typing import Optional

from common.protocol import Message, MsgType, send_msg, recv_msg, _recv_exact
from common.constants import CONNECT_TIMEOUT, RECV_TIMEOUT, MAX_WORKERS, CHUNK_SIZE
from common.throttle import TokenBucket
from peer.chunk_manager import FileManifest, verify_chunk

logger = logging.getLogger(__name__)

# ── .part file format ──────────────────────────────────────────────────────────
# Sidecar JSON:  downloads/movie.mkv.part.json
# Partial data:  downloads/movie.mkv.part
# Both removed on successful completion.

def _part_path(output_path: str) -> str:
    return output_path + ".part"

def _meta_path(output_path: str) -> str:
    return output_path + ".part.json"

def _load_resume_state(output_path: str, total_chunks: int) -> tuple[set[int], dict[int, bytes]]:
    """Load previously completed chunks from disk."""
    meta = _meta_path(output_path)
    part = _part_path(output_path)
    completed: set[int] = set()
    chunks: dict[int, bytes] = {}

    if not (os.path.exists(meta) and os.path.exists(part)):
        return completed, chunks

    try:
        with open(meta) as f:
            state = json.load(f)
        done_indices = state.get("done", [])
        with open(part, "rb") as f:
            for idx in done_indices:
                f.seek(idx * CHUNK_SIZE)
                data = f.read(CHUNK_SIZE)
                if data:
                    chunks[idx] = data
                    completed.add(idx)
        logger.info(f"Resuming: {len(completed)}/{total_chunks} chunks already done")
    except (OSError, json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Could not load resume state: {e} — starting fresh")
        completed.clear()
        chunks.clear()

    return completed, chunks


def _save_resume_state(output_path: str, completed: set[int],
                       chunks: dict[int, bytes], manifest: FileManifest) -> None:
    """Persist completed chunks to .part + .part.json."""
    part = _part_path(output_path)
    meta = _meta_path(output_path)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    try:
        with open(part, "r+b" if os.path.exists(part) else "wb") as f:
            for idx in completed:
                f.seek(idx * CHUNK_SIZE)
                f.write(chunks[idx])
        with open(meta, "w") as f:
            json.dump({"done": sorted(completed),
                       "filename": manifest.filename,
                       "total_chunks": manifest.total_chunks}, f)
    except OSError as e:
        logger.warning(f"Could not save resume state: {e}")


def _cleanup_resume(output_path: str) -> None:
    for p in (_part_path(output_path), _meta_path(output_path)):
        try:
            os.remove(p)
        except OSError:
            pass


# ── Rarest-first scheduler ─────────────────────────────────────────────────────

def _build_rarity_queue(
    total_chunks: int,
    peer_chunks: list[list[int]],
    skip: set[int],
) -> PriorityQueue:
    """
    Priority queue of (rarity_score, chunk_index).
    rarity_score = number of peers that have this chunk.
    Lower score = rarer = higher priority (min-heap).
    """
    counts: dict[int, int] = defaultdict(int)
    for peer_chunk_list in peer_chunks:
        for idx in peer_chunk_list:
            counts[idx] += 1

    q: PriorityQueue = PriorityQueue()
    for i in range(total_chunks):
        if i in skip:
            continue
        q.put((counts.get(i, 0), i))
    return q


# ── Progress tracker ───────────────────────────────────────────────────────────

@dataclass
class _ProgressTracker:
    total_chunks: int
    total_bytes: int
    already_done: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _done: int = 0
    _bytes_recv: int = 0
    _start: float = field(default_factory=time.monotonic)
    _window: list = field(default_factory=list)
    peer_stats: dict = field(default_factory=lambda: defaultdict(lambda: {"chunks": 0, "bytes": 0}))

    def record(self, chunk_size: int, peer_id: str) -> None:
        now = time.monotonic()
        with self._lock:
            self._done += 1
            self._bytes_recv += chunk_size
            self._window.append((now, chunk_size))
            cutoff = now - 3.0
            self._window = [(t, b) for t, b in self._window if t >= cutoff]
            self.peer_stats[peer_id]["chunks"] += 1
            self.peer_stats[peer_id]["bytes"] += chunk_size

    def snapshot(self) -> dict:
        now = time.monotonic()
        with self._lock:
            done = self._done + self.already_done
            total = self.total_chunks
            elapsed = max(now - self._start, 0.001)
            window_bytes = sum(b for _, b in self._window)
            window_time = max((now - self._window[0][0]) if len(self._window) > 1 else elapsed, 0.1)
            speed = window_bytes / window_time
            remaining_bytes = max(self.total_bytes - self._bytes_recv, 0)
            eta = remaining_bytes / speed if speed > 0 else float("inf")
            return {
                "done": done,
                "total": total,
                "pct": done / total * 100 if total else 0,
                "speed_mbps": speed / 1024 / 1024,
                "eta_sec": eta,
                "elapsed": elapsed,
            }


# ── PeerAddr ───────────────────────────────────────────────────────────────────

@dataclass
class PeerAddr:
    host: str
    port: int
    peer_id: str = ""
    chunks: list = field(default_factory=list)

    def __str__(self):
        return f"{self.host}:{self.port}"


class DownloadError(Exception):
    pass


# ── Main entry point ───────────────────────────────────────────────────────────

def download_file(
    manifest: FileManifest,
    peers: list[PeerAddr],
    output_path: str,
    on_progress=None,
    download_limiter: Optional[TokenBucket] = None,
) -> dict:
    """
    Download all chunks with rarest-first scheduling and resume support.
    Returns a stats dict: {done, total, pct, speed_mbps, eta_sec, elapsed, peer_stats}.
    """
    total = manifest.total_chunks
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    completed, results = _load_resume_state(output_path, total)

    peer_chunk_lists = [p.chunks for p in peers]
    work_q = _build_rarity_queue(total, peer_chunk_lists, skip=completed)

    results_lock = threading.Lock()
    progress = _ProgressTracker(
        total_chunks=total,
        total_bytes=manifest.total_size,
        already_done=len(completed),
    )

    def worker(peer: PeerAddr):
        conn = _connect(peer)
        if conn is None:
            logger.warning(f"Could not connect to {peer}")
            return

        try:
            while True:
                try:
                    _, chunk_idx = work_q.get_nowait()
                except Empty:
                    break

                with results_lock:
                    if chunk_idx in completed:
                        work_q.task_done()
                        continue

                data = _fetch_chunk(
                    conn, manifest.filename, chunk_idx,
                    manifest.chunks[chunk_idx].sha256,
                    limiter=download_limiter,
                )

                if data is None:
                    work_q.put((0, chunk_idx))
                    logger.warning(f"Peer {peer} failed chunk {chunk_idx}, re-queuing")
                    break

                with results_lock:
                    results[chunk_idx] = data
                    completed.add(chunk_idx)

                progress.record(len(data), str(peer))

                if len(completed) % 10 == 0:
                    _save_resume_state(output_path, completed, results, manifest)

                if on_progress:
                    on_progress(progress.snapshot())

                work_q.task_done()
        finally:
            try:
                conn.close()
            except OSError:
                pass

    n_workers = min(MAX_WORKERS, len(peers))
    threads = []
    for i in range(n_workers):
        peer = peers[i % len(peers)]
        t = threading.Thread(target=worker, args=(peer,), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    missing = [i for i in range(total) if i not in completed]
    if missing:
        _save_resume_state(output_path, completed, results, manifest)
        raise DownloadError(f"Incomplete — {len(missing)} chunks missing. Re-run to resume.")

    with open(output_path, "wb") as f:
        for i in range(total):
            f.write(results[i])

    _cleanup_resume(output_path)
    logger.info(f"File assembled at {output_path}")

    snap = progress.snapshot()
    return {**snap, "peer_stats": dict(progress.peer_stats), "output_path": output_path}


# ── Socket helpers ─────────────────────────────────────────────────────────────

def _connect(peer: PeerAddr) -> Optional[socket.socket]:
    try:
        conn = socket.create_connection((peer.host, peer.port), timeout=CONNECT_TIMEOUT)
        conn.settimeout(RECV_TIMEOUT)
        return conn
    except (OSError, TimeoutError) as e:
        logger.debug(f"Connect to {peer} failed: {e}")
        return None


def _fetch_chunk(
    conn: socket.socket,
    filename: str,
    index: int,
    expected_sha256: str,
    limiter: Optional[TokenBucket] = None,
) -> Optional[bytes]:
    try:
        send_msg(conn, Message(
            type=MsgType.REQUEST_CHUNK,
            payload={"filename": filename, "chunk_index": index},
        ))
        msg = recv_msg(conn)
        if msg is None or msg.type != MsgType.CHUNK_RESPONSE:
            return None
        size = msg.payload["size"]
        if limiter:
            limiter.consume(size)
        data = _recv_exact(conn, size)
        if data is None:
            return None
        if not verify_chunk(data, expected_sha256):
            logger.error(f"Chunk {index} integrity check failed!")
            return None
        return data
    except (OSError, KeyError, TimeoutError) as e:
        logger.debug(f"Fetch chunk {index} error: {e}")
        return None