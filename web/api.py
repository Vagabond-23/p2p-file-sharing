"""
FastAPI backend for the P2P File Sharing Web UI.

Wraps the existing Peer class with REST endpoints and a WebSocket
for live download progress. No changes to core P2P logic.
"""

import asyncio
import json
import logging
import os
import sys
import threading
import time
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peer.chunk_manager import build_manifest, FileManifest
from peer.downloader import download_file, PeerAddr
from common.throttle import TokenBucket

logger = logging.getLogger("web.api")

# ── Static file path ──────────────────────────────────────────────────────────

_STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

# ── App & state ───────────────────────────────────────────────────────────────

app = FastAPI(title="P2P File Sharing", docs_url=None, redoc_url=None)

# Peer instance — injected by run.py before server starts
_peer = None

# Event loop reference for broadcasting from sync threads
_loop: Optional[asyncio.AbstractEventLoop] = None

# WebSocket clients
_ws_clients: set[WebSocket] = set()
_ws_lock = threading.Lock()

# Download tracking
_downloads_lock = threading.Lock()
_active_downloads: dict[str, dict] = {}
_completed_downloads: list[dict] = []


def set_peer(peer) -> None:
    """Called by run.py to inject the Peer instance."""
    global _peer
    _peer = peer


# ── WebSocket broadcasting ────────────────────────────────────────────────────

async def _broadcast(event: dict) -> None:
    """Send an event to all connected WebSocket clients."""
    dead = set()
    with _ws_lock:
        clients = set(_ws_clients)
    for ws in clients:
        try:
            await ws.send_json(event)
        except Exception:
            dead.add(ws)
    if dead:
        with _ws_lock:
            _ws_clients.difference_update(dead)


def _broadcast_sync(event: dict) -> None:
    """Broadcast from a synchronous context (e.g. download thread)."""
    loop = _loop
    if loop and loop.is_running():
        asyncio.run_coroutine_threadsafe(_broadcast(event), loop)


# ── Lifecycle ─────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def _startup():
    global _loop
    _loop = asyncio.get_event_loop()


# ── WebSocket endpoint ────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    with _ws_lock:
        _ws_clients.add(ws)
    logger.info(f"WebSocket client connected ({len(_ws_clients)} total)")
    try:
        while True:
            await ws.receive_text()  # keep-alive; we only push
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        with _ws_lock:
            _ws_clients.discard(ws)
        logger.info(f"WebSocket client disconnected ({len(_ws_clients)} total)")


# ── Pydantic models ──────────────────────────────────────────────────────────

class ThrottleRequest(BaseModel):
    direction: str   # "up" or "down"
    limit: str       # e.g. "50MB", "0"


class SharePathRequest(BaseModel):
    path: str


# ── REST API ──────────────────────────────────────────────────────────────────

@app.get("/api/status")
def get_status():
    """Peer info, shared files, throttle settings, active downloads."""
    files = []
    for fname, (path, manifest) in _peer.shared_files.items():
        files.append({
            "filename": fname,
            "path": path,
            "total_size": manifest.total_size,
            "total_chunks": manifest.total_chunks,
        })

    up_rate = _peer.upload_limiter.rate
    down_rate = _peer.download_limiter.rate

    with _downloads_lock:
        active = list(_active_downloads.values())

    return {
        "peer_id": _peer.peer_id,
        "host": _peer.host,
        "port": _peer.port,
        "tracker_host": _peer.tracker.tracker_host,
        "tracker_port": _peer.tracker.tracker_port,
        "upload_limit": up_rate,
        "download_limit": down_rate,
        "upload_limit_label": f"{up_rate/1024/1024:.1f} MB/s" if up_rate > 0 else "Unlimited",
        "download_limit_label": f"{down_rate/1024/1024:.1f} MB/s" if down_rate > 0 else "Unlimited",
        "shared_files": files,
        "active_downloads": active,
    }


@app.get("/api/files")
def get_files():
    """List all files in the swarm (queries tracker)."""
    try:
        result = _peer.tracker.get_peers("__list__")
        files = result.get("files", [])
        # Tag files this peer is sharing
        for f in files:
            f["is_mine"] = f["filename"] in _peer.shared_files
        # Tag files being downloaded
        with _downloads_lock:
            for f in files:
                f["is_downloading"] = f["filename"] in _active_downloads
        return {"files": files}
    except Exception as e:
        logger.error(f"Failed to list files: {e}")
        return {"files": [], "error": str(e)}


@app.post("/api/share")
async def share_file(file: UploadFile = File(...)):
    """Upload a file and share it with the swarm."""
    if not file.filename:
        raise HTTPException(400, "No filename provided")

    filepath = os.path.join(_peer.shared_dir, file.filename)

    # Stream-write to disk (handles large files)
    try:
        with open(filepath, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)  # 1 MB at a time
                if not chunk:
                    break
                f.write(chunk)
    except OSError as e:
        raise HTTPException(500, f"Failed to save file: {e}")

    # Hash and announce (CPU-bound, run in thread pool)
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, _process_share, filepath)
    except Exception as e:
        raise HTTPException(500, f"Failed to share file: {e}")

    _broadcast_sync({
        "type": "file_shared",
        "filename": result["filename"],
        "total_size": result["total_size"],
        "total_chunks": result["total_chunks"],
    })

    return {"status": "ok", **result}


@app.post("/api/share-path")
async def share_path(body: SharePathRequest):
    """Share a file already on this machine by path."""
    filepath = body.path.strip()
    if not filepath or not os.path.isfile(filepath):
        raise HTTPException(400, f"File not found: {filepath}")

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, _process_share, filepath)
    except Exception as e:
        raise HTTPException(500, f"Failed to share file: {e}")

    _broadcast_sync({
        "type": "file_shared",
        "filename": result["filename"],
        "total_size": result["total_size"],
        "total_chunks": result["total_chunks"],
    })

    return {"status": "ok", **result}


def _process_share(filepath: str) -> dict:
    """Hash file and announce to tracker. Runs in thread pool."""
    manifest = build_manifest(filepath)
    _peer.shared_files[manifest.filename] = (filepath, manifest)
    _peer.tracker.announce_file(manifest)
    logger.info(f"Shared '{manifest.filename}' ({manifest.total_chunks} chunks, "
                f"{manifest.total_size:,} bytes)")
    return {
        "filename": manifest.filename,
        "total_size": manifest.total_size,
        "total_chunks": manifest.total_chunks,
    }


@app.post("/api/download/{filename:path}")
def start_download(filename: str):
    """Start downloading a file in the background."""
    with _downloads_lock:
        if filename in _active_downloads:
            raise HTTPException(409, f"'{filename}' is already being downloaded")

        _active_downloads[filename] = {
            "filename": filename,
            "status": "queued",
            "total_chunks": 0,
            "total_size": 0,
            "done_chunks": 0,
            "pct": 0.0,
            "speed_mbps": 0.0,
            "eta_sec": 0.0,
            "error": None,
            "peer_stats": {},
        }

    thread = threading.Thread(target=_do_download, args=(filename,), daemon=True)
    thread.start()
    return {"status": "started", "filename": filename}


@app.get("/api/downloads")
def get_downloads():
    """Get active and recently completed downloads."""
    with _downloads_lock:
        return {
            "active": dict(_active_downloads),
            "completed": _completed_downloads[-20:],
        }


@app.put("/api/throttle")
def set_throttle(body: ThrottleRequest):
    """Update bandwidth throttle settings."""
    try:
        rate = TokenBucket.parse_limit(body.limit)
    except ValueError:
        raise HTTPException(400, f"Invalid limit: '{body.limit}'. Examples: 50MB, 500KB, 0")

    label = f"{rate/1024/1024:.1f} MB/s" if rate > 0 else "Unlimited"

    if body.direction == "up":
        _peer.upload_limiter.set_rate(rate)
        _peer.uploader.upload_limiter = _peer.upload_limiter
    elif body.direction == "down":
        _peer.download_limiter.set_rate(rate)
    else:
        raise HTTPException(400, "direction must be 'up' or 'down'")

    logger.info(f"Throttle {body.direction} → {label}")
    return {"status": "ok", "direction": body.direction, "limit": label, "rate": rate}


# ── Download worker ───────────────────────────────────────────────────────────

def _do_download(filename: str) -> None:
    """Run a download in a background thread."""
    try:
        # 1. Query tracker
        result = _peer.tracker.get_peers(filename)
        if not result.get("peers"):
            _fail_download(filename, "No peers have this file")
            return

        manifest = FileManifest.from_dict(result["manifest"])
        peers = [
            PeerAddr(
                host=p["host"],
                port=p["port"],
                peer_id=p["peer_id"],
                chunks=p.get("chunks", list(range(manifest.total_chunks))),
            )
            for p in result["peers"]
            if p["peer_id"] != _peer.peer_id
        ]

        if not peers:
            _fail_download(filename, "You are the only seeder of this file")
            return

        # 2. Update tracking state
        with _downloads_lock:
            if filename in _active_downloads:
                _active_downloads[filename].update({
                    "status": "downloading",
                    "total_chunks": manifest.total_chunks,
                    "total_size": manifest.total_size,
                })

        output_path = os.path.join(_peer.download_dir, filename)

        # 3. Progress callback → WebSocket broadcast
        last_broadcast = [0.0]

        def on_progress(snap: dict) -> None:
            now = time.monotonic()
            if now - last_broadcast[0] < 0.2:  # 5 Hz max
                return
            last_broadcast[0] = now

            with _downloads_lock:
                if filename in _active_downloads:
                    _active_downloads[filename].update({
                        "done_chunks": snap["done"],
                        "pct": snap["pct"],
                        "speed_mbps": snap["speed_mbps"],
                        "eta_sec": snap["eta_sec"],
                    })

            _broadcast_sync({
                "type": "download_progress",
                "filename": filename,
                "done": snap["done"],
                "total": snap["total"],
                "pct": round(snap["pct"], 1),
                "speed_mbps": round(snap["speed_mbps"], 2),
                "eta_sec": round(snap["eta_sec"], 1),
            })

        # 4. Run the download
        limiter = _peer.download_limiter if _peer.download_limiter.rate > 0 else None
        stats = download_file(
            manifest, peers, output_path,
            on_progress=on_progress,
            download_limiter=limiter,
        )

        # 5. Success — register as seeder
        _peer.shared_files[filename] = (output_path, manifest)
        _peer.tracker.announce_file(manifest)

        elapsed = stats.get("elapsed", 0)
        speed = stats.get("speed_mbps", 0)
        peer_stats = stats.get("peer_stats", {})

        with _downloads_lock:
            dl = _active_downloads.pop(filename, {})
            dl.update({
                "status": "completed",
                "pct": 100.0,
                "done_chunks": manifest.total_chunks,
                "speed_mbps": speed,
                "peer_stats": peer_stats,
                "elapsed": elapsed,
            })
            _completed_downloads.append(dl)

        _broadcast_sync({
            "type": "download_complete",
            "filename": filename,
            "elapsed": round(elapsed, 1),
            "speed_mbps": round(speed, 2),
            "total_size": manifest.total_size,
            "peer_stats": peer_stats,
        })

        logger.info(f"Download complete: {filename}")

    except Exception as e:
        _fail_download(filename, str(e))
        logger.error(f"Download failed: {filename} — {e}", exc_info=True)


def _fail_download(filename: str, error: str) -> None:
    """Mark a download as failed and notify clients."""
    with _downloads_lock:
        if filename in _active_downloads:
            _active_downloads[filename].update({"status": "failed", "error": error})
        else:
            _active_downloads[filename] = {
                "filename": filename, "status": "failed", "error": error,
                "total_chunks": 0, "total_size": 0, "done_chunks": 0,
                "pct": 0, "speed_mbps": 0, "eta_sec": 0, "peer_stats": {},
            }

    _broadcast_sync({
        "type": "download_error",
        "filename": filename,
        "error": error,
    })


# ── Dismiss failed/completed downloads ────────────────────────────────────────

@app.delete("/api/downloads/{filename:path}")
def dismiss_download(filename: str):
    """Remove a failed download from the active list."""
    with _downloads_lock:
        dl = _active_downloads.get(filename)
        if dl and dl.get("status") in ("failed", "completed"):
            _active_downloads.pop(filename, None)
            return {"status": "ok"}
        elif dl:
            raise HTTPException(409, "Cannot dismiss an in-progress download")
        else:
            raise HTTPException(404, "Download not found")


# ── Static files & SPA fallback ───────────────────────────────────────────────

app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/")
def serve_index():
    return FileResponse(os.path.join(_STATIC_DIR, "index.html"))
