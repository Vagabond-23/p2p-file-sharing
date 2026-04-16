"""
Handles file splitting into chunks, SHA-256 integrity verification, and reassembly.
"""

import hashlib
import os
import math
from dataclasses import dataclass, field
from typing import Optional
from common.constants import CHUNK_SIZE


@dataclass
class ChunkInfo:
    index: int
    size: int
    sha256: str


@dataclass
class FileManifest:
    filename: str
    total_size: int
    chunk_size: int
    chunks: list[ChunkInfo]

    @property
    def total_chunks(self) -> int:
        return len(self.chunks)

    def to_dict(self) -> dict:
        return {
            "filename": self.filename,
            "total_size": self.total_size,
            "chunk_size": self.chunk_size,
            "chunks": [{"index": c.index, "size": c.size, "sha256": c.sha256}
                       for c in self.chunks],
        }

    @staticmethod
    def from_dict(d: dict) -> "FileManifest":
        return FileManifest(
            filename=d["filename"],
            total_size=d["total_size"],
            chunk_size=d["chunk_size"],
            chunks=[ChunkInfo(**c) for c in d["chunks"]],
        )


def build_manifest(filepath: str) -> FileManifest:
    """Build a FileManifest by reading and hashing each chunk of a file."""
    filename = os.path.basename(filepath)
    total_size = os.path.getsize(filepath)
    n_chunks = math.ceil(total_size / CHUNK_SIZE)
    chunks = []

    with open(filepath, "rb") as f:
        for i in range(n_chunks):
            data = f.read(CHUNK_SIZE)
            sha256 = hashlib.sha256(data).hexdigest()
            chunks.append(ChunkInfo(index=i, size=len(data), sha256=sha256))

    return FileManifest(
        filename=filename,
        total_size=total_size,
        chunk_size=CHUNK_SIZE,
        chunks=chunks,
    )


def read_chunk(filepath: str, index: int) -> bytes:
    """Read a specific chunk from a file by index."""
    with open(filepath, "rb") as f:
        f.seek(index * CHUNK_SIZE)
        return f.read(CHUNK_SIZE)


def verify_chunk(data: bytes, expected_sha256: str) -> bool:
    """Verify chunk integrity."""
    return hashlib.sha256(data).hexdigest() == expected_sha256


def assemble_file(output_path: str, chunks: dict[int, bytes], manifest: FileManifest) -> None:
    """
    Write chunks to disk in order. `chunks` is {index: data}.
    Raises ValueError if any chunk fails integrity check.
    """
    for info in manifest.chunks:
        data = chunks.get(info.index)
        if data is None:
            raise ValueError(f"Missing chunk {info.index}")
        if not verify_chunk(data, info.sha256):
            raise ValueError(f"Integrity check failed for chunk {info.index}")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "wb") as f:
        for i in range(manifest.total_chunks):
            f.write(chunks[i])