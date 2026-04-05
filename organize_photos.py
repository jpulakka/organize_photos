#!/usr/bin/env python3
"""
organize_photos.py — Sort photos into YYYY/ (or YYYY-MM/ with
                     --by-month) directories by date, with duplicate
                     detection, parallel hashing, and a persistent hash
                     cache so repeat runs are near-instant.

Date resolution priority:
  1. EXIF DateTimeOriginal  (most accurate — when the shutter fired)
  2. EXIF DateTimeDigitized (usually same as above, fallback)
  3. EXIF DateTime          (file write time, less reliable)
  4. Filename patterns      (IMG_20231014_..., 2023-10-14_..., 20231014_..., etc.)
  5. File modification time (last resort)

Duplicate detection (two-stage):
  Stage 1 — exact:       SHA-256 of full content. Identical bytes → duplicate.
  Stage 2 — perceptual:  pHash via imagehash (images only). Hamming distance
                         <= threshold catches re-saves, crops, slight edits.
                         Uses a BK-tree for O(n log n) matching.

Performance:
  - Resolved dates, SHA-256, and pHash all cached in SQLite keyed on
    (path, size, mtime_ns); repeat runs skip EXIF/hash I/O entirely.
  - Near-duplicate query results cached too — the slow BK-tree step
    is skipped when the file set hasn't changed.
  - SHA-256 computed in parallel via ThreadPoolExecutor  (I/O-bound)
  - pHash   computed in parallel via ProcessPoolExecutor (CPU-bound)

Cache location: ~/.cache/organize_photos.db  (override with --cache)

Usage:
  # Dry run — shows what WOULD happen, touches nothing:
  python organize_photos.py --src /path/to/messy --dst /path/to/sorted

  # Copy (recommended first run — originals untouched):
  python organize_photos.py --src /path/to/messy --dst /path/to/sorted --copy

  # Move:
  python organize_photos.py --src /path/to/messy --dst /path/to/sorted --move

  # Organise into YYYY-MM/ instead of the default YYYY/:
  python organize_photos.py --src /path/to/messy --dst /path/to/sorted --copy --by-month

  # Exact-only dedup (no imagehash needed, faster):
  python organize_photos.py --src /path/to/messy --dst /path/to/sorted --copy --exact-only

  # Tighter perceptual threshold (0=identical, 8=default, 64=max):
  python organize_photos.py --src /path/to/messy --dst /path/to/sorted --copy --phash-threshold 6

  # Control worker counts (defaults: threads=4, processes=cpu_count):
  python organize_photos.py --src /m --dst /s --copy --sha-workers 8 --phash-workers 4

  # Use a custom cache location:
  python organize_photos.py --src /m --dst /s --copy --cache /data/myphotos.db

  # Wipe the cache and start fresh:
  python organize_photos.py --src /m --dst /s --copy --clear-cache

Dependencies:
  pip install Pillow       # EXIF + pHash image decoding
  pip install imagehash    # perceptual hashing (skip with --exact-only)
  pip install tqdm         # optional — nicer progress bars
"""

import argparse
import hashlib
import json
import multiprocessing
import os
import re
import shutil
import sqlite3
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# ── extensions ───────────────────────────────────────────────────────────────────
PHOTO_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".heic", ".heif",
    ".tiff", ".tif", ".bmp", ".webp", ".raw",
    ".cr2", ".cr3", ".nef", ".arw", ".dng", ".orf", ".rw2",
}
VIDEO_EXTENSIONS = {
    ".mp4", ".mov", ".avi", ".mkv", ".m4v", ".3gp", ".mts", ".m2ts",
}
ALL_EXTENSIONS = PHOTO_EXTENSIONS | VIDEO_EXTENSIONS
PHASH_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".tiff", ".tif", ".bmp", ".webp"}

# ── filename date patterns ────────────────────────────────────────────────────────
FILENAME_PATTERNS = [
    (r"(?:IMG|VID|PANO|DCIM|DSC|DJI|PXL|MVIMG)_(\d{8})(?:[_\-]\d+)?", "%Y%m%d"),
    (r"(\d{4})[-_.](\d{2})[-_.](\d{2})",                                "%Y%m%d"),
    (r"(\d{8})[_\-]\d{4,6}",                                            "%Y%m%d"),
    (r"(?<!\d)(\d{8})(?!\d)",                                           "%Y%m%d"),
    (r"(?:IMG|VID)-(\d{8})-WA\d+",                                      "%Y%m%d"),
    (r"Screenshot[_\-](\d{4})[_\-](\d{2})[_\-](\d{2})",                "%Y%m%d"),
]
DATE_SOURCE_RANK = {"exif": 0, "filename": 1, "mtime": 2}

# SQLite batch commit interval
CACHE_BATCH = 500


# ════════════════════════════════════════════════════════════════════════════════
# Hash cache (SQLite)
# ════════════════════════════════════════════════════════════════════════════════

class HashCache:
    """
    Persistent cache of (sha256, phash) keyed on (path, size, mtime_ns).

    Cache key uses nanosecond mtime — any write to the file changes it,
    so a cache hit guarantees the file hasn't been modified since last run.

    Thread-safety: SQLite WAL mode + a single connection owned by the main
    thread.  Workers never touch the DB; they return results to the main
    thread which does all reads/writes.
    """

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS file_hashes (
        path      TEXT    NOT NULL,
        size      INTEGER NOT NULL,
        mtime_ns  INTEGER NOT NULL,
        sha256    TEXT,
        phash     TEXT,
        PRIMARY KEY (path, size, mtime_ns)
    );
    CREATE INDEX IF NOT EXISTS idx_path ON file_hashes(path);
    CREATE TABLE IF NOT EXISTS file_dates (
        path        TEXT    NOT NULL,
        size        INTEGER NOT NULL,
        mtime_ns    INTEGER NOT NULL,
        date_iso    TEXT    NOT NULL,
        date_source TEXT    NOT NULL,
        PRIMARY KEY (path, size, mtime_ns)
    );
    CREATE TABLE IF NOT EXISTS near_dedup_results (
        fingerprint TEXT PRIMARY KEY,
        dup_pairs   TEXT
    );
    """

    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._con = sqlite3.connect(str(db_path))
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.execute("PRAGMA synchronous=NORMAL")
        self._con.executescript(self.SCHEMA)
        self._con.commit()
        self._pending: list[tuple] = []        # hash rows to batch-write
        self._pending_dates: list[tuple] = []  # date rows to batch-write
        self._pending_updates: int = 0

    # ── read ─────────────────────────────────────────────────────────────────────

    def get(self, path: Path, size: int, mtime_ns: int) -> "tuple[str|None, str|None]":
        """Return (sha256, phash) if cached, else (None, None)."""
        row = self._con.execute(
            "SELECT sha256, phash FROM file_hashes WHERE path=? AND size=? AND mtime_ns=?",
            (str(path), size, mtime_ns),
        ).fetchone()
        return (row[0], row[1]) if row else (None, None)

    # ── write (batched) ──────────────────────────────────────────────────────────

    def put(self, path: Path, size: int, mtime_ns: int, sha: str, ph: "str|None"):
        self._pending.append((str(path), size, mtime_ns, sha, ph))
        if len(self._pending) >= CACHE_BATCH:
            self.flush()

    def flush(self):
        dirty = False
        if self._pending:
            self._con.executemany(
                """INSERT OR REPLACE INTO file_hashes(path, size, mtime_ns, sha256, phash)
                   VALUES (?,?,?,?,?)""",
                self._pending,
            )
            self._pending.clear()
            dirty = True
        if self._pending_dates:
            self._con.executemany(
                """INSERT OR REPLACE INTO file_dates(path, size, mtime_ns, date_iso, date_source)
                   VALUES (?,?,?,?,?)""",
                self._pending_dates,
            )
            self._pending_dates.clear()
            dirty = True
        if dirty:
            self._con.commit()

    def update_phash(self, path: Path, size: int, mtime_ns: int, ph: str):
        """Fill in phash for a row that already has sha256 (two-pass scenario)."""
        self._con.execute(
            "UPDATE file_hashes SET phash=? WHERE path=? AND size=? AND mtime_ns=?",
            (ph, str(path), size, mtime_ns),
        )
        # Don't commit immediately — will be flushed in next batch or close
        self._pending_updates += 1
        if self._pending_updates >= CACHE_BATCH:
            self._con.commit()
            self._pending_updates = 0

    def close(self):
        self.flush()
        if self._pending_updates:
            self._con.commit()
        self._con.close()

    # ── date cache ───────────────────────────────────────────────────────────

    def load_dates(self) -> dict:
        """Bulk-load all cached dates into a dict for fast in-memory lookup.

        Returns {(path_str, size, mtime_ns): (date_iso, date_source)}.
        """
        result = {}
        for row in self._con.execute(
            "SELECT path, size, mtime_ns, date_iso, date_source FROM file_dates"
        ):
            result[(row[0], row[1], row[2])] = (row[3], row[4])
        return result

    def put_date(self, path: Path, size: int, mtime_ns: int,
                 date_iso: str, date_source: str):
        self._pending_dates.append((str(path), size, mtime_ns, date_iso, date_source))
        if len(self._pending_dates) >= CACHE_BATCH:
            self.flush()

    # ── near-dedup result cache ────────────────────────────────────────────────

    def get_near_dedup(self, fingerprint: str) -> "list | None":
        """Return cached [(kept_path, skipped_path), ...] or None."""
        row = self._con.execute(
            "SELECT dup_pairs FROM near_dedup_results WHERE fingerprint=?",
            (fingerprint,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def put_near_dedup(self, fingerprint: str, pairs: list):
        """Store near-dedup result, replacing any previous entry."""
        self._con.execute("DELETE FROM near_dedup_results")
        self._con.execute(
            "INSERT INTO near_dedup_results(fingerprint, dup_pairs) VALUES (?,?)",
            (fingerprint, json.dumps(pairs)),
        )
        self._con.commit()

    def clear(self):
        self._con.execute("DELETE FROM file_hashes")
        self._con.execute("DELETE FROM file_dates")
        self._con.execute("DELETE FROM near_dedup_results")
        self._con.commit()
        print("  Cache cleared.")


# ════════════════════════════════════════════════════════════════════════════════
# Date resolution
# ════════════════════════════════════════════════════════════════════════════════

def exif_date(path: Path) -> "datetime | None":
    if path.suffix.lower() not in {".jpg", ".jpeg", ".tiff", ".tif", ".heic", ".heif"}:
        return None
    try:
        from PIL import Image
        with Image.open(path) as img:
            exif = img.getexif()
            if not exif:
                return None
            # DateTimeOriginal (36867) and DateTimeDigitized (36868) live in
            # the EXIF sub-IFD (0x8769), not in IFD0.  DateTime (306) is IFD0.
            exif_ifd = exif.get_ifd(0x8769)
            for tag_id in (36867, 36868):          # DateTimeOriginal, DateTimeDigitized
                val = exif_ifd.get(tag_id)
                if val:
                    try:
                        return datetime.strptime(val, "%Y:%m:%d %H:%M:%S")
                    except (ValueError, TypeError):
                        pass
            val = exif.get(306)                     # DateTime (IFD0)
            if val:
                try:
                    return datetime.strptime(val, "%Y:%m:%d %H:%M:%S")
                except (ValueError, TypeError):
                    pass
    except Exception:
        pass
    return None


def filename_date(path: Path) -> "datetime | None":
    stem = path.stem
    for pattern, _ in FILENAME_PATTERNS:
        m = re.search(pattern, stem, re.IGNORECASE)
        if m:
            groups = m.groups()
            date_str = groups[0] if len(groups) == 1 else "".join(groups)
            try:
                dt = datetime.strptime(date_str[:8], "%Y%m%d")
                if 1990 <= dt.year <= datetime.now().year + 5:
                    return dt
            except ValueError:
                pass
    return None


def mtime_date(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime)


def resolve_date(path: Path) -> "tuple[datetime, str]":
    dt = exif_date(path)
    if dt:
        return dt, "exif"
    dt = filename_date(path)
    if dt:
        return dt, "filename"
    return mtime_date(path), "mtime"


# ════════════════════════════════════════════════════════════════════════════════
# Hashing — module-level so ProcessPoolExecutor can pickle them
# ════════════════════════════════════════════════════════════════════════════════

def _compute_sha256(path_str: str) -> "tuple[str, str]":
    """Worker: return (path_str, hex_digest). Runs in a thread."""
    h = hashlib.sha256()
    with open(path_str, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return path_str, h.hexdigest()


def _compute_phash(path_str: str) -> "tuple[str, str | None]":
    """Worker: return (path_str, phash_hex_str). Runs in a process."""
    try:
        import imagehash
        from PIL import Image
        with Image.open(path_str) as img:
            ph = imagehash.phash(img)
        return path_str, str(ph)          # imagehash hashes are str-serialisable
    except Exception:
        return path_str, None


# ════════════════════════════════════════════════════════════════════════════════
# FileRecord
# ════════════════════════════════════════════════════════════════════════════════

class FileRecord:
    __slots__ = ("path", "dt", "date_source", "size", "mtime_ns",
                 "exact_hash", "phash_str", "_phash_int")

    def __init__(self, path: Path, dt: datetime, date_source: str,
                 size: int, mtime_ns: int):
        self.path = path
        self.dt = dt
        self.date_source = date_source
        self.size = size
        self.mtime_ns = mtime_ns
        self.exact_hash: "str | None" = None
        self.phash_str:  "str | None" = None
        self._phash_int = None

    @property
    def phash_int(self) -> "int | None":
        """Perceptual hash as a plain int (fast Hamming via XOR + popcount)."""
        v = self._phash_int
        if v is not None:
            return v
        if self.phash_str is None:
            return None
        v = int(self.phash_str, 16)
        self._phash_int = v
        return v

    def rank(self):
        return (DATE_SOURCE_RANK[self.date_source], -self.size)


def best_in_group(group: list) -> "FileRecord":
    return min(group, key=lambda r: r.rank())


# ════════════════════════════════════════════════════════════════════════════════
# Progress helper (tqdm if available, plain counter if not)
# ════════════════════════════════════════════════════════════════════════════════

def _make_progress(total: int, desc: str):
    try:
        from tqdm import tqdm
        return tqdm(total=total, desc=desc, unit="file", ncols=80)
    except ImportError:
        class _Plain:
            def __init__(self, total, desc):
                self._t = total
                self._n = 0
                self._desc = desc
                print(f"  {desc} (0/{total})", end="\r", flush=True)
            def update(self, n=1):
                self._n += n
                if self._n % 100 == 0 or self._n == self._t:
                    print(f"  {self._desc} ({self._n}/{self._t})", end="\r", flush=True)
            def close(self):
                print()
        return _Plain(total, desc)


# ════════════════════════════════════════════════════════════════════════════════
# Parallel SHA-256 (ThreadPoolExecutor — I/O bound)
# ════════════════════════════════════════════════════════════════════════════════

def batch_sha256(
    records: list,
    cache: HashCache,
    workers: int,
) -> None:
    """Fill record.exact_hash for every record, using cache + parallel threads."""
    need = []
    for r in records:
        cached_sha, _ = cache.get(r.path, r.size, r.mtime_ns)
        if cached_sha:
            r.exact_hash = cached_sha
        else:
            need.append(r)
    cached_count = len(records) - len(need)

    if not need:
        print(f"  SHA-256: all {len(records)} served from cache.")
        return

    print(f"  SHA-256: {cached_count} from cache, computing {len(need)}...")
    bar = _make_progress(len(need), "SHA-256")

    path_to_record = {str(r.path): r for r in need}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_compute_sha256, str(r.path)): r for r in need}
        for fut in as_completed(futures):
            try:
                path_str, digest = fut.result()
            except Exception as exc:
                r = futures[fut]
                print(f"\n  WARNING: SHA-256 failed for {r.path}: {exc}", flush=True)
                bar.update()
                continue
            r = path_to_record[path_str]
            r.exact_hash = digest
            # Write to cache (phash unknown yet — will be filled in batch_phash)
            cache.put(r.path, r.size, r.mtime_ns, digest, None)
            bar.update()

    bar.close()
    cache.flush()


# ════════════════════════════════════════════════════════════════════════════════
# Parallel pHash (ProcessPoolExecutor — CPU bound)
# ════════════════════════════════════════════════════════════════════════════════

def batch_phash(
    records: list,
    cache: HashCache,
    workers: int,
) -> None:
    """Fill record.phash_str for every record, using cache + parallel processes."""
    # Populate from cache first
    need = []
    for r in records:
        _, cached_ph = cache.get(r.path, r.size, r.mtime_ns)
        if cached_ph is not None:
            r.phash_str = cached_ph
        else:
            need.append(r)

    cached_count = len(records) - len(need)

    if not need:
        print(f"  pHash:  all {len(records)} served from cache.")
        return

    print(f"  pHash:  {cached_count} from cache, computing {len(need)}...")
    bar = _make_progress(len(need), "pHash  ")

    path_to_record = {str(r.path): r for r in need}

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_compute_phash, str(r.path)): r for r in need}
        for fut in as_completed(futures):
            try:
                path_str, ph_str = fut.result()
            except Exception as exc:
                r = futures[fut]
                print(f"\n  WARNING: pHash failed for {r.path}: {exc}", flush=True)
                bar.update()
                continue
            r = path_to_record[path_str]
            r.phash_str = ph_str
            if r.exact_hash and ph_str:
                cache.update_phash(r.path, r.size, r.mtime_ns, ph_str)
            bar.update()

    bar.close()
    cache.flush()


# ════════════════════════════════════════════════════════════════════════════════
# BK-tree (O(n log n) Hamming-distance lookup)
# ════════════════════════════════════════════════════════════════════════════════

class BKTree:
    """
    Burkhard-Keller tree for Hamming-distance lookup on perceptual hashes.

    Hashes are stored as plain ints; Hamming distance = (a ^ b).bit_count().
    This avoids the overhead of imagehash/numpy objects during the millions
    of distance computations that happen at tree-build and query time.

    Each node: [hash_int, FileRecord, children_dict, *exact_dupes]
    Children keyed by Hamming distance from this node's hash.
    Triangle inequality prunes subtrees: if a child was inserted at
    distance d, and we query within radius r, only visit that child
    when |dist(query, node) - d| <= r.
    """

    def __init__(self):
        self._root = None

    def add(self, record: FileRecord):
        h = record.phash_int
        if h is None:
            return
        if self._root is None:
            self._root = [h, record, {}]
            return
        node = self._root
        while True:
            dist = (h ^ node[0]).bit_count()
            if dist == 0:
                node.append(record)
                return
            children = node[2]
            if dist not in children:
                children[dist] = [h, record, {}]
                return
            node = children[dist]

    def find(self, query_int: int, threshold: int) -> list:
        if self._root is None:
            return []
        results = []
        stack = [self._root]
        while stack:
            node = stack.pop()
            node_hash, record, children = node[0], node[1], node[2]
            extra = node[3:]
            dist = (query_int ^ node_hash).bit_count()
            if dist <= threshold:
                results.append(record)
                results.extend(extra)
            lo = dist - threshold
            hi = dist + threshold
            for edge, child in children.items():
                if lo <= edge <= hi:
                    stack.append(child)
        return results


# ════════════════════════════════════════════════════════════════════════════════
# Duplicate detection
# ════════════════════════════════════════════════════════════════════════════════

def find_duplicates(
    records: list,
    cache: HashCache,
    exact_only: bool,
    phash_threshold: int,
    sha_workers: int,
    phash_workers: int,
) -> "tuple[list, list]":
    """
    Returns (keepers, dup_pairs).
    Stage 1 — SHA-256 exact dedup.
    Stage 2 — pHash near-dedup via BK-tree.
    """

    # ── Stage 1 ──────────────────────────────────────────────────────────────────
    print(f"\n[1/2] Exact dedup (SHA-256)")
    batch_sha256(records, cache, sha_workers)

    by_hash: dict = defaultdict(list)
    unhashed = []
    for r in records:
        if r.exact_hash:
            by_hash[r.exact_hash].append(r)
        else:
            unhashed.append(r)

    dup_pairs = []
    after_exact = list(unhashed)  # can't dedup without a hash — keep them all
    exact_dup_count = 0
    for group in by_hash.values():
        keeper = best_in_group(group)
        after_exact.append(keeper)
        for r in group:
            if r is not keeper:
                dup_pairs.append((keeper, r))
                exact_dup_count += 1

    print(f"  Exact duplicates found: {exact_dup_count}")

    if exact_only:
        return after_exact, dup_pairs

    # ── Stage 2 ──────────────────────────────────────────────────────────────────
    print(f"\n[2/2] Near-dedup (pHash, threshold={phash_threshold})")

    phashable     = [r for r in after_exact if r.path.suffix.lower() in PHASH_EXTENSIONS]
    non_phashable = [r for r in after_exact if r.path.suffix.lower() not in PHASH_EXTENSIONS]

    if not phashable:
        print("  No phashable images after exact dedup.")
        return after_exact, dup_pairs

    try:
        import imagehash  # noqa: F401
    except ImportError:
        print("  [imagehash not installed — skipping perceptual check]")
        print("  Install with:  pip install imagehash")
        return after_exact, dup_pairs

    batch_phash(phashable, cache, phash_workers)

    valid  = [r for r in phashable if r.phash_str]
    failed = [r for r in phashable if not r.phash_str]

    # Fingerprint the input set: same files + same pHashes + same threshold
    # → deterministic result, so we can skip the BK-tree entirely on repeat.
    fp = hashlib.sha256(str(phash_threshold).encode())
    for r in sorted(valid, key=lambda r: str(r.path)):
        fp.update(str(r.path).encode())
        fp.update(b'\0')
        fp.update(r.phash_str.encode())
        fp.update(b'\0')
    fingerprint = fp.hexdigest()

    cached = cache.get_near_dedup(fingerprint)
    if cached is not None:
        path_to_record = {str(r.path): r for r in valid}
        near_dup_pairs = []
        skipped_paths = set()
        for kept_path, skipped_path in cached:
            kept = path_to_record.get(kept_path)
            skipped = path_to_record.get(skipped_path)
            if kept and skipped:
                near_dup_pairs.append((kept, skipped))
                skipped_paths.add(skipped_path)
        keepers = [r for r in valid if str(r.path) not in skipped_paths]
        print(f"  Near-duplicates: {len(near_dup_pairs)} (from cache)")
        return keepers + non_phashable + failed, dup_pairs + near_dup_pairs

    tree = BKTree()
    for r in valid:
        tree.add(r)

    used = set()
    keepers = []
    near_dup_pairs = []

    bar = _make_progress(len(valid), "Near-dedup query")
    for r in valid:
        bar.update()
        if id(r) in used:
            continue
        used.add(id(r))
        qh = r.phash_int
        if qh is None:
            keepers.append(r)
            continue
        neighbours = tree.find(qh, phash_threshold)
        cluster = [n for n in neighbours if id(n) not in used or n is r]
        for n in cluster:
            used.add(id(n))
        keeper = best_in_group(cluster) if cluster else r
        keepers.append(keeper)
        for n in cluster:
            if n is not keeper:
                near_dup_pairs.append((keeper, n))
    bar.close()

    # Cache result so repeat runs skip the BK-tree entirely
    cache.put_near_dedup(fingerprint,
                         [(str(k.path), str(s.path))
                          for k, s in near_dup_pairs])

    print(f"  Near-duplicates found:  {len(near_dup_pairs)}")
    return keepers + non_phashable + failed, dup_pairs + near_dup_pairs


# ════════════════════════════════════════════════════════════════════════════════
# Planning
# ════════════════════════════════════════════════════════════════════════════════

def safe_destination(dst_file: Path, seen: set) -> Path:
    if dst_file not in seen and not dst_file.exists():
        return dst_file
    stem, suffix = dst_file.stem, dst_file.suffix
    parent = dst_file.parent
    counter = 1
    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if candidate not in seen and not candidate.exists():
            return candidate
        counter += 1


def plan(
    src: Path,
    dst: Path,
    cache: HashCache,
    skip_unknown: bool,
    exact_only: bool,
    phash_threshold: int,
    sha_workers: int,
    phash_workers: int,
    extensions: "set[str]",
    by_month: bool = False,
) -> "tuple[list, list, int]":

    all_files = []
    for p in src.rglob("*"):
        if p.is_file() and p.suffix.lower() in extensions:
            all_files.append(p)
            if len(all_files) % 1000 == 0:
                print(f"\r  Scanning... {len(all_files)} media files found",
                      end="", flush=True)
    all_files.sort()
    print(f"\r  Found {len(all_files)} media files in source.          ")

    # Resolve dates — check cache first (EXIF reading is the slow part).
    date_cache = cache.load_dates()
    records = []
    need_date = []  # (path, size, mtime_ns) not yet cached
    for f in all_files:
        try:
            st = f.stat()
        except OSError as exc:
            print(f"  WARNING: cannot stat {f}: {exc}", flush=True)
            continue
        key = (str(f), st.st_size, st.st_mtime_ns)
        cached = date_cache.get(key)
        if cached:
            try:
                dt = datetime.fromisoformat(cached[0])
                records.append(FileRecord(f, dt, cached[1],
                                          st.st_size, st.st_mtime_ns))
                continue
            except (ValueError, TypeError):
                pass  # corrupt cache entry — re-resolve
        need_date.append((f, st.st_size, st.st_mtime_ns))

    if not need_date:
        print(f"  Dates: all {len(records)} served from cache.")
    else:
        if records:
            print(f"  Dates: {len(records)} from cache, resolving {len(need_date)}...")
        bar = _make_progress(len(need_date), "Resolving dates")
        with ThreadPoolExecutor(max_workers=sha_workers) as pool:
            futures = {}
            for f, size, mtime_ns in need_date:
                futures[pool.submit(resolve_date, f)] = (f, size, mtime_ns)
            for fut in as_completed(futures):
                f, size, mtime_ns = futures[fut]
                try:
                    dt, source = fut.result()
                    records.append(FileRecord(f, dt, source, size, mtime_ns))
                    cache.put_date(f, size, mtime_ns, dt.isoformat(), source)
                except Exception as exc:
                    print(f"\n  WARNING: date scan failed for {f}: {exc}",
                          flush=True)
                bar.update()
        bar.close()
        cache.flush()

    keepers, dup_pairs = find_duplicates(
        records, cache, exact_only, phash_threshold, sha_workers, phash_workers,
    )

    skipped_unknown = 0
    if skip_unknown:
        before = len(keepers)
        keepers = [r for r in keepers if r.date_source != "mtime"]
        skipped_unknown = before - len(keepers)

    seen_destinations: set = set()
    moves = []
    for r in sorted(keepers, key=lambda r: r.path):
        if r.date_source == "mtime":
            folder = dst / "_unknown" / r.path.relative_to(src).parent
        elif by_month:
            folder = dst / r.dt.strftime("%Y-%m")
        else:
            folder = dst / str(r.dt.year)
        dest = safe_destination(folder / r.path.name, seen_destinations)
        seen_destinations.add(dest)
        moves.append((r, dest))

    return moves, dup_pairs, skipped_unknown


# ════════════════════════════════════════════════════════════════════════════════
# Execution
# ════════════════════════════════════════════════════════════════════════════════

def _safe_move(src_path: Path, dst_file: Path, expected_hash: "str | None") -> None:
    """
    Move a file safely.

    Same-device: uses os.rename (atomic, no data risk).
    Cross-device: copies, verifies SHA-256 of the copy matches the
    previously-computed hash, then deletes the source.  If verification
    fails the source is preserved and an error is raised.
    """
    try:
        os.rename(str(src_path), str(dst_file))
        return                                      # same-device: atomic rename
    except OSError:
        pass                                        # cross-device — copy + verify

    try:
        shutil.copy2(str(src_path), dst_file)
    except BaseException:
        # Remove partial/corrupt destination so it doesn't confuse future runs.
        # Cleanup errors must not suppress the original exception (especially
        # KeyboardInterrupt), so swallow any OSError from the unlink.
        try:
            if dst_file.exists():
                dst_file.unlink()
        except OSError:
            pass
        raise

    if expected_hash:
        h = hashlib.sha256()
        with open(dst_file, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        if h.hexdigest() != expected_hash:
            dst_file.unlink()
            raise RuntimeError(
                "SHA-256 verification failed after cross-device copy — "
                "source preserved, corrupt copy removed"
            )

    src_path.unlink()


def write_dup_log(dst: Path, dup_pairs: list, moved_to: "dict[Path, Path] | None" = None) -> None:
    log_path = dst / "duplicates.log"
    dst.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"# Duplicate report — {datetime.now():%Y-%m-%d %H:%M:%S}\n")
        f.write(f"# {len(dup_pairs)} file(s) skipped as duplicates\n\n")
        for kept, skipped in dup_pairs:
            dest = moved_to.get(kept.path) if moved_to else None
            if dest:
                f.write(f"KEPT    {dest}  (from {kept.path})\n")
            else:
                f.write(f"KEPT    {kept.path}\n")
            f.write(f"SKIP    {skipped.path}\n\n")
    print(f"  Duplicate log written: {log_path}")


def run(moves: list, dup_pairs: list, dst: Path, mode: str, dry_run: bool,
        skipped_unknown: int = 0) -> None:
    total = len(moves)
    pad = len(str(total))
    counters: dict = {"exif": 0, "filename": 0, "mtime": 0}
    failures = 0

    print()
    for i, (r, dst_file) in enumerate(moves, 1):
        counters[r.date_source] = counters.get(r.date_source, 0) + 1
        label = f"[{i:{pad}d}/{total}] ({r.date_source:8s})"
        if dry_run:
            print(f"  DRY  {label}  {r.path}  ->  {dst_file}")
        else:
            try:
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                if mode == "move":
                    _safe_move(r.path, dst_file, r.exact_hash)
                else:
                    shutil.copy2(str(r.path), dst_file)
                verb = "MOVE" if mode == "move" else "COPY"
                print(f"  {verb} {label}  {r.path.name}  ->  {dst_file}")
            except Exception as exc:
                print(f"  FAIL {label}  {r.path.name}: {exc}")
                failures += 1

    if not dry_run and dup_pairs:
        try:
            moved_to = {r.path: dst_file for r, dst_file in moves} if mode == "move" else None
            write_dup_log(dst, dup_pairs, moved_to)
        except Exception as exc:
            print(f"  WARNING: could not write duplicate log: {exc}")

    print()
    print("── Summary ─────────────────────────────────────────────")
    print(f"  Files kept       : {total}")
    print(f"  Duplicates skip  : {len(dup_pairs)}")
    if skipped_unknown:
        print(f"  Unknown date skip: {skipped_unknown} (--skip-unknown)")
    print(f"  ├ EXIF date      : {counters.get('exif', 0)}")
    print(f"  ├ Filename date  : {counters.get('filename', 0)}")
    print(f"  └ Mtime only     : {counters.get('mtime', 0)}")
    if failures:
        print(f"  Errors           : {failures} file(s) failed to {mode}")
    if dup_pairs and mode == "move" and not dry_run:
        print()
        print(f"  NOTE: {len(dup_pairs)} duplicate(s) were left in the source directory.")
        print( "     See duplicates.log in the destination for the full list.")
        print( "     Exact duplicates (SHA-256 match) are safe to delete.")
    if dry_run:
        print()
        print("  ⚠  DRY RUN — nothing was changed.")
        print("     Re-run with --move or --copy to apply.")


# ════════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════════

def main():
    cpu_count = multiprocessing.cpu_count()

    parser = argparse.ArgumentParser(
        description="Organise photos into YYYY/ dirs (or YYYY-MM/ with --by-month), deduplicated.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--src",  required=True, help="Source directory (recursive)")
    parser.add_argument("--dst",  required=True, help="Destination root directory")

    g = parser.add_mutually_exclusive_group()
    g.add_argument("--move", action="store_true", help="Move files")
    g.add_argument("--copy", action="store_true", help="Copy files (originals untouched)")

    parser.add_argument("--by-month",         action="store_true",
                        help="Organise into YYYY-MM/ instead of the default YYYY/")
    parser.add_argument("--skip-unknown",    action="store_true",
                        help="Skip files whose date falls back to mtime")
    parser.add_argument("--exact-only",      action="store_true",
                        help="SHA-256 only — no perceptual hashing")
    parser.add_argument("--phash-threshold", type=int, default=8, metavar="N",
                        help="Hamming distance for near-duplicates (default 8, range 0-64)")
    parser.add_argument("--sha-workers",     type=int, default=4, metavar="N",
                        help="Threads for SHA-256 (default 4; more helps on fast NVMe)")
    parser.add_argument("--phash-workers",   type=int, default=cpu_count, metavar="N",
                        help=f"Processes for pHash (default {cpu_count} = cpu count)")
    parser.add_argument("--cache",           type=Path,
                        default=Path.home() / ".cache" / "organize_photos.db",
                        help="SQLite cache path (default ~/.cache/organize_photos.db)")
    parser.add_argument("--clear-cache",     action="store_true",
                        help="Wipe the cache before running")
    parser.add_argument("--extensions",      nargs="+", metavar="EXT",
                        help="Override extensions (e.g. .jpg .png .heic)")
    args = parser.parse_args()

    if not (0 <= args.phash_threshold <= 64):
        parser.error("--phash-threshold must be in range 0-64")
    if args.sha_workers < 1:
        parser.error("--sha-workers must be >= 1")
    if args.phash_workers < 1:
        parser.error("--phash-workers must be >= 1")

    src = Path(args.src).expanduser().resolve()
    dst = Path(args.dst).expanduser().resolve()

    if not src.is_dir():
        print(f"ERROR: source not found: {src}", file=sys.stderr)
        sys.exit(1)

    if src == dst:
        print("ERROR: source and destination are the same directory.", file=sys.stderr)
        sys.exit(1)
    try:
        dst.relative_to(src)
        print("ERROR: destination is inside source — files would be re-scanned "
              "and could be moved in circles. Use a destination outside the "
              "source tree.", file=sys.stderr)
        sys.exit(1)
    except ValueError:
        pass
    try:
        src.relative_to(dst)
        print("ERROR: source is inside destination. Use separate directory "
              "trees.", file=sys.stderr)
        sys.exit(1)
    except ValueError:
        pass

    extensions = (
        {e if e.startswith(".") else f".{e}" for e in args.extensions}
        if args.extensions else ALL_EXTENSIONS
    )

    dry_run = not (args.move or args.copy)
    mode    = "move" if args.move else "copy"
    dup_mode = ("SHA-256 only" if args.exact_only
                else f"SHA-256 + pHash (threshold={args.phash_threshold})")
    dir_mode = "YYYY-MM/" if args.by_month else "YYYY/"

    print(f"Source           : {src}")
    print(f"Dest             : {dst}")
    print(f"Mode             : {'DRY RUN' if dry_run else mode.upper()}")
    print(f"Directory layout : {dir_mode}")
    print(f"Dup detection    : {dup_mode}")
    print(f"Workers          : {args.sha_workers} SHA threads, {args.phash_workers} pHash processes")
    print(f"Cache            : {args.cache}")
    print()

    cache = HashCache(args.cache)
    if args.clear_cache:
        cache.clear()

    try:
        moves, dup_pairs, skipped_unknown = plan(
            src, dst, cache,
            skip_unknown    = args.skip_unknown,
            exact_only      = args.exact_only,
            phash_threshold = args.phash_threshold,
            sha_workers     = args.sha_workers,
            phash_workers   = args.phash_workers,
            extensions      = extensions,
            by_month        = args.by_month,
        )
    finally:
        cache.close()

    if not moves:
        print("No files to process.")
        return

    run(moves, dup_pairs, dst, mode=mode, dry_run=dry_run,
        skipped_unknown=skipped_unknown)


if __name__ == "__main__":
    # Required on Windows/macOS for ProcessPoolExecutor
    multiprocessing.freeze_support()
    main()
