# organize_photos

Sorts a messy photo/video collection into tidy `YYYY/YYYY-MM/` directories,
with fast parallel hashing and two-stage duplicate detection. A persistent
SQLite cache makes repeat runs near-instant.

```
before/                          after/
  IMG_20231014_120000.jpg    →   2023/2023-10/IMG_20231014_120000.jpg
  DSC_0042.JPG               →   2021/2021-06/DSC_0042.JPG
  copy of DSC_0042.JPG       →   (duplicate — skipped, logged)
  random_name.jpg            →   _unknown/random_name.jpg
```

## Features

- **Date resolution** — tries EXIF `DateTimeOriginal` first, then common
  filename patterns (`IMG_20231014_…`, `2023-10-14_…`, `20231014_…`, etc.),
  falls back to file modification time
- **Exact dedup** — SHA-256; bit-for-bit identical files are collapsed to one
- **Perceptual dedup** — pHash via [imagehash](https://github.com/JohannesBuchner/imagehash);
  catches re-saves, slight crops, and social-media re-uploads
- **Parallel hashing** — SHA-256 in threads (I/O-bound), pHash in processes
  (CPU-bound)
- **Safe moves** — same-device moves use an atomic `os.rename`; cross-device
  moves copy, verify SHA-256, then delete the source
- **Dry-run by default** — nothing changes until you pass `--copy` or `--move`

## Requirements

```bash
pip install Pillow        # EXIF reading + pHash image decoding
pip install imagehash     # perceptual hashing (skip with --exact-only)
pip install tqdm          # optional — nicer progress bars
```

Python 3.10+ required.

## Quick start

```bash
# 1. Preview what would happen (nothing is changed)
python organize_photos.py --src /path/to/messy --dst /path/to/sorted

# 2. Copy files (originals stay in source — recommended for a first run)
python organize_photos.py --src /path/to/messy --dst /path/to/sorted --copy

# 3. Move files once you are happy with the result
python organize_photos.py --src /path/to/messy --dst /path/to/sorted --move
```

## Output layout

```
/path/to/sorted/
  2023/
    2023-10/
      IMG_20231014_120000.jpg
      IMG_20231014_130000.jpg
  2024/
    2024-06/
      DSC_0099.JPG
  _unknown/           ← files whose date could only be guessed from mtime
    old_scan.jpg
  duplicates.log      ← written after --copy / --move when duplicates exist
```

## Options

| Option | Default | Description |
|---|---|---|
| `--src DIR` | *(required)* | Source directory (scanned recursively) |
| `--dst DIR` | *(required)* | Destination root directory |
| `--copy` | — | Copy files; originals are untouched |
| `--move` | — | Move files |
| `--exact-only` | off | SHA-256 dedup only; skip perceptual hashing |
| `--phash-threshold N` | `8` | Hamming distance for near-duplicate detection (0 = identical hashes only, 64 = maximum) |
| `--skip-unknown` | off | Omit files whose date falls back to mtime |
| `--sha-workers N` | `4` | Threads for SHA-256 hashing |
| `--phash-workers N` | cpu count | Processes for pHash computation |
| `--cache PATH` | `~/.cache/organize_photos.db` | SQLite cache location |
| `--clear-cache` | off | Wipe the cache before running |
| `--extensions EXT …` | all photo/video | Restrict to specific extensions, e.g. `.jpg .heic` |

## Duplicate detection explained

**Stage 1 — exact (always on)**
Files with identical SHA-256 digests are duplicates. The "best" copy is
kept — EXIF-dated files beat filename-dated files beat mtime-dated files;
ties go to the larger file (thumbnails and previews lose).

**Stage 2 — perceptual (skipped with `--exact-only`)**
Images are perceptually hashed (pHash). Pairs within `--phash-threshold`
Hamming distance are near-duplicates. A BK-tree makes this O(n log n).
The same "best copy" rule picks the keeper.

With `--move`, duplicate files are left behind in the source directory and
listed in `duplicates.log` in the destination. Exact duplicates (SHA-256
match) are safe to delete. Near-duplicates should be reviewed first — they
may be meaningfully different photos.

## Supported formats

**Photos:** `.jpg` `.jpeg` `.png` `.heic` `.heif` `.tiff` `.tif` `.bmp`
`.webp` `.raw` `.cr2` `.cr3` `.nef` `.arw` `.dng` `.orf` `.rw2`

**Video:** `.mp4` `.mov` `.avi` `.mkv` `.m4v` `.3gp` `.mts` `.m2ts`

Use `--extensions` to restrict or extend this list.

## Safety guarantees

- **Overlap check** — aborts if `--src` and `--dst` overlap in either
  direction; re-scanning already-moved files is always a mistake
- **No silent overwrites** — destination name conflicts get a numeric suffix
  (`photo_1.jpg`, `photo_2.jpg`, …); existing files are never overwritten
- **Verified cross-device moves** — when source and destination are on
  different filesystems, the tool copies first, re-hashes the copy, and only
  deletes the source if the hash matches
- **Interrupted copy cleanup** — if a copy is interrupted (disk full, Ctrl+C),
  any partial destination file is removed so future runs start clean

## Performance

The hash cache (SQLite) stores SHA-256 and pHash keyed on
`(path, size, mtime_ns)`. On repeat runs, unmodified files are served from
the cache without re-reading.

```
First run  (50 000 photos):  several minutes (dominated by I/O)
Repeat run (nothing changed): seconds
```

To move the cache (e.g., to a fast local SSD when the source is a NAS):

```bash
python organize_photos.py --src /mnt/nas/photos --dst /sorted \
    --cache /tmp/photos_cache.db --copy
```

## License

MIT — see [LICENSE](LICENSE).
