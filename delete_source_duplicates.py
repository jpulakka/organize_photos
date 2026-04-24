#!/usr/bin/env python3
"""
delete_source_duplicates.py — Delete leftover duplicate files listed in duplicates.log.

After running organize_photos.py with --move, duplicate files are left in the source
directory and recorded in duplicates.log (in the destination). This script reads that
log and deletes the SKIP entries.

Always run with --dry-run first to preview what will be deleted.

Usage:
    python delete_source_duplicates.py --log /path/to/duplicates.log --dry-run
    python delete_source_duplicates.py --log /path/to/duplicates.log
"""

import argparse
import sys
from pathlib import Path


def parse_skip_paths(log_path: Path) -> list[Path]:
    """Return all SKIP paths from duplicates.log."""
    skips = []
    with open(log_path, encoding="utf-8") as f:
        for line in f:
            if line.startswith("SKIP    "):
                skips.append(Path(line[8:].rstrip("\n")))
    return skips


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete leftover duplicate files listed in duplicates.log.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--log", required=True, metavar="PATH",
                        help="Path to duplicates.log produced by organize_photos.py")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview deletions without touching files")
    args = parser.parse_args()

    log_path = Path(args.log)
    if not log_path.is_file():
        print(f"ERROR: log file not found: {log_path}", file=sys.stderr)
        sys.exit(1)

    skips = parse_skip_paths(log_path)
    if not skips:
        print("No SKIP entries found in log.")
        return

    print(f"  {'DRY RUN — ' if args.dry_run else ''}Processing {len(skips)} SKIP entries from {log_path}")
    print()

    deleted = 0
    missing = 0
    errors = 0

    for path in skips:
        if not path.exists():
            missing += 1
            continue
        if args.dry_run:
            print(f"  WOULD DELETE  {path}")
            deleted += 1
        else:
            try:
                path.unlink()
                print(f"  DELETED  {path}")
                deleted += 1
            except OSError as e:
                print(f"  ERROR    {path}: {e}", file=sys.stderr)
                errors += 1

    print()
    action = "Would delete" if args.dry_run else "Deleted"
    print(f"  {action}: {deleted}  |  Already gone: {missing}  |  Errors: {errors}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
