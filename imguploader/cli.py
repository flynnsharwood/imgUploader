"""Command line: ``imguploader scan`` builds the manifest, ``imguploader upload`` works through it."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

from imguploader import __version__, manifest
from imguploader.hosts import AuthError, Catbox, ImgChest, StopUploads
from imguploader.uploader import FolderUploader

logger = logging.getLogger("imguploader")

CHECKPOINT_EVERY = 25  # folders between manifest rewrites


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="imguploader", description=__doc__)
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    p.add_argument("-m", "--manifest", default="manifest.yml", help="manifest file (default: %(default)s)")
    p.add_argument("-v", "--verbose", action="store_true", help="debug output on the console")
    sub = p.add_subparsers(dest="command", required=True)

    scan = sub.add_parser("scan", help="list the folders to upload in the manifest")
    scan.add_argument("directories", nargs="*", help="folders to scan")
    scan.add_argument("-c", "--pixboards-config", help="also scan the 'directories' of this pixBoards config")

    up = sub.add_parser("upload", help="upload the folders in the manifest")
    up.add_argument("--db", default=None, help="PostgreSQL URL (default: $DATABASE_URL)")
    up.add_argument("--table", default="image_cache", help="link cache table (default: %(default)s)")
    up.add_argument("--limit", type=int, default=0, help="stop after this many folders")
    up.add_argument("--catbox-fallback", action="store_true", help="upload to catbox when imgchest fails")
    up.add_argument(
        "--wait", action="store_true", help="when imgchest's hourly limit is hit, wait and continue"
    )
    up.add_argument("-n", "--dry-run", action="store_true", help="hash and compare, upload nothing")
    return p


def setup_logging(verbose: bool) -> None:
    Path("logs").mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file = logging.FileHandler(f"logs/imguploader_{date.today().isoformat()}.log", encoding="utf-8")
    file.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    for h in (file, console):
        h.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers[:] = [file, console]
    logging.getLogger("urllib3").setLevel(logging.INFO)


def load_env() -> None:
    try:
        from dotenv import find_dotenv, load_dotenv
    except ImportError:
        return
    load_dotenv(find_dotenv(usecwd=True))


def cmd_scan(args) -> int:
    directories = list(args.directories)
    if args.pixboards_config:
        directories += manifest.directories_from_pixboards_config(args.pixboards_config)
    if not directories:
        logger.error("nothing to scan: give directories or --pixboards-config")
        return 2
    folders = manifest.scan(directories)
    manifest.save(folders, args.manifest)
    files = sum(len(f.files) for f in folders)
    logger.info("wrote %s: %d folders, %d files", args.manifest, len(folders), files)
    return 0


def cmd_upload(args, store=None, imgchest=None) -> int:
    if not os.path.exists(args.manifest):
        logger.error("no manifest at %s; run 'imguploader scan' first", args.manifest)
        return 2
    pending = manifest.load(args.manifest)
    if not pending:
        logger.info("%s is empty, nothing to do", args.manifest)
        return 0

    if store is None:
        from imguploader.store import PostgresStore

        dsn = args.db or os.environ.get("DATABASE_URL")
        if not dsn:
            logger.error("no database: pass --db or set DATABASE_URL")
            return 2
        store = PostgresStore.connect(dsn, table=args.table)
        store.ensure_schema()
    try:
        if imgchest is None and not args.dry_run:
            imgchest = ImgChest(os.environ.get("IMG_CHEST_API_KEY", ""))
    except AuthError as e:
        logger.error("%s", e)
        return 2
    catbox = Catbox(os.environ.get("CATBOX_USERHASH")) if args.catbox_fallback else None
    uploader = FolderUploader(store, imgchest, catbox, dry_run=args.dry_run, wait=args.wait)

    todo = pending[: args.limit] if args.limit else pending
    later = pending[len(todo) :]
    failed, code = [], 0
    i = 0
    try:
        for i, folder in enumerate(todo, start=1):
            if not uploader.upload(folder):
                failed.append(folder)
            if not args.dry_run and i % CHECKPOINT_EVERY == 0:
                manifest.save(failed + todo[i:] + later, args.manifest)
    except StopUploads as e:
        logger.error("stopping: %s", e)
        i = max(i - 1, 0)  # the current folder is not done
        code = 1
    except KeyboardInterrupt:
        logger.warning("interrupted")
        i = max(i - 1, 0)
        code = 130
    finally:
        if not args.dry_run:
            remaining = failed + todo[i:] + later
            manifest.save(remaining, args.manifest)
            logger.info("%d folders left in %s", len(remaining), args.manifest)

    stats = uploader.stats
    logger.info("done: %s", stats.summary())
    if stats.duplicate_names:
        logger.warning(
            "%d filenames already belong to other images; their filename column is empty:",
            len(stats.duplicate_names),
        )
        for name, path in stats.duplicate_names:
            logger.warning("  %s  (%s)", name, path)
    return code or (1 if failed else 0)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.verbose)
    load_env()
    start = time.monotonic()
    code = cmd_scan(args) if args.command == "scan" else cmd_upload(args)
    logger.debug("finished in %.1fs", time.monotonic() - start)
    return code


if __name__ == "__main__":
    sys.exit(main())
