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
from imguploader.accounts import AccountPool, load_keys
from imguploader.hosts import AuthError, Catbox, StopUploads
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
    up.add_argument(
        "--host",
        choices=["imgchest", "catbox"],
        default="imgchest",
        help="where to upload (default: imgchest)",
    )
    up.add_argument(
        "--account",
        action="append",
        metavar="NAME",
        help="imgchest account(s) to use, in order; repeat or comma-separate (default: all)",
    )
    up.add_argument("--catbox-fallback", action="store_true", help="upload to catbox when imgchest fails")
    up.add_argument("--wait", action="store_true", help="when the hourly limit is hit, wait and continue")
    up.add_argument("-n", "--dry-run", action="store_true", help="hash and compare, upload nothing")

    mir = sub.add_parser("mirrors", help="mirror links: stats, sync from image_cache, Pinterest originals")
    mir.add_argument(
        "action", nargs="?", default="stats", choices=["stats", "sync", "pinterest"], help="default: stats"
    )
    mir.add_argument("directories", nargs="*", help="pinterest: gallery-dl folders with metadata files")
    mir.add_argument("--db", default=None, help="PostgreSQL URL (default: $DATABASE_URL)")
    mir.add_argument("--table", default="image_cache", help="link cache table (default: %(default)s)")

    acc = sub.add_parser("accounts", help="list the configured imgchest accounts and keys")
    acc.add_argument("--db", default=None, help="PostgreSQL URL (default: $DATABASE_URL)")
    acc.add_argument("--flag-full", metavar="NAME", help="mark an account as out of storage (skipped)")
    acc.add_argument("--unflag", metavar="NAME", help="clear the full flag, e.g. after freeing space")
    acc.add_argument("--table", default="image_cache", help="link cache table (default: %(default)s)")
    return p


def _account_names(values: list[str] | None) -> list[str]:
    return [name for v in values or [] for name in v.split(",") if name.strip()]


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
    catbox = None
    if args.host == "catbox" or args.catbox_fallback:
        catbox = Catbox(os.environ.get("CATBOX_USERHASH"))
    try:
        if imgchest is None and args.host == "imgchest" and not args.dry_run:
            flags = store if hasattr(store, "full_accounts") else None
            imgchest = AccountPool(load_keys(only=_account_names(args.account)), flags=flags)
            first = imgchest.current
            if first is None:
                raise AuthError("every imgchest account is flagged full (see 'imguploader accounts')")
            logger.info("imgchest: %d account(s), starting with '%s'", len(imgchest.keys), first.account)
    except AuthError as e:
        logger.error("%s", e)
        return 2
    if args.host == "catbox":
        imgchest = None
        logger.info("uploading to catbox%s", "" if catbox.userhash else " anonymously (no CATBOX_USERHASH)")
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
    except Exception as e:  # database trouble: stop cleanly, the manifest keeps the rest
        if type(e).__module__.split(".")[0] != "psycopg2":
            raise
        logger.error("stopping, database error: %s", str(e).strip())
        i = max(i - 1, 0)
        code = 1
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


def cmd_accounts(args, store=None) -> int:
    try:
        keys = load_keys()
    except AuthError as e:
        logger.error("%s", e)
        return 2
    dsn = args.db or os.environ.get("DATABASE_URL")
    if store is None and dsn:
        from imguploader.store import PostgresStore

        try:
            store = PostgresStore.connect(dsn, table=args.table)
        except Exception as e:  # listing keys must still work without the database
            logger.warning("database unavailable: %s", e)
    by_name = {k.account: k for k in keys}

    for name, flag in ((args.flag_full, True), (args.unflag, False)):
        if not name:
            continue
        key = by_name.get(name.lower())
        if key is None:
            logger.error("no account '%s' (accounts: %s)", name, ", ".join(by_name) or "none")
            return 2
        if store is None:
            logger.error("flags are kept in the database: pass --db or set DATABASE_URL")
            return 2
        if flag:
            store.ensure_schema()  # creates imgchest_accounts if needed; never ALTERs existing columns
            store.flag_full(key.fingerprint, key.account, "flagged by hand")
            print(f"account '{key.account}' flagged full")
        else:
            print(
                f"account '{key.account}' "
                + ("unflagged" if store.unflag(key.fingerprint) else "was not flagged")
            )

    full, counts = {}, {}
    if store is not None:
        try:
            full = store.full_accounts()
            counts = store.links_per_account()
        except Exception as e:
            logger.warning("cannot read the database: %s", e)

    if not keys:
        print("no imgchest keys configured (IMG_CHEST_API_KEY or IMG_CHEST_API_KEY_<NAME>)")
    else:
        print("imgchest accounts, in upload order (full ones are skipped):")
        for k in keys:
            status = f"FULL ({full[k.fingerprint]})" if k.fingerprint in full else "ok"
            links = f"{counts.get(k.account, 0)} links"
            print(f"  {k.account:<12} {k.var:<26} {k.masked}  {links:<12} {status}")
    others = {a: n for a, n in counts.items() if a not in by_name}
    if others:
        print("links held elsewhere:")
        for name, n in others.items():
            print(f"  {name or '(unrecorded, older uploads)'}  {n} links")
    return 0


def cmd_mirrors(args, store=None) -> int:
    from imguploader.hashing import md5_file
    from imguploader.mirrors import gallerydl_metadata, pinterest_original

    if store is None:
        from imguploader.store import PostgresStore

        dsn = args.db or os.environ.get("DATABASE_URL")
        if not dsn:
            logger.error("no database: pass --db or set DATABASE_URL")
            return 2
        store = PostgresStore.connect(dsn, table=args.table)
        store.ensure_schema()

    if args.action == "sync":
        logger.info("added %d mirror(s) from %s", store.sync_mirrors(), args.table)
    elif args.action == "pinterest":
        if not args.directories:
            logger.error("give the gallery-dl folders that hold the metadata (.json) files")
            return 2
        seen = added = no_url = unknown = 0
        for media, data in gallerydl_metadata(args.directories):
            seen += 1
            url = pinterest_original(data)
            if url is None:
                no_url += 1
                continue
            try:
                hash_val = md5_file(media)
            except OSError as e:
                logger.warning("cannot read %s: %s", media, e)
                continue
            if not store.known_hashes([hash_val]):
                unknown += 1  # mirrors belong to images in image_cache; upload it first
                continue
            added += store.add_mirror(hash_val, url, "gallery-dl")
        store.commit()
        logger.info(
            "%d metadata files: %d Pinterest originals added, %d without an original URL, "
            "%d images not in %s yet (upload them, then run this again)",
            seen,
            added,
            no_url,
            unknown,
            args.table,
        )
        if seen == 0:
            logger.warning(
                "no gallery-dl metadata found; fetch it without downloading again with "
                "gallery-dl --no-download --write-metadata <board url>"
            )
    for name, count in store.mirror_stats():
        print(f"  {name:<24} {count}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.verbose)
    load_env()
    start = time.monotonic()
    commands = {"scan": cmd_scan, "upload": cmd_upload, "accounts": cmd_accounts, "mirrors": cmd_mirrors}
    code = commands[args.command](args)
    logger.debug("finished in %.1fs", time.monotonic() - start)
    return code


if __name__ == "__main__":
    sys.exit(main())
