"""Mirror links: more than one place an image can be loaded from (merged from mirrorMaintainer).

``image_cache.link`` stays the main link that pixBoards uses. ``image_mirrors`` lists every known
copy, including that main link, tagged with its host, so a board can fall back if a host goes away.
"""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# host substring -> mirror name
HOSTS = [
    ("catbox.moe", "catbox"),
    ("imgchest.com", "imgchest"),
    ("pinimg.com", "pinterest"),
    ("pixhost", "pixhost"),
    ("bunkr", "bunkr"),
]

PINIMG_ORIGINAL = re.compile(r"^https://i\.pinimg\.com/originals/[0-9a-f/]+\.\w+$")


def detect_host(link: str | None) -> str:
    if not link:
        return "unknown"
    host = urlparse(link).netloc.lower()
    for needle, name in HOSTS:
        if needle in host:
            return name
    return host or "unknown"


def pinterest_original(metadata: dict) -> str | None:
    """The full-size image URL from a gallery-dl Pinterest metadata dict.

    gallery-dl stores the pin's image sizes under ``images``; ``orig`` is the original file
    (``https://i.pinimg.com/originals/..``), which is what we want, not the pin page.
    """
    orig = (metadata.get("images") or {}).get("orig") or {}
    url = orig.get("url") if isinstance(orig, dict) else None
    if url and PINIMG_ORIGINAL.match(url):
        return url
    # fallback: some extractor versions only give the download URL
    url = metadata.get("url")
    return url if isinstance(url, str) and PINIMG_ORIGINAL.match(url) else None


def gallerydl_metadata(directories: list[str]):
    """Yield (media_path, metadata) for every gallery-dl sidecar ``<file>.<ext>.json``."""
    for directory in directories:
        for root, _dirs, files in os.walk(directory):
            names = set(files)
            for name in sorted(files):
                if not name.endswith(".json"):
                    continue
                media = name[: -len(".json")]
                if media not in names:
                    continue
                try:
                    with open(os.path.join(root, name), encoding="utf-8") as f:
                        data = json.load(f)
                except (OSError, ValueError) as e:
                    logger.warning("bad metadata file %s: %s", name, e)
                    continue
                if isinstance(data, dict):
                    yield Path(root, media), data


# --- health ---------------------------------------------------------------------
HALF_LIFE_DAYS = 30  # a mirror last seen alive 30 days ago scores 0.5


def health_score(last_ok_at, fail_count: int, now) -> float | None:
    """0..1, or None if the mirror was never checked.

    Based on when the image was last confirmed to exist: the score halves for every
    ``HALF_LIFE_DAYS`` since then, and halves again for each failed check in a row. A fresh
    upload counts as confirmed at upload time. Mirrors that never passed a check score 0.
    """
    if last_ok_at is None:
        return 0.0 if fail_count else None
    age_days = max((now - last_ok_at).total_seconds(), 0) / 86400
    return round(0.5 ** (age_days / HALF_LIFE_DAYS) * 0.5**fail_count, 3)


def check_link(session, link: str, timeout: int = 20) -> tuple[int | None, bool | None]:
    """(HTTP status, alive). ``alive`` is None when the answer proves nothing.

    A Cloudflare challenge (imgchest's CDN answers scripts with one) is inconclusive, not a
    failure; the checker does not disguise itself as a browser to get past it.
    """
    import requests

    try:
        resp = session.head(link, allow_redirects=True, timeout=timeout)
        if resp.status_code in (405, 501):  # HEAD not supported: fetch the first byte instead
            resp = session.get(link, headers={"Range": "bytes=0-0"}, stream=True, timeout=timeout)
            resp.close()
    except requests.RequestException:
        return None, False
    status = resp.status_code
    if status == 403 and (resp.headers.get("cf-mitigated") or "cloudflare" in resp.headers.get("server", "")):
        return status, None
    if 200 <= status < 300:
        return status, True
    if status == 429:
        return status, None
    return status, False


def check_mirrors(rows, store, session=None, now=None, workers: int = 8, check=check_link) -> dict:
    """Check ``rows`` from ``store.mirrors_to_check()`` and record the results.

    Returns counts: ok, failed, inconclusive.
    """
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime, timezone

    from imguploader.hosts import new_session

    session = session or new_session()
    now = now or datetime.now(timezone.utc)
    counts = {"ok": 0, "failed": 0, "inconclusive": 0}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = pool.map(lambda r: check(session, r[1]), rows)
        for done, ((mirror_id, link, last_ok_at, fail_count), (status, alive)) in enumerate(
            zip(rows, results, strict=True), start=1
        ):
            if alive is True:
                last_ok_at, fail_count = now, 0
                counts["ok"] += 1
            elif alive is False:
                fail_count += 1
                counts["failed"] += 1
                logger.debug("mirror down (%s): %s", status, link)
            else:
                counts["inconclusive"] += 1
            store.record_check(
                mirror_id, status, last_ok_at, fail_count, health_score(last_ok_at, fail_count, now)
            )
            if done % 200 == 0:
                store.commit()
                logger.info("checked %d/%d", done, len(rows))
    store.commit()
    return counts


# --- Pinterest originals from the pin id in the file name ---------------------------
# gallery-dl names pins pinterest_<pinId>.<ext>, or pinterest_<pinId>_<n>.<ext> for one of several
# files in a pin. The pin id finds the pin; gallery-dl -g lists its file URLs without downloading.
PIN_FILE = re.compile(r"^pinterest_(\d+)(?:_\d+)?\.[^.]+$")
PINIMG = re.compile(r"^https://[a-z0-9.-]*pinimg\.com/\S+$")


def pin_id(filename: str) -> str | None:
    m = PIN_FILE.match(filename)
    return m.group(1) if m else None


class PinNotFound(Exception):
    """Pinterest says the pin no longer exists; retrying won't help."""


def resolve_pin(
    pin: str, run=None, timeout: int = 60, retries: int = 2, sleep=None, cookies_from_browser: str | None = None
) -> list[str]:
    """The file URLs of a pin, from ``gallery-dl -g`` (uses the user's gallery-dl config).

    Pins on secret boards answer ``NotFoundError`` to logged-out requests, so without
    ``cookies_from_browser`` (e.g. ``firefox``) they look deleted.

    Pinterest sometimes answers a lookup with nothing (about 30% of one 500-pin batch, while
    the same pins resolved fine minutes later), so an empty answer is retried after 5 s and
    15 s. gallery-dl's own error message is logged, never thrown away. A deleted pin raises
    PinNotFound straight away.
    """
    import subprocess
    import time

    run = run or subprocess.run
    sleep = sleep or time.sleep
    for attempt in range(retries + 1):
        if attempt:
            sleep(5 if attempt == 1 else 15)
        try:
            cmd = ["gallery-dl", "-g", f"https://www.pinterest.com/pin/{pin}/"]
            if cookies_from_browser:
                cmd[1:1] = ["--cookies-from-browser", cookies_from_browser]
            out = run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as e:
            logger.warning("gallery-dl failed for pin %s: %s", pin, e)
            continue
        urls = [u for u in (line.strip() for line in (out.stdout or "").splitlines()) if PINIMG.match(u)]
        if urls:
            return urls
        error = (getattr(out, "stderr", "") or "").strip().splitlines()
        if any("NotFoundError" in line for line in error):
            raise PinNotFound(error[-1])
        logger.debug(
            "pin %s: gallery-dl returned no URLs (attempt %d, exit %s): %s",
            pin,
            attempt + 1,
            getattr(out, "returncode", "?"),
            error[-1] if error else "no error message",
        )
    return []


def matching_url(session, urls: list[str], size: int) -> str | None:
    """The URL whose Content-Length equals the local file's size (a pin can hold several files)."""
    for url in urls:
        try:
            resp = session.head(url, allow_redirects=True, timeout=20)
        except Exception as e:  # network trouble: try the next candidate
            logger.debug("HEAD %s failed: %s", url, e)
            continue
        if resp.status_code == 200 and resp.headers.get("Content-Length") == str(size):
            return url
    return None


def same_file(session, url: str, md5: str) -> bool:
    import hashlib

    h = hashlib.md5()
    with session.get(url, stream=True, timeout=60) as resp:
        if resp.status_code != 200:
            return False
        for chunk in resp.iter_content(1 << 16):
            h.update(chunk)
    return h.hexdigest() == md5
