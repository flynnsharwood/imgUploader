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
