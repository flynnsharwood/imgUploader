"""The manifest: a YAML list of folders still waiting to be uploaded.

    folders:
      - path: /pics/artist/set01
        files: [/pics/artist/set01/001.jpg, ...]

``scan`` writes it, ``upload`` works through it and rewrites it with whatever is
left, so an interrupted run picks up where it stopped.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

MEDIA_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4"})

# Folders with these names say nothing about their content (gallery-dl often
# writes ".../<artist>/gallery"), so posts are titled after the parent instead.
GENERIC_FOLDER_NAMES = frozenset({"gallery", "images", "image", "pics"})


@dataclass
class Folder:
    path: str
    files: list[str] = field(default_factory=list)

    @property
    def title(self) -> str:
        return post_title(self.path)


def post_title(folder_path: str) -> str:
    """Folder name, or the parent's name for generic folders like ``gallery``."""
    path = Path(folder_path)
    if path.name.lower() in GENERIC_FOLDER_NAMES and path.parent.name:
        return path.parent.name
    return path.name


def scan(directories: list[str], extensions=MEDIA_EXTENSIONS) -> list[Folder]:
    """Every folder under ``directories`` that directly contains media files."""
    folders: dict[str, list[str]] = {}
    for directory in directories:
        if not os.path.isdir(directory):
            logger.warning("not a directory, skipped: %s", directory)
            continue
        for root, dirs, files in os.walk(directory):
            dirs.sort()
            media = sorted(
                os.path.join(root, f) for f in files if os.path.splitext(f)[1].lower() in extensions
            )
            if media:
                folders.setdefault(root, []).extend(media)
    return [Folder(path, files) for path, files in sorted(folders.items())]


def load(path: str | Path) -> list[Folder]:
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return [Folder(entry["path"], list(entry.get("files") or [])) for entry in data.get("folders", [])]


def save(folders: list[Folder], path: str | Path) -> None:
    """Write atomically, so a crash mid-write never loses the manifest."""
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp")
    data = {"folders": [{"path": f.path, "files": f.files} for f in folders]}
    with open(tmp, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)
    os.replace(tmp, path)


def directories_from_pixboards_config(config_path: str | Path) -> list[str]:
    """The ``directories`` list of a pixBoards config file, with ~ and $VARS expanded."""
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    return [os.path.expandvars(os.path.expanduser(d)) for d in config.get("directories") or []]
