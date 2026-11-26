"""Upload one manifest folder: hash, skip known images, post in chunks of 20, save links."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

from imguploader.hashing import md5_file
from imguploader.hosts import MAX_IMAGES_PER_POST, AuthError, Catbox, HostError, ImgChest
from imguploader.manifest import Folder
from imguploader.store import Store

logger = logging.getLogger(__name__)


@dataclass
class Stats:
    folders_done: int = 0
    folders_failed: int = 0
    uploaded: int = 0
    already_linked: int = 0
    missing: int = 0
    # (filename, path): name already belongs to another image, so the filename column stays empty
    duplicate_names: list[tuple[str, str]] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"{self.folders_done} folders done, {self.folders_failed} failed; "
            f"{self.uploaded} images uploaded, {self.already_linked} already linked, "
            f"{self.missing} missing or unreadable"
        )


def chunked(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


class FolderUploader:
    def __init__(
        self,
        store: Store,
        imgchest: ImgChest,
        catbox: Catbox | None = None,
        dry_run: bool = False,
    ):
        self.store = store
        self.imgchest = imgchest
        self.catbox = catbox
        self.dry_run = dry_run
        self.stats = Stats()

    def _hash_files(self, folder: Folder) -> dict[str, str]:
        hashes = {}
        for path in folder.files:
            try:
                hashes[path] = md5_file(path)
            except OSError as e:
                logger.warning("cannot read %s: %s", path, e)
                self.stats.missing += 1
        return hashes

    def _save(self, path: str, hash_val: str, link: str) -> None:
        filename = os.path.basename(path)
        owner = self.store.filename_owner(filename)
        if owner is not None and owner != hash_val:
            self.stats.duplicate_names.append((filename, path))
            filename = None
        self.store.save_link(hash_val, link, filename)
        self.stats.uploaded += 1

    def upload(self, folder: Folder) -> bool:
        """Returns False if the folder should stay in the manifest for another try.

        Every post is committed on its own, so a failure halfway through a big folder
        only repeats the chunks that did not make it. AuthError is re-raised: with a
        bad token or a full quota, there is no point trying the next folder.
        """
        hashes = self._hash_files(folder)
        known = self.store.known_hashes(list(set(hashes.values())))
        todo, seen = [], set(known)
        for path, h in hashes.items():
            if h in seen:
                self.stats.already_linked += 1
            else:
                seen.add(h)  # the same image twice in one folder is uploaded once
                todo.append(path)

        if not todo:
            logger.info("[skip] %s: all %d images already linked", folder.path, len(hashes))
            self.stats.folders_done += 1
            return True

        chunks = list(chunked(todo, MAX_IMAGES_PER_POST))
        logger.info("[folder] %s: %d to upload in %d post(s)", folder.path, len(todo), len(chunks))
        if self.dry_run:
            self.stats.folders_done += 1
            return True

        ok = True
        for seq, chunk in enumerate(chunks, start=1):
            title = folder.title if len(chunks) == 1 else f"{folder.title} ({seq}/{len(chunks)})"
            try:
                self._upload_chunk(folder, seq, title, chunk, hashes)
                self.store.commit()
            except AuthError:
                self.store.rollback()
                raise
            except HostError as e:
                self.store.rollback()
                logger.warning("[fail] %s post %d/%d: %s", folder.path, seq, len(chunks), e)
                ok = False

        if ok:
            self.stats.folders_done += 1
            logger.info("[ok] %s", folder.path)
        else:
            self.stats.folders_failed += 1
        return ok

    def _upload_chunk(self, folder, seq, title, chunk, hashes):
        try:
            post = self.imgchest.create_post(title, chunk)
        except AuthError:
            raise
        except HostError as e:
            if not self.catbox:
                raise
            logger.info("imgchest failed (%s); uploading %d file(s) to catbox", e, len(chunk))
            for path in chunk:
                self._save(path, hashes[path], self.catbox.upload(path))
            return
        self.store.save_post(post.id, folder.path, seq, len(post.links))
        for path, link in zip(chunk, post.links, strict=True):
            self._save(path, hashes[path], link)
