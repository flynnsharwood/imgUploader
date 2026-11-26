"""Upload one manifest folder: hash, skip known images, post in chunks of 20, save links."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field

from imguploader.hashing import md5_file
from imguploader.hosts import MAX_IMAGES_PER_POST, Catbox, HostError, ImgChest, RateLimited, StopUploads
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
        imgchest: ImgChest | None,
        catbox: Catbox | None = None,
        dry_run: bool = False,
        wait: bool = False,
        sleep=time.sleep,
    ):
        self.store = store
        self.imgchest = imgchest
        self.catbox = catbox  # imgchest=None: catbox is the main host, else only a fallback
        if imgchest is None and catbox is None and not dry_run:
            raise ValueError("need an imgchest client, a catbox client, or dry_run")
        self.dry_run = dry_run
        self.wait = wait  # on HTTP 429, sleep until the limit resets instead of stopping
        self.sleep = sleep
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

    def _save(self, path: str, hash_val: str, link: str, account: str | None) -> None:
        filename = os.path.basename(path)
        owner = self.store.filename_owner(filename)
        if owner is not None and owner != hash_val:
            self.stats.duplicate_names.append((filename, path))
            filename = None
        self.store.save_link(hash_val, link, filename, account)
        self.stats.uploaded += 1

    def upload(self, folder: Folder) -> bool:
        """Returns False if the folder should stay in the manifest for another try.

        Every post is committed on its own, so a failure halfway through a big folder
        only repeats the chunks that did not make it. StopUploads (bad token, full quota,
        rate limit) is re-raised: there is no point trying the next folder. With
        ``wait``, a rate limit is waited out and the same post is tried again.
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

        if self.imgchest is None and not self.dry_run:
            return self._upload_to_catbox(folder, todo, hashes)

        chunks = list(chunked(todo, MAX_IMAGES_PER_POST))
        logger.info("[folder] %s: %d to upload in %d post(s)", folder.path, len(todo), len(chunks))
        if self.dry_run:
            self.stats.folders_done += 1
            return True

        ok = True
        for seq, chunk in enumerate(chunks, start=1):
            title = folder.title if len(chunks) == 1 else f"{folder.title} ({seq}/{len(chunks)})"
            while True:
                try:
                    self._upload_chunk(folder, seq, title, chunk, hashes)
                    self.store.commit()
                    break
                except RateLimited as e:
                    self.store.rollback()
                    if not self.wait:
                        raise
                    logger.info("rate limited, waiting %d min: %s", e.retry_after // 60 + 1, e)
                    self.sleep(e.retry_after + 60)
                except StopUploads:
                    self.store.rollback()
                    raise
                except HostError as e:
                    self.store.rollback()
                    logger.warning("[fail] %s post %d/%d: %s", folder.path, seq, len(chunks), e)
                    ok = False
                    break

        if ok:
            self.stats.folders_done += 1
            logger.info("[ok] %s", folder.path)
        else:
            self.stats.folders_failed += 1
        return ok

    def _upload_chunk(self, folder, seq, title, chunk, hashes):
        try:
            post = self.imgchest.create_post(title, chunk)
        except StopUploads:
            raise
        except HostError as e:
            if not self.catbox:
                raise
            logger.info("imgchest failed (%s); uploading %d file(s) to catbox", e, len(chunk))
            for path in chunk:
                self._save(path, hashes[path], self.catbox.upload(path), "catbox")
            return
        self.store.save_post(post.id, folder.path, seq, len(post.links), post.account)
        for path, link in zip(chunk, post.links, strict=True):
            self._save(path, hashes[path], link, post.account)

    def _upload_to_catbox(self, folder: Folder, todo: list[str], hashes: dict[str, str]) -> bool:
        """Catbox takes one file per request; each one is committed on its own."""
        logger.info("[folder] %s: %d to upload to catbox", folder.path, len(todo))
        failed = 0
        for path in todo:
            while True:
                try:
                    link = self.catbox.upload(path)
                    self._save(path, hashes[path], link, "catbox")
                    self.store.commit()
                    break
                except RateLimited as e:
                    self.store.rollback()
                    if not self.wait:
                        raise
                    logger.info("rate limited, waiting %d min: %s", e.retry_after // 60 + 1, e)
                    self.sleep(e.retry_after + 60)
                except StopUploads:
                    self.store.rollback()
                    raise
                except HostError as e:
                    self.store.rollback()
                    logger.warning("[fail] %s: %s", path, e)
                    failed += 1
                    break
        if failed:
            self.stats.folders_failed += 1
            return False
        self.stats.folders_done += 1
        logger.info("[ok] %s", folder.path)
        return True
