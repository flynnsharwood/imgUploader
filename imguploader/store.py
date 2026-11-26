"""PostgreSQL link cache, shared with pixBoards (``pixboards --upload --useSaved``).

``image_cache`` has the same columns pixBoards creates, so either tool can fill it.
"""

from __future__ import annotations

from typing import Protocol

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS {cache} (
           hash TEXT PRIMARY KEY,
           link TEXT NOT NULL,
           filename TEXT,
           uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )""",
    "CREATE INDEX IF NOT EXISTS {cache_idx} ON {cache} (filename)",
    """CREATE TABLE IF NOT EXISTS {posts} (
           post_id TEXT PRIMARY KEY,
           folder_path TEXT NOT NULL,
           sequence INTEGER NOT NULL,
           image_count INTEGER NOT NULL,
           uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )""",
    # every known copy of an image (merged from mirrorMaintainer); image_cache.link is one of them
    """CREATE TABLE IF NOT EXISTS image_mirrors (
           id BIGSERIAL PRIMARY KEY,
           hash TEXT NOT NULL,
           mirror_name TEXT NOT NULL,
           link TEXT NOT NULL,
           uploaded_at TIMESTAMPTZ,  -- NULL: unknown (uploaded before times were recorded)
           source_type TEXT,
           CONSTRAINT fk_image_mirrors_hash FOREIGN KEY (hash)
               REFERENCES {cache} (hash) ON DELETE CASCADE
       )""",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_image_mirrors_hash_link ON image_mirrors (hash, link)",
    "CREATE INDEX IF NOT EXISTS idx_image_mirrors_mirror_name ON image_mirrors (mirror_name)",
    # Pinterest pin lookups that found nothing, so deleted pins aren't looked up every day
    """CREATE TABLE IF NOT EXISTS pinterest_lookups (
           pin TEXT PRIMARY KEY,
           hash TEXT,
           status TEXT NOT NULL,
           attempts INTEGER NOT NULL DEFAULT 1,
           last_tried TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )""",
    # imgchest accounts that are out of storage; keyed by a fingerprint of the API key
    """CREATE TABLE IF NOT EXISTS imgchest_accounts (
           key_id TEXT PRIMARY KEY,
           account TEXT NOT NULL,
           reason TEXT,
           flagged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )""",
]

# Columns added after the first release: (table key, column, type). Added only when missing,
# because ALTER TABLE locks the whole table even with IF NOT EXISTS, which deadlocks against a
# running upload.
ADDED_COLUMNS = [
    ("cache", "account", "TEXT"),  # which imgchest account (or "catbox") holds the file
    ("posts", "account", "TEXT"),
    # mirror health (see mirrors.health_score)
    ("mirrors", "last_checked_at", "TIMESTAMPTZ"),
    ("mirrors", "last_ok_at", "TIMESTAMPTZ"),
    ("mirrors", "last_status", "INTEGER"),
    ("mirrors", "fail_count", "INTEGER NOT NULL DEFAULT 0"),
    ("mirrors", "health", "REAL"),
]


class Store(Protocol):
    def known_hashes(self, hashes: list[str]) -> set[str]: ...
    def filename_owner(self, filename: str) -> str | None: ...
    def save_link(
        self, hash_val: str, link: str, filename: str | None, account: str | None = None
    ) -> None: ...
    def save_post(
        self, post_id: str, folder_path: str, sequence: int, image_count: int, account: str | None = None
    ) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


class PostgresStore:
    def __init__(self, conn, table: str = "image_cache", posts_table: str = "imgchest_posts"):
        from psycopg2 import sql

        self.conn = conn
        self._tables = {"cache": table, "posts": posts_table, "mirrors": "image_mirrors"}
        self._names = {
            "cache": sql.Identifier(table),
            "mirrors": sql.Identifier("image_mirrors"),
            "cache_idx": sql.Identifier(f"idx_{table}_filename"),
            "posts": sql.Identifier(posts_table),
        }

    @classmethod
    def connect(cls, dsn: str, **kwargs) -> PostgresStore:
        import psycopg2

        return cls(psycopg2.connect(dsn), **kwargs)

    def _q(self, query: str):
        from psycopg2 import sql

        return sql.SQL(query).format(**self._names)

    def _execute(self, query: str, params=()):
        cur = self.conn.cursor()
        cur.execute(self._q(query), params)
        return cur

    def _columns(self, table: str) -> set[str]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = current_schema() AND table_name = %s",
            (table,),
        )
        return {row[0] for row in cur.fetchall()}

    def ensure_schema(self) -> None:
        for statement in SCHEMA:
            self._execute(statement)
        for key, column, kind in ADDED_COLUMNS:
            if column not in self._columns(self._tables[key]):
                from psycopg2 import sql

                cur = self.conn.cursor()
                cur.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} " + kind).format(
                        self._names[key], sql.Identifier(column)
                    )
                )
        # older mirror tables required uploaded_at; links from before the uploader recorded upload
        # times have none, so it must be allowed to be empty
        cur = self.conn.cursor()
        cur.execute(
            "SELECT is_nullable FROM information_schema.columns WHERE table_schema = current_schema()"
            " AND table_name = 'image_mirrors' AND column_name = 'uploaded_at'"
        )
        row = cur.fetchone()
        if row and row[0] == "NO":
            cur.execute("ALTER TABLE image_mirrors ALTER COLUMN uploaded_at DROP NOT NULL")
        self.conn.commit()

    def known_hashes(self, hashes):
        if not hashes:
            return set()
        cur = self._execute("SELECT hash FROM {cache} WHERE hash = ANY(%s)", (list(hashes),))
        return {row[0] for row in cur.fetchall()}

    def filename_owner(self, filename):
        cur = self._execute("SELECT hash FROM {cache} WHERE filename = %s LIMIT 1", (filename,))
        row = cur.fetchone()
        return row[0] if row else None

    def save_link(self, hash_val, link, filename, account=None):
        # Never overwrite a filename that is already set.
        self._execute(
            """INSERT INTO {cache} (hash, link, filename, account) VALUES (%s, %s, %s, %s)
               ON CONFLICT (hash) DO UPDATE SET
                   link = EXCLUDED.link,
                   filename = COALESCE({cache}.filename, EXCLUDED.filename),
                   account = EXCLUDED.account""",
            (hash_val, link, filename, account),
        )

    def save_post(self, post_id, folder_path, sequence, image_count, account=None):
        self._execute(
            """INSERT INTO {posts} (post_id, folder_path, sequence, image_count, account)
               VALUES (%s, %s, %s, %s, %s) ON CONFLICT (post_id) DO NOTHING""",
            (post_id, folder_path, sequence, image_count, account),
        )

    def links_per_account(self) -> dict[str | None, int]:
        """Read-only; works on tables from before the account column (all rows count as unrecorded)."""
        if "account" not in self._columns(self._tables["cache"]):
            cur = self._execute("SELECT NULL, count(*) FROM {cache}")
        else:
            cur = self._execute("SELECT account, count(*) FROM {cache} GROUP BY account ORDER BY 2 DESC")
        rows = dict(cur.fetchall())
        self.conn.rollback()  # end the read transaction
        return rows

    # --- mirrors ----------------------------------------------------------------
    def add_mirror(self, hash_val, link, source_type, uploaded_at=None, verified=False) -> bool:
        """True if the mirror is new. Part of the current transaction.

        ``verified``: the link was just returned by the host (a fresh upload), so it counts as
        checked and alive now.
        """
        from imguploader.mirrors import detect_host

        cur = self.conn.cursor()
        if verified:
            cur.execute(
                """INSERT INTO image_mirrors (hash, mirror_name, link, uploaded_at, source_type,
                       last_checked_at, last_ok_at, last_status, fail_count, health)
                   VALUES (%s, %s, %s, COALESCE(%s, NOW()), %s, NOW(), NOW(), 200, 0, 1.0)
                   ON CONFLICT (hash, link) DO NOTHING""",
                (hash_val, detect_host(link), link, uploaded_at, source_type),
            )
        else:
            cur.execute(
                """INSERT INTO image_mirrors (hash, mirror_name, link, uploaded_at, source_type)
                   VALUES (%s, %s, %s, %s, %s) ON CONFLICT (hash, link) DO NOTHING""",
                (hash_val, detect_host(link), link, uploaded_at, source_type),
            )
        return cur.rowcount > 0

    # A timestamp shared by this many rows came from adding the uploaded_at column with
    # DEFAULT NOW(), not from uploads (one upload commits at most 20 rows at once).
    BACKFILL_ROWS = 100

    def sync_mirrors(self) -> int:
        """Copy every image_cache link into image_mirrors (what mirrorMaintainer did). Idempotent.

        Upload times that are only the column's backfill default are stored as unknown (NULL),
        also for rows synced earlier.
        """
        backfilled = self._execute(
            "SELECT uploaded_at FROM {cache} GROUP BY uploaded_at HAVING count(*) > %s",
            (self.BACKFILL_ROWS,),
        ).fetchall()
        fake = [row[0] for row in backfilled]
        if fake:
            self.conn.cursor().execute(
                "UPDATE image_mirrors SET uploaded_at = NULL WHERE uploaded_at = ANY(%s)", (fake,)
            )
        cur = self._execute(
            """SELECT c.hash, c.link, c.uploaded_at FROM {cache} c
               WHERE NOT EXISTS (SELECT 1 FROM image_mirrors m WHERE m.hash = c.hash AND m.link = c.link)"""
        )
        rows = cur.fetchall()
        added = sum(self.add_mirror(h, link, "image_cache", None if t in fake else t) for h, link, t in rows)
        self.conn.commit()
        return added

    def hashes_with_mirror(self, host: str) -> set[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT DISTINCT hash FROM image_mirrors WHERE mirror_name = %s", (host,))
        rows = {r[0] for r in cur.fetchall()}
        self.conn.rollback()
        return rows

    def record_pin_lookup(self, pin: str, hash_val: str, status: str) -> None:
        """status: not_found (deleted pin) or failed; part of the current transaction."""
        self.conn.cursor().execute(
            """INSERT INTO pinterest_lookups (pin, hash, status) VALUES (%s, %s, %s)
               ON CONFLICT (pin) DO UPDATE SET status = EXCLUDED.status, hash = EXCLUDED.hash,
                   attempts = pinterest_lookups.attempts + 1, last_tried = NOW()""",
            (pin, hash_val, status),
        )

    def pins_to_skip(self, days: float = 30) -> set[str]:
        """Pins found deleted within the last ``days``."""
        cur = self.conn.cursor()
        cur.execute(
            "SELECT pin FROM pinterest_lookups WHERE status = 'not_found'"
            " AND last_tried > NOW() - make_interval(days => %s)",
            (days,),
        )
        rows = {r[0] for r in cur.fetchall()}
        self.conn.rollback()
        return rows

    def mirrors_to_check(self, host=None, stale_days=7, limit=0):
        """(id, link, last_ok_at, fail_count) for mirrors not checked within ``stale_days``,
        never-checked first, then the longest ago."""
        query = """SELECT id, link, last_ok_at, fail_count FROM image_mirrors
                   WHERE (last_checked_at IS NULL OR last_checked_at < NOW() - make_interval(days => %s))"""
        params = [stale_days]
        if host:
            query += " AND mirror_name = %s"
            params.append(host)
        query += " ORDER BY last_checked_at NULLS FIRST, id"
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        cur = self.conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        self.conn.rollback()
        return rows

    def record_check(self, mirror_id, status, last_ok_at, fail_count, health) -> None:
        self.conn.cursor().execute(
            """UPDATE image_mirrors SET last_checked_at = NOW(), last_status = %s, last_ok_at = %s,
                   fail_count = %s, health = %s WHERE id = %s""",
            (status, last_ok_at, fail_count, health, mirror_id),
        )

    def refresh_health(self, now=None) -> int:
        """Recompute every checked mirror's score, since scores fade as checks get older."""
        from imguploader.mirrors import health_score

        cur = self.conn.cursor()
        cur.execute(
            "SELECT id, last_ok_at, fail_count, NOW() FROM image_mirrors WHERE last_checked_at IS NOT NULL"
        )
        rows = cur.fetchall()
        cur.executemany(
            "UPDATE image_mirrors SET health = %s WHERE id = %s",
            [(health_score(ok, fails, now or db_now), mid) for mid, ok, fails, db_now in rows],
        )
        self.conn.commit()
        return len(rows)

    def mirror_stats(self) -> list[tuple]:
        """(host, mirrors, checked, healthy (score >= 0.5), average score, upload time unknown)."""
        cur = self.conn.cursor()
        cur.execute(
            """SELECT mirror_name, count(*), count(last_checked_at), count(*) FILTER (WHERE health >= 0.5),
                      round(avg(health)::numeric, 2), count(*) FILTER (WHERE uploaded_at IS NULL)
               FROM image_mirrors GROUP BY 1 ORDER BY 2 DESC"""
        )
        rows = cur.fetchall()
        cur.execute(
            "SELECT count(*) FROM (SELECT hash FROM image_mirrors GROUP BY hash HAVING count(*) > 1) t"
        )
        self.multi_mirror_images = cur.fetchone()[0]
        self.conn.rollback()
        return rows

    # --- full accounts (shared with pixBoards) ---------------------------------
    def full_accounts(self) -> dict[str, str]:
        """key fingerprint -> "account, reason, date" for every account flagged full."""
        cur = self.conn.cursor()
        cur.execute("SELECT to_regclass('imgchest_accounts')")
        if cur.fetchone()[0] is None:
            return {}
        cur.execute("SELECT key_id, account, reason, flagged_at FROM imgchest_accounts")
        rows = {k: f"{a}, since {t:%Y-%m-%d}: {r or 'flagged by hand'}" for k, a, r, t in cur.fetchall()}
        self.conn.rollback()
        return rows

    def flag_full(self, key_id: str, account: str, reason: str | None = None) -> None:
        """Committed straight away, so the flag survives a rollback of the upload."""
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO imgchest_accounts (key_id, account, reason) VALUES (%s, %s, %s)
               ON CONFLICT (key_id) DO UPDATE SET account = EXCLUDED.account, reason = EXCLUDED.reason,
                   flagged_at = NOW()""",
            (key_id, account, reason),
        )
        self.conn.commit()

    def unflag(self, key_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM imgchest_accounts WHERE key_id = %s", (key_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()
