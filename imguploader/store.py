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
        self._tables = {"cache": table, "posts": posts_table}
        self._names = {
            "cache": sql.Identifier(table),
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
