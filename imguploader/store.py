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
]


class Store(Protocol):
    def known_hashes(self, hashes: list[str]) -> set[str]: ...
    def filename_owner(self, filename: str) -> str | None: ...
    def save_link(self, hash_val: str, link: str, filename: str | None) -> None: ...
    def save_post(self, post_id: str, folder_path: str, sequence: int, image_count: int) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


class PostgresStore:
    def __init__(self, conn, table: str = "image_cache", posts_table: str = "imgchest_posts"):
        from psycopg2 import sql

        self.conn = conn
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

    def ensure_schema(self) -> None:
        for statement in SCHEMA:
            self._execute(statement)
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

    def save_link(self, hash_val, link, filename):
        # Never overwrite a filename that is already set.
        self._execute(
            """INSERT INTO {cache} (hash, link, filename) VALUES (%s, %s, %s)
               ON CONFLICT (hash) DO UPDATE SET
                   link = EXCLUDED.link,
                   filename = COALESCE({cache}.filename, EXCLUDED.filename)""",
            (hash_val, link, filename),
        )

    def save_post(self, post_id, folder_path, sequence, image_count):
        self._execute(
            """INSERT INTO {posts} (post_id, folder_path, sequence, image_count)
               VALUES (%s, %s, %s, %s) ON CONFLICT (post_id) DO NOTHING""",
            (post_id, folder_path, sequence, image_count),
        )

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()
