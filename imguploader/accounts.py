"""Several imgchest accounts, one API key each, with failover and a saved "full" flag.

Keys come from the environment (or .env); every variable is its own account:

    IMG_CHEST_API_KEY            the "default" account
    IMG_CHEST_API_KEY_<NAME>     account <name> (lowercased), e.g. IMG_CHEST_API_KEY_MAIN, ..._2
    IMG_CHEST_ACCOUNTS=main,2    optional order to try them in

When imgchest refuses a key (HTTP 401/403) the next account takes over. A 403 that mentions the
storage quota marks the account as full in the database (``imgchest_accounts``), so later runs of
imguploader and pixBoards skip it until it is unflagged. A rate limit (HTTP 429) is never a reason
to switch: it covers the whole connection, not one account.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass

from imguploader.hosts import AuthError, ImgChest, Post

logger = logging.getLogger(__name__)

PREFIX = "IMG_CHEST_API_KEY"
DEFAULT_ACCOUNT = "default"
_VAR = re.compile(rf"^{PREFIX}(?:_(?P<name>[A-Za-z0-9_]+))?$")


@dataclass(frozen=True)
class Key:
    account: str
    var: str  # environment variable it came from
    token: str

    @property
    def masked(self) -> str:
        return mask(self.token)

    @property
    def fingerprint(self) -> str:
        """Stable id for the key that reveals nothing about it; survives renaming the variable."""
        return fingerprint(self.token)


def mask(token: str) -> str:
    return f"{token[:4]}…{token[-4:]}" if len(token) > 12 else "…"


def fingerprint(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()[:16]


def is_storage_full(error: AuthError) -> bool:
    """imgchest's 403 for a full account reads "You've used 36.8 GB of your 10 GB storage."."""
    return error.status == 403 and "storage" in str(error).lower()


def load_keys(environ: Mapping[str, str] | None = None, only: list[str] | None = None) -> list[Key]:
    """All configured keys, in the order they should be tried.

    ``only`` limits (and orders) the accounts, e.g. from ``--account``.
    """
    environ = os.environ if environ is None else environ
    keys = []
    for var, token in environ.items():
        m = _VAR.match(var)
        if m and token.strip():
            keys.append(Key((m.group("name") or DEFAULT_ACCOUNT).lower(), var, token.strip()))

    wanted = [a.strip().lower() for a in (only or []) if a.strip()]
    if wanted:
        unknown = set(wanted) - {k.account for k in keys}
        if unknown:
            raise AuthError(f"no API key for account(s): {', '.join(sorted(unknown))}")
        keys = [k for k in keys if k.account in wanted]
        order = wanted
    else:
        order = [a.strip().lower() for a in environ.get("IMG_CHEST_ACCOUNTS", "").split(",") if a.strip()]

    def rank(k: Key):
        natural = [int(p) if p.isdigit() else p for p in re.split(r"(\d+)", k.account)]
        pos = order.index(k.account) if k.account in order else len(order)
        return (pos, k.account != DEFAULT_ACCOUNT, natural)

    return sorted(keys, key=rank)


class AccountPool:
    """Drop-in for ``ImgChest``: uploads with the first working account, fails over on 401/403.

    ``flags`` (optional) is the store that remembers full accounts: ``full_accounts()`` gives the
    fingerprints to skip, ``flag_full(fingerprint, account, reason)`` records a new one.
    """

    def __init__(self, keys: list[Key], client_factory=ImgChest, flags=None):
        if not keys:
            raise AuthError(f"no imgchest API key: set {PREFIX} (or {PREFIX}_<NAME>) in .env")
        self.keys = list(keys)
        self.client_factory = client_factory
        self.flags = flags
        self._clients: dict[str, object] = {}
        self.retired: dict[str, str] = {}  # var -> reason
        if flags is not None:
            full = flags.full_accounts()
            for k in self.keys:
                if k.fingerprint in full:
                    self.retired[k.var] = f"flagged full: {full[k.fingerprint]}"
                    logger.info("skipping account '%s' (%s): flagged full", k.account, k.var)

    @property
    def current(self) -> Key | None:
        return next((k for k in self.keys if k.var not in self.retired), None)

    def _client(self, key: Key):
        if key.var not in self._clients:
            self._clients[key.var] = self.client_factory(key.token)
        return self._clients[key.var]

    def _retire(self, key: Key, error: AuthError) -> None:
        self.retired[key.var] = str(error)
        full = is_storage_full(error)
        if full and self.flags is not None:
            self.flags.flag_full(key.fingerprint, key.account, str(error))
        nxt = self.current
        logger.warning(
            "imgchest refused account '%s' (%s): %s%s; %s",
            key.account,
            key.var,
            error,
            " -> flagged full" if full else "",
            f"switching to account '{nxt.account}'" if nxt else "no accounts left",
        )

    def create_post(self, title: str, paths: list[str]) -> Post:
        while True:
            key = self.current
            if key is None:
                reasons = "; ".join(f"{v}: {r}" for v, r in self.retired.items())
                raise AuthError(f"no imgchest account left ({reasons})")
            try:
                post = self._client(key).create_post(title, paths)
            except AuthError as e:
                self._retire(key, e)
                continue
            post.account = key.account
            return post
