"""Several imgchest accounts and API keys, with failover.

Keys come from the environment (or .env):

    IMG_CHEST_API_KEY                 the "default" account
    IMG_CHEST_API_KEY_<ACCOUNT>       a named account, e.g. IMG_CHEST_API_KEY_MAIN
    IMG_CHEST_API_KEY_<ACCOUNT>_<N>   more keys for the same account (IMG_CHEST_API_KEY_MAIN_2);
                                      a bare number (IMG_CHEST_API_KEY_2) is another default key
    IMG_CHEST_ACCOUNTS=main,alt       optional order to try the accounts in

When a key is rejected (HTTP 401) only that key is retired. When an account is refused (HTTP 403,
usually the storage quota) all of its keys are. The next key or account takes over. A rate limit
(HTTP 429) is never a reason to switch: it covers the whole connection, not one account.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass

from imguploader.hosts import AuthError, ImgChest, Post

logger = logging.getLogger(__name__)

PREFIX = "IMG_CHEST_API_KEY"
DEFAULT_ACCOUNT = "default"
_VAR = re.compile(rf"^{PREFIX}(?:_(?P<label>[A-Za-z0-9_]+))?$")


@dataclass(frozen=True)
class Key:
    account: str
    var: str  # environment variable it came from
    token: str

    @property
    def masked(self) -> str:
        return mask(self.token)


def mask(token: str) -> str:
    return f"{token[:4]}…{token[-4:]}" if len(token) > 12 else "…"


def _account_of(label: str | None) -> str:
    if not label:
        return DEFAULT_ACCOUNT
    name = re.sub(r"_\d+$", "", label)
    if not name or name.isdigit():
        return DEFAULT_ACCOUNT
    return name.lower()


def _natural(var: str):
    return [int(p) if p.isdigit() else p for p in re.split(r"(\d+)", var)]


def load_keys(environ: Mapping[str, str] | None = None, only: list[str] | None = None) -> list[Key]:
    """All configured keys, in the order they should be tried.

    ``only`` limits (and orders) the accounts, e.g. from ``--account``.
    """
    environ = os.environ if environ is None else environ
    keys = []
    for var, token in environ.items():
        m = _VAR.match(var)
        if m and token.strip():
            keys.append(Key(_account_of(m.group("label")), var, token.strip()))

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
        pos = order.index(k.account) if k.account in order else len(order)
        default_first = k.account != DEFAULT_ACCOUNT
        return (pos, default_first, k.account, _natural(k.var))

    return sorted(keys, key=rank)


def accounts(keys: list[Key]) -> dict[str, list[Key]]:
    grouped: dict[str, list[Key]] = {}
    for k in keys:
        grouped.setdefault(k.account, []).append(k)
    return grouped


class AccountPool:
    """Drop-in for ``ImgChest``: uploads with the first working key, fails over on 401/403."""

    def __init__(self, keys: list[Key], client_factory=ImgChest):
        if not keys:
            raise AuthError(f"no imgchest API key: set {PREFIX} (or {PREFIX}_<ACCOUNT>) in .env")
        self.keys = list(keys)
        self.client_factory = client_factory
        self._clients: dict[str, object] = {}
        self.retired: dict[str, str] = {}  # var -> reason

    def _live(self) -> list[Key]:
        return [k for k in self.keys if k.var not in self.retired]

    @property
    def current(self) -> Key | None:
        live = self._live()
        return live[0] if live else None

    def _client(self, key: Key):
        if key.var not in self._clients:
            self._clients[key.var] = self.client_factory(key.token)
        return self._clients[key.var]

    def _retire(self, key: Key, error: AuthError) -> None:
        if error.status == 403:
            # the account itself is refused (storage quota): its other keys would be too
            for k in self.keys:
                if k.account == key.account:
                    self.retired.setdefault(k.var, str(error))
            what = f"account '{key.account}'"
        else:
            self.retired[key.var] = str(error)
            what = f"key {key.var} ({key.masked})"
        nxt = self.current
        logger.warning(
            "imgchest refused %s: %s; %s",
            what,
            error,
            f"switching to {nxt.var} (account '{nxt.account}')" if nxt else "no keys left",
        )

    def create_post(self, title: str, paths: list[str]) -> Post:
        while True:
            key = self.current
            if key is None:
                reasons = "; ".join(f"{v}: {r}" for v, r in self.retired.items())
                raise AuthError(f"every imgchest key was refused ({reasons})")
            try:
                post = self._client(key).create_post(title, paths)
            except AuthError as e:
                self._retire(key, e)
                continue
            post.account = key.account
            return post
