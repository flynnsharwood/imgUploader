import pytest

from imguploader import manifest
from imguploader.cli import build_parser, cmd_scan, cmd_upload
from imguploader.hashing import md5_file
from imguploader.hosts import AuthError, HostError, ImgChest, Post, RateLimited
from imguploader.manifest import Folder, post_title
from imguploader.uploader import FolderUploader


def write(path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return str(path)


class FakeStore:
    def __init__(self, links=None):
        self.links = dict(links or {})  # hash -> (link, filename)
        self.posts = []
        self.pending = {}
        self.accounts = {}
        self.commits = 0

    def known_hashes(self, hashes):
        return {h for h in hashes if h in self.links}

    def filename_owner(self, filename):
        for h, (_, name) in {**self.links, **self.pending}.items():
            if name == filename:
                return h
        return None

    def save_link(self, hash_val, link, filename, account=None):
        self.pending[hash_val] = (link, filename)
        self.accounts[hash_val] = account

    def save_post(self, post_id, folder_path, sequence, image_count, account=None):
        self.posts.append((post_id, folder_path, sequence, image_count))

    def commit(self):
        self.links.update(self.pending)
        self.pending.clear()
        self.commits += 1

    def rollback(self):
        self.pending.clear()


class FakeHost:
    def __init__(self, fail_on=(), error=HostError):
        self.posts = []
        self.fail_on = set(fail_on)
        self.error = error

    def create_post(self, title, paths):
        n = len(self.posts) + 1
        if n in self.fail_on:
            self.fail_on.discard(n)
            raise self.error(f"post {n} failed")
        self.posts.append((title, list(paths)))
        return Post(f"p{n}", [f"https://cdn/{n}/{i}.jpg" for i in range(len(paths))])


# manifest -------------------------------------------------------------------


def test_scan_finds_every_folder_with_media(tmp_path):
    write(tmp_path / "a" / "1.jpg", b"1")
    write(tmp_path / "a" / "notes.txt", b"x")
    write(tmp_path / "a" / "b" / "2.PNG", b"2")  # a is not a leaf, but still has media
    (tmp_path / "empty").mkdir()
    folders = manifest.scan([str(tmp_path), str(tmp_path / "missing")])
    assert [f.path for f in folders] == [str(tmp_path / "a"), str(tmp_path / "a" / "b")]
    assert folders[0].files == [str(tmp_path / "a" / "1.jpg")]


def test_manifest_round_trip(tmp_path):
    folders = [Folder("/x/ギャラリー", ["/x/ギャラリー/1.jpg"]), Folder("/y", [])]
    manifest.save(folders, tmp_path / "m.yml")
    assert manifest.load(tmp_path / "m.yml") == folders
    assert not (tmp_path / "m.yml.tmp").exists()


@pytest.mark.parametrize(
    "path,title",
    [("/pics/artist/gallery", "artist"), ("/pics/artist/Images", "artist"), ("/pics/set01", "set01")],
)
def test_post_title(path, title):
    assert post_title(path) == title


def test_directories_from_pixboards_config(tmp_path, monkeypatch):
    monkeypatch.setenv("PICS", "/data/pics")
    cfg = tmp_path / "config.yml"
    cfg.write_text("directories:\n  - $PICS/a\n  - ~/b\n")
    dirs = manifest.directories_from_pixboards_config(cfg)
    assert dirs[0] == "/data/pics/a" and not dirs[1].startswith("~")


# uploader -------------------------------------------------------------------


def make_folder(tmp_path, n, name="set"):
    files = [write(tmp_path / name / f"{i:03}.jpg", f"img{name}{i}".encode()) for i in range(n)]
    return Folder(str(tmp_path / name), files)


def test_upload_chunks_into_posts_of_20(tmp_path):
    folder = make_folder(tmp_path, 45)
    store, host = FakeStore(), FakeHost()
    up = FolderUploader(store, host)
    assert up.upload(folder)
    assert [len(p[1]) for p in host.posts] == [20, 20, 5]
    assert [p[0] for p in host.posts] == ["set (1/3)", "set (2/3)", "set (3/3)"]
    assert len(store.links) == 45 and store.commits == 3
    assert store.links[md5_file(folder.files[21])] == ("https://cdn/2/1.jpg", "021.jpg")
    assert up.stats.uploaded == 45


def test_upload_skips_known_and_repeated_images(tmp_path):
    folder = make_folder(tmp_path, 3)
    folder.files.append(write(tmp_path / "set" / "copy.jpg", b"imgset0"))  # same bytes as 000.jpg
    store = FakeStore({md5_file(folder.files[1]): ("old", "001.jpg")})
    host = FakeHost()
    up = FolderUploader(store, host)
    assert up.upload(folder)
    assert host.posts == [("set", [folder.files[0], folder.files[2]])]
    assert up.stats.already_linked == 2


def test_failed_post_keeps_the_others(tmp_path):
    folder = make_folder(tmp_path, 25)
    store, host = FakeStore(), FakeHost(fail_on={1})
    up = FolderUploader(store, host)
    assert not up.upload(folder)
    assert len(store.links) == 5  # the second post was still saved
    # a retry uploads only what is missing
    assert up.upload(folder)
    assert len(host.posts[-1][1]) == 20 and len(store.links) == 25


def test_auth_error_stops(tmp_path):
    store = FakeStore()
    up = FolderUploader(store, FakeHost(fail_on={1}, error=AuthError))
    with pytest.raises(AuthError):
        up.upload(make_folder(tmp_path, 2))
    assert not store.links


def test_duplicate_filename_is_not_saved(tmp_path):
    store = FakeStore({"otherhash": ("old", "000.jpg")})
    up = FolderUploader(store, FakeHost())
    up.upload(make_folder(tmp_path, 1))
    (new_hash,) = set(store.links) - {"otherhash"}
    assert store.links[new_hash][1] is None
    assert up.stats.duplicate_names == [("000.jpg", str(tmp_path / "set" / "000.jpg"))]


def test_catbox_fallback(tmp_path):
    class FakeCatbox:
        def upload(self, path):
            return f"https://files.catbox.moe/{path[-7:]}"

    store = FakeStore()
    up = FolderUploader(store, FakeHost(fail_on={1}), catbox=FakeCatbox())
    assert up.upload(make_folder(tmp_path, 2))
    assert sorted(link for link, _ in store.links.values())[0].startswith("https://files.catbox.moe/")


def test_dry_run_uploads_nothing(tmp_path):
    store, host = FakeStore(), FakeHost()
    assert FolderUploader(store, host, dry_run=True).upload(make_folder(tmp_path, 3))
    assert not host.posts and not store.links


def test_missing_files_are_counted(tmp_path):
    folder = make_folder(tmp_path, 2)
    folder.files.append(str(tmp_path / "set" / "gone.jpg"))
    up = FolderUploader(FakeStore(), FakeHost())
    assert up.upload(folder)
    assert up.stats.missing == 1 and up.stats.uploaded == 2


# imgchest client ------------------------------------------------------------


class Resp:
    def __init__(self, status, data):
        self.status_code, self._data = status, data
        self.ok = status < 400
        self.text = str(data)

    def json(self):
        return self._data


class Session:
    def __init__(self, post_resp, get_resp=None):
        self.headers = {}
        self.post_resp, self.get_resp = post_resp, get_resp
        self.sent = None

    def post(self, url, files, data, timeout):
        self.sent = [(name, f[0], f[2]) for name, f in files]
        return self.post_resp

    def get(self, url, timeout):
        return self.get_resp


def test_imgchest_create_post(tmp_path):
    paths = [write(tmp_path / "a.jpg", b"a"), write(tmp_path / "b.mp4", b"b")]
    images = [{"link": "https://cdn/a.jpg"}, {"link": "https://cdn/b.mp4"}]
    s = Session(Resp(200, {"data": {"id": "xyz"}}), Resp(200, {"data": {"images": images}}))
    post = ImgChest("tok", session=s).create_post("t", paths)
    assert post == Post("xyz", ["https://cdn/a.jpg", "https://cdn/b.mp4"])
    assert s.headers["Authorization"] == "Bearer tok"
    assert s.sent == [("images[]", "a.jpg", "image/jpeg"), ("images[]", "b.mp4", "video/mp4")]


def test_imgchest_errors(tmp_path):
    p = [write(tmp_path / "a.jpg", b"a")]
    with pytest.raises(AuthError):
        ImgChest("tok", session=Session(Resp(403, {"error": "quota"}))).create_post("t", p)
    with pytest.raises(HostError):
        ImgChest("tok", session=Session(Resp(500, {}))).create_post("t", p)
    short = Session(Resp(200, {"data": {"id": "x"}}), Resp(200, {"data": {"images": []}}))
    with pytest.raises(HostError, match="expected 1"):
        ImgChest("tok", session=short).create_post("t", p)
    with pytest.raises(ValueError):
        ImgChest("tok", session=Session(None)).create_post("t", p * 21)
    with pytest.raises(AuthError):
        ImgChest("")


# cli ------------------------------------------------------------------------


def test_scan_then_upload_resumes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    make_folder(tmp_path / "pics", 2, "a")
    make_folder(tmp_path / "pics", 2, "b")
    make_folder(tmp_path / "pics", 2, "c")
    parser = build_parser()
    assert cmd_scan(parser.parse_args(["scan", str(tmp_path / "pics")])) == 0
    assert len(manifest.load("manifest.yml")) == 3

    store, host = FakeStore(), FakeHost(fail_on={2}, error=AuthError)
    args = parser.parse_args(["upload"])
    assert cmd_upload(args, store=store, imgchest=host) == 1
    left = manifest.load("manifest.yml")
    assert [f.path.rsplit("/", 1)[1] for f in left] == ["b", "c"]

    assert cmd_upload(args, store=store, imgchest=host) == 0
    assert manifest.load("manifest.yml") == [] and len(store.links) == 6


def test_rate_limit_parsed(tmp_path):
    p = [write(tmp_path / "a.jpg", b"a")]
    msg = "You can only upload 16 more files this hour. Please try again in 57 minutes."
    with pytest.raises(RateLimited) as e:
        ImgChest("tok", session=Session(Resp(429, {"error": msg}))).create_post("t", p)
    assert e.value.retry_after == 57 * 60


def test_rate_limit_stops_or_waits(tmp_path):
    def limited(n):
        return FakeHost(fail_on={n}, error=lambda m: RateLimited(m, 120))

    folder = make_folder(tmp_path, 25)
    store = FakeStore()
    with pytest.raises(RateLimited):
        FolderUploader(store, limited(2)).upload(folder)
    assert len(store.links) == 20  # the first post was kept

    slept = []
    store = FakeStore()
    up = FolderUploader(store, limited(2), wait=True, sleep=slept.append)
    assert up.upload(folder)
    assert slept == [180] and len(store.links) == 25


# catbox as the main host ------------------------------------------------------


class FakeCatbox:
    userhash = None

    def __init__(self, fail=()):
        self.fail = set(fail)
        self.sent = []

    def upload(self, path):
        if path in self.fail:
            raise HostError("catbox: HTTP 500")
        self.sent.append(path)
        return f"https://files.catbox.moe/{len(self.sent)}.jpg"


def test_catbox_host_uploads_file_by_file(tmp_path):
    folder = make_folder(tmp_path, 3)
    store, cat = FakeStore(), FakeCatbox(fail={folder.files[1]})
    up = FolderUploader(store, None, catbox=cat)
    assert not up.upload(folder)  # one file failed: keep the folder for a retry
    assert len(store.links) == 2 and store.commits == 2
    assert set(store.accounts.values()) == {"catbox"}
    cat.fail.clear()
    assert up.upload(folder)
    assert cat.sent[-1] == folder.files[1] and len(store.links) == 3


def test_cli_catbox_host(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("IMG_CHEST_API_KEY", raising=False)
    make_folder(tmp_path / "pics", 2, "a")
    parser = build_parser()
    cmd_scan(parser.parse_args(["scan", str(tmp_path / "pics")]))
    store, cat = FakeStore(), FakeCatbox()
    monkeypatch.setattr("imguploader.cli.Catbox", lambda userhash: cat)
    assert cmd_upload(parser.parse_args(["upload", "--host", "catbox"]), store=store) == 0
    assert len(cat.sent) == 2 and manifest.load("manifest.yml") == []


# accounts -------------------------------------------------------------------

from imguploader.accounts import AccountPool, fingerprint, load_keys  # noqa: E402

ENV = {
    "IMG_CHEST_API_KEY": "default-key-0000",
    "IMG_CHEST_API_KEY_2": "second-key-22222",
    "IMG_CHEST_API_KEY_10": "tenth-key-101010",
    "IMG_CHEST_API_KEY_ALT": "alt-key-00000000",
    "IMG_CHEST_API_KEY_MAIN": "main-key-1111111",
    "IMG_CHEST_API_KEY_EMPTY": "",
    "OTHER": "x",
}


def test_every_key_is_its_own_account():
    keys = load_keys(ENV)
    assert [k.account for k in keys] == ["default", "2", "10", "alt", "main"]
    ordered = load_keys({**ENV, "IMG_CHEST_ACCOUNTS": "main, alt"})
    assert [k.account for k in ordered][:3] == ["main", "alt", "default"]
    assert [k.account for k in load_keys(ENV, only=["alt", "2"])] == ["alt", "2"]
    assert keys[0].masked == "defa…0000"
    assert keys[0].fingerprint == fingerprint("default-key-0000") and len(keys[0].fingerprint) == 16
    with pytest.raises(AuthError, match="nope"):
        load_keys(ENV, only=["nope"])


class KeyHost:
    """One fake client per key; refuses tokens listed in ``bad`` with (status, message)."""

    def __init__(self, bad):
        self.bad, self.used = bad, []

    def __call__(self, token):
        host = self

        class Client:
            def create_post(self, title, paths):
                host.used.append(token)
                if token in host.bad:
                    status, msg = host.bad[token]
                    raise AuthError(f"HTTP {status} {msg}", status)
                return Post("p", ["l"] * len(paths))

        return Client()


class Flags:
    def __init__(self, full=None):
        self.full = dict(full or {})

    def full_accounts(self):
        return dict(self.full)

    def flag_full(self, key_id, account, reason):
        self.full[key_id] = f"{account}: {reason}"


QUOTA = "You've used 10.2 GB of your 10 GB storage."


def test_pool_fails_over_and_flags_full_accounts():
    keys = load_keys(ENV, only=["main", "alt", "2"])
    flags = Flags()
    hosts = KeyHost({"main-key-1111111": (403, QUOTA), "alt-key-00000000": (401, "Unauthenticated")})
    pool = AccountPool(keys, client_factory=hosts, flags=flags)
    assert pool.create_post("t", ["a"]).account == "2"
    assert hosts.used == ["main-key-1111111", "alt-key-00000000", "second-key-22222"]
    # only the storage refusal is remembered; a bad key is just skipped for this run
    assert list(flags.full) == [fingerprint("main-key-1111111")]

    # the next run skips the full account without trying it
    hosts = KeyHost({})
    pool = AccountPool(keys, client_factory=hosts, flags=flags)
    assert pool.create_post("t", ["a"]).account == "alt"
    assert hosts.used == ["alt-key-00000000"]

    hosts = KeyHost({k.token: (403, QUOTA) for k in keys})
    with pytest.raises(AuthError, match="no imgchest account left"):
        AccountPool(keys, client_factory=hosts).create_post("t", ["a"])


def test_cli_accounts_flag_and_unflag(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    for var in [v for v in __import__("os").environ if v.startswith("IMG_CHEST")]:
        monkeypatch.delenv(var)
    monkeypatch.setenv("IMG_CHEST_API_KEY_MAIN", "main-key-1111111")
    monkeypatch.setenv("IMG_CHEST_API_KEY_ALT", "alt-key-00000000")

    class Store(Flags):
        def ensure_schema(self):
            pass

        def unflag(self, key_id):
            return self.full.pop(key_id, None) is not None

        def links_per_account(self):
            return {"main": 3, None: 7}

    from imguploader.cli import cmd_accounts

    store = Store()
    parser = build_parser()
    assert cmd_accounts(parser.parse_args(["accounts", "--flag-full", "main"]), store=store) == 0
    out = capsys.readouterr().out
    assert "flagged full" in out and "FULL" in out and "main-key" not in out
    assert cmd_accounts(parser.parse_args(["accounts", "--unflag", "MAIN"]), store=store) == 0
    assert not store.full
    assert cmd_accounts(parser.parse_args(["accounts", "--flag-full", "nope"]), store=store) == 2


def test_pool_does_not_switch_on_rate_limit():
    keys = load_keys(ENV, only=["main", "alt"])

    def factory(token):
        class Client:
            def create_post(self, title, paths):
                raise RateLimited("HTTP 429", 60)

        return Client()

    pool = AccountPool(keys, client_factory=factory)
    with pytest.raises(RateLimited):
        pool.create_post("t", ["a"])
    assert not pool.retired


def test_catbox_retries_empty_replies(tmp_path):
    from imguploader.hosts import Catbox

    replies = [Resp(200, ""), Resp(502, "bad gateway"), Resp(200, "https://files.catbox.moe/ab.jpg")]

    class CatSession:
        def post(self, url, data, files, timeout):
            return replies.pop(0)

    for r in replies:
        r.text = r._data
    slept = []
    cat = Catbox(session=CatSession(), sleep=slept.append)
    assert cat.upload(write(tmp_path / "a.jpg", b"a")) == "https://files.catbox.moe/ab.jpg"
    assert slept == [5, 10]

    bad = Resp(400, "No file")
    bad.text = "No file"
    replies[:] = [bad]
    with pytest.raises(HostError, match="No file"):
        cat.upload(str(tmp_path / "a.jpg"))


# mirrors --------------------------------------------------------------------

from imguploader.mirrors import detect_host, gallerydl_metadata, pinterest_original  # noqa: E402


@pytest.mark.parametrize(
    "link,host",
    [
        ("https://files.catbox.moe/ab.jpg", "catbox"),
        ("https://cdn.imgchest.com/files/x.jpg", "imgchest"),
        ("https://i.pinimg.com/originals/aa/bb/cc/abc.jpg", "pinterest"),
        ("https://img1.pixhost.to/x.jpg", "pixhost"),
        ("https://example.org/x.jpg", "example.org"),
        ("", "unknown"),
    ],
)
def test_detect_host(link, host):
    assert detect_host(link) == host


def test_pinterest_original():
    orig = "https://i.pinimg.com/originals/1a/2b/3c/1a2b3c4d.jpg"
    meta = {"images": {"orig": {"url": orig}, "236x": {"url": "https://i.pinimg.com/236x/1a.jpg"}}}
    assert pinterest_original(meta) == orig
    assert pinterest_original({"url": orig}) == orig
    # the pin page and resized copies are not originals
    assert pinterest_original({"url": "https://www.pinterest.com/pin/123/"}) is None
    assert pinterest_original({"images": {"orig": {"url": "https://i.pinimg.com/736x/1a.jpg"}}}) is None


def test_gallerydl_metadata_pairs_sidecars(tmp_path):
    img = write(tmp_path / "b" / "pinterest_1.jpg", b"x")
    (tmp_path / "b" / "pinterest_1.jpg.json").write_text('{"id": 1}')
    (tmp_path / "b" / "orphan.png.json").write_text("{}")  # no media file
    (tmp_path / "b" / "broken.jpg").write_bytes(b"y")
    (tmp_path / "b" / "broken.jpg.json").write_text("{not json")
    assert [(str(p), d) for p, d in gallerydl_metadata([str(tmp_path)])] == [(img, {"id": 1})]


def test_uploads_are_recorded_as_mirrors(tmp_path):
    class MirrorStore(FakeStore):
        def __init__(self):
            super().__init__()
            self.mirrors = []

        def add_mirror(self, hash_val, link, source_type, uploaded_at=None, verified=False):
            self.mirrors.append((link, source_type, verified))
            return True

    store = MirrorStore()
    FolderUploader(store, FakeHost()).upload(make_folder(tmp_path, 2))
    # a fresh upload counts as checked and alive
    assert store.mirrors == [("https://cdn/1/0.jpg", "upload", True), ("https://cdn/1/1.jpg", "upload", True)]


# mirror health --------------------------------------------------------------

from datetime import datetime, timedelta, timezone  # noqa: E402

from imguploader.mirrors import check_link, check_mirrors, health_score  # noqa: E402

NOW = datetime(2026, 11, 26, tzinfo=timezone.utc)


def test_health_score():
    assert health_score(None, 0, NOW) is None  # never checked
    assert health_score(None, 2, NOW) == 0.0  # checked, never seen alive
    assert health_score(NOW, 0, NOW) == 1.0
    assert health_score(NOW - timedelta(days=30), 0, NOW) == 0.5
    assert health_score(NOW - timedelta(days=60), 0, NOW) == 0.25
    assert health_score(NOW - timedelta(days=30), 1, NOW) == 0.25  # alive then, failed since
    assert health_score(NOW + timedelta(days=1), 0, NOW) == 1.0  # clock skew


class HeadResp:
    def __init__(self, status, headers=None):
        self.status_code, self.headers = status, headers or {}

    def close(self):
        pass


class HeadSession:
    def __init__(self, answers):
        self.answers = answers  # link -> HeadResp or exception
        self.gets = []

    def head(self, link, allow_redirects, timeout):
        answer = self.answers[link]
        if isinstance(answer, Exception):
            raise answer
        return answer

    def get(self, link, headers, stream, timeout):
        self.gets.append(link)
        return HeadResp(206)


def test_check_link():
    import requests

    s = HeadSession(
        {
            "ok": HeadResp(200),
            "gone": HeadResp(404),
            "cf": HeadResp(403, {"server": "cloudflare", "cf-mitigated": "challenge"}),
            "denied": HeadResp(403, {"server": "nginx"}),
            "nohead": HeadResp(405),
            "slow": requests.Timeout("timed out"),
            "busy": HeadResp(429),
        }
    )
    assert check_link(s, "ok") == (200, True)
    assert check_link(s, "gone") == (404, False)
    assert check_link(s, "cf") == (403, None)  # a challenge page proves nothing
    assert check_link(s, "denied") == (403, False)
    assert check_link(s, "nohead") == (206, True) and s.gets == ["nohead"]
    assert check_link(s, "slow") == (None, False)
    assert check_link(s, "busy") == (429, None)


def test_check_mirrors_updates_health():
    class Store:
        def __init__(self):
            self.checks, self.commits = {}, 0

        def record_check(self, mirror_id, status, last_ok_at, fail_count, health):
            self.checks[mirror_id] = (status, last_ok_at, fail_count, health)

        def commit(self):
            self.commits += 1

    week_ago = NOW - timedelta(days=7)
    rows = [(1, "ok", None, 0), (2, "gone", week_ago, 1), (3, "cf", week_ago, 0)]
    answers = {"ok": (200, True), "gone": (404, False), "cf": (403, None)}
    store = Store()
    counts = check_mirrors(rows, store, session=object(), now=NOW, check=lambda s, link: answers[link])
    assert counts == {"ok": 1, "failed": 1, "inconclusive": 1}
    assert store.checks[1] == (200, NOW, 0, 1.0)
    assert store.checks[2][1:3] == (week_ago, 2)  # last seen alive a week ago, second failure
    assert store.checks[3][1:3] == (week_ago, 0)  # inconclusive: unchanged
    assert store.checks[2][3] == health_score(week_ago, 2, NOW)


def test_clients_name_themselves():
    from imguploader.hosts import Catbox, new_session

    agent = new_session().headers["User-Agent"]
    assert agent.startswith("imguploader/") and "Mozilla" not in agent
    assert Catbox().session.headers["User-Agent"] == agent


# Pinterest originals by pin id -------------------------------------------------

from imguploader.mirrors import matching_url, pin_id, resolve_pin  # noqa: E402


def test_pin_id():
    assert pin_id("pinterest_969610994795192841.jpg") == "969610994795192841"
    assert pin_id("pinterest_969610994783863728_5408156139806317195.mp4") == "969610994783863728"
    assert pin_id("holiday.jpg") is None and pin_id("pinterest_abc.jpg") is None


def test_resolve_pin_runs_gallery_dl():
    calls = []

    class Out:
        stdout = (
            "https://i.pinimg.com/originals/ba/bf/f9/x.jpg\nnot a url\nhttps://v1.pinimg.com/videos/a.mp4\n"
        )

    def run(cmd, **kw):
        calls.append(cmd)
        return Out()

    assert resolve_pin("123", run=run) == [
        "https://i.pinimg.com/originals/ba/bf/f9/x.jpg",
        "https://v1.pinimg.com/videos/a.mp4",
    ]
    assert calls == [["gallery-dl", "-g", "https://www.pinterest.com/pin/123/"]]

    def broken(cmd, **kw):
        raise OSError("gallery-dl not found")

    assert resolve_pin("123", run=broken) == []


def test_matching_url_picks_the_same_size():
    class S:
        def head(self, url, allow_redirects, timeout):
            return HeadResp(200, {"Content-Length": {"a": "10", "b": "42"}[url]})

    assert matching_url(S(), ["a", "b"], 42) == "b"
    assert matching_url(S(), ["a"], 42) is None


def test_cli_pin_lookup(tmp_path, monkeypatch):
    import imguploader.mirrors as m
    from imguploader.cli import cmd_mirrors

    known = write(tmp_path / "board" / "pinterest_111.jpg", b"known image")
    write(tmp_path / "board" / "pinterest_222.jpg", b"not uploaded yet")
    write(tmp_path / "board" / "other.jpg", b"no pin id")
    hash_known = md5_file(known)

    class Store:
        def __init__(self):
            self.mirrors = []

        def known_hashes(self, hashes):
            return {h for h in hashes if h == hash_known}

        def hashes_with_mirror(self, host):
            return set()

        def add_mirror(self, hash_val, link, source_type, uploaded_at=None, verified=False):
            self.mirrors.append((hash_val, link, source_type, verified))
            return True

        def commit(self):
            pass

        def mirror_stats(self):
            return []

    looked_up = []
    monkeypatch.setattr(
        m, "resolve_pin", lambda pin: looked_up.append(pin) or ["https://i.pinimg.com/originals/a.jpg"]
    )
    monkeypatch.setattr(m, "matching_url", lambda session, urls, size: urls[0])
    store = Store()
    args = build_parser().parse_args(["mirrors", "pinterest", str(tmp_path), "--delay", "0"])
    assert cmd_mirrors(args, store=store) == 0
    assert looked_up == ["111"]  # only images already in image_cache, only pinterest_ names
    assert store.mirrors == [(hash_known, "https://i.pinimg.com/originals/a.jpg", "pin-lookup", True)]
