import pytest

from imguploader import manifest
from imguploader.cli import build_parser, cmd_scan, cmd_upload
from imguploader.hashing import md5_file
from imguploader.hosts import AuthError, HostError, ImgChest, Post
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
        self.commits = 0

    def known_hashes(self, hashes):
        return {h for h in hashes if h in self.links}

    def filename_owner(self, filename):
        for h, (_, name) in {**self.links, **self.pending}.items():
            if name == filename:
                return h
        return None

    def save_link(self, hash_val, link, filename):
        self.pending[hash_val] = (link, filename)

    def save_post(self, post_id, folder_path, sequence, image_count):
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
