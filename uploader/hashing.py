import hashlib


def compute_md5(image_path):
    h = hashlib.md5()
    with open(image_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def get_existing_hashes(cur, hashes):
    if not hashes:
        return set()

    cur.execute("SELECT hash FROM public.image_cache WHERE hash = ANY(%s)", (hashes,))
    return {row[0] for row in cur.fetchall()}
