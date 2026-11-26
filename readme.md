# imguploader

imguploader bulk-uploads folders of images and videos to [imgchest](https://imgchest.com). It
records every link in PostgreSQL, keyed by the file's MD5 hash.

It works alongside [pixBoards](https://github.com/flynnsharwood/pixBoards). pixBoards reads the
same `image_cache` table (`pixboards --upload --useSaved`), so after one long upload run here,
later board builds don't need to upload anything.

- **Resumable.**
  - `scan` writes a manifest listing the folders to upload.
  - `upload` works through the manifest, commits after every post and rewrites the manifest with
    whatever is left.
  - You can interrupt it at any time and run it again to continue.
- **Never uploads the same image twice.** It hashes every image first and skips any that are
  already in the cache or appear twice in one folder.
- **Readable on imgchest.** Each folder becomes one post, titled after the folder. Folders with
  more than 20 images (imgchest's limit) are split into several posts. Generic gallery-dl folder
  names such as `gallery` use the parent folder's name instead.
- **Fails safely.**
  - When a post fails, its folder stays in the manifest for the next run, which retries only the
    missing images.
  - A bad token or a full quota (HTTP 401/403) stops the run at once.
  - imgchest also limits uploads per hour (about 250 files). When it answers HTTP 429 the run
    stops and keeps the rest in the manifest; with `--wait` it sleeps until the limit resets and
    carries on, so one run can work through a whole library.
  - With `--catbox-fallback`, images from a failed post go to catbox instead.

## Install

```sh
pipx install git+https://github.com/flynnsharwood/imgUploader
# or, from a clone:  pip install -e ".[test]"
```

Copy `.env.example` to `.env` and fill in two values:
- `IMG_CHEST_API_KEY`, from your imgchest profile;
- `DATABASE_URL`, for your PostgreSQL database.

The tables are created on the first run.

## Use

```sh
imguploader scan ~/pics/artists ~/gallery-dl/pinterest     # or: scan -c path/to/pixboards.yml
imguploader upload --dry-run                               # show what would be uploaded
imguploader upload                                         # upload; run again to resume
imguploader upload --limit 50 --catbox-fallback
imguploader upload --wait                                  # wait out the hourly limit
```

Logs go to `./logs/imguploader_<date>.log`.

## Tables

| table            | columns                                                            |
|------------------|--------------------------------------------------------------------|
| `image_cache`    | `hash` (MD5, primary key), `link`, `filename`, `uploaded_at`       |
| `imgchest_posts` | `post_id`, `folder_path`, `sequence`, `image_count`, `uploaded_at` |

`filename` is filled only if no other image already uses that name, so pixBoards can also look
up links by filename.

## Layout

```
imguploader/
  cli.py        argument parsing, logging, the scan and upload commands
  manifest.py   folder scanning, post titles, atomic YAML manifest
  uploader.py   per-folder logic: hash, skip, split into posts, save, track retries
  hosts.py      imgchest and catbox clients
  store.py      PostgreSQL link cache (same schema as pixBoards)
  hashing.py    streaming MD5
tests/          pytest suite with fake hosts and an in-memory store
```

Go easy on the hosts: an account can be banned if bulk uploads look like abuse.

## To do
- [ ] Add images to an existing imgchest post (using the stored post id) instead of starting a
      new post.
