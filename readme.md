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
- **Several accounts.** Each key in `.env` is a separate account, as described below.
  - If imgchest refuses a key (401/403), the next account takes over.
  - An account that runs out of storage is flagged full in the database, and later runs skip it.
    pixBoards uses the same flags.
  - `imguploader accounts` lists each account with its masked key, link count and full flag.
- **Mirror links.** Every known copy of an image is listed in `image_mirrors`, tagged with its
  host, so a board can fall back if a host disappears. This was merged from mirrorMaintainer.
  - New uploads are recorded there automatically.
  - `mirrors sync` copies in the links already in `image_cache`.
  - `mirrors pinterest` adds each pin's original image (`i.pinimg.com/originals/...`) from
    gallery-dl's metadata files.
- **Two hosts.** `--host catbox` uploads everything to [catbox](https://catbox.moe) instead of
  imgchest, one file per request, and retries catbox's occasional empty replies.

## Install

```sh
pipx install git+https://github.com/flynnsharwood/imgUploader
# or, from a clone:  pip install -e ".[test]"
```

Copy `.env.example` to `.env` and fill in two values:
- `IMG_CHEST_API_KEY`, from your imgchest profile;
- `DATABASE_URL`, for your PostgreSQL database.

For more accounts, add one line each:

```sh
IMG_CHEST_API_KEY_MAIN=...        # account "main"
IMG_CHEST_API_KEY_2=...           # account "2"
IMG_CHEST_ACCOUNTS=main,2         # optional order (default: IMG_CHEST_API_KEY first, then by name)
```

The tables are created on the first run.

## Use

```sh
imguploader scan ~/pics/artists ~/gallery-dl/pinterest     # or: scan -c path/to/pixboards.yml
imguploader upload --dry-run                               # show what would be uploaded
imguploader upload                                         # upload; run again to resume
imguploader upload --limit 50 --catbox-fallback
imguploader upload --wait                                  # wait out the hourly limit
imguploader upload --account alt                           # only use account "alt"
imguploader upload --host catbox                           # upload to catbox instead
imguploader accounts                                       # list accounts, keys, links, full flags
imguploader accounts --flag-full main                      # skip "main" (out of storage)
imguploader accounts --unflag main                         # use it again after freeing space
imguploader mirrors                                        # mirror counts per host
imguploader mirrors sync                                   # add image_cache links as mirrors
imguploader mirrors pinterest ~/gallery-dl/pinterest       # add Pinterest originals
```

Logs go to `./logs/imguploader_<date>.log`.

## Tables

| table            | columns                                                            |
|------------------|--------------------------------------------------------------------|
| `image_cache`    | `hash` (MD5, primary key), `link`, `filename`, `uploaded_at`, `account` |
| `imgchest_posts` | `post_id`, `folder_path`, `sequence`, `image_count`, `uploaded_at`, `account` |

| `image_mirrors`  | `hash` → `image_cache`, `mirror_name` (host), `link`, `uploaded_at`, `source_type` |
| `imgchest_accounts` | `key_id` (key fingerprint), `account`, `reason`, `flagged_at` |

`account` names the imgchest account that holds the file (or `catbox`); it is empty for uploads
made before the column existed. `imgchest_accounts` lists accounts flagged full, keyed by a
fingerprint of the API key, so renaming the variable keeps the flag.

`filename` is filled only if no other image already uses that name, so pixBoards can also look
up links by filename.

## Layout

```
imguploader/
  cli.py        argument parsing, logging, the scan and upload commands
  manifest.py   folder scanning, post titles, atomic YAML manifest
  uploader.py   per-folder logic: hash, skip, split into posts, save, track retries
  hosts.py      imgchest and catbox clients
  accounts.py   API keys from .env (one account each), failover, full-account flags
  store.py      PostgreSQL link cache (same schema as pixBoards)
  hashing.py    streaming MD5
  mirrors.py    host detection, Pinterest originals from gallery-dl metadata
tests/          pytest suite with fake hosts and an in-memory store
```

Go easy on the hosts: an account can be banned if bulk uploads look like abuse.

## Pinterest originals

The original image address isn't in gallery-dl's file names, so it has to come from its metadata.
To fetch the metadata without downloading the images again, use gallery-dl's `--write-metadata`
option (check its docs for how it combines with `--no-download` or skipped files in your
version), then run `imguploader mirrors pinterest <folder>`. Only images already in `image_cache`
get mirrors, so run it again after uploading more.

## To do
- [ ] pixBoards: fall back to another mirror when an image fails to load.
- [ ] Add images to an existing imgchest post (using the stored post id) instead of starting a
      new post.
