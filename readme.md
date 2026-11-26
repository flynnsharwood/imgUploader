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
imguploader mirrors check --limit 500                      # check links, update health scores
imguploader mirrors check --host catbox --stale-days 1
```

Logs go to `./logs/imguploader_<date>.log`.

## Tables

| table            | columns                                                            |
|------------------|--------------------------------------------------------------------|
| `image_cache`    | `hash` (MD5, primary key), `link`, `filename`, `uploaded_at`, `account` |
| `imgchest_posts` | `post_id`, `folder_path`, `sequence`, `image_count`, `uploaded_at`, `account` |

| `image_mirrors`  | `hash` → `image_cache`, `mirror_name` (host), `link`, `uploaded_at` (may be empty), `source_type`, `last_checked_at`, `last_ok_at`, `last_status`, `fail_count`, `health` |
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
  mirrors.py    host detection, Pinterest originals, link checks and health scores
tests/          pytest suite with fake hosts and an in-memory store
```

Go easy on the hosts: an account can be banned if bulk uploads look like abuse.

## Mirror health

`imguploader mirrors check` asks each host whether a link still exists. It starts with links
never checked, then those checked longest ago, and skips anything checked within
`--stale-days` (default 7). Each mirror then gets a score from 0 to 1:

- **1.0** right after an upload or a successful check;
- it **halves every 30 days** since the image was last confirmed to exist;
- it **halves again for each failed check** in a row (404, server error, timeout);
- **empty** for a link that has never been checked.

Upload dates feed in only where they are real. Links uploaded before the uploader recorded upload
times have no date: the 5,009 older rows all carried the time the column was added, and those
are stored as unknown. Fresh uploads count as checked at upload time.

Some answers prove nothing, so they are recorded without changing the score:
- a Cloudflare challenge (imgchest's CDN shows one to scripts);
- a rate limit (HTTP 429).

The checker identifies itself as `imguploader/<version>` and never pretends to be a browser.
catbox's file server drops connections from the default `python-requests` name.

## Pinterest originals

`imguploader mirrors pinterest <folder>` adds each pin's original image
(`https://i.pinimg.com/originals/...`) as a `pinterest` mirror. It finds them in two ways:

1. **From the file name (default).** gallery-dl names pins `pinterest_<pinId>[_n].<ext>`.
   - The pin id is looked up with `gallery-dl -g https://www.pinterest.com/pin/<pinId>/`, which
     lists the pin's file URLs without downloading them and uses your gallery-dl config.
   - Because a pin can hold several files, the URL whose size matches the local file is chosen.
     `--verify` also downloads it and compares MD5s. In a test on 10 real pins, 9 matched at once,
     all identical by MD5, and the tenth matched on the next run.
   - Lookups go one at a time, with `--delay` seconds between them (default 1).
   - Pins deleted from Pinterest can't be recovered. They are recorded in `pinterest_lookups` and
     skipped for 30 days. In the first 500-pin batch about 30% came back empty; nearly all of
     those had been deleted. Other failed lookups are retried twice, then again on the next run.
   - `--limit N` works in batches; a run picks up the pins that don't have a mirror yet.
2. **From gallery-dl metadata files** (`<file>.<ext>.json`, written with `--write-metadata`),
   if you have them. These need no lookups. `--no-lookup` uses only these.

Only images already in `image_cache` get mirrors, so run it again after uploading more.

## To do
- [ ] pixBoards: fall back to another mirror when an image fails to load.
- [ ] Add images to an existing imgchest post (using the stored post id) instead of starting a
      new post.
