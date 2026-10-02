#!/usr/bin/env python3
"""
Flickr Album Downloader
=======================
Downloads every photo/video in your Flickr account (original quality where
available) into one folder per album, mirroring how you organized them.

* Photos that are in multiple albums are downloaded once and hard-linked
  (or copied) into the other album folders.
* Photos that aren't in any album go into "_Not in any album".
* Re-running is safe: files that already exist are skipped (resume support).

Setup
-----
1. pip install requests requests-oauthlib
2. Create a (non-commercial) API key at https://www.flickr.com/services/apps/create/
3. Run:
       export FLICKR_API_KEY=xxxx
       export FLICKR_API_SECRET=yyyy
       python flickr_album_downloader.py ./flickr_backup

   The first run opens an authorization URL; approve it and paste the
   verifier code back. The token is cached in ~/.flickr_album_downloader.json
   (read-only permission).

   DO NOT commit this file to version control.
   If it does get published or commited, visit https://www.flickr.com/services/auth/list.gne
   immediately to revoke the token.
"""

import argparse
import getpass
import json
import logging
import os
import re
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List, NotRequired, TypedDict
from urllib.parse import urlparse

import requests
from requests.sessions import Session
from requests_oauthlib import OAuth1Session

REST_URL = "https://api.flickr.com/services/rest/"
REQUEST_TOKEN_URL = "https://www.flickr.com/services/oauth/request_token"
AUTHORIZE_URL = "https://www.flickr.com/services/oauth/authorize"
ACCESS_TOKEN_URL = "https://www.flickr.com/services/oauth/access_token"
TOKEN_FILE = Path.home() / ".flickr_album_downloader.json"

# Largest -> smallest. url_o is the original (owner always gets it).
SIZE_KEYS = ["url_o", "url_k", "url_h", "url_b", "url_c", "url_z"]
EXTRAS = ",".join(SIZE_KEYS + ["original_format", "media", "date_taken"])
UNSORTED_NAME = "_Not in any album"

CHUNK_SIZE = 1 << 16
LOGGING_FILE = "flickr_backup.log"

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Flickr Response types
# --------------------------------------------------------------------------


class Photo(TypedDict):
    datetaken: str
    datetakengranularity: int
    datetakenunknown: int | str
    farm: int
    height_c: int
    height_z: int
    id: str
    isfamily: int
    isprimary: str
    ispublic: int
    media: str
    media_status: str
    originalformat: str
    originalsecret: str
    secret: str
    server: str
    title: str
    upgrade_sizes: list[str]
    url_c: str
    url_z: str
    width_c: int
    width_z: int


# --------------------------------------------------------------------------
# Auth + API
# --------------------------------------------------------------------------
def authenticate(api_key: str, api_secret: str):
    """Return an OAuth1Session authorized for the user (cached after 1st run)."""
    if TOKEN_FILE.exists():
        try:
            data = json.loads(TOKEN_FILE.read_text())
            if data.get("api_key") == api_key:
                return OAuth1Session(
                    api_key,
                    client_secret=api_secret,
                    resource_owner_key=data["oauth_token"],
                    resource_owner_secret=data["oauth_token_secret"],
                )
        except json.JSONDecodeError:
            logger.error(
                f'Failed to parse the oath token json file, trying to generate a new one at "{TOKEN_FILE}".'
            )
        except KeyError:
            logger.error(
                'Failed to find required key in oath json file. This should never happen, trying to generate a new one at "{TOKEN_FILE}".'
            )

        except Exception:
            logger.error(
                f'Failed to load the oath token file, trying to generate a new one at "{TOKEN_FILE}".'
            )

    oauth = OAuth1Session(api_key, client_secret=api_secret, callback_uri="oob")
    req = oauth.fetch_request_token(REQUEST_TOKEN_URL)
    url = oauth.authorization_url(AUTHORIZE_URL, perms="read")
    print("\nOpen this URL in your browser and authorize access:\n")
    print(f"  {url}\n")
    verifier = getpass.getpass("Paste the verifier code (9 digits) here: ").strip()

    oauth = OAuth1Session(
        api_key,
        client_secret=api_secret,
        resource_owner_key=req["oauth_token"],
        resource_owner_secret=req["oauth_token_secret"],
        verifier=verifier,
    )
    tok = oauth.fetch_access_token(ACCESS_TOKEN_URL)
    TOKEN_FILE.write_text(
        json.dumps(
            {
                "api_key": api_key,
                "oauth_token": tok["oauth_token"],
                "oauth_token_secret": tok["oauth_token_secret"],
            }
        )
    )
    try:
        os.chmod(TOKEN_FILE, 0o600)
    except OSError:
        logger.error(
            f"Failed to set the token file permissions correctly at location {TOKEN_FILE}"
        )
        sys.exit(f"Exiting due to unsafe credentials exposed at {TOKEN_FILE}")
    return oauth


class FlickrAPI:
    """Holds a Flickr API Oath Session and manages interval between api calls"""

    def __init__(self, session: OAuth1Session, min_interval: float = 0.25):
        """
        Create a new FlickrAPI session using an OAuth session and minimum call interval
        """
        self.session = session
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._last = 0.0

    def call(self, method: str, **params):
        """Call the given flickr api method with the specified parameters, will retry on failure."""
        params.update({"method": method, "format": "json", "nojsoncallback": 1})
        last_err = RuntimeError("Unknown API call error occurred")
        for attempt in range(6):
            with self._lock:  # gentle throttle (Flickr allows 3600 calls/hour)
                wait = self.min_interval - (time.time() - self._last)
                if wait > 0:
                    time.sleep(wait)
                self._last = time.time()
            try:
                r = self.session.get(REST_URL, params=params, timeout=60)
                r.raise_for_status()
                data = r.json()
                if data.get("stat") == "ok":
                    return data
                last_err = RuntimeError(
                    f"{method}: {data.get('message')} ({data.get('code')})"
                )
                if data.get("code") in (1, 2, 3, 98, 99, 100, 105):
                    break  # not retryable
            except Exception as e:
                last_err = e
            time.sleep(2**attempt)
        raise last_err

    def paginate(self, method, container, **params):
        page = 1
        while True:
            data = self.call(method, page=page, per_page=500, **params)
            block = data[container]
            for p in block.get("photo", []):
                yield p
            if page >= int(block.get("pages", 1)):
                break
            page += 1


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def sanitize(name: str, fallback: str = "untitled", max_len: int = 80):
    """Sanitize a filename of disallowed characters"""
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name or "").strip(" .")
    return (name[:max_len].strip(" .")) or fallback


def best_url(photo):
    for key in SIZE_KEYS:
        if photo.get(key):
            return photo[key]
    return None


def make_filename(photo, ext: str):
    """Make a filename from the photo object from flickr"""
    title = sanitize(photo.get("title", ""), fallback="")
    base = f"{title}_{photo['id']}" if title else photo["id"]
    return f"{base}.{ext}"


def parse_date(photo: dict[str, str]):
    try:
        return (
            datetime.strptime(photo["datetaken"], "%Y-%m-%d %H:%M:%S")
            .astimezone()
            .timestamp()
        )
    except KeyError:
        logger.error("Parsing date: missing key 'datetaken' in dict")
        return None
    except ValueError:
        logger.error(f"Parsing date: could not parse date from {photo['datetaken']}")
        return None
    except OSError:
        logger.error("Parsing date: operating system error occurred")
        return None
    except OverflowError:
        logger.error("Parsing date: overflow error occurred")
        return None
    return None


def resolve_media(api, photo):
    """Return (url, ext, signed) for a photo or video."""
    if photo.get("media") == "video":
        sizes = api.call("flickr.photos.getSizes", photo_id=photo["id"])["sizes"][
            "size"
        ]
        by_label = {s["label"]: s for s in sizes}
        for label in (
            "Video Original",
            "1080p",
            "720p",
            "HD MP4",
            "Site MP4",
            "Mobile MP4",
        ):
            if label in by_label:
                ext = "mp4"
                if label == "Video Original" and photo.get("originalformat"):
                    ext = photo["originalformat"]
                return by_label[label]["source"], ext, True
        raise RuntimeError("no downloadable video size")

    url = best_url(photo)
    if not url:
        # Fall back to getSizes
        sizes = api.call("flickr.photos.getSizes", photo_id=photo["id"])["sizes"][
            "size"
        ]
        sizes = [s for s in sizes if s.get("media") == "photo"]
        if not sizes:
            raise RuntimeError("no downloadable size")
        url = sizes[-1]["source"]
    ext = os.path.splitext(urlparse(url).path)[1].lstrip(".") or "jpg"
    return url, ext, False


def download_file(sess: Session, url: str | bytes, dest: Path, mtime: int | float):
    tmp = dest.with_name(dest.name + ".part")
    last = RuntimeError(f"Unknown error downloading file {url}")
    for attempt in range(5):
        try:
            with sess.get(url, stream=True, timeout=90) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    f.writelines(r.iter_content(CHUNK_SIZE))
            os.replace(tmp, dest)
            if mtime:
                os.utime(dest, (mtime, mtime))
            return
        except Exception as e:
            last = e
            time.sleep(2**attempt)
    if tmp.exists():
        tmp.unlink()
    raise last


def link_or_copy(src: Path, dst: Path, mode):
    if dst.exists():
        return
    if mode == "link":
        try:
            os.link(src, dst)
            return
        except OSError:
            pass
    shutil.copy2(src, dst)


# --------------------------------------------------------------------------
# Logging setup
# --------------------------------------------------------------------------


def setup_logging():
    file_handler = logging.FileHandler(LOGGING_FILE)
    file_handler.setLevel(logging.DEBUG)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(stderr_handler)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    setup_logging()
    logger.debug(f"{datetime.now(datetime.now().astimezone().tzinfo)} new session")

    ap = argparse.ArgumentParser(
        description="Download Flickr photos into album folders."
    )
    ap.add_argument("output", help="Destination folder")
    ap.add_argument(
        "--key",
        default=os.getenv("FLICKR_API_KEY"),
        help="Flickr API key. This option should in general not be used to avoid exposing the key.",
    )
    ap.add_argument(
        "--secret",
        default=os.getenv("FLICKR_API_SECRET"),
        help="Flickr API secret. This option should in general not be used to avoid exposing the secret.",
    )
    ap.add_argument(
        "--workers", type=int, default=4, help="Parallel downloads (default 4)"
    )
    ap.add_argument(
        "--skip-unsorted", action="store_true", help="Skip photos not in any album"
    )
    ap.add_argument(
        "--duplicates",
        choices=["link", "copy"],
        default="link",
        help="How to place photos that are in several albums (default: (hard) link, saves space)",
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="List what would be downloaded"
    )
    args = ap.parse_args()

    if not args.key or not args.secret:
        sys.exit("Provide --key/--secret or set FLICKR_API_KEY / FLICKR_API_SECRET.")

    out = Path(args.output).expanduser()
    out.mkdir(parents=True, exist_ok=True)

    session = authenticate(args.key, args.secret)
    api = FlickrAPI(session)

    me = api.call("flickr.test.login")["user"]
    user_id = me["id"]
    logger.info(f"Logged in as {me['username']['_content']} ({user_id})")

    # ---- Collect albums --------------------------------------------------
    albums = []
    page = 1
    while True:
        data = api.call(
            "flickr.photosets.getList", user_id=user_id, page=page, per_page=500
        )
        block = data["photosets"]
        albums.extend(block["photoset"])
        if page >= int(block["pages"]):
            break
        page += 1
    logger.info(f"Found {len(albums)} albums")

    used_dirs = set()
    # photo_id -> {"photo": dict, "dests": [Path, ...]}
    plan = {}
    albums_meta = []

    def album_dir(title, aid):
        name = sanitize(title, fallback=f"album_{aid}")
        if name.lower() in used_dirs:
            name = f"{name}_{aid}"
        used_dirs.add(name.lower())
        return out / name

    def add_to_plan(photo, folder):
        entry = plan.setdefault(photo["id"], {"photo": photo, "dests": []})
        if folder not in [d.parent for d in entry["dests"]]:
            entry["dests"].append(folder)  # store folder; filename decided later

    for i, album in enumerate(albums, 1):
        title = album["title"]["_content"]
        folder = album_dir(title, album["id"])
        folder.mkdir(exist_ok=True)
        count = 0
        for photo in api.paginate(
            "flickr.photosets.getPhotos",
            "photoset",
            photoset_id=album["id"],
            user_id=user_id,
            extras=EXTRAS,
        ):
            add_to_plan(photo, folder)
            count += 1
        albums_meta.append(
            {
                "id": album["id"],
                "title": title,
                "description": album.get("description", {}).get("_content", ""),
                "folder": folder.name,
                "photo_count": count,
            }
        )
        logger.info(f"[{i}/{len(albums)}] {title}: {count} items")

    # ---- Photos not in any album ----------------------------------------
    if not args.skip_unsorted:
        folder = out / UNSORTED_NAME
        folder.mkdir(exist_ok=True)
        n = 0
        for photo in api.paginate("flickr.photos.getNotInSet", "photos", extras=EXTRAS):
            add_to_plan(photo, folder)
            n += 1
        logger.info(f"{UNSORTED_NAME}: {n} items")

    (out / "_albums.json").write_text(
        json.dumps(albums_meta, indent=2, ensure_ascii=False)
    )

    total = len(plan)
    logger.info(f"\n{total} unique photos/videos to process")
    if args.dry_run:
        for e in plan.values():
            p = e["photo"]
            logger.info(
                f"  {p['id']}  {p.get('title', '')!r} -> {[d.name for d in e['dests']]}"
            )
        return

    # ---- Download --------------------------------------------------------
    plain = requests.Session()
    done = {"n": 0}
    failures = []

    def work(entry):
        photo = entry["photo"]
        folders = entry["dests"]
        url, ext, signed = resolve_media(api, photo)
        filename = make_filename(photo, ext)
        primary = folders[0] / filename
        if not (primary.exists() and primary.stat().st_size > 0):
            download_file(session if signed else plain, url, primary, parse_date(photo))
        for extra in folders[1:]:
            link_or_copy(primary, extra / filename, args.duplicates)
        return photo

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(work, e): e for e in plan.values()}
        for fut in as_completed(futures):
            photo = futures[fut]["photo"]
            done["n"] += 1
            try:
                fut.result()
                if done["n"] % 25 == 0 or done["n"] == total:
                    logger.info(f"  {done['n']}/{total} done")
            except Exception as e:
                failures.append((photo["id"], photo.get("title", ""), str(e)))
                logger.info(f"  FAILED {photo['id']} {photo.get('title', '')!r}: {e}")

    if failures:
        with open(out / "_failed.txt", "w", encoding="utf-8") as f:
            for pid, title, err in failures:
                f.write(f"{pid}\t{title}\t{err}\n")
        logger.info(
            f"\nFinished with {len(failures)} failures (see _failed.txt). Re-run to retry them."
        )
    else:
        logger.info("\nAll done. 🎉")


if __name__ == "__main__":
    main()
