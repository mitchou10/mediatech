"""
Raw dataset pipeline.

For each download_name (or all if omitted):
  1. Fetches available archive URLs via the download factory
  2. Downloads archives not yet tracked in the target HuggingFace dataset
  3. Reads every file inside each archive (tar.gz or zip) in-memory
  4. Pushes rows to {user_id}/{download_name}-raw-documents with columns:
       source_zip   – archive filename (basename)
       source_file  – path of the file inside the archive
       content      – raw text content (utf-8, errors replaced)
"""

import argparse
import json
import logging
import os
import sys
import tarfile
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from datasets import Dataset, load_dataset
from huggingface_hub import CommitOperationAdd, HfApi
from huggingface_hub.utils import HfHubHTTPError

logging.basicConfig(level=logging.INFO, format="%(message)s")

try:
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]
except AttributeError:
    pass

with open("config/data_config.json", "r") as f:
    CONFIG_LOADER = json.load(f)

# Archives are read one at a time to keep memory bounded, but pushing one
# commit per archive quickly hits HuggingFace's commit rate limit (128/hour).
# Instead, shards are written to local parquet files as archives are read,
# and flushed together as a single commit every ARCHIVES_PER_COMMIT archives.
ARCHIVES_PER_COMMIT = 2

# Archive contents are streamed row-by-row (not loaded fully into memory).
# A shard is flushed to disk every CHUNK_ROWS rows, regardless of archive
# boundaries, so that huge archives (e.g. the full "Freemium_legi_global"
# LEGI snapshot, ~1GB compressed / tens of GB uncompressed) cannot blow up
# memory the way loading a whole archive into one list/DataFrame would.
CHUNK_ROWS = 5000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build raw HF datasets from archives.")
    parser.add_argument(
        "--user-id", type=str, required=True, help="HuggingFace user ID."
    )
    parser.add_argument(
        "--download-name",
        type=str,
        choices=list(CONFIG_LOADER.keys()),
        default=None,
        help="Source to process. Omit to process all sources.",
    )
    return parser.parse_args()


def get_uploaded_archives(repo_id: str) -> set[str]:
    try:
        ds = load_dataset(repo_id, split="train", columns=["source_zip"])
        return set(ds["source_zip"])
    except Exception:
        return set()


def read_tar(archive_path: Path, source_url: str):
    """Yield rows one at a time so callers can bound memory usage."""
    try:
        with tarfile.open(archive_path, "r:*") as tf:
            for member in tf:
                if not member.isfile():
                    continue
                try:
                    f = tf.extractfile(member)
                    if f is None:
                        continue
                    content = f.read().decode("utf-8", errors="replace")
                except Exception as e:
                    content = f"<read error: {e}>"
                yield {
                    "source_url": source_url,
                    "source_zip": archive_path.name,
                    "source_file": member.name,
                    "content": content,
                }
    except Exception as e:
        print(f"  [SKIP] cannot open tar {archive_path.name}: {e}")


def read_zip(archive_path: Path, source_url: str):
    """Yield rows one at a time so callers can bound memory usage."""
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            for member in zf.infolist():
                if member.is_dir():
                    continue
                try:
                    content = zf.read(member.filename).decode("utf-8", errors="replace")
                except Exception as e:
                    content = f"<read error: {e}>"
                yield {
                    "source_url": source_url,
                    "source_zip": archive_path.name,
                    "source_file": member.filename,
                    "content": content,
                }
    except Exception as e:
        print(f"  [SKIP] cannot open zip {archive_path.name}: {e}")


def read_archive(archive_path: Path, source_url: str):
    """Yield rows for the given archive one at a time (generator)."""
    name = archive_path.name
    if name.endswith(".tar.gz") or name.endswith(".tar.bz2") or name.endswith(".tar"):
        yield from read_tar(archive_path, source_url)
        return
    if name.endswith(".zip"):
        yield from read_zip(archive_path, source_url)
        return
    print(f"  [SKIP] unknown archive format: {name}")


def is_valid_archive(archive_path: Path) -> bool:
    """Check an archive can be fully read, to detect truncated downloads."""
    name = archive_path.name
    try:
        if (
            name.endswith(".tar.gz")
            or name.endswith(".tar.bz2")
            or name.endswith(".tar")
        ):
            with tarfile.open(archive_path, "r:*") as tf:
                for member in tf:
                    if member.isfile():
                        f = tf.extractfile(member)
                        if f is not None:
                            while f.read(1024 * 1024):
                                pass
            return True
        if name.endswith(".zip"):
            with zipfile.ZipFile(archive_path, "r") as zf:
                return zf.testzip() is None
    except Exception:
        return False
    return True


def write_shard(rows: list[dict]) -> tuple[str, str]:
    """Write rows to a local parquet file. Returns (local_path, path_in_repo)."""
    new_df = pd.DataFrame(rows)
    dataset = Dataset.from_pandas(new_df, preserve_index=False)
    del new_df

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    dataset.to_parquet(tmp_path)
    del dataset

    shard_name = f"data/train-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S%f')}.parquet"
    return tmp_path, shard_name


def commit_with_retry(
    api: HfApi, repo_id: str, operations: list, commit_message: str
) -> None:
    while True:
        try:
            api.create_commit(
                repo_id=repo_id,
                repo_type="dataset",
                operations=operations,
                commit_message=commit_message,
            )
            return
        except HfHubHTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 60))
                print(f"    [429] Rate limited, retrying in {retry_after}s …")
                time.sleep(retry_after + 1)
                continue
            raise


def flush_shards(
    repo_id: str, shards: list[tuple[str, str]], n_archives: int, api: HfApi
) -> None:
    if not shards:
        return
    try:
        operations = [
            CommitOperationAdd(path_in_repo=shard_name, path_or_fileobj=tmp_path)
            for tmp_path, shard_name in shards
        ]
        commit_with_retry(
            api,
            repo_id,
            operations,
            f"Add {n_archives} archive(s) ({len(shards)} shard(s))",
        )
    finally:
        for tmp_path, _ in shards:
            os.remove(tmp_path)


def download_batch(batch_urls: list[str], folder: Path, downloader) -> None:
    def _download(url: str) -> None:
        dest = folder / Path(url).name
        if dest.exists():
            print(f"    Checking existing {dest.name} …")
            if not is_valid_archive(dest):
                print(f"    {dest.name} is corrupted/incomplete, re-downloading …")
                dest.unlink()
        if not dest.exists():
            print(f"    Downloading {dest.name} …")
            downloader.download(url=url, destination_path=str(dest))
            print(f"    Downloaded {dest.name}")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_download, url): url for url in batch_urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"    [SKIP] download failed for {Path(url).name}: {e}")


def read_and_push_batch(
    batch_urls: list[str], folder: Path, repo_id: str, api: HfApi
) -> None:
    batch_archives = [
        (folder / Path(u).name, u)
        for u in batch_urls
        if (folder / Path(u).name).exists()
    ]

    pending_shards: list[tuple[str, str]] = []
    pending_archive_count = 0
    buffer: list[dict] = []

    def flush_buffer() -> None:
        nonlocal buffer
        if buffer:
            pending_shards.append(write_shard(buffer))
            buffer = []

    for archive_path, source_url in batch_archives:
        print(f"    Reading {archive_path.name} …")
        n_rows = 0
        for row in read_archive(archive_path, source_url):
            buffer.append(row)
            n_rows += 1
            if len(buffer) >= CHUNK_ROWS:
                flush_buffer()
        print(f"    Read {n_rows} file(s) from {archive_path.name}")
        if n_rows:
            pending_archive_count += 1
        archive_path.unlink(missing_ok=True)

    flush_buffer()

    if pending_shards:
        flush_shards(repo_id, pending_shards, pending_archive_count, api)
        print(f"    Pushed {pending_archive_count} archive(s) to {repo_id}")


def process_source(download_name: str, config: dict, user_id: str, api: HfApi) -> None:
    repo_id = f"{user_id}/{download_name}-raw-documents"
    folder = Path(config["download_folder"])
    folder.mkdir(parents=True, exist_ok=True)

    api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)

    uploaded = get_uploaded_archives(repo_id)
    print(f"  Already uploaded archives: {len(uploaded)}")

    from scripts.download_factory import factory_download

    downloader = factory_download(config, str(folder))
    all_urls: list[str] = downloader.get_urls()

    new_urls = [u for u in all_urls if Path(u).name not in uploaded]
    print(f"  Archives to process: {len(new_urls)} / {len(all_urls)} total")

    if not new_urls:
        print("  Nothing new to push.")
        return

    # Process in batches of ARCHIVES_PER_COMMIT: download only that batch,
    # read + push it, delete its local archives, then move to the next batch.
    # Bounds both disk usage (only one batch of archives on disk at a time)
    # and HF commits (one commit per batch instead of one per archive).
    n_batches = (len(new_urls) + ARCHIVES_PER_COMMIT - 1) // ARCHIVES_PER_COMMIT
    for batch_start in range(0, len(new_urls), ARCHIVES_PER_COMMIT):
        batch_urls = new_urls[batch_start : batch_start + ARCHIVES_PER_COMMIT]
        batch_num = batch_start // ARCHIVES_PER_COMMIT + 1
        print(
            f"  Batch {batch_num}/{n_batches}: downloading {len(batch_urls)} archive(s) …"
        )

        download_batch(batch_urls, folder, downloader)
        read_and_push_batch(batch_urls, folder, repo_id, api)


def main() -> None:
    args = parse_args()
    api = HfApi()

    sources = (
        {args.download_name: CONFIG_LOADER[args.download_name]}
        if args.download_name
        else CONFIG_LOADER
    )

    for download_name, config in sources.items():
        print(f"\n{'='*60}")
        print(f"Processing: {download_name}")
        try:
            process_source(download_name, config, args.user_id, api)
        except Exception as e:
            print(f"  [ERROR] {download_name}: {e}")


if __name__ == "__main__":
    main()
