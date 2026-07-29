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
import os
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

with open("config/data_config.json", "r") as f:
    CONFIG_LOADER = json.load(f)

# Archives are read one at a time to keep memory bounded, but pushing one
# commit per archive quickly hits HuggingFace's commit rate limit (128/hour).
# Instead, shards are written to local parquet files as archives are read,
# and flushed together as a single commit every ARCHIVES_PER_COMMIT archives.
ARCHIVES_PER_COMMIT = 20


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


def read_tar(archive_path: Path, source_url: str) -> list[dict]:
    rows = []
    try:
        with tarfile.open(archive_path, "r:*") as tf:
            for member in tf.getmembers():
                if not member.isfile():
                    continue
                try:
                    f = tf.extractfile(member)
                    if f is None:
                        continue
                    content = f.read().decode("utf-8", errors="replace")
                except Exception as e:
                    content = f"<read error: {e}>"
                rows.append(
                    {
                        "source_url": source_url,
                        "source_zip": archive_path.name,
                        "source_file": member.name,
                        "content": content,
                    }
                )
    except Exception as e:
        print(f"  [SKIP] cannot open tar {archive_path.name}: {e}")
    return rows


def read_zip(archive_path: Path, source_url: str) -> list[dict]:
    rows = []
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            for member in zf.infolist():
                if member.is_dir():
                    continue
                try:
                    content = zf.read(member.filename).decode("utf-8", errors="replace")
                except Exception as e:
                    content = f"<read error: {e}>"
                rows.append(
                    {
                        "source_url": source_url,
                        "source_zip": archive_path.name,
                        "source_file": member.filename,
                        "content": content,
                    }
                )
    except Exception as e:
        print(f"  [SKIP] cannot open zip {archive_path.name}: {e}")
    return rows


def read_archive(archive_path: Path, source_url: str) -> list[dict]:
    name = archive_path.name
    if name.endswith(".tar.gz") or name.endswith(".tar.bz2") or name.endswith(".tar"):
        return read_tar(archive_path, source_url)
    if name.endswith(".zip"):
        return read_zip(archive_path, source_url)
    print(f"  [SKIP] unknown archive format: {name}")
    return []


def is_valid_archive(archive_path: Path) -> bool:
    """Check an archive can be fully read, to detect truncated downloads."""
    name = archive_path.name
    try:
        if name.endswith(".tar.gz") or name.endswith(".tar.bz2") or name.endswith(".tar"):
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


def commit_with_retry(api: HfApi, repo_id: str, operations: list, commit_message: str) -> None:
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


def flush_shards(repo_id: str, shards: list[tuple[str, str]], n_archives: int, api: HfApi) -> None:
    if not shards:
        return
    try:
        operations = [
            CommitOperationAdd(path_in_repo=shard_name, path_or_fileobj=tmp_path)
            for tmp_path, shard_name in shards
        ]
        commit_with_retry(
            api, repo_id, operations, f"Add {n_archives} archive(s) ({len(shards)} shard(s))"
        )
    finally:
        for tmp_path, _ in shards:
            os.remove(tmp_path)


def process_source(download_name: str, config: dict, user_id: str, api: HfApi) -> None:
    repo_id = f"{user_id}/{download_name}-raw-documents"
    folder = Path(config["download_folder"])
    folder.mkdir(parents=True, exist_ok=True)

    api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)

    uploaded = get_uploaded_archives(repo_id)
    print(f"  Already uploaded archives: {len(uploaded)}")

    # -- Download missing archives ------------------------------------------
    from scripts.download_factory import factory_download

    downloader = factory_download(config, str(folder))
    all_urls: list[str] = downloader.get_urls()

    new_urls = [u for u in all_urls if Path(u).name not in uploaded]
    print(f"  Archives to download: {len(new_urls)} / {len(all_urls)} total")

    def _download(url: str) -> None:
        dest = folder / Path(url).name
        if dest.exists() and not is_valid_archive(dest):
            print(f"    {dest.name} is corrupted/incomplete, re-downloading …")
            dest.unlink()
        if not dest.exists():
            print(f"    Downloading {dest.name} …")
            downloader.download(url=url, destination_path=str(dest))

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_download, url): url for url in new_urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"    [SKIP] download failed for {Path(url).name}: {e}")

    # -- Read archives and push ----------------------------------------------
    new_archives = [
        (folder / Path(u).name, u) for u in new_urls if (folder / Path(u).name).exists()
    ]

    if not new_archives:
        print("  Nothing new to push.")
        return

    pending_shards: list[tuple[str, str]] = []
    pending_archive_count = 0
    for archive_path, source_url in new_archives:
        print(f"    Reading {archive_path.name} …")
        rows = read_archive(archive_path, source_url)
        if rows:
            pending_shards.append(write_shard(rows))
            pending_archive_count += 1
            del rows
        archive_path.unlink(missing_ok=True)

        if pending_archive_count >= ARCHIVES_PER_COMMIT:
            flush_shards(repo_id, pending_shards, pending_archive_count, api)
            print(f"    Pushed {pending_archive_count} archive(s) to {repo_id}")
            pending_shards, pending_archive_count = [], 0

    if pending_shards:
        flush_shards(repo_id, pending_shards, pending_archive_count, api)
        print(f"    Pushed {pending_archive_count} archive(s) to {repo_id}")


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
