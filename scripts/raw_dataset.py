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
import tarfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from datasets import Dataset, load_dataset
from huggingface_hub import HfApi

with open("config/data_config.json", "r") as f:
    CONFIG_LOADER = json.load(f)


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


def push_rows(repo_id: str, rows: list[dict], n_new_archives: int, api: HfApi) -> None:
    import tempfile

    new_df = pd.DataFrame(rows)
    dataset = Dataset.from_pandas(new_df, preserve_index=False)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        dataset.to_parquet(tmp.name)
        # Chaque batch est un fichier parquet séparé — HF les agrège automatiquement
        shard_name = f"data/train-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.parquet"
        api.upload_file(
            path_or_fileobj=tmp.name,
            path_in_repo=shard_name,
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=f"Add {n_new_archives} archive(s) ({len(rows)} files)",
        )


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
        if not dest.exists():
            print(f"    Downloading {dest.name} …")
            downloader.download(url=url, destination_path=str(dest))

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_download, url): url for url in new_urls}
        for future in as_completed(futures):
            future.result()

    # -- Read archives and push ----------------------------------------------
    new_archives = [
        (folder / Path(u).name, u) for u in new_urls if (folder / Path(u).name).exists()
    ]

    if not new_archives:
        print("  Nothing new to push.")
        return

    rows: list[dict] = []
    for archive_path, source_url in new_archives:
        print(f"    Reading {archive_path.name} …")
        rows.extend(read_archive(archive_path, source_url))

    print(f"  Total rows: {len(rows)}")
    push_rows(repo_id, rows, len(new_archives), api)
    print(f"  Pushed to {repo_id}")


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
