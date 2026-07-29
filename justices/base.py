import argparse
import hashlib
import logging
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from zipfile import BadZipFile, ZipFile

import pandas as pd
from datasets import Dataset, load_dataset
from huggingface_hub import HfApi
from tqdm import tqdm

from justices.utils import download_file

logger = logging.getLogger(__name__)


def get_content_file(path: str) -> str:
    with open(path, "r") as f:
        return f.read()


def compute_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()


def get_existing_filenames(repo_id: str, filename_col: str) -> set[str]:
    try:
        ds = load_dataset(repo_id, split="train", columns=[filename_col])
        return set(ds[filename_col])
    except Exception:
        return set()


def get_existing_hashes(repo_id: str) -> set[str]:
    try:
        ds = load_dataset(repo_id, split="train", columns=["content_hash"])
        return set(ds["content_hash"])
    except Exception:
        return set()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract and export data based on configuration."
    )
    parser.add_argument(
        "--user-id",
        type=str,
        required=True,
        help="Hugging Face user ID for dataset upload.",
    )
    return parser.parse_args()


class BaseJustice:
    filename_xml_col: str
    filename_zip_col: str
    date_col: str
    on_bad_lines: str = "error"

    def __init__(self, config_loader: dict, folder_download: str, base_url: str):
        self.config_loader = config_loader
        self.folder_download = folder_download
        self.base_url = base_url

    def load_data(self) -> pd.DataFrame:
        df = pd.read_csv(
            self.config_loader["download_url"],
            sep=";",
            encoding="cp1252",
            on_bad_lines=self.on_bad_lines,
        )
        df[self.date_col] = pd.to_datetime(df[self.date_col], format="%d/%m/%Y")
        return df

    def _download_and_extract_zip(
        self, zip_url: str, zip_path: str, max_retries: int = 3
    ) -> None:
        if Path(zip_path).exists() and not self._is_valid_zip(zip_path):
            logger.warning(f"{zip_path} existe mais est corrompu/incomplet, suppression.")
            Path(zip_path).unlink()

        last_error: Exception | None = None
        for attempt in range(1, max_retries + 1):
            if not Path(zip_path).exists():
                try:
                    download_file(url=zip_url, destination_path=zip_path)
                except Exception as e:
                    last_error = e
                    logger.warning(
                        f"Échec du téléchargement de {zip_url} (essai {attempt}/{max_retries}): {e}"
                    )
                    Path(zip_path).unlink(missing_ok=True)
                    time.sleep(2 * attempt)
                    continue

            if self._is_valid_zip(zip_path):
                break

            logger.warning(
                f"Zip invalide après téléchargement: {zip_path} (essai {attempt}/{max_retries})"
            )
            Path(zip_path).unlink(missing_ok=True)
        else:
            raise RuntimeError(
                f"Impossible d'obtenir un zip valide pour {zip_url} après {max_retries} essais"
            ) from last_error

        with ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(self.folder_download)

    @staticmethod
    def _is_valid_zip(zip_path: str) -> bool:
        try:
            with ZipFile(zip_path, "r") as zip_ref:
                return zip_ref.testzip() is None
        except (BadZipFile, OSError):
            return False

    @staticmethod
    def _list_zip_xml_names(zip_path: str) -> set[str]:
        with ZipFile(zip_path, "r") as zip_ref:
            return {
                Path(n).name for n in zip_ref.namelist() if n.lower().endswith(".xml")
            }

    def _reconcile_zip_rows(
        self,
        zip_path: str,
        zip_name: str,
        rows: pd.DataFrame,
        full_index: pd.DataFrame,
        seen_names: set[str],
    ) -> tuple[pd.DataFrame, list[str]]:
        """Recale les lignes à traiter sur le contenu réel du zip.

        - signale les xml attendus (CSV) mais absents de l'archive.
        - ajoute au batch les xml présents dans l'archive mais absents du
          dataset HF (ex: ligne perdue par le CSV via on_bad_lines).
        """
        zip_xml_names = self._list_zip_xml_names(zip_path)
        expected_names = set(rows[self.filename_xml_col])

        missing_in_zip = sorted(expected_names - zip_xml_names)
        if missing_in_zip:
            print(
                f"  ATTENTION: {len(missing_in_zip)}/{len(expected_names)} "
                f"xml absent(s) de l'archive {zip_name}: {missing_in_zip}"
            )

        extra_names = zip_xml_names - expected_names - seen_names
        recoverable = [n for n in extra_names if n in full_index.index]
        unknown = extra_names - set(recoverable)
        if recoverable:
            print(
                f"  {len(recoverable)} xml présent(s) dans {zip_name} mais absent(s) "
                f"du dataset, ajout au rattrapage: {recoverable}"
            )
            rows = pd.concat([rows, full_index.loc[recoverable]], ignore_index=True)
        if unknown:
            logger.warning(
                f"{len(unknown)} xml présents dans {zip_name} sans métadonnée CSV "
                f"correspondante, ignorés: {sorted(unknown)}"
            )

        return rows, missing_in_zip

    def _build_xml_path(self, name: str) -> str:
        return f"{self.folder_download}/{name}"

    def _read_rows(self, df: pd.DataFrame, max_workers: int = 8) -> pd.DataFrame:
        """Lit les fichiers XML et calcule les hashes en parallèle.

        Les fichiers XML manquants/illisibles sont journalisés et exclus du
        résultat plutôt que de faire échouer tout le batch, afin de pouvoir
        les rattraper lors d'une prochaine exécution.
        """
        def _process(name: str) -> tuple[str, str, str] | None:
            path = self._build_xml_path(name)
            try:
                content = get_content_file(path)
            except OSError as e:
                logger.warning(f"XML manquant ou illisible, ignoré: {path} ({e})")
                return None
            return path, content, compute_hash(content)

        names = df[self.filename_xml_col].tolist()
        name_to_result: dict[str, tuple[str, str, str] | None] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_process, name): name for name in names}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Reading & hashing XML"):
                name_to_result[futures[future]] = future.result()

        found = {name: result for name, result in name_to_result.items() if result is not None}
        missing = len(name_to_result) - len(found)
        if missing:
            print(f"  {missing} fichier(s) XML manquant(s), ignoré(s) pour rattrapage ultérieur.")

        df = df[df[self.filename_xml_col].isin(found)].copy()
        df["path_xml"] = df[self.filename_xml_col].map(lambda n: found[n][0])
        df["content"] = df[self.filename_xml_col].map(lambda n: found[n][1])
        df["content_hash"] = df[self.filename_xml_col].map(lambda n: found[n][2])
        return df

    def _build_zip_groups(self, df: pd.DataFrame) -> dict[str, tuple[str, pd.DataFrame]]:
        """Regroupe les lignes par zip et retourne {zip_path: (zip_url, rows)}."""
        groups: dict[str, tuple[str, list]] = {}
        for _, row in df.iterrows():
            zip_url = self.base_url.replace(
                "YEAR", str(row[self.date_col].year)
            ).replace("MONTH", f"{row[self.date_col].month:02d}")
            zip_path = f"{self.folder_download}/{row[self.filename_zip_col]}"
            if zip_path not in groups:
                groups[zip_path] = (zip_url, [])
            groups[zip_path][1].append(row)
        return {k: (v[0], pd.DataFrame(v[1])) for k, v in groups.items()}

    def run(self, repo_id: str) -> None:
        api = HfApi()
        repo_url = api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
        print(f"Repository URL: {repo_url}")

        existing_filenames = get_existing_filenames(repo_id, self.filename_xml_col)
        existing_hashes = get_existing_hashes(repo_id)
        print(f"Already in HuggingFace: {len(existing_filenames)} documents")

        all_data = self.load_data()
        # Index de toutes les métadonnées connues (CSV complet), pour retrouver
        # la ligne d'un xml récupéré directement depuis un zip.
        full_index = all_data.drop_duplicates(subset=[self.filename_xml_col]).set_index(
            self.filename_xml_col, drop=False
        )

        new_rows = all_data[~all_data[self.filename_xml_col].isin(existing_filenames)]
        new_rows = new_rows.drop_duplicates(subset=[self.filename_xml_col])
        print(f"New documents to process: {len(new_rows)}")
        print(79 * "*")

        if len(new_rows) == 0:
            print("Nothing new to upload.")
            sys.exit(0)

        # Noms et hashes déjà vus (HF + session), pour ne jamais ajouter deux fois le même document
        seen_names: set[str] = set(existing_filenames)
        seen_hashes: set[str] = set(existing_hashes)
        zip_groups = self._build_zip_groups(new_rows)
        total_uploaded = 0
        failed_zips: list[str] = []
        missing_xml_by_zip: dict[str, list[str]] = {}

        for zip_path, (zip_url, rows) in zip_groups.items():
            zip_name = Path(zip_path).name
            print(f"\n--- Processing {zip_name} ({len(rows)} documents) ---")

            try:
                self._download_and_extract_zip(zip_url, zip_path)
                rows, missing_in_zip = self._reconcile_zip_rows(
                    zip_path, zip_name, rows, full_index, seen_names
                )
                if missing_in_zip:
                    missing_xml_by_zip[zip_name] = missing_in_zip

                batch_df = self._read_rows(rows)
            except Exception as e:
                logger.error(f"Échec du traitement de {zip_name}, passage au suivant: {e}")
                failed_zips.append(zip_name)
                continue

            # Dédup contre HF + zips déjà uploadés dans cette session
            n_before = len(batch_df)
            batch_df = batch_df[~batch_df["content_hash"].isin(seen_hashes)].reset_index(drop=True)
            if len(batch_df) < n_before:
                print(f"  Skipped {n_before - len(batch_df)} duplicate(s).")

            if batch_df.empty:
                print("  Nothing new to upload for this zip.")
                continue

            seen_hashes.update(batch_df["content_hash"].tolist())
            seen_names.update(batch_df[self.filename_xml_col].tolist())

            dataset = Dataset.from_pandas(batch_df, preserve_index=False)
            shard_name = f"data/train-{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
            commit_message = f"Add {zip_name} (+{len(batch_df)} documents)"
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                dataset.to_parquet(tmp.name)
                api.upload_file(
                    path_or_fileobj=tmp.name,
                    path_in_repo=shard_name,
                    repo_id=repo_id,
                    repo_type="dataset",
                    commit_message=commit_message,
                )
            total_uploaded += len(batch_df)
            print(f"  Uploaded {len(batch_df)} documents.")

        print(f"\nDone. Total uploaded: {total_uploaded} documents.")

        if missing_xml_by_zip:
            total_missing = sum(len(v) for v in missing_xml_by_zip.values())
            print(
                f"\n{total_missing} xml référencé(s) dans le CSV sont absents de leur "
                f"archive zip source ({len(missing_xml_by_zip)} zip(s) concerné(s)):"
            )
            for zip_name, names in missing_xml_by_zip.items():
                print(f"  - {zip_name}: {names}")

        if failed_zips:
            print(
                f"\n{len(failed_zips)} zip(s) n'ont pas pu être traités et seront "
                f"rattrapés à la prochaine exécution: {', '.join(failed_zips)}"
            )

        if failed_zips or missing_xml_by_zip:
            sys.exit(1)
