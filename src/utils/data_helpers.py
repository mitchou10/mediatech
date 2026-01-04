import os
import requests
from tqdm import tqdm
import logging
import tarfile

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def download_file(url: str, destination_path: str):
    """
    Downloads a file from an URL to a destination with a real-time progress bar.
    Uses streaming to handle large files efficiently.
    Args:
        url (str): The URL of the file to download.
        destination_path (str): The path (including filename) where the file will be saved.
    """
    logger.info(f"Downloading from {url}")
    try:
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            block_size = 1024 * 1024  # 1 MB

            logger.debug(f"Saving to {destination_path}")
            with tqdm(
                total=total_size,
                unit="iB",
                unit_scale=True,
                desc=os.path.basename(destination_path),
                leave=True,
            ) as progress_bar:
                with open(destination_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=block_size):
                        if chunk:
                            progress_bar.update(len(chunk))
                            f.write(chunk)

            # Check file size after download to verify completeness
            file_size = os.path.getsize(destination_path)
            if total_size > 0 and file_size != total_size:
                logger.warning(
                    f"Downloaded file size {file_size} bytes does not match expected {total_size} bytes. File may be incomplete."
                )
            elif total_size == 0:
                logger.info(
                    f"Downloaded file size: {file_size} bytes (content-length not provided by server)."
                )

        logger.info(
            f"Successfully downloaded {os.path.basename(destination_path)}")
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        raise e


def extract_tar_file(file_path: str, extract_path: str):
    """
    Extracts a .tar.gz file to a specified directory and then removes the archive.

    Args:
        file_path (str): The path to the .tar.gz file to be extracted.
        extract_path (str): The directory where the contents will be extracted.

    Returns:
        List[str]: A list of extracted file names.
    """
    extracted_files = []  # Liste pour stocker les fichiers extraits
    if not os.path.exists(file_path):
        logger.debug(f"File {file_path} does not exist")
        return extracted_files
    if not os.path.isdir(extract_path):
        logger.debug(f"Directory {extract_path} does not exist")
        return extracted_files
    if file_path.endswith(".tar.gz"):
        logger.debug(f"Found {file_path}")
        logger.debug(f"Extracting {file_path}")
        with tarfile.open(file_path, "r:gz") as tar:
            members = tar.getmembers()

            # Filtrer uniquement les fichiers (exclure les dossiers)
            files = [m.name for m in members if m.isfile()]

            logger.info(f"Extracting {len(files)} files from archive")

            # Extraction avec barre de progression
            with tqdm(total=len(members), unit="file", desc="Extracting", leave=True) as progress_bar:
                for member in members:
                    tar.extract(member, path=extract_path)
                    progress_bar.update(1)

            # Ajouter les fichiers extraits à la liste
            extracted_files.extend(files)

    return extracted_files  # Retourner la liste des fichiers extraits
