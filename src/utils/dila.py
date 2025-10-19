import os
from bs4 import BeautifulSoup
import requests
import re
import logging
from src.utils.pattern import is_matching


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_dila_url(config: dict, pattern: re.Pattern) -> list[str]:
    url = config.get("download_url", "")
    response = requests.get(url)
    response.raise_for_status()

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")

    # Find all links that end with ".tar.gz"
    logger.debug(f"Parsing DILA page at {url} for .tar.gz files.")
    logger.debug(
        f"{soup.prettify()[:1000]}"
    )  # Log first 1000 characters of the page content
    links = soup.find_all("a", href=True)
    logger.info(f"Found {len(links)} links in the DILA page.")

    tar_gz_files = sorted(
        [
            os.path.join(url, link["href"])
            for link in links
            if is_matching(link["href"], pattern)
        ]
    )

    return tar_gz_files
