from src.utils.dila import get_dila_url
import re


def test_get_dila_url():
    config = {
        "download_url": "https://echanges.dila.gouv.fr/OPENDATA/CNIL/",
        "download_folder": "data/unprocessed/cnil",
        "type": "dila_folder",
    }
    url = get_dila_url(config, pattern=re.compile(r"CNIL.*\.tar\.gz"))
    assert len(url) > 0
    assert all(url_item.endswith(".tar.gz") for url_item in url)
