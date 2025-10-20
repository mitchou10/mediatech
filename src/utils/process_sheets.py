import json
from bs4 import BeautifulSoup
import logging
import string
import unicodedata
from src.utils.sheets_parser import (
    _parse_xml_text_structured,
    extract,
    extract_all,
    _parse_xml_text,
    _get_metadata,
    normalize,
    get_text,
)
from bs4.element import NavigableString, Tag

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def doc_to_chunk(doc: dict) -> str | None:
    """Converts a document dictionary into a single formatted string.

    This method combines the title, context, introduction, and text fields
    from a document dictionary into a unified string. It intelligently includes
    the introduction only if it is not already part of the main text to
    avoid redundancy.

    Args:
        doc (dict): A dictionary representing a document. Expected keys include
            'title', 'text', and optionally 'context' and 'introduction'.

    Returns:
        str: A single string representing the formatted document chunk.
    """
    context = ""
    if doc.get("context"):
        context = "  ( > ".join(doc["context"]) + ")"
    if doc.get("introduction") not in doc["text"]:
        chunk_text = "\n".join(
            [doc["title"] + context, doc["introduction"], doc["text"]]
        )
    else:
        chunk_text = "\n".join([doc["title"] + context, doc["text"]])

    return chunk_text


def process_sheets(
    filename: str,
    table_name: str,
):
    """
    Process sheets data with checkpoint support for resume capability.

    Args:
        target_dir (str): Directory containing the sheets data
        model (str): Model name for embedding generation
        batch_size (int): Number of documents to process per batch
    """
    if filename.endswith(".json"):
        with open(filename, encoding="utf-8") as f:
            documents: list[dict] = json.load(f)
            total_documents = len(documents)
            logger.info(f"Total documents to process: {total_documents}, ")
            return documents

    else:  # XML file
        document = _parse_xml_text(
            filename, structured=(table_name == "service_public")
        )
        return document
