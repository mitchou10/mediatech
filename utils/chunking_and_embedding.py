from openai import OpenAI
import pandas as pd
import numpy as np
import os
import json
import hashlib
from .sheets_parser import _parse_xml, RagSource
from langchain.text_splitter import RecursiveCharacterTextSplitter
from typing import Optional
from collections import defaultdict
import re
from abc import ABC, abstractmethod
from typing import Generator
from tqdm import tqdm
from config import (
    get_logger,
    API_URL,
    API_KEY,
    TRAVAIL_EMPLOI_DATA_FOLDER,
    SERVICE_PUBLIC_PRO_DATA_FOLDER,
    SERVICE_PUBLIC_PART_DATA_FOLDER,
)

logger = get_logger(__name__)


class CorpusHandler(ABC):
    def __init__(self, name, corpus):
        self._name = name
        self._corpus = corpus

    @classmethod
    def create_handler(cls, corpus_name: str, corpus: list[dict]) -> "CorpusHandler":
        """Get the appropriate handler subclass from the string corpus name."""
        corpuses = {
            # "spp_experiences": SppExperiencesHandler, # Legacy from pyalbert, unused here
            TRAVAIL_EMPLOI_DATA_FOLDER.split("/")[-1]: SheetChunksHandler,
            SERVICE_PUBLIC_PRO_DATA_FOLDER.split("/")[-1]: SheetChunksHandler,
            SERVICE_PUBLIC_PART_DATA_FOLDER.split("/")[-1]: SheetChunksHandler,
        }
        if corpus_name not in corpuses:
            raise ValueError(f"Corpus '{corpus_name}' is not recognized")
        return corpuses[corpus_name](corpus_name, corpus)

    def iter_docs(
        self, batch_size: int, desc: str = None
    ) -> Generator[list, None, None]:
        if not desc:
            desc = f"Processing corpus: {self._name}..."

        corpus = self._corpus
        num_chunks = len(corpus) // batch_size
        if len(corpus) % batch_size != 0:
            num_chunks += 1

        for i in tqdm(range(num_chunks), desc=desc):
            start_idx = i * batch_size
            end_idx = min(start_idx + batch_size, len(corpus))
            yield corpus[start_idx:end_idx]

    def iter_docs_embeddings(
        self, batch_size: int
    ) -> Generator[tuple[list], None, None]:
        desc = f"Processing corpus {self._name} with embeddings..."
        for batch in self.iter_docs(batch_size=batch_size, desc=desc):
            batch_embeddings = generate_embeddings([self.doc_to_chunk(x) for x in batch])
            if len([x for x in batch_embeddings if x is not None]) == 0:
                continue
            yield batch, batch_embeddings

    @abstractmethod
    def doc_to_chunk(self, doc: dict) -> str:
        raise NotImplementedError("Subclasses should implement this!")


# class SppExperiencesHandler(CorpusHandler):
#     def doc_to_chunk(self, doc: dict) -> str | None:
#         text = doc["reponse_structure_1"]
#         if not text:
#             return None
#         # Clean some text garbage
#         # --
#         # Define regular expression pattern to match non-breaking spaces:
#         # - \xa0 for Latin-1 (as a raw string)
#         # - \u00a0 for Unicode non-breaking space
#         # - \r carriage return
#         # - &nbsp; html non breaking space
#         text = re.sub(r"[\xa0\u00a0\r]", " ", text)
#         text = re.sub(r"&nbsp;", " ", text)

#         # Add a space after the first "," if not already followed by a space.
#         text = re.sub(r"\,(?!\s)", ". ", text, count=1)
#         return text


class SheetChunksHandler(CorpusHandler):
    def doc_to_chunk(self, doc: dict) -> str | None:
        context = ""
        if "context" in doc:
            context = "  ( > ".join(doc["context"]) + ")"

        text = "\n".join([doc["title"] + context, doc["introduction"], doc["text"]])
        return text

# def embed(data: None | str | list[str]) -> None | list:
#     if data is None:
#         return None

#     if isinstance(data, list):
#         # Keep track of None positions
#         indices_of_none = [i for i, x in enumerate(data) if x is None]
#         filtered_data = [x for x in data if x is not None]
#         if not filtered_data:
#             return [None] * len(data)

#         # Apply the original function on filtered data
#         try:
#             embeddings = LlmClient.create_embeddings(filtered_data)
#         except Exception as err:
#             print(filtered_data)
#             raise err

#         # Reinsert None at their original positions in reverse order
#         for index in reversed(indices_of_none):
#             embeddings.insert(index, None)

#         return embeddings

#     # Fall back to single data input
#     return LlmClient.create_embeddings(data)
    
def generate_embeddings(data: str | list[str], model: str = "BAAI/bge-m3"):
    """
    Generates embeddings for a given text using a specified model.

    Args:
        data (str or list[str]): The input to generate embeddings for.
        model (str, optional): The model identifier to use for generating embeddings. Defaults to "BAAI/bge-m3".

    Returns:
        list[float]: The embedding vector for the input text.

    Raises:
        Any exceptions raised by the OpenAI client during the embedding generation process.

    Note:
        Requires properly configured API_URL and API_KEY for the OpenAI client.
    """
    client_openai = OpenAI(base_url=API_URL, api_key=API_KEY)
    vector = client_openai.embeddings.create(
        input=data, model=model, encoding_format="float"
    )
    embeddings = [item.embedding for item in vector.data]
    
    return embeddings
    # if isinstance(data, str):
    #     return embeddings[0]
    # elif isinstance(data, list) and len(data) == 1:
    #     return embeddings[0]
    # else:
    #     return embeddings


def make_chunks(text: str, chunk_size: int = 1500, chunk_overlap: int = 200):
    """
    Splits the input text into overlapping chunks using a recursive character-based text splitter.
    Args:
        text (str): The input text to be split into chunks.
        chunk_size (int, optional): The maximum size of each chunk. Defaults to 512.
        chunk_overlap (int, optional): The number of overlapping characters between consecutive chunks. Defaults to 100.
    Returns:
        List[str]: A list of text chunks generated from the input text.
    """

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n\n", "\n", " ", ""],
    )
    chunks = text_splitter.split_text(text)
    return chunks


def make_chunks_directories(
    nom: str, mission: Optional[str], types: Optional[str], adresses: Optional[list]
) -> str:
    """
    Generates a concatenated string from provided entity information and a list of addresses for embedding purposes.

    Args:
        nom (str): The name of the entity.
        mission (str): The mission or purpose of the entity.
        types (str): The type(s) or category of the entity.
        adresses (list): A list of dictionaries, each representing an address with possible keys:
            - 'complement1' (str, optional): Additional address information.
            - 'complement2' (str, optional): Additional address information.
            - 'numero_voie' (str, optional): Street number.
            - 'code_postal' (str, optional): Postal code.
            - 'nom_commune' (str, optional): City or commune name.
            - 'pays' (str, optional): Country name.

    Returns:
        str: A single string containing the concatenated and formatted information, suitable for embedding or search optimization.
    """
    adresses_to_concatenate = []
    for adresse in adresses:
        adresses_to_concatenate.append(
            f" {adresse.get('complement1', '')} {adresse.get('complement2', '')} {adresse.get('numero_voie', '')}, {adresse.get('code_postal', '')} {adresse.get('nom_commune', '')} {adresse.get('pays', '')}".strip()
        )

    # Concatenate all addresses in order to add them to the data to embed
    adresses_to_concatenate = " ".join(adresses_to_concatenate)

    # Text to embed in order to makes the search more efficient
    fields = [
        nom,
        mission if mission else "",
        types if types else "",
    ]
    text_to_embed = ". ".join([f for f in fields if f]).strip()

    return text_to_embed


def make_chunks_sheets(
    storage_dir: str, structured=False, chunk_size=1500, chunk_overlap=200
) -> None:
    """Chunkify sheets and save to a JSON file"""

    if structured:
        chunk_overlap = 20

    if storage_dir is None:
        raise ValueError(
            "You must give a datas directory to chunkify in the param 'storage_dir'."
        )

    sheets = RagSource.get_sheets(storage_dir, structured=structured)

    chunks = []
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
        is_separator_regex=False,
    )
    hashes = []
    info = defaultdict(lambda: defaultdict(list))
    for data in sheets:
        texts = data["text"]
        surtitre = data["surtitre"]

        if not texts:
            continue
        if surtitre in ("Dossier", "Recherche guidée"):
            # TODO: can be used for cross-reference, see also <LienInterne>
            continue

        if structured:
            s = [x["text"] for x in texts]
        else:
            s = texts

        info[surtitre]["len"].append(len(" ".join(s).split()))
        index = 0
        for natural_chunk in texts:
            if isinstance(natural_chunk, dict):
                natural_text_chunk = natural_chunk["text"]
            else:
                natural_text_chunk = natural_chunk

            for fragment in text_splitter.split_text(natural_text_chunk):
                if not fragment:
                    logger.warning("Warning: empty fragment")
                    continue

                info[surtitre]["chunk_len"].append(len(fragment.split()))

                chunk = {
                    **data,
                    "chunk_index": index,
                    "text": fragment,  # overwrite previous value
                }
                if isinstance(natural_chunk, dict) and "context" in natural_chunk:
                    chunk["context"] = natural_chunk["context"]
                    chunk_content = "".join(chunk["context"]) + fragment
                else:
                    chunk_content = fragment

                # add an unique hash/id
                h = hashlib.blake2b(chunk_content.encode(), digest_size=8).hexdigest()
                if h in hashes:
                    # print("Warning: duplicate chunk (%s)" % chunk["sid"])
                    # print(chunk_content)
                    continue
                hashes.append(h)
                chunk["hash"] = h

                chunks.append(chunk)
                index += 1

    json_file_target = os.path.join(storage_dir, "sheets_as_chunks.json")
    with open(json_file_target, mode="w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=4)

    info_summary = ""
    for k, v in info.items():
        v_len = v["len"]
        v_chunk_len = v["chunk_len"]
        template = "{}: {:.0f} ± {:.0f}    max:{} min:{}\n"
        info_summary += f"### {k}\n"
        info_summary += f"total doc: {len(v_len)}\n"
        info_summary += template.format(
            "mean length", np.mean(v_len), np.std(v_len), np.max(v_len), np.min(v_len)
        )
        info_summary += f"total chunk: {len(v_chunk_len)}\n"
        info_summary += template.format(
            "mean chunks length",
            np.mean(v_chunk_len),
            np.std(v_chunk_len),
            np.max(v_chunk_len),
            np.min(v_chunk_len),
        )
        info_summary += "\n"

    logger.info(f"Info summary:\n {str(json_file_target)}")

    logger.info(f"Chunks created in : {str(json_file_target)}")


def make_questions(storage_dir: str) -> None:
    target_dir = storage_dir
    if storage_dir.split("/")[-1].strip("/") != "data.gouv":
        target_dir = os.path.join(storage_dir, "data.gouv")
    questions = _parse_xml(target_dir, "questions")
    df = pd.DataFrame(questions)
    df = df.drop_duplicates(subset=["question"])
    questions = df.to_dict(orient="records")
    q_fn = os.path.join(storage_dir, "questions.json")
    with open(q_fn, mode="w", encoding="utf-8") as f:
        json.dump(questions, f, ensure_ascii=False, indent=4)
    print("Questions created in", q_fn)


