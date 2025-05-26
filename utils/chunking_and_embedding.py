from openai import OpenAI
import numpy as np
import os
import json
import hashlib
import time
from langchain.text_splitter import RecursiveCharacterTextSplitter
from typing import Optional
from collections import defaultdict
from abc import ABC, abstractmethod
from openai import PermissionDeniedError
from typing import Generator
from tqdm import tqdm
from .sheets_parser import RagSource
from .data_helpers import doc_to_chunk
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
        self, batch_size: int, model: str = "BAAI/bge-m3"
    ) -> Generator[tuple[list], None, None]:
        desc = f"Processing corpus {self._name} with embeddings..."
        for batch in self.iter_docs(batch_size=batch_size, desc=desc):
            # batch_embeddings = generate_embeddings_with_retry(
            #     data=[self.doc_to_chunk(x) for x in batch], attempts=5
            # )
            batch_embeddings = generate_embeddings_with_retry(
                data=[x.get("chunk_text") for x in batch], attempts=5, model=model
            )
            if len([x for x in batch_embeddings if x is not None]) == 0:
                continue
            yield batch, batch_embeddings

    @abstractmethod
    def doc_to_chunk(self, doc: dict) -> str:
        raise NotImplementedError("Subclasses should implement this!")


class SheetChunksHandler(CorpusHandler):
    def doc_to_chunk(self, doc: dict) -> str | None:
        context = ""
        if doc.get("context"):
            context = "  ( > ".join(doc["context"]) + ")"
        # print(f"Text is : {doc["text"]}")
        # print(f"Context is : {context}")
        # print(f"Title is : {doc['title']}")
        # print(f"Introduction is : {doc['introduction']}")
        if doc.get("introduction") not in doc["text"]:
            chunk_text = "\n".join(
                [doc["title"] + context, doc["introduction"], doc["text"]]
            )
        else:
            chunk_text = "\n".join([doc["title"] + context, doc["text"]])
        # print(f"Text to embed: {chunk_text}")

        return chunk_text


def generate_embeddings(
    data: str | list[str], model: str = "BAAI/bge-m3"
) -> list[float]:
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


def generate_embeddings_with_retry(
    data: str | list[str], attempts: int = 5, model: str = "BAAI/bge-m3"
) -> list[float]:
    """
    Generate embeddings for the provided data with retry mechanism.

    This function attempts to generate embeddings and retries in case of failures.
    It will immediately raise PermissionDeniedError if encountered, but retry for
    other exceptions.

    Args:
        data (str | list[str]): The text data to generate embeddings for.
            Can be a single string or a list of strings.
        attempts (int, optional): Maximum number of retry attempts. Defaults to 5.
        model (str, optional): The embedding model to use. Defaults to "BAAI/bge-m3".
            Note: This parameter is passed to the function but not directly used.

    Returns:
        list[float]: The generated embeddings as a list of floating point numbers.

    Raises:
        PermissionDeniedError: If there's an API key issue (raised immediately without retrying).
        Exception: If embedding generation fails after all retry attempts.
    """

    for attempt in range(attempts):  # Retry embedding up to 5 times
        try:
            embeddings = generate_embeddings(data=data, model=model)
            return embeddings
        except PermissionDeniedError as e:
            logger.error(
                f"PermissionDeniedError (API key issue). Unable to generate embeddings : {e}"
            )
            raise
        except Exception as e:
            if attempt == 4:
                logger.error(
                    f"Error generating embeddings for : {data}. Error: {e}. Maximum retries reached ({attempts}). Raising exception."
                )
                raise
            logger.error(
                f"Error generating embeddings for : {data}. Error: {e}. Retrying in 3 seconds (attempt {attempt + 1}/5)"
            )
            time.sleep(3)  # Waiting 3 seconds before retrying


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
    nom: str,
    mission: Optional[str],
    responsables: Optional[list],
    adresses: Optional[list],
) -> str:
    """
    Generates a concatenated string from provided entity information and a list of addresses for embedding purposes.

    Args:
        nom (str): The name of the entity.
        mission (str): The mission or purpose of the entity.
        types (str): The type(s) or category of the entity.
        responsables (list): A list of dictionaries, each representing a responsible person with possible keys:
            - 'fonction' (str, optional): The function or role of the person.
            - 'civilite' (str, optional): The title or salutation of the person.
            - 'prenom' (str, optional): The first name of the person.
            - 'nom' (str, optional): The last name of the person.
            - 'grade' (str, optional): The grade or rank of the person.
            - 'telephone' (str, optional): The telephone number of the person.
            - 'adresse_courriel' (list, optional): A list of email addresses, each represented as a dictionary with keys:
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
    try:
        for adresse in adresses:
            adresses_to_concatenate.append(
                f" {adresse.get('complement1', '')} {adresse.get('complement2', '')} {adresse.get('numero_voie', '')}, {adresse.get('code_postal', '')} {adresse.get('nom_commune', '')} {adresse.get('pays', '')}".strip()
            )

        # Concatenate all addresses in order to add them to the data to embed
        adresses_to_concatenate = " ".join(adresses_to_concatenate)
    except Exception:
        pass

    responsables_to_concatenate = []
    try:
        for responsable in responsables:
            resp = f"{responsable.get('fonction', '')}"
            if responsable.get("personne", {}):
                personne = responsable.get("personne", {})
                resp += f" : {personne.get('civilite', '')} {personne.get('prenom', '')} {personne.get('nom', '')}"
                if personne.get("grade", ""):
                    resp += f" ({personne.get('grade', '')})"
            #     if personne.get("adresse_courriel", []):
            #         for mail in personne.get("adresse_courriel", []):
            #             resp += f"\nEmail : {mail.get('valeur', '')} ({mail.get('libelle', '')})"
            # if responsable.get("telephone", ""):
            #     resp += f"\nTéléphone : {responsable.get('telephone', '')}"
            responsables_to_concatenate.append(resp)
        responsables_to_concatenate = ".\n".join(responsables_to_concatenate)
    except Exception:
        pass

    # Text to embed in order to makes the search more efficient
    fields = [
        nom,
        mission if mission else "",
        responsables_to_concatenate if responsables_to_concatenate else "",
    ]  # adresses not added for now
    text_to_embed = ". ".join([f for f in fields if f]).strip()

    return text_to_embed


def make_chunks_sheets(
    storage_dir: str, structured=True, chunk_size=1500, chunk_overlap=200
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

                chunk["chunk_text"] = doc_to_chunk(doc=chunk)
                # print(f"Chunk text: {chunk['chunk_text']}")
                # print("*" * 20)
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
