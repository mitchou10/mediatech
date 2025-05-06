from openai import OpenAI
from langchain.text_splitter import RecursiveCharacterTextSplitter
from config import STAGGING_URL, STAGGING_API_KEY


def generate_embeddings(text: str, model: str = "BAAI/bge-m3"):
    """
    Generates embeddings for a given text using a specified model.

    Args:
        text (str): The input text to generate embeddings for.
        model (str, optional): The model identifier to use for generating embeddings. Defaults to "BAAI/bge-m3".

    Returns:
        list[float]: The embedding vector for the input text.

    Raises:
        Any exceptions raised by the OpenAI client during the embedding generation process.

    Note:
        Requires properly configured STAGGING_URL and STAGGING_API_KEY for the OpenAI client.
    """
    client_openai = OpenAI(base_url=STAGGING_URL, api_key=STAGGING_API_KEY)
    vector = client_openai.embeddings.create(
        input=text, model=model, encoding_format="float"
    )
    embeddings = [item.embedding for item in vector.data]

    return embeddings[0]


def make_chunks(text: str, chunk_size: int = 512, chunk_overlap: int = 100):
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


def make_chunks_directories(nom: str, mission: str, types: str, adresses: list) -> str:
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
        mission,
        types,
        adresses_to_concatenate,
    ]
    text_to_embed = ". ".join([f for f in fields if f]).strip()

    return text_to_embed
