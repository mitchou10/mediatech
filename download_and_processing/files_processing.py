import os
import xml.etree.ElementTree as ET
import json
import time
from database import insert_data
from config import (
    get_logger,
    CNIL_DATA_FOLDER,
    LEGI_DATA_FOLDER,
    CONSTIT_DATA_FOLDER,
    LOCAL_DIRECTORY_FOLDER,
    NATIONAL_DIRECTORY_FOLDER,
    TRAVAIL_EMPLOI_DATA_FOLDER,
    SERVICE_PUBLIC_PRO_DATA_FOLDER,
    SERVICE_PUBLIC_PART_DATA_FOLDER,
    DOLE_DATA_FOLDER,
)
from utils import (
    CorpusHandler,
    generate_embeddings,
    make_chunks,
    make_chunks_directories,
    make_chunks_sheets,
    remove_folder,
    remove_file,
    make_schedule,
)
from datetime import datetime
from openai import PermissionDeniedError

# logger.getLogger("httpx").setLevel(logger.WARNING)
# logger.getLogger("openai").setLevel(logger.WARNING)

# logger.basicConfig(
#     filename="logs/data.log",
#     level=logger.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
# )

logger = get_logger(__name__)


def process_directories(target_dir: str, config_file_path: str):
    """
    Processes directory data from JSON files specified in a configuration file, extracts and transforms relevant fields,
    generates embeddings for each directory, and inserts the processed data into a database.

    Args:
        target_dir (str): The directory path where the JSON files are located.
        config_file_path (str): The path to the configuration JSON file that specifies which directory files to process.

    Raises:
        FileNotFoundError: If the configuration file or any specified directory JSON file is not found.
        json.JSONDecodeError: If there is an error decoding JSON from the configuration or data files.
        Exception: For any other unexpected errors during file loading or embedding generation.

    Workflow:
        1. Loads the configuration file to determine which directory JSON files to process.
        2. Reads and aggregates directory data from the specified JSON files.
        3. Extracts and processes various fields such as addresses, phone numbers, types, SIRET/SIREN, URLs, emails,
           opening hours, mobile applications, social networks, additional information, people in charge, and hierarchy.
        4. Generates text chunks and embeddings for each directory entry.
        5. Inserts the processed data, including embeddings, into a table in the database.

    Logging:
        Logs errors and information throughout the process, including file loading issues, JSON decoding errors,
        embedding generation retries, and the number of directories loaded.
    """
    # Open data config
    try:
        file = open(config_file_path, "r")
        config = json.load(file)
        file.close()
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(
            f"Error decoding JSON from the configuration file: {config_file_path}"
        )
        raise
    except Exception as e:
        logger.error(f"Unexpected error while loading configuration file: {e}")
        raise

    ### Loading directories
    directory = []
    for file_name, values in config["directories"].items():
        try:
            with open(f"{target_dir}/{file_name}.json", encoding="utf-8") as json_file:
                json_data = json.load(json_file)
                if not directory:  # First file
                    directory = json_data["service"]
                else:
                    directory.extend(json_data["service"])
        except FileNotFoundError:
            logger.error(f"File not found: {target_dir}/{file_name}.json.")
            raise
        except json.JSONDecodeError:
            logger.error(
                f"Error decoding JSON from the file: {target_dir}/{file_name}.json."
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error while loading file {target_dir}/{file_name}.json: {e}"
            )
            raise
    logger.info(f"Loaded {len(directory)} directories from {target_dir}")

    ## Processing data
    for k, data in enumerate(directory):
        chunk_id = data.get("id", "")
        nom = data.get("nom", "")
        url_annuaire = data.get("url_service_public", "")

        # Adresses
        adresses = []
        # adresses_to_concatenate = []
        try:
            for adresse in data.get("adresse", [{}]):
                # Metadata
                adresses.append(
                    {
                        "adresse": f"{adresse.get('complement1', '')} {adresse.get('complement2', '')} {adresse.get('numero_voie', '')}".strip(),
                        "code_postal": adresse.get("code_postal", ""),
                        "commune": adresse.get("nom_commune", ""),
                        "pays": adresse.get("pays", ""),
                        "longitude": adresse.get("longitude", ""),
                        "latitude": adresse.get("latitude", ""),
                    }
                )

        except Exception:
            pass

        # Phone numbers
        telephones = []
        try:
            for telephone in data.get("telephone", [{}]):
                if telephone.get("description", ""):
                    telephones.append(
                        f"{telephone.get('valeur', '')}. {telephone.get('description', '')}"
                    )
                else:
                    telephones.append(f"{telephone.get('valeur', '')}")
        except Exception:
            pass

        # File modification date
        try:
            date_str = data.get("date_modification", "")
            if date_str:
                date_modification_dt = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
                date_modification = date_modification_dt.strftime("%d/%m/%Y")
            else:
                date_modification = ""
        except ValueError:
            date_modification = ""
            logger.debug(f"Date format error for value: {date_str}")

        # Types
        types = ""
        type_list = []
        pivot_data = data.get("pivot", [])
        type_organisme = data.get("type_organisme", "")

        if pivot_data and type_organisme:
            for pivot in pivot_data:
                if isinstance(pivot, dict) and "type_service_local" in pivot:
                    type_list.append(f"{pivot['type_service_local']}")
            types = ", ".join(type_list)
            types += f" ({type_organisme})"
        elif pivot_data and not type_organisme:
            for pivot in pivot_data:
                if isinstance(pivot, dict) and "type_service_local" in pivot:
                    type_list.append(f"{pivot['type_service_local']}")
            types = ", ".join(type_list)
        elif type_organisme and not pivot_data:
            types += f"{type_organisme}"

        # SIRET and SIREN
        siret = data.get("siret", "")
        siren = data.get("siren", "")

        # URLs
        urls = []
        try:
            for site in data.get("site_internet", [{}]):
                if isinstance(site.get("valeur", ""), list):
                    urls.extend(site.get("valeur", []))
                else:
                    urls.append(site.get("valeur", ""))
        except Exception:
            pass

        # Contact forms
        formulaires_contact = []
        try:
            for formulaire in data.get("formulaire_contact", []):
                if isinstance(formulaire, list):
                    formulaires_contact.extend(formulaire)
                else:
                    formulaires_contact.append(formulaire)
        except Exception:
            pass

        # Emails
        email = []
        try:
            for mail in data.get("adresse_courriel", []):
                if isinstance(mail, list):
                    email.extend(mail)
                else:
                    email.append(mail)
        except Exception:
            pass

        # Opening hours
        horaires_ouverture = make_schedule(data.get("plage_ouverture", []))

        # Mobile applications
        applications_mobile = []
        try:
            for application in data.get("application_mobile", [{}]):
                applications_mobile.append(
                    f"{application.get('description', '')} ({application.get('custom_dico2', '')}) : {application.get('valeur', '')}"
                )
        except Exception:
            pass

        # Social networks
        reseaux_sociaux = []
        try:
            for reseau in data.get("reseau_social", [{}]):
                reseaux_sociaux.append(
                    f"{reseau.get('custom_dico2', '')} ({reseau.get('description', '')}) : {reseau.get('valeur', '')}"
                )
        except Exception:
            pass

        # Additional information and mission description
        information_complementaire = data.get("information_complementaire", "")
        mission = data.get("mission", "")

        # People in charge
        responsables = []
        try:
            for responsable in data.get("affectation_personne", []):
                resp = f"{responsable.get('fonction', '')} : {responsable.get('prenom', '')} {responsable.get('nom', '')}"
                if responsable.get("grade", ""):
                    resp += f" ({responsable.get('grade', '')})"
                if responsable.get("telephone", ""):
                    resp += f"\nTéléphone : {responsable.get('telephone', '')}"
                if responsable.get("adresse_courriel", []):
                    for mail in responsable.get("adresse_courriel", []):
                        resp += f"\nEmail : {mail.get('valeur', '')} ({mail.get('libelle', '')})"
                responsables.append(resp)

        except Exception:
            pass

        # Organizational chart and hierarchy
        organigramme = []
        try:
            for org in data.get("organigramme", []):
                if org.get("libelle"):
                    organigramme.append(
                        f"{org.get('libelle', '')} : {org.get('valeur', '')}"
                    )
                else:
                    organigramme.append(f"{org.get('valeur', '')}")
        except Exception:
            pass

        hierarchie = json.dumps(data.get("hierarchie", []))

        chunk_text = make_chunks_directories(
            nom=nom, mission=mission, types=types, adresses=adresses
        )

        for attempt in range(5):  # Retry embedding up to 5 times
            try:
                embeddings = generate_embeddings(data=chunk_text)
                break
            except PermissionDeniedError as e:
                logger.error(
                    f"PermissionDeniedError (API key issue). Unable to generate embeddings : {e}"
                )
                raise
            except Exception as e:
                if attempt == 4:
                    logger.error(
                        f"Error generating embeddings for text: {chunk_text}. Error: {e}. Maximum retries reached."
                    )
                    raise
                logger.error(
                    f"Error generating embeddings for text: {chunk_text}. Error: {e}. Retrying in 3 seconds (attempt {attempt + 1}/5)"
                )
                time.sleep(3)  # Waiting 3 seconds before retrying

        ## Insert data into the database
        new_data = (
            chunk_id,
            types,
            nom,
            mission,
            json.dumps(adresses),  # Converts to string
            telephones,
            email,
            urls,
            reseaux_sociaux,
            applications_mobile,
            horaires_ouverture,
            formulaires_contact,
            information_complementaire,
            date_modification,
            siret,
            siren,
            responsables,
            organigramme,
            hierarchie,
            url_annuaire,
            chunk_text,
            embeddings,
        )

        insert_data(
            data=[new_data],
            table_name="directories",
        )


def process_dila_xml_files(target_dir: str):
    """
    Processes XML files in the specified target directory and extracts relevant data
    to insert into corresponding database tables.

    Parameters:
        target_dir (str): The root directory containing the XML files to process.

    Raises:
        Logs errors encountered during file processing, including parsing issues
        or missing fields.

    Notes:
        - The extracted data is immediately inserted into the database using the
          `insert_data` function.
        - Textual content is cleaned and formatted for improved readability.
        - Processed files are removed from the filesystem after successful insertion
          into the database.
    """
    for root_dir, dirs, files in os.walk(target_dir):
        for file_name in files:
            if file_name.startswith("LEGIARTI") and file_name.endswith(".xml"):
                print(f"Processing file: {file_name}")
                table_name = "legi"
                file_path = os.path.join(root_dir, file_name)
                try:
                    tree = ET.parse(file_path)
                    root = tree.getroot()
                    etat = root.find(".//ETAT").text
                    if etat in ["VIGUEUR", "ABROGE_DIFF"]:
                        cid = root.find(".//ID").text
                        nature = root.find(".//NATURE").text
                        titre = (
                            root.find(".//CONTEXTE//TEXTE//TITRE_TXT")
                            .get("c_titre_court")
                            .strip(".")
                        )
                        sous_titres = []
                        for elem in root.find(".//CONTEXTE//TEXTE").iter("TITRE_TM"):
                            sous_titres.append(elem.text)
                        sous_titres = " | ".join(sous_titres)
                        numero = root.find(".//NUM").text

                        date_debut = datetime.strptime(
                            root.find(".//DATE_DEBUT").text, "%Y-%m-%d"
                        ).strftime("%d-%m-%Y")
                        date_fin = datetime.strptime(
                            root.find(".//DATE_FIN").text, "%Y-%m-%d"
                        ).strftime("%d-%m-%Y")
                        titre_complet = root.find(".//TITRE_TXT").text

                        nota = []
                        contenu_nota = root.find(".//NOTA//CONTENU")
                        for paragraph in contenu_nota.findall(".//p"):
                            nota.append(paragraph.text)
                        nota = "\n".join(nota)

                        contenu = root.find(".//BLOC_TEXTUEL/CONTENU")
                        texte = []

                        if contenu is not None:
                            # Extract all text
                            content = ET.tostring(
                                contenu, encoding="unicode", method="xml"
                            )
                            content = "".join(ET.fromstring(content).itertext())
                            # Post-process the text to improve readability
                            lines = content.splitlines()  # Split the text into lines
                            cleaned_lines = [
                                line for line in lines if line
                            ]  # Remove empty lines and extra spaces
                            content = "\n".join(
                                cleaned_lines
                            )  # Rejoin the cleaned lines with a newline
                            texte.append(content)
                        texte = "\n".join(texte)

                        chunks = make_chunks(
                            text=texte, chunk_size=1500, chunk_overlap=200
                        )
                        data_to_insert = []

                        for k, chunk_text in enumerate(chunks):
                            try:
                                for attempt in range(5):
                                    try:
                                        embeddings = generate_embeddings(
                                            text=chunk_text
                                        )
                                        break
                                    except PermissionDeniedError as e:
                                        logger.error(
                                            f"PermissionDeniedError (API key issue) for chunk {k} of file {file_path}: {e}"
                                        )
                                        raise
                                    except Exception as e:
                                        if attempt == 4:
                                            logger.error(
                                                f"Error generating embeddings for chunk {k} of file {file_path}: {e}. Maximum retries reached."
                                            )
                                            raise
                                        logger.error(
                                            f"Error generating embeddings for chunk {k} of file {file_path}: {e}. Retrying in 3 seconds (attempt {attempt + 1}/5)"
                                        )
                                        time.sleep(3)

                                chunk_id = f"{cid}_{k}"  # Unique ID for each chunk, starting from 0

                                new_data = (
                                    chunk_id,  # Primary key
                                    cid,  # Original document ID
                                    k,  # Chunk number
                                    nature,
                                    etat,
                                    titre,
                                    titre_complet,
                                    sous_titres,
                                    numero,
                                    date_debut,
                                    date_fin,
                                    nota,
                                    chunk_text,
                                    embeddings,
                                )
                                data_to_insert.append(new_data)
                            except PermissionDeniedError as e:
                                logger.error(
                                    f"PermissionDeniedError (API key issue) for chunk {k} of file {file_path}: {e}"
                                )
                                raise

                        # Insérer tous les chunks en une fois
                        if data_to_insert:
                            insert_data(data=data_to_insert, table_name=table_name)

                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
                else:
                    remove_file(file_path=file_path)  # Remove the file after processing

            elif file_name.startswith("CNILTEXT") and file_name.endswith(".xml"):
                table_name = "cnil"
                file_path = os.path.join(root_dir, file_name)
                print(f"Processing file: {file_name}")
                try:
                    tree = ET.parse(file_path)
                    root = tree.getroot()
                    etat = root.find(".//ETAT_JURIDIQUE").text

                    if etat in ["VIGUEUR"]:
                        cid = root.find(".//ID").text
                        nature = root.find(".//NATURE").text
                        nature_delib = root.find(".//NATURE_DELIB").text
                        titre = root.find(".//TITRE").text
                        titre_complet = root.find(".//TITREFULL").text
                        numero = root.find(".//NUMERO").text
                        date = datetime.strptime(
                            root.find(".//DATE_TEXTE").text, "%Y-%m-%d"
                        ).strftime("%d-%m-%Y")

                        contenu = root.find(".//BLOC_TEXTUEL/CONTENU")
                        texte = []

                        if contenu is not None:
                            # Extract all text
                            content = ET.tostring(
                                contenu, encoding="unicode", method="xml"
                            )
                            content = "".join(ET.fromstring(content).itertext())
                            # Post-process the text to improve readability
                            lines = content.splitlines()  # Split the content into lines
                            cleaned_lines = [
                                line for line in lines if line
                            ]  # Remove empty lines and extra spaces
                            content = "\n".join(
                                cleaned_lines
                            )  # Rejoin the cleaned lines with a newline
                            texte.append(content)
                        texte = "\n".join(texte)

                        chunks = make_chunks(
                            text=texte, chunk_size=1500, chunk_overlap=200
                        )
                        data_to_insert = []

                        for k, chunk_text in enumerate(chunks):
                            try:
                                for attempt in range(5):
                                    try:
                                        embeddings = generate_embeddings(
                                            text=chunk_text
                                        )
                                        break
                                    except PermissionDeniedError as e:
                                        logger.error(
                                            f"PermissionDeniedError (API key issue) for chunk {k} of file {file_path}: {e}"
                                        )
                                        raise
                                    except Exception as e:
                                        if attempt == 4:
                                            logger.error(
                                                f"Error generating embeddings for chunk {k} of file {file_path}: {e}. Maximum retries reached."
                                            )
                                            raise
                                        logger.error(
                                            f"Error generating embeddings for chunk {k} of file {file_path}: {e}. Retrying in 3 seconds (attempt {attempt + 1}/5)"
                                        )
                                        time.sleep(3)

                                chunk_id = f"{cid}_{k}"  # Unique ID for each chunk, starting from 0

                                new_data = (
                                    chunk_id,  # Primary key
                                    cid,  # Original document ID
                                    k,  # Chunk number
                                    nature,
                                    etat,
                                    nature_delib,
                                    titre,
                                    titre_complet,
                                    numero,
                                    date,
                                    chunk_text,
                                    embeddings,
                                )
                                data_to_insert.append(new_data)
                            except PermissionDeniedError as e:
                                logger.error(
                                    f"PermissionDeniedError (API key issue) for chunk {k} of file {file_path}: {e}"
                                )
                                raise

                        # Insérer tous les chunks en une fois
                        if data_to_insert:
                            insert_data(data=data_to_insert, table_name=table_name)

                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
                else:
                    remove_file(file_path=file_path)  # Remove the file after processing

            elif file_name.startswith("CONSTEXT") and file_name.endswith(".xml"):
                table_name = "constit"
                file_path = os.path.join(root_dir, file_name)
                print(f"Processing file: {file_name}")
                try:
                    tree = ET.parse(file_path)
                    root = tree.getroot()

                    cid = root.find(".//ID").text
                    nature = root.find(".//NATURE").text
                    titre = root.find(".//TITRE").text
                    numero = root.find(".//NUMERO").text
                    solution = root.find(".//SOLUTION").text
                    date_decision = datetime.strptime(
                        root.find(".//DATE_DEC").text, "%Y-%m-%d"
                    ).strftime("%d-%m-%Y")
                    contenu = root.find(".//BLOC_TEXTUEL//CONTENU")

                    texte = []

                    if contenu is not None:
                        # Extract all text
                        content = ET.tostring(contenu, encoding="unicode", method="xml")
                        content = "".join(ET.fromstring(content).itertext())
                        # Post-process the content to improve readability
                        lines = content.splitlines()  # Split the content into lines
                        cleaned_lines = [
                            line for line in lines if line
                        ]  # Remove empty lines and extra spaces
                        content = "\n".join(
                            cleaned_lines
                        )  # Rejoin the cleaned lines with a newline
                        texte.append(content)
                    texte = "\n".join(texte)

                    chunks = make_chunks(text=texte, chunk_size=1500, chunk_overlap=200)
                    data_to_insert = []

                    for k, chunk_text in enumerate(chunks):
                        try:
                            for attempt in range(5):
                                try:
                                    embeddings = generate_embeddings(data=chunk_text)
                                    break
                                except PermissionDeniedError as e:
                                    logger.error(
                                        f"PermissionDeniedError (API key issue) for chunk {k} of file {file_path}: {e}"
                                    )
                                    raise
                                except Exception as e:
                                    if attempt == 4:
                                        logger.error(
                                            f"Error generating embeddings for chunk {k} of file {file_path}: {e}. Maximum retries reached."
                                        )
                                        raise
                                    logger.error(
                                        f"Error generating embeddings for chunk {k} of file {file_path}: {e}. Retrying in 3 seconds (attempt {attempt + 1}/5)"
                                    )
                                    time.sleep(3)

                            chunk_id = f"{cid}_{k}"  # Unique ID for each chunk, starting from 0

                            new_data = (
                                chunk_id,  # Primary key
                                cid,  # Original document ID
                                k,  # Chunk number
                                nature,
                                solution,
                                titre,
                                numero,
                                date_decision,
                                chunk_text,
                                embeddings,
                            )
                            data_to_insert.append(new_data)
                        except PermissionDeniedError as e:
                            logger.error(
                                f"PermissionDeniedError (API key issue) for chunk {k} of file {file_path}: {e}"
                            )
                            raise

                    # Insérer tous les chunks en une fois
                    if data_to_insert:
                        insert_data(data=data_to_insert, table_name=table_name)

                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
                else:
                    remove_file(file_path=file_path)  # Remove the file after processing


def process_sheets(target_dir: str, batch_size: int = 10):
    table_name = ""
    if target_dir in [
        SERVICE_PUBLIC_PRO_DATA_FOLDER,
        SERVICE_PUBLIC_PART_DATA_FOLDER,
    ]:
        table_name = "service_public"
    elif target_dir == TRAVAIL_EMPLOI_DATA_FOLDER:
        table_name = "travail_emploi"
    else:
        logger.error(f"Unknown target directory '{target_dir}' for processing sheets.")
        return

    with open(os.path.join(target_dir, "sheets_as_chunks.json")) as f:
        documents = json.load(f)

    corpus_name = target_dir.split("/")[-1]
    corpus_handler = CorpusHandler.create_handler(corpus_name, documents)
    if table_name == "travail_emploi":
        for batch_documents, batch_embeddings in corpus_handler.iter_docs_embeddings(
            batch_size
        ):
            new_data = []

            for document, embeddings in zip(batch_documents, batch_embeddings):
                chunk_id = document["hash"].encode("utf8").hex()
                sid = document["sid"]
                chunk_index = document["chunk_index"]
                title = document["title"]
                surtitre = document["surtitre"]
                source = document["source"]
                introduction = document["introduction"]
                date = document["date"]
                url = document["url"]
                context = document["context"] if "context" in document else ""
                chunk_text = document["text"]

                new_data.append(
                    chunk_id,
                    sid,
                    chunk_index,
                    title,
                    surtitre,
                    source,
                    introduction,
                    date,
                    url,
                    context,
                    chunk_text,
                    embeddings,
                )
            insert_data(data=new_data, table_name=table_name)
    elif table_name == "service_public":
        for batch_documents, batch_embeddings in corpus_handler.iter_docs_embeddings(
            batch_size
        ):
            new_data = []

            for document, embeddings in zip(batch_documents, batch_embeddings):
                chunk_id = document["hash"].encode("utf8").hex()
                sid = document["sid"]
                chunk_index = document["chunk_index"]
                audience = document["audience"]
                theme = document["theme"]
                title = document["title"]
                surtitre = document["surtitre"]
                source = document["source"]
                introduction = document["introduction"]
                url = document["url"]
                related_questions = document["related_questions"]
                web_services = document["web_services"]
                context = document["context"] if "context" in document else ""
                chunk_text = document["text"]

                new_data.append(
                    chunk_id,
                    sid,
                    chunk_index,
                    audience,
                    theme,
                    title,
                    surtitre,
                    source,
                    introduction,
                    url,
                    related_questions,
                    web_services,
                    context,
                    chunk_text,
                    embeddings,
                )
            insert_data(data=new_data, table_name=table_name)
    else:
        logger.error(
            f"Unknown table name '{table_name}' for target directory '{target_dir}'."
        )
        return


def get_data(base_folder: str):
    """
    Processes data files located in the specified base folder according to its type.
    Depending on the value of `base_folder`, this function performs several operations.

    Args:
        base_folder (str): The path to the base folder containing the data to process.

    Raises:
        Any exceptions raised by the underlying processing or file operations are propagated.
    """

    all_dirs = sorted(os.listdir(base_folder))
    if (
        base_folder == NATIONAL_DIRECTORY_FOLDER
        or base_folder == LOCAL_DIRECTORY_FOLDER
    ):
        logger.info(f"Processing directory files located in : {base_folder}")
        process_directories(
            target_dir=base_folder, config_file_path="config/data_config.json"
        )
        logger.info(
            logger.info(
                f"Folder: {base_folder} successfully processed and data successfully inserted into the postgres database"
            )
        )

        remove_folder(folder_path=base_folder)

    elif base_folder == TRAVAIL_EMPLOI_DATA_FOLDER:
        logger.info(f"Processing directory files located in : {base_folder}")

        make_chunks_sheets(
            storage_dir=base_folder,
            structured=True,
            chunk_size=1500,
            chunk_overlap=200,
        )

        process_sheets(target_dir=base_folder)

        logger.info(
            f"Folder: {base_folder} successfully processed and data successfully inserted into the postgres database"
        )

        # remove_folder(folder_path=base_folder)

    elif (
        base_folder == SERVICE_PUBLIC_PRO_DATA_FOLDER
        or base_folder == SERVICE_PUBLIC_PART_DATA_FOLDER
    ):
        logger.info(f"Processing directory files located in : {base_folder}")

        make_chunks_sheets(
            storage_dir=base_folder,
            structured=True,
            chunk_size=1500,
            chunk_overlap=200,
        )
        process_sheets(target_dir=base_folder)

        logger.info(
            f"Folder: {base_folder} successfully processed and data successfully inserted into the postgres database"
        )

        # remove_folder(folder_path=base_folder)

    elif base_folder == CNIL_DATA_FOLDER:
        try:
            all_dirs.remove("cnil")
            all_dirs.insert(0, "cnil")  # Placing the 'cnil' folder at the beginning
        except ValueError:
            logger.info(f"There is no 'cnil' directory in {base_folder}")

        for (
            root_dir
        ) in all_dirs:  # root_dir is the name of each folder inside the base_folder
            target_dir = os.path.join(base_folder, root_dir, "cnil/global/CNIL/TEXT")
            folder_to_remove = os.path.join(base_folder, root_dir)
            if root_dir == "cnil":
                # This is the freemium extracted folder
                target_dir = os.path.join(base_folder, "cnil/global/CNIL/TEXT")
                logger.info(f"Processing folder: {target_dir}")
                print(f"Processing folder: {target_dir}")

                process_dila_xml_files(target_dir=target_dir)
                logger.info(f"Folder: {target_dir} successfully processed")

                remove_folder(folder_path=folder_to_remove)
            else:  # for each folder except the freemium one
                logger.info(f"Processing folder: {target_dir}")
                print(f"Processing folder: {target_dir}")

                process_dila_xml_files(target_dir=target_dir)
                logger.info(
                    f"Folder: {target_dir} successfully processed and data successfully inserted into the postgres database"
                )

                remove_folder(folder_path=folder_to_remove)

    elif base_folder == CONSTIT_DATA_FOLDER:
        try:
            all_dirs.remove("constit")
            all_dirs.insert(0, "constit")  # Placing the 'cnil' folder at the beginning
        except ValueError:
            logger.info(f"There is no 'constit' directory in {base_folder}")

        for (
            root_dir
        ) in all_dirs:  # root_dir is the name of each folder inside the base_folder
            target_dir = os.path.join(base_folder, root_dir, "constit/global/CONS/TEXT")
            folder_to_remove = os.path.join(base_folder, root_dir)
            if root_dir == "constit":
                # This is the freemium extracted folder
                target_dir = os.path.join(base_folder, "constit/global/CONS/TEXT")
                logger.info(f"Processing folder: {target_dir}")

                process_dila_xml_files(target_dir=target_dir)
                logger.info(
                    f"Folder: {target_dir} successfully processed and data successfully inserted into the postgres database"
                )

                remove_folder(folder_path=folder_to_remove)
            else:  # for each folder except the freemium one
                logger.info(f"Processing folder: {target_dir}")
                print(f"Processing folder: {target_dir}")

                process_dila_xml_files(target_dir=target_dir)
                logger.info(
                    f"Folder: {target_dir} successfully processed and data successfully inserted into the postgres database"
                )

                remove_folder(folder_path=folder_to_remove)

    elif base_folder == LEGI_DATA_FOLDER:
        try:
            all_dirs.remove("legi")
            all_dirs.insert(0, "legi")  # Placing the 'legi' folder at the beginning
        except ValueError:
            logger.info(f"There is no 'legi' directory in {base_folder}")

        for (
            root_dir
        ) in all_dirs:  # root_dir is the name of each folder inside the base_folder
            target_dir = os.path.join(
                base_folder, root_dir, "legi/global/code_et_TNC_en_vigueur"
            )
            folder_to_remove = os.path.join(base_folder, root_dir)

            if root_dir == "legi":
                # This is the freemium extracted folder
                target_dir = os.path.join(
                    base_folder, "legi/global/code_et_TNC_en_vigueur"
                )
                logger.info(f"Processing folder: {target_dir}")
                print(f"Processing folder: {target_dir}")

                process_dila_xml_files(target_dir=target_dir)
                logger.info(
                    f"Folder: {target_dir} successfully processed and data successfully inserted into the database"
                )

                remove_folder(folder_path=folder_to_remove)

            else:  # for each folder except the freemium one
                logger.info(f"Processing folder: {target_dir}")
                print(f"Processing folder: {target_dir}")

                process_dila_xml_files(target_dir=target_dir)
                logger.info(
                    f"Folder: {target_dir} successfully processed and data successfully inserted into the database"
                )

                remove_folder(folder_path=folder_to_remove)


def get_all_data(unprocessed_data_folder: str):
    """
    Processes all data directories within the specified unprocessed data folder.

    Args:
        unprocessed_data_folder (str): Path to the folder containing unprocessed data directories.

    Returns:
        None

    Note:
        This function iterates over the contents of the given folder, constructs the full path for each subdirectory,
        and processes the data using the `get_data` function.
    """
    for directory in os.listdir(unprocessed_data_folder):
        base_folder = os.path.join(unprocessed_data_folder, directory)
        get_data(base_folder=base_folder)
