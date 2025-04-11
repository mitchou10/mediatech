import os
import xml.etree.ElementTree as ET
import logging
import shutil
from database import insert_data
from config import LEGI_DATA_FOLDER, CNIL_DATA_FOLDER
from datetime import datetime

logging.basicConfig(
    filename="logs/data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def remove_folder(folder_path: str):
    """
    Removes a folder and all its contents if it exists.

    Args:
        folder_path (str): The path to the folder to be removed.

    Returns:
        None

    Logs:
        - INFO: When the folder is successfully removed or doesn't exist.
        - ERROR: When there is an error while removing the folder.
    """
    if os.path.exists(folder_path):
        try:
            shutil.rmtree(folder_path)
            logging.info(f"Removed folder: {folder_path}")
        except Exception as e:
            logging.error(f"Error removing folder {folder_path}: {e}")
    else:
        logging.info(f"Folder {folder_path} does not exist")


def process_xml_files(target_dir: str):
    """
    Processes XML files in the specified target directory and extracts relevant data
    to insert into corresponding database tables.

    The function handles two types of XML files:
    1. Files starting with "LEGIARTI" and ending with ".xml", which are processed and
       inserted into the "legi" table.
    2. Files starting with "CNILTEXT" and ending with ".xml", which are processed and
       inserted into the "cnil" table.

    Parameters:
        target_dir (str): The root directory containing the XML files to process.

    Raises:
        Logs errors encountered during file processing, including parsing issues
        or missing fields.

    Notes:
        - The extracted data is immediately inserted into the database using the
          `insert_data` function.
        - Textual content is cleaned and formatted for improved readability.
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
                        titre_court = (
                            root.find(".//CONTEXTE//TEXTE//TITRE_TXT")
                            .get("c_titre_court")
                            .strip(".")
                        )
                        sous_titres = []
                        for elem in root.find(".//CONTEXTE//TEXTE").iter("TITRE_TM"):
                            sous_titres.append(elem.text)
                        sous_titres = " | ".join(sous_titres)
                        num = root.find(".//NUM").text

                        date_debut = datetime.strptime(
                            root.find(".//DATE_DEBUT").text, "%Y-%m-%d"
                        ).strftime("%d-%m-%Y")
                        date_fin = datetime.strptime(
                            root.find(".//DATE_FIN").text, "%Y-%m-%d"
                        ).strftime("%d-%m-%Y")
                        titre_txt = root.find(".//TITRE_TXT").text

                        nota_paragraphs = []
                        contenu_nota = root.find(".//NOTA//CONTENU")
                        for paragraph in contenu_nota.findall(".//p"):
                            nota_paragraphs.append(paragraph.text)
                        nota_paragraphs = "\n".join(nota_paragraphs)

                        contenu = root.find(".//BLOC_TEXTUEL/CONTENU")
                        paragraphs = []

                        if contenu is not None:
                            # Extract all text
                            text = ET.tostring(
                                contenu, encoding="unicode", method="xml"
                            )
                            text = "".join(ET.fromstring(text).itertext())
                            # Post-process the text to improve readability
                            lines = text.splitlines()  # Split the text into lines
                            cleaned_lines = [
                                line.strip() for line in lines if line.strip()
                            ]  # Remove empty lines and extra spaces
                            text = "\n".join(
                                cleaned_lines
                            )  # Rejoin the cleaned lines with a newline
                            paragraphs.append(text.strip())
                        paragraphs = "\n".join(paragraphs)
                        new_data = (
                            cid,
                            num,
                            etat,
                            nature,
                            date_debut,
                            date_fin,
                            titre_court,
                            sous_titres,
                            titre_txt,
                            nota_paragraphs,
                            paragraphs,
                        )
                        insert_data(
                            data=[new_data], table_name=table_name
                        )  # Insert data immediately

                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")
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
                        titre_full = root.find(".//TITREFULL").text
                        numero = root.find(".//NUMERO").text
                        date = datetime.strptime(
                            root.find(".//DATE_TEXTE").text, "%Y-%m-%d"
                        ).strftime("%d-%m-%Y")
                        contenu = root.find(".//BLOC_TEXTUEL//CONTENU")

                        contenu = root.find(".//BLOC_TEXTUEL/CONTENU")
                        paragraphs = []

                        if contenu is not None:
                            # Extract all text
                            text = ET.tostring(
                                contenu, encoding="unicode", method="xml"
                            )
                            text = "".join(ET.fromstring(text).itertext())
                            # Post-process the text to improve readability
                            lines = text.splitlines()  # Split the text into lines
                            cleaned_lines = [
                                line.strip() for line in lines if line.strip()
                            ]  # Remove empty lines and extra spaces
                            text = "\n".join(
                                cleaned_lines
                            )  # Rejoin the cleaned lines with a newline
                            paragraphs.append(text.strip())
                        paragraphs = "\n".join(paragraphs)

                        new_data = (
                            cid,
                            numero,
                            etat,
                            nature,
                            nature_delib,
                            date,
                            titre,
                            titre_full,
                            paragraphs,
                        )
                        insert_data(
                            data=[new_data], table_name=table_name
                        )  # Insert data immediately

                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")


def get_data(base_folder: str):
    """
    Processes XML files in the specified base folder and removes processed folders.

    Depending on the type of base folder (`LEGI_DATA_FOLDER` or `CNIL_DATA_FOLDER`),
    this function processes XML files located in subdirectories of the base folder.


    Args:
        base_folder (str): The path to the base folder containing subdirectories
                           with XML files to process.

    Logging:
        - Logs the processing status of each folder.
        - Logs a message if the prioritized folder ("legi" or "cnil") is not found.

    Notes:
        - The `process_xml_files` function is used to process the XML files and insert their datas into the PostgreSQL database.
        - The `remove_folder` function is used to remove processed folders.

    Raises:
        ValueError: If there are issues with folder structure or processing.
    """

    all_dirs = sorted(os.listdir(base_folder))
    if base_folder == LEGI_DATA_FOLDER:
        try:
            all_dirs.remove("legi")
            all_dirs.insert(0, "legi")  # Placing the 'legi' folder at the beginning
        except ValueError:
            logging.info(f"There is no 'legi' directory in {base_folder}")

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
                logging.info(f"Processing folder: {target_dir}")
                print(f"Processing folder: {target_dir}")

                process_xml_files(target_dir=target_dir)
                logging.info(f"Folder: {target_dir} successfully processed")

                remove_folder(folder_path=folder_to_remove)

            else:  # for each folder except the freemium one
                logging.info(f"Processing folder: {target_dir}")
                print(f"Processing folder: {target_dir}")

                process_xml_files(target_dir=target_dir)
                logging.info(f"Folder: {target_dir} successfully processed")

                remove_folder(folder_path=folder_to_remove)

    elif base_folder == CNIL_DATA_FOLDER:
        try:
            all_dirs.remove("cnil")
            all_dirs.insert(0, "cnil")  # Placing the 'cnil' folder at the beginning
        except ValueError:
            logging.info(f"There is no 'cnil' directory in {base_folder}")

        for (
            root_dir
        ) in all_dirs:  # root_dir is the name of each folder inside the base_folder
            target_dir = os.path.join(base_folder, root_dir, "cnil/global/CNIL/TEXT")
            folder_to_remove = os.path.join(base_folder, root_dir)
            if root_dir == "cnil":
                # This is the freemium extracted folder
                target_dir = os.path.join(base_folder, "cnil/global/CNIL/TEXT")
                logging.info(f"Processing folder: {target_dir}")
                print(f"Processing folder: {target_dir}")

                process_xml_files(target_dir=target_dir)
                logging.info(f"Folder: {target_dir} successfully processed")

                remove_folder(folder_path=folder_to_remove)
            else:  # for each folder except the freemium one
                logging.info(f"Processing folder: {target_dir}")
                print(f"Processing folder: {target_dir}")

                process_xml_files(target_dir=target_dir)
                logging.info(f"Folder: {target_dir} successfully processed")

                remove_folder(folder_path=folder_to_remove)
