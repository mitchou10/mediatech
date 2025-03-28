import os
import xml.etree.ElementTree as ET
import logging
import shutil
from database import insert_data

logging.basicConfig(
    filename="logs/data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def remove_folder(folder_path: str):
    if os.path.exists(folder_path):
        try:
            shutil.rmtree(folder_path)
            logging.info(f"Removed folder: {folder_path}")
        except Exception as e:
            logging.error(f"Error removing folder {folder_path}: {e}")
    else:
        logging.info(f"Folder {folder_path} does not exist")


def process_xml_files(target_dir: str, db_path: str) -> list:
    for root_dir, dirs, files in os.walk(target_dir):
        for file_name in files:
            if file_name.startswith("LEGIARTI") and file_name.endswith(".xml"):
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
                        date_debut = root.find(".//DATE_DEBUT").text
                        date_fin = root.find(".//DATE_FIN").text
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
                            etat,
                            nature,
                            titre_court,
                            sous_titres,
                            num,
                            date_debut,
                            date_fin,
                            titre_txt,
                            nota_paragraphs,
                            paragraphs,
                        )
                        insert_data(db_path, [new_data])  # Insert data immediately

                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")
    logging.info(f"Data in {target_dir} processed successfully")


def get_data(base_folder: str, db_path: str):
    all_dirs = sorted(os.listdir(base_folder))
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
        print(f"{folder_to_remove} will be removed")

        if root_dir == "legi":
            # This is the freemium extracted folder
            target_dir = os.path.join(base_folder, "legi/global/code_et_TNC_en_vigueur")
            logging.info(f"Processing folder: {target_dir}")
            print(f"Processing folder: {target_dir}")

            process_xml_files(target_dir=target_dir, db_path=db_path)
            logging.info(f"Folder: {target_dir} successfully processed")

            remove_folder(folder_path=folder_to_remove)

        else:  # for each folder except the freemium one
            logging.info(f"Processing folder: {target_dir}")
            print(f"Processing folder: {target_dir}")

            process_xml_files(target_dir=target_dir, db_path=db_path)
            logging.info(f"Folder: {target_dir} successfully processed")

            remove_folder(folder_path=folder_to_remove)

# if __name__ == "__main__":
#     base_folder = "data/legi"
#     db_path = "data/legi.db"
#     get_data(base_folder=base_folder, db_path=db_path)
