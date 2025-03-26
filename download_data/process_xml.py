import os
import xml.etree.ElementTree as ET
import logging

logging.basicConfig(
    filename="logs/data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def process_xml_files(target_dir: str) -> list:
    data = []
    for root_dir, dirs, files in os.walk(target_dir):
        for file_name in files:
            if file_name.startswith("LEGIARTI") and file_name.endswith(".xml"):
                file_path = os.path.join(root_dir, file_name)
                logging.info(f"Processing file: {file_path}")
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
                        for paragraph in contenu.findall(".//p"):
                            paragraphs.append(paragraph.text)
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
                        data.append(new_data)
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")
    return data


def get_data(base_folder):
    for root_dir in os.listdir(base_folder):  # root_dir is the name of each folder
        target_dir = os.path.join(
            base_folder, root_dir, "legi/global/code_et_TNC_en_vigueur"
        )  # for each folder except the freemium one
        if os.path.exists(target_dir):
            return process_xml_files(target_dir=target_dir)
        else:
            target_dir = os.path.join(
                base_folder, "legi/global/code_et_TNC_en_vigueur"
            )  # for the freemium folder
            return process_xml_files(target_dir=target_dir)


if __name__ == "__main__":
    base_folder = "data/legi"
    get_data(base_folder)
