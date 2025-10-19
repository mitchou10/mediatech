import xml.etree.ElementTree as ET
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_legiarti(root: ET.Element, file_name: str) -> dict:  # noqa
    # Implementation for processing LEGIARTI XML files

    if file_name.startswith("LEGIARTI") and file_name.endswith(".xml"):

        try:
            status = root.find(".//ETAT").text
            if status in ["VIGUEUR", "ABROGE_DIFF", "MODIFIE"]:
                cid = root.find(".//ID").text  # doc_id # noqa
                nature = root.find(".//NATURE").text  # noqa
                title = (
                    root.find(".//CONTEXTE//TEXTE//TITRE_TXT")
                    .get("c_titre_court")
                    .strip(".")
                )
                category = root.find(".//CONTEXTE//TEXTE").get("nature")  # noqa
                ministry = root.find(".//CONTEXTE//TEXTE").get("ministere", None)
                subtitles = []
                for elem in root.find(".//CONTEXTE//TEXTE").iter("TITRE_TM"):
                    subtitles.append(elem.text)
                subtitles = " - ".join(subtitles)
                if not subtitles:
                    subtitles = None
                number = root.find(".//NUM").text

                start_date = datetime.strptime(
                    root.find(".//DATE_DEBUT").text, "%Y-%m-%d"
                ).strftime("%Y-%m-%d")
                end_date = datetime.strptime(
                    root.find(".//DATE_FIN").text, "%Y-%m-%d"
                ).strftime("%Y-%m-%d")
                full_title = root.find(".//TITRE_TXT").text

                nota = []
                contenu_nota = root.find(".//NOTA//CONTENU")
                for paragraph in contenu_nota.findall(".//p"):
                    nota.append(paragraph.text)
                nota = "\n".join(nota).strip()
                if not nota:
                    nota = None

                links = []
                for link in root.find(".//LIENS"):
                    links.append(
                        {
                            "text_doc_id": link.get("cidtexte"),
                            "text_signature_date": link.get("datesignatexte"),
                            "doc_id": link.get("id"),
                            "category": link.get("naturetexte"),
                            "nor": link.get("nortexte"),
                            "number": link.get("num"),
                            "text_number": link.get("numtexte"),
                            "link_direction": link.get("sens"),
                            "link_type": link.get("typelien"),
                            "title": link.text,
                        }
                    )

                contenu = root.find(".//BLOC_TEXTUEL/CONTENU")
                text_content = []

                if contenu is not None:
                    # Extract all text
                    content = ET.tostring(contenu, encoding="unicode", method="xml")
                    content = "".join(ET.fromstring(content).itertext())
                    # Post-process the text to improve readability
                    lines = content.splitlines()  # Split the text into lines
                    cleaned_lines = [
                        line for line in lines if line
                    ]  # Remove empty lines and extra spaces
                    content = "\n".join(
                        cleaned_lines
                    )  # Rejoin the cleaned lines with a newline
                    text_content.append(content)
                text_content = "\n".join(text_content)

                new_data = {
                    "cid": cid,  # Original document ID
                    "nature": nature,
                    "category": category,
                    "ministere": ministry,
                    "etat_juridique": status,
                    "title": title,
                    "full_title": full_title,
                    "subtitles": subtitles,
                    "number": number,
                    "start_date": start_date,
                    "end_date": end_date,
                    "nota": nota,
                    "links": links,
                    "text_content": text_content,
                }
                return new_data
        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")
            raise e


def process_cnil_text(root: ET.Element, file_name: str) -> dict:
    if file_name.startswith("CNILTEXT") and file_name.endswith(".xml"):
        try:
            status = root.find(".//ETAT_JURIDIQUE").text

            if status in ["VIGUEUR", "MODIFIE"]:
                cid = root.find(".//ID").text
                nature = root.find(".//NATURE").text
                nature_delib = root.find(".//NATURE_DELIB").text
                title = root.find(".//TITRE").text
                full_title = root.find(".//TITREFULL").text
                number = root.find(".//NUMERO").text
                date = datetime.strptime(
                    root.find(".//DATE_TEXTE").text, "%Y-%m-%d"
                ).strftime("%Y-%m-%d")

                contenu = root.find(".//BLOC_TEXTUEL/CONTENU")
                text_content = []

                if contenu is not None:
                    # Extract all text
                    content = ET.tostring(contenu, encoding="unicode", method="xml")
                    content = "".join(ET.fromstring(content).itertext())
                    # Post-process the text to improve readability
                    lines = content.splitlines()  # Split the content into lines
                    cleaned_lines = [
                        line for line in lines if line
                    ]  # Remove empty lines and extra spaces

                    content = "\n".join(
                        cleaned_lines
                    )  # Rejoin the cleaned lines with a newline
                    text_content.append(content)
                text_content = "\n".join(text_content)

                new_data = {
                    "cid": cid,  # Original document ID
                    "nature": nature,
                    "etat_juridique": status,
                    "nature_delib": nature_delib,
                    "title": title,
                    "full_title": full_title,
                    "number": number,
                    "date": date,
                    "text_content": text_content,  # Original text
                }
                return new_data

        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")
            raise e


def process_constit_text(root: ET.Element, file_name: str) -> dict:
    if file_name.startswith("CONSTEXT") and file_name.endswith(".xml"):

        try:
            cid = root.find(".//ID").text
            nature = root.find(".//NATURE").text
            title = root.find(".//TITRE").text
            number = root.find(".//NUMERO").text
            solution = root.find(".//SOLUTION").text
            decision_date = datetime.strptime(
                root.find(".//DATE_DEC").text, "%Y-%m-%d"
            ).strftime("%Y-%m-%d")
            contenu = root.find(".//BLOC_TEXTUEL//CONTENU")

            text_content = []

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
                text_content.append(content)
            text_content = "\n".join(text_content)

            new_data = {
                "cid": cid,  # Original document ID
                "nature": nature,
                "solution": solution,
                "title": title,
                "number": number,
                "decision_date": decision_date,
                "text_content": text_content,  # Original text
            }
            return new_data

        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")
            raise e


def process_jofre(root: ET.Element, file_name: str) -> dict:
    raise NotImplementedError("Processing for JOFRE files is not implemented yet.")
