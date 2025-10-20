import json
from tqdm import tqdm
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



def format_time(time: str) -> str:
    """
    Formats a time string from "HH:MM:SS" to "HhMM" format.

    Args:
        time (str): A time string in the format "HH:MM:SS".

    Returns:
        str: The formatted time string in "HhMM" format (e.g., "9h30" for "09:30:00").
             Returns an empty string if the input is empty.
             Returns the original input if parsing fails.
    """
    if not time:
        return ""
    try:
        return datetime.strptime(time, "%H:%M:%S").strftime("%-Hh%M")
    except Exception:
        return time


def _make_schedule(plages: list) -> str:
    """
    Generates a formatted schedule string from a list of time slot dictionaries.
    Used for the metadata 'plage_ouverture' from the directories.

    Each dictionary in the input list should represent a time slot (plage) with the following keys:
        - "nom_jour_debut": Name of the starting day.
        - "nom_jour_fin": Name of the ending day (optional, can be the same as start).
        - "valeur_heure_debut_1": Start time for the first period (optional).
        - "valeur_heure_fin_1": End time for the first period (optional).
        - "valeur_heure_debut_2": Start time for the second period (optional).
        - "valeur_heure_fin_2": End time for the second period (optional).
        - "commentaire": Additional comment for the time slot (optional).

    Returns:
        str: A formatted multi-line string representing the schedule, where each line corresponds to a time slot.
             If the input list is empty, returns the input as is.
    """
    lines = []
    if plages:
        for plage in plages:
            jour = plage["nom_jour_debut"]
            if (
                plage["nom_jour_debut"] != plage["nom_jour_fin"]
                and plage["nom_jour_fin"]
            ):
                jour += f" à {plage['nom_jour_fin']}"
            heures = []
            if plage["valeur_heure_debut_1"] and plage["valeur_heure_fin_1"]:
                heures.append(
                    f"{format_time(plage['valeur_heure_debut_1'])} à {format_time(plage['valeur_heure_fin_1'])}"
                )
            if plage["valeur_heure_debut_2"] and plage["valeur_heure_fin_2"]:
                heures.append(
                    f"{format_time(plage['valeur_heure_debut_2'])} à {format_time(plage['valeur_heure_fin_2'])}"
                )
            heure_str = " et de ".join(heures)
            line = f"{jour} : {heure_str}" if heure_str else f"{jour}"
            if plage.get("commentaire"):
                line += f". {plage['commentaire']}"
            lines.append(line.strip())
        schedule = "\n".join(lines)
    else:
        return plages
    return schedule

def process_directories(
    target_file: str
):
    
    
    ### Loading directory
    directory = []
    with open(target_file, encoding="utf-8") as json_file:
        json_data = json.load(json_file)
        if not directory:  # First file
            directory = json_data["service"]
        else:
            directory.extend(json_data["service"])
    
    
    total_entries = len(directory)
    
    ## Processing data
    all_new_doc_ids = []
    for k, data in tqdm(
        enumerate(directory), total=total_entries, desc="Processing "
    ):
        

        chunk_id = data.get("id", "")
        name = data.get("nom", "")
        directory_url = data.get("url_service_public", "")

        # Addresses
        addresses = []
        try:
            for adresse in data.get("adresse", [{}]):
                # Metadata
                addresses.append(
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
        phone_numbers = []
        try:
            for telephone in data.get("telephone", [{}]):
                if telephone.get("description", ""):
                    phone_numbers.append(
                        f"{telephone.get('valeur', '')}. {telephone.get('description', '')}"
                    )
                else:
                    phone_numbers.append(f"{telephone.get('valeur', '')}")
        except Exception:
            pass

        # File modification date
        try:
            date_str = data.get("date_modification", "")
            if date_str:
                modification_date_dt = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
                modification_date = modification_date_dt.strftime("%Y-%m-%d")
            else:
                modification_date = ""
        except ValueError:
            modification_date = ""
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
        contact_forms = []
        try:
            for formulaire in data.get("formulaire_contact", []):
                if isinstance(formulaire, list):
                    contact_forms.extend(formulaire)
                else:
                    contact_forms.append(formulaire)
        except Exception:
            pass

        # Emails
        mails = []
        try:
            for mail in data.get("adresse_courriel", []):
                if isinstance(mail, list):
                    mails.extend(mail)
                else:
                    mails.append(mail)
        except Exception:
            pass

        # Opening hours
        opening_hours = _make_schedule(data.get("plage_ouverture", []))

        # Mobile applications
        mobile_applications = []
        try:
            for application in data.get("application_mobile", [{}]):
                mobile_applications.append(
                    f"{application.get('description', '')} ({application.get('custom_dico2', '')}) : {application.get('valeur', '')}"
                )
        except Exception:
            pass

        # Social medias
        social_medias = []
        try:
            for reseau in data.get("reseau_social", [{}]):
                if reseau.get("description", ""):
                    social_medias.append(
                        f"{reseau.get('custom_dico2', '')} ({reseau.get('description', '')}) : {reseau.get('valeur', '')}"
                    )
                else:
                    social_medias.append(
                        f"{reseau.get('custom_dico2', '')} : {reseau.get('valeur', '')}"
                    )
        except Exception:
            pass

        # Additional information and mission description
        additional_information = data.get("information_complementaire", "")
        mission_description = data.get("mission", "")

        # People in charge
        people_in_charge = data.get("affectation_personne", [{}])

        # Organizational chart and hierarchy
        organizational_chart = []
        try:
            for org in data.get("organigramme", []):
                if org.get("libelle"):
                    organizational_chart.append(
                        f"{org.get('libelle', '')} : {org.get('valeur', '')}"
                    )
                else:
                    organizational_chart.append(f"{org.get('valeur', '')}")
        except Exception:
            pass

        hierarchy = data.get("hierarchie", [])
        
        
        ## Insert data into the database
        new_data = {
            "doc_id": chunk_id,
            "types": types,
            "name": name,
            "mission_description": mission_description,
            "addresses": json.dumps(addresses),  # Converts to string
            "phone_numbers": json.dumps(phone_numbers),
            "mails": json.dumps(mails),
            "urls": json.dumps(urls),
            "social_medias": json.dumps(social_medias),
            "mobile_applications": json.dumps(mobile_applications),
            "opening_hours": json.dumps(opening_hours),
            "contact_forms": contact_forms,
            "additional_information": additional_information,
            "modification_date": modification_date,
            "siret": siret,
            "siren": siren,
            "people_in_charge": json.dumps(people_in_charge),
            "organizational_chart": json.dumps(organizational_chart),
            "hierarchy": json.dumps(hierarchy),
            "directory_url": directory_url,
        }
        all_new_doc_ids.append(new_data)
    return all_new_doc_ids

