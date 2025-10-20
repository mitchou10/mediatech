import json
import re
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
                    "doc_id": cid,  # Original document ID
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


def _dole_cut_file_content(text: str):
    """
    Splits legal text into individual articles based on article numbering patterns.

    Args:
        text (str): The legal document text to be split into articles.

    Returns:
        list: A list of dictionaries containing 'article_number' and 'article_text' keys.
              Returns single item with None article_number if no articles found.
    """
    article_pattern = re.compile(r"Article\s+(?:\d{1,2}(?!-)(?:er)?|unique)\b")
    article_match = article_pattern.search(text)

    if not text:
        logger.debug("Empty text provided for cutting.")
        return [{"article_number": None, "article_text": ""}]
    if article_match is None:
        return [{"article_number": None, "article_text": text.strip()}]
    else:
        all_articles = []

        # Finding all articles in the text
        matches = []
        for m in article_pattern.finditer(text):
            matches.append(m)
        if matches:
            for match in matches:
                start = match.start()
                num_match = re.search(r"\d+", match.group())
                if num_match is not None:
                    article_num = int(num_match.group())
                    # Finding the end of the article (next article with a strictly higher number)
                    next_start = len(text)
                    for next_match in matches:
                        next_num_match = re.search(r"\d+", next_match.group())
                        if next_num_match and int(next_num_match.group()) > article_num:
                            next_start = next_match.start()
                            break
                    article_text = text[start:next_start].strip()
                    all_articles.append(
                        {
                            "article_number": article_num,
                            "article_text": article_text.replace("\n\n", "\n"),
                        }
                    )
            return all_articles
        else:
            logger.debug("No articles found in the text.")
            return []


def _dole_cut_exp_memo(text: str, section: str) -> str:
    """
    Extract and parse legal document sections (introduction or articles) from French legal text.

    Args:
        text (str): The legal document text to parse
        section (str): Section type to extract - "introduction" or "articles"

    Returns:
        str: Introduction text if section="introduction"
        list: List of article dictionaries if section="articles", each containing
              article_number, article_synthesis, and title_content
        str: Empty string if text is empty or no content found
    """

    def _get_number_of_articles(text: str) -> int:
        # Finding all occurrences of "Article n" or "Article n-er"
        pattern = re.compile(r"(?:L['’] ?)?[Aa]rticle\s+(?:\d{1,2})(?!-)(?:er)?\b")
        number_pattern = re.compile(r"\d{1,2}")
        matches = pattern.findall(text)
        if matches:
            try:
                # Extract numbers from matches
                # Using regex to find numbers in the matches
                numbers = [
                    int(number_pattern.search(m).group())
                    for m in matches
                    if number_pattern.search(m)
                ]
                # Return the maximum
                return max(numbers)
            except ValueError:
                # If conversion fails, return 0
                logger.debug("Error converting article numbers to integers.")
                return 0
        else:
            return 0

    if not text:
        logger.debug("Empty text provided for cutting.")
        return ""
    article_pattern = re.compile(
        r"(?:L['’] ?a|(?<!')A|l['’]a)rticle\s+\d{1,2}(?!-)(?:er)?$\b"
    )
    title_pattern = re.compile(r"(TITRE\s+\w+)")

    # Finding the first occurrence of TITRE or Article
    titre_match = title_pattern.search(text)
    article_match = article_pattern.search(text)

    # Determine the introduction and the rest of the text
    if titre_match:
        intro = text[: titre_match.start()].strip()
        rest = text[titre_match.start() :].strip()
    elif article_match and not titre_match:
        intro = text[: article_match.start()].strip()
        rest = text[article_match.start() :].strip()
    else:
        intro = None
        rest = text.strip()

    if section.lower() == "introduction":
        return intro

    elif section.lower() == "articles":
        number_of_articles = _get_number_of_articles(text=text)
        all_articles = []

        # Finding all articles and titles in the rest of the text
        matches = []
        for m in article_pattern.finditer(rest):
            matches.append(("article", m))
        for m in title_pattern.finditer(rest):
            matches.append(("title", m))

        if matches:
            # Sort matches by their start position
            matches.sort(key=lambda x: x[1].start())

            articles_texts = {}
            title_ranges = []

            # Preparing title ranges
            for idx, (kind, match) in enumerate(matches):
                if kind == "title":
                    title_start = match.start()
                    # Finding the end of the title
                    title_end = len(rest)
                    for next_idx in range(idx + 1, len(matches)):
                        if matches[next_idx][0] in ("title", "article"):
                            title_end = matches[next_idx][1].start()
                            break
                    title_content = rest[title_start:title_end].strip()
                    title_ranges.append(
                        {
                            "title_start": title_start,
                            "title_end": title_end,
                            "title_content": title_content,
                        }
                    )

            # Associating articles with their titles
            for idx, (kind, match) in enumerate(matches):
                if kind != "article":
                    continue
                start = match.start()
                num_match = re.search(r"\d+", match.group())
                if num_match is not None:
                    article_num = int(num_match.group())
                    # Finding the end of the article (next article with a strictly higher number)
                    next_start = len(rest)
                    for next_idx in range(idx + 1, len(matches)):
                        next_kind, next_match = matches[next_idx]
                        if next_kind == "article":
                            next_num_match = re.search(r"\d+", next_match.group())
                            if (
                                next_num_match
                                and int(next_num_match.group()) > article_num
                            ):
                                next_start = next_match.start()
                                break
                        elif next_kind == "title":
                            next_start = next_match.start()
                            break
                    article_text = rest[start:next_start].strip()

                    # Finding the title content for this article
                    title_content = None

                    for idx, title in enumerate(title_ranges):
                        t_start = title["title_start"]
                        t_end = (
                            title_ranges[idx + 1]["title_start"]
                            if idx + 1 < len(title_ranges)
                            else len(rest)
                        )
                        t_content = title["title_content"]
                        if t_start < start < t_end:
                            title_content = t_content
                            break

                    articles_texts[article_num] = {
                        "article_number": article_num,
                        "article_synthesis": article_text,
                        "title_content": title_content,
                    }

            for number in range(1, number_of_articles + 1):
                article_dict = articles_texts.get(
                    number,
                    {
                        "article_number": number,
                        "article_synthesis": "",
                        "title_content": "",
                    },
                )
                all_articles.append(article_dict)

            return all_articles
        else:
            logger.debug("No articles found in the text.")
            return []


def process_dole_text(root: ET.Element, file_name: str) -> dict:
    if file_name.startswith("JORFDOLE") and file_name.endswith(".xml"):
        cid = root.find(".//ID").text  # doc_id
        title = root.find(".//TITRE").text
        number = root.find(".//NUMERO").text
        category = root.find(".//TYPE").text
        wording = root.find(".//LIBELLE").text  # Libellé
        creation_date = datetime.strptime(
            root.find(".//DATE_CREATION").text, "%Y-%m-%d"
        ).strftime("%Y-%m-%d")

        exp_memo = root.find(".//EXPOSE_MOTIF")

        if exp_memo:
            # Extract all text
            content = ET.tostring(exp_memo, method="xml")
            content = "".join(ET.fromstring(content).itertext())
            exp_memo = _dole_cut_exp_memo(text=content, section="introduction")
            articles_synthesis_dict = _dole_cut_exp_memo(
                text=content, section="articles"
            )
        else:
            exp_memo = title
            articles_synthesis_dict = []

        # Creating chunks for explanatory memorandum
        data_to_insert: list[dict] = []
        content_type = "explanatory_memorandum"
        new_data = {
            "doc_id": cid,  # doc_id
            "category": category,
            "content_type": content_type,
            "title": title,
            "number": number if number else None,
            "wording": wording,
            "creation_date": creation_date,
            "article_number": None,
            "article_title": None,
            "article_synthesis": None,
            "text": exp_memo,
        }

        data_to_insert.append(new_data)
        file_content_list = []
        for k in range(1, 6):  # There can be up to 5 contenu_dossier sections
            contenu_dossier = root.find(f".//CONTENU_DOSSIER_{k}")
            if contenu_dossier is not None:
                # Extract all text
                content = ET.tostring(contenu_dossier, method="xml")
                content = "".join(ET.fromstring(content).itertext()).strip()

                if len(content) > 0:
                    file_content_list.extend(_dole_cut_file_content(text=content))

        results = []
        if len(file_content_list) == 0 and len(articles_synthesis_dict) == 0:
            results = [
                {
                    "article_number": None,
                    "article_synthesis": None,
                    "article_text": None,
                    "article_title": None,
                }
            ]
        elif len(articles_synthesis_dict) > 0 and len(file_content_list) == 0:
            for article in articles_synthesis_dict:
                results.append(
                    {
                        "article_number": article.get("article_number", None),
                        "article_synthesis": article.get("article_synthesis", None),
                        "article_text": None,  # Because there is no file content
                        "article_title": article.get("title_content", None),
                    }
                )

        elif len(articles_synthesis_dict) == 0 and len(file_content_list) > 0:
            for content in file_content_list:
                results.append(
                    {
                        "article_number": content.get("article_number", None),
                        "article_synthesis": None,  # Because there is no article synthesis
                        "article_text": content.get("article_text", None),
                        "article_title": None,  # Because there is no article synthesis
                    }
                )

        else:  # Both articles_synthesis_dict and file_content_list are not empty
            # Merging articles_synthesis_dict and file_content_list by article_number
            d1 = {
                d["article_number"]: d
                for d in articles_synthesis_dict
                if d["article_number"] is not None
            }
            d2 = {
                d["article_number"]: d
                for d in file_content_list
                if d["article_number"] is not None
            }

            for num in set(d1) | set(d2):
                try:
                    merged = {
                        "article_number": num,
                        "article_synthesis": (
                            d1.get(num, {}).get("article_synthesis", None).strip()
                            if d1.get(num, {}).get("article_synthesis")
                            else None
                        ),
                        "article_text": (
                            d2.get(num, {}).get("article_text", None).strip()
                            if d2.get(num, {}).get("article_text")
                            else None
                        ),
                        "article_title": (
                            d1.get(num, {}).get("title_content", None).strip()
                            if d1.get(num, {}).get("title_content")
                            else None
                        ),
                    }
                    results.append(merged)
                except Exception as e:
                    logger.error(f"Error merging data for article number {num}: {e}")
                    raise e

            # Adding all articles with article_number = None
            for d in articles_synthesis_dict:
                if d["article_number"] is None:
                    merged = {
                        "article_number": None,
                        "article_synthesis": (
                            d.get("article_synthesis").strip()
                            if d.get("article_synthesis")
                            else None
                        ),
                        "article_text": None,
                        "article_title": (
                            d.get("title_content").strip()
                            if d.get("title_content")
                            else None
                        ),
                    }
                    results.append(merged)

            for d in file_content_list:
                if d["article_number"] is None:
                    merged = {
                        "article_number": None,
                        "article_synthesis": None,
                        "article_text": (
                            d["article_text"].strip() if d.get("article_text") else None
                        ),
                        "article_title": None,
                    }
                    results.append(merged)

        for result_number, result in enumerate(results):
            if (
                result.get("article_number") is not None
            ):  # The chunks will be created and chunked by article number
                content_type = "article"
                chunks = [
                    (
                        str(result.get("article_synthesis", ""))
                        if result.get("article_synthesis") is not None
                        else ""
                    ),
                ]
                if result.get("article_text"):
                    article_text = result.get("article_text")
                    if article_text is not None:
                        chunks.append(str(article_text).strip())
                text = (
                    "\n".join(chunks).replace("\n\n", "\n").strip()
                )  # Combining article synthesis and text

                new_data = {
                    "doc_id": cid,  # doc_id
                    "category": category,
                    "content_type": content_type,
                    "title": title,
                    "number": number if number else None,
                    "wording": wording,
                    "creation_date": creation_date,
                    "article_number": result.get("article_number", None),
                    "article_title": result.get("article_title", None),
                    "article_synthesis": result.get("article_synthesis", None),
                    "text": text,
                }
                data_to_insert.append(new_data)

    return data_to_insert


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


def process_directories(target_file: str):

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
    for k, data in tqdm(enumerate(directory), total=total_entries, desc="Processing "):

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


def process_contist(root: ET.Element, file_name: str):
    if file_name.startswith("CONSTEXT") and file_name.endswith(".xml"):
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
            "doc_id": cid,  # Original document ID
            "nature": nature,
            "solution": solution,
            "title": title,
            "number": number,
            "decision_date": decision_date,
            "text": text,  # Original text
        }
        return new_data
