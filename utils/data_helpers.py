import os
import shutil
import logging
from datetime import datetime
import polars as pl
import psycopg2
from config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    parquet_files_folder,
)


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


def make_schedule(plages: list) -> str:
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


def remove_file(file_path: str):
    """
    Removes a file if it exists.

    Args:
        file_path (str): The path to the file to be removed.

    Returns:
        None

    Logs:
        - INFO: When the file is successfully removed or doesn't exist.
        - ERROR: When there is an error while removing the file.
    """
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logging.info(f"Removed file: {file_path}")
        except Exception as e:
            logging.error(f"Error removing file {file_path}: {e}")
    else:
        logging.info(f"File {file_path} does not exist")


def export_tables_to_parquet(output_folder: str = parquet_files_folder):
    """
    Exports all tables from the postgresql database to Parquet files.

    Args:
        parquet_files_folder (str): The path where the Parquet files will be saved.

    Returns:
        None
    """

    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cursor = conn.cursor()

        cursor.execute(
            "SELECT table_name FROM information_schema.tables where table_schema='public' AND table_type='BASE TABLE';"
        )
        tables = cursor.fetchall()
        tables = [table[0] for table in tables]
        os.makedirs(output_folder, exist_ok=True)
        for table_name in tables:
            cursor.execute(f"SELECT * FROM {table_name}")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            output_path = f"{parquet_files_folder}/{table_name}.parquet"
            if rows:
                logging.info(
                    f"Exporting table '{table_name}' to Parquet file: {output_path}"
                )
                df = pl.DataFrame(rows, schema=columns, orient="row")
                df.write_parquet(output_path)
                logging.info(
                    f"Sucessfully exported table '{table_name}' to Parquet file: {output_path}"
                )
            else:
                logging.warning(
                    f"No data found in table '{table_name}'. No Parquet file created."
                )
    except Exception as e:
        logging.error(f"Error exporting table '{table_name}' to Parquet: {e}")
    finally:
        if "conn" in locals():
            conn.close()
