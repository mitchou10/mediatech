import psycopg2
import logging
import json
from config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    config_file_path,
)

logging.basicConfig(
    filename="logs/data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def create_tables(delete_existing: bool = False):
    """
    Creates tables in a PostgreSQL database based on a configuration file. Optionally deletes existing tables.

    Args:
        delete_existing (bool): If True, drops existing tables before creating new ones. Defaults to False.

    Raises:
        Exception: Logs errors encountered during database connection or table creation.

    Behavior:
        - Connects to a PostgreSQL database using credentials defined in global variables.
        - If `delete_existing` is True, drops the tables if they exist.
        - Reads table configuration from a JSON file specified by `config_file_path`.
        - Checks if each table defined in the configuration already exists in the database.
        - Creates the "LEGI" and "CNIL" tables with predefined schemas if they do not exist.
        - Logs the status of table creation or existence.

    Notes:
        - The function assumes the presence of a JSON configuration file at `config_file_path`.
        - The database connection is closed after the operation.
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
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")

    if delete_existing:
        cursor.execute("DROP TABLE IF EXISTS LEGI;")
        cursor.execute("DROP TABLE IF EXISTS CNIL;")
        conn.commit()
        logging.info(f"Existing tables dropped in database {POSTGRES_DB}")

    try:
        with open(config_file_path, "r") as file:
            config = json.load(file)
        for table_name, data in config.items():
            # Vérifier si la table existe déjà
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = '{table_name.lower()}'
                );
            """)
            table_exists = cursor.fetchone()[0]

            if table_exists:
                logging.info(
                    f"Table '{table_name.lower()}' already exists in database {POSTGRES_DB}"
                )
            else:
                # Créer la table si elle n'existe pas
                if table_name.lower() == "legi":
                    cursor.execute("""
                        CREATE TABLE LEGI (
                            cid TEXT PRIMARY KEY,
                            etat TEXT,
                            nature TEXT,
                            titre_court TEXT,
                            sous_titres TEXT,
                            numero TEXT,
                            date_debut TEXT,
                            date_fin TEXT,
                            titre TEXT,
                            nota TEXT,
                            contenu TEXT
                        )
                    """)
                elif table_name.lower() == "cnil":
                    cursor.execute("""
                        CREATE TABLE CNIL (
                            cid TEXT PRIMARY KEY,
                            numero TEXT,
                            etat TEXT,
                            nature TEXT,
                            nature_delib TEXT,
                            date TEXT,
                            titre_court TEXT,
                            titre TEXT,
                            contenu TEXT
                        )
                    """)
                conn.commit()
                logging.info(
                    f"Table '{table_name}' created successfully in database {POSTGRES_DB}"
                )

        conn.close()
    except Exception as e:
        logging.error(f"Error creating table in PostgreSQL: {e}")


def insert_data(data: list, table_name: str):
    """
    Inserts data into a specified PostgreSQL table. Supports the "legi" and "cnil" tables.
    If a conflict occurs on the "cid" column, the existing row is updated with the new values.

    Args:
        data (list): A list of tuples containing the data to be inserted. Each tuple should match
                     the structure of the target table.
        table_name (str): The name of the table into which the data will be inserted.
                          Supported values are "legi" and "cnil".

    Raises:
        Exception: Logs an error if there is an issue with the database connection or query execution.

    Notes:
        - For the "legi" table, the expected tuple structure is:
          (cid, numero, etat, nature, date_debut, date_fin, titre_court, sous_titres, titre, nota, contenu)
        - For the "cnil" table, the expected tuple structure is:
          (cid, numero, etat, nature, nature_delib, date, titre_court, titre, contenu)
        - Logs an error and exits if an unsupported table name is provided.
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
        if table_name.lower() == "legi":
            insert_query = """
                INSERT INTO LEGI (cid, numero, etat, nature, date_debut, date_fin, titre_court, sous_titres, titre, nota, contenu)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cid) DO UPDATE SET
                numero = EXCLUDED.numero,
                etat = EXCLUDED.etat,
                nature = EXCLUDED.nature,
                date_debut = EXCLUDED.date_debut,
                date_fin = EXCLUDED.date_fin,
                titre_court = EXCLUDED.titre_court,
                sous_titres = EXCLUDED.sous_titres,
                titre = EXCLUDED.titre,
                nota = EXCLUDED.nota,
                contenu = EXCLUDED.contenu;
            """
        elif table_name.lower() == "cnil":
            insert_query = """
                INSERT INTO CNIL (cid, numero, etat, nature, nature_delib, date, titre_court, titre, contenu)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cid) DO UPDATE SET
                numero = EXCLUDED.numero,
                etat = EXCLUDED.etat,
                nature = EXCLUDED.nature,
                nature_delib = EXCLUDED.nature_delib,
                date = EXCLUDED.date,
                titre_court = EXCLUDED.titre_court,
                titre = EXCLUDED.titre,
                contenu = EXCLUDED.contenu;
            """
        else:
            logging.error(f"Unknown table name: {table_name}")
            conn.commit()
            conn.close()
            return
        cursor.executemany(insert_query, data)
        conn.commit()
        conn.close()
        # logging.info(f"Data inserted into PostgreSQL database")
    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")
