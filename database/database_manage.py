import psycopg2
from psycopg2 import sql
import logging
from config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

logging.basicConfig(
    filename="logs/data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def create_table():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cursor = conn.cursor()

        # Vérifier si la table existe déjà
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'legi'
            );
        """)
        table_exists = cursor.fetchone()[0]

        if table_exists:
            logging.info(f"Table 'LEGI' already exists in database {POSTGRES_DB}")
        else:
            # Créer la table si elle n'existe pas
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
            conn.commit()
            logging.info(f"Table 'LEGI' created successfully in database {POSTGRES_DB}")

        conn.close()
    except Exception as e:
        logging.error(f"Error creating table in PostgreSQL: {e}")


def insert_data(data: list):
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO LEGI (cid, etat, nature, titre_court, sous_titres, numero, date_debut, date_fin, titre, nota, contenu)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cid) DO UPDATE SET
            etat = EXCLUDED.etat,
            nature = EXCLUDED.nature,
            titre_court = EXCLUDED.titre_court,
            sous_titres = EXCLUDED.sous_titres,
            numero = EXCLUDED.numero,
            date_debut = EXCLUDED.date_debut,
            date_fin = EXCLUDED.date_fin,
            titre = EXCLUDED.titre,
            nota = EXCLUDED.nota,
            contenu = EXCLUDED.contenu;
        """
        cursor.executemany(insert_query, data)
        conn.commit()
        conn.close()
        # logging.info(f"Data inserted into PostgreSQL database")
    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")


if __name__ == "__main__":
    # Create the table if does not exist
    create_table()
