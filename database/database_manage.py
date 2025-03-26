import sqlite3
import logging
import os

logging.basicConfig(
    filename="logs/data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def create_database(db_path):
    if os.path.exists(db_path):
        logging.info(f"Database already exists at {db_path}")
        return
    else:
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS LEGI (
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
            conn.close()
            logging.info(f"Database created at {db_path}")
        except Exception as e:
            logging.error(f"Error creating database at {db_path}: {e}")


def insert_data(db_path, data):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT OR REPLACE INTO LEGI (cid, etat, nature, titre_court, sous_titres, numero, date_debut, date_fin, titre, nota, contenu)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            data,
        )
        conn.commit()
        conn.close()
        logging.info(f"Data inserted into database at {db_path}")
    except Exception as e:
        logging.error(f"Error inserting data into database at {db_path}: {e}")


if __name__ == "__main__":
    data = [
        (
            "cid1",
            "etat1",
            "nature1",
            "titre_court1",
            "sous_titres1",
            "numero1",
            "date_debut1",
            "date_fin1",
            "titre1",
            "nota1",
            "contenu1",
        ),
        (
            "cid2",
            "etat2",
            "nature2",
            "titre_court2",
            "sous_titres2",
            "numero2",
            "date_debut2",
            "date_fin2",
            "titre2",
            "nota2",
            "contenu2",
        ),
        # Ajoutez autant de lignes que nécessaire
    ]
    create_database("data/legi.db")
    insert_data("data/legi.db", data)
