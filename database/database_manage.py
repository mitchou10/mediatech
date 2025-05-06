import psycopg2
from psycopg2.extras import RealDictCursor
from qdrant_client import QdrantClient, models
import logging
import json
from fastembed import SparseTextEmbedding
from tqdm import tqdm
from config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    config_file_path,
)
from utils import generate_embeddings

logging.basicConfig(
    filename="logs/data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def create_tables(delete_existing: bool = False):
    """
    Creates the necessary tables in the PostgreSQL database as specified in the data configuration file.
    Optionally deletes existing tables before creation.
    This function:
    - Connects to the PostgreSQL database using credentials from environment variables.
    - Ensures the `pgvector` extension is enabled for vector-based operations.
    - Reads the table configuration from a JSON file.
    - Iterates through the configured table names, and for each:
        - Optionally drops the table if it exists and `delete_existing` is True.
        - Checks if the table already exists; if not, creates it with the appropriate schema.
        - Adds a vector column for embeddings and creates an HNSW index for efficient similarity search.
    - Commits all changes and logs the process.
    Args:
        delete_existing (bool, optional): If True, existing tables will be dropped before creation. Defaults to False.
    Raises:
        Logs errors if database connection, extension enabling, table creation, or index creation fails.
    """

    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cursor = conn.cursor()
        logging.info("Connected to PostgreSQL database")
        probe_vector = generate_embeddings(text="Hey, I'am a probe")
        embedding_size = len(probe_vector)

        # Vérification de l'extension pgvector
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            conn.commit()

            # Vérifier si l'extension a été correctement installée
            cursor.execute("SELECT * FROM pg_extension WHERE extname = 'vector';")
            if cursor.fetchone() is None:
                logging.error(
                    "pgvector extension could not be enabled. Please check if it's installed in your PostgreSQL instance."
                )
                return
            logging.info("pgvector extension enabled successfully")
        except Exception as e:
            logging.error(f"Error enabling pgvector extension: {e}")
            return

        with open(config_file_path, "r") as file:
            config = json.load(file)

        # Listing all the tables in the config file
        table_names = []
        for category, data in config.items():
            if category.lower() == "dila":
                for table in data.keys():
                    table_names.append(table)
            else:
                table_names.append(category)

        for table_name in table_names:
            if delete_existing:
                # Drop the table if it exists
                cursor.execute(f"DROP TABLE IF EXISTS {table_name.upper()} CASCADE;")

                conn.commit()
                logging.info(
                    f"Table '{table_name.upper()}' dropped successfully in database {POSTGRES_DB}"
                )

            # Checking if the table already exists
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
                    f"Table '{table_name.upper()}' already exists in database {POSTGRES_DB}"
                )
            else:
                # Créer la table si elle n'existe pas

                if table_name.lower() == "directories":
                    cursor.execute(f"""
                        CREATE TABLE DIRECTORIES (
                            chunk_id TEXT PRIMARY KEY,
                            types TEXT,
                            nom TEXT,
                            mission TEXT,
                            adresses TEXT,
                            telephones TEXT,
                            email TEXT,
                            urls TEXT,
                            reseaux_sociaux TEXT,
                            applications_mobile TEXT,
                            horaires_ouverture TEXT,
                            formulaires_contact TEXT,
                            information_complementaire TEXT,
                            date_modification TEXT,
                            siret TEXT,
                            siren TEXT,
                            responsables TEXT,
                            organigramme TEXT,
                            hierarchie TEXT,
                            url_annuaire TEXT,
                            chunk_text TEXT,
                            embeddings vector({embedding_size}),
                            UNIQUE(chunk_id)
                        )
                    """)

                elif table_name.lower() == "cnil":
                    cursor.execute(f"""
                        CREATE TABLE CNIL (
                            chunk_id TEXT PRIMARY KEY,
                            cid TEXT NOT NULL,
                            chunk_number INTEGER NOT NULL,
                            nature TEXT,
                            etat TEXT,
                            nature_delib TEXT,
                            titre TEXT,
                            titre_complet TEXT,
                            numero TEXT,
                            date TEXT,
                            chunk_text TEXT,
                            embeddings vector({embedding_size}),
                            UNIQUE(cid, chunk_number)
                        )
                    """)

                elif table_name.lower() == "constit":
                    cursor.execute(f"""
                        CREATE TABLE CONSTIT (
                            chunk_id TEXT PRIMARY KEY,
                            cid TEXT NOT NULL,
                            chunk_number INTEGER NOT NULL,
                            nature TEXT,
                            solution TEXT,
                            titre TEXT,
                            numero TEXT,
                            date_decision TEXT,
                            chunk_text TEXT,
                            embeddings vector({embedding_size}),
                            UNIQUE(cid, chunk_number)
                        )
                    """)

                elif table_name.lower() == "dole":
                    cursor.execute(f"""
                        CREATE TABLE DOLE (
                            chunk_id TEXT PRIMARY KEY,
                            cid TEXT NOT NULL,
                            chunk_number INTEGER NOT NULL,
                            nature TEXT,
                            type TEXT,
                            titre TEXT,
                            numero TEXT,
                            date TEXT,
                            chunk_text TEXT,
                            embeddings vector({embedding_size}),
                            UNIQUE(cid, chunk_number)
                        )
                    """)

                elif table_name.lower() == "legi":
                    cursor.execute(f"""
                        CREATE TABLE LEGI (
                            chunk_id TEXT PRIMARY KEY,
                            cid TEXT NOT NULL,
                            chunk_number INTEGER NOT NULL,
                            nature TEXT,
                            etat TEXT,
                            titre TEXT,
                            titre_complet TEXT,
                            sous_titres TEXT,
                            numero TEXT,
                            date_debut TEXT,
                            date_fin TEXT,
                            nota TEXT,
                            chunk_text TEXT,
                            embeddings vector({embedding_size}),
                            UNIQUE(cid, chunk_number)
                        )
                    """)

                # Create index for vector similarity search
                try:
                    cursor.execute(f"""
                        CREATE INDEX ON {table_name.upper()} USING hnsw (embeddings vector_cosine_ops)
                        WITH (m = 16, ef_construction = 128);
                    """)
                except Exception as e:
                    logging.error(
                        f"Error creating HNSW index on {table_name.upper()} table: {e}"
                    )

                conn.commit()
                logging.info(
                    f"Table '{table_name.upper()}' created successfully in database {POSTGRES_DB}"
                )

    except Exception as e:
        logging.error(f"Error creating tables in PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()


def insert_data(data: list, table_name: str):
    """
    Inserts a list of data rows into the specified PostgreSQL table, handling upserts and duplicate avoidance.

    Depending on the table name, constructs the appropriate INSERT ... ON CONFLICT SQL statement and executes it for all provided data rows.
    For tables other than "directories", existing rows with the same 'cid' are deleted before insertion to avoid duplicates and outdated data.

    Args:
        data (list): A list of tuples, each representing a row to insert into the database.
        table_name (str): The name of the target table. Supported values are "directories", "cnil", "constit", and "legi".

    Raises:
        Logs errors if any exception occurs during database operations.

    Notes:
        - Uses psycopg2 for PostgreSQL connection and execution.
        - Table and column names are hardcoded for each supported table.
        - Performs upsert (insert or update on conflict) based on the primary key 'chunk_id'.
        - Logs an error and returns if an unknown table name is provided.
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

        source_cid = data[0][1]

        if table_name.lower() != "directories":  # Only for data having a cid
            # Delete the existing data for the same cid in order to avoid duplicates and outdated data
            delete_query = f"DELETE FROM {table_name.upper()} WHERE cid = %s"
            cursor.execute(delete_query, (source_cid,))

        if table_name.lower() == "directories":
            insert_query = """
                INSERT INTO DIRECTORIES (chunk_id, types, nom, mission, adresses, telephones, email, urls, reseaux_sociaux, applications_mobile, horaires_ouverture, formulaires_contact, information_complementaire, date_modification, siret, siren, responsables, organigramme, hierarchie, url_annuaire, chunk_text, embeddings)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chunk_id) DO UPDATE SET
                types = EXCLUDED.types,
                nom = EXCLUDED.nom,
                mission = EXCLUDED.mission,
                adresses = EXCLUDED.adresses,
                telephones = EXCLUDED.telephones,
                email = EXCLUDED.email,
                urls = EXCLUDED.urls,
                reseaux_sociaux = EXCLUDED.reseaux_sociaux,
                applications_mobile = EXCLUDED.applications_mobile,
                horaires_ouverture = EXCLUDED.horaires_ouverture,
                formulaires_contact = EXCLUDED.formulaires_contact,
                information_complementaire = EXCLUDED.information_complementaire,
                date_modification = EXCLUDED.date_modification,
                siret = EXCLUDED.siret,
                siren = EXCLUDED.siren,
                responsables = EXCLUDED.responsables,
                organigramme = EXCLUDED.organigramme,
                hierarchie = EXCLUDED.hierarchie,
                url_annuaire = EXCLUDED.url_annuaire,
                chunk_text = EXCLUDED.chunk_text,
                embeddings = EXCLUDED.embeddings;
                """

        elif table_name.lower() == "cnil":
            insert_query = """
                INSERT INTO CNIL (chunk_id, cid, chunk_number, nature, etat, nature_delib, titre, titre_complet, numero, date, chunk_text, embeddings)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chunk_id) DO UPDATE SET
                cid = EXCLUDED.cid,
                chunk_number = EXCLUDED.chunk_number,
                nature = EXCLUDED.nature,
                etat = EXCLUDED.etat,
                nature_delib = EXCLUDED.nature_delib,
                titre = EXCLUDED.titre,
                titre_complet = EXCLUDED.titre_complet,
                numero = EXCLUDED.numero,
                date = EXCLUDED.date,
                chunk_text = EXCLUDED.chunk_text,
                embeddings = EXCLUDED.embeddings;
            """
        elif table_name.lower() == "constit":
            insert_query = """
                INSERT INTO CONSTIT (chunk_id, cid, chunk_number, nature, solution, titre, numero, date_decision, chunk_text, embeddings)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chunk_id) DO UPDATE SET
                cid = EXCLUDED.cid,
                chunk_number = EXCLUDED.chunk_number,
                nature = EXCLUDED.nature,
                solution = EXCLUDED.solution,
                titre = EXCLUDED.titre,
                numero = EXCLUDED.numero,
                date_decision = EXCLUDED.date_decision,
                chunk_text = EXCLUDED.chunk_text,
                embeddings = EXCLUDED.embeddings;
            """

        elif table_name.lower() == "legi":
            insert_query = """
                INSERT INTO LEGI (chunk_id, cid, chunk_number, nature, etat, titre, titre_complet, sous_titres, numero, date_debut, date_fin, nota, chunk_text, embeddings)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chunk_id) DO UPDATE SET
                cid = EXCLUDED.cid,
                chunk_number = EXCLUDED.chunk_number,
                nature = EXCLUDED.nature,
                etat = EXCLUDED.etat,
                titre = EXCLUDED.titre,
                titre_complet = EXCLUDED.titre_complet,
                sous_titres = EXCLUDED.sous_titres,
                numero = EXCLUDED.numero,
                date_debut = EXCLUDED.date_debut,
                date_fin = EXCLUDED.date_fin,
                nota = EXCLUDED.nota,
                chunk_text = EXCLUDED.chunk_text,
                embeddings = EXCLUDED.embeddings;
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
        logging.error(f"Error inserting data into PostgreSQL: {e}\n{data}")


def postgres_to_qdrant(
    table_name: str,
    qdrant_client: QdrantClient,
    collection_name: str,
    delete_existing: bool = False,
):
    """
    Transfer data from a PostgreSQL table to a Qdrant vector database collection.

    This function reads data from a specified PostgreSQL table, generates embeddings
    for hybrid search using the BM25 model, and stores the data in a Qdrant collection
    with vector and sparse vector configurations.

    Args:
        table_name (str): Name of the PostgreSQL table to read data from.
        qdrant_client (QdrantClient): Initialized Qdrant client for database operations.
        collection_name (str): Name of the Qdrant collection to write data to.
        delete_existing (bool, optional): Whether to delete existing collection data.
            Defaults to False (though the collection is recreated regardless).

    Raises:
        Exception: Any error encountered during database operations is logged.

    Note:
        The function uses BAAI/bge-m3 for dense vector embeddings and Qdrant/bm25 by default for
        sparse vector embeddings to support hybrid search.
    """

    probe_vector = generate_embeddings(text="Hey, I'am a probe", model="BAAI/bge-m3")
    embedding_size = len(probe_vector)
    bm25_embedding_model = SparseTextEmbedding("Qdrant/bm25")  # For hybrid search

    if delete_existing:
        # Drop the collection if it exists
        try:
            qdrant_client.delete_collection(collection_name=collection_name)
            logging.info(f"Collection '{collection_name}' deleted successfully")
        except Exception as e:
            logging.error(f"Error deleting collection '{collection_name}': {e}")

    # Create the Qdrant collection if it doesn't exist
    qdrant_client.recreate_collection(
        collection_name=collection_name,
        vectors_config={
            "BAAI/bge-m3": models.VectorParams(
                size=embedding_size, distance=models.Distance.COSINE
            )
        },
        sparse_vectors_config={
            "bm25": models.SparseVectorParams(
                modifier=models.Modifier.IDF,
            )  # For hybrid search
        },
    )

    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Read data from PostgreSQL
        cursor.execute(f"SELECT * FROM {table_name.upper()}")
        rows = cursor.fetchall()

        # Prepare data for Qdrant
        for row in tqdm(rows, desc="Inserting data into Qdrant", unit="rows"):
            bm25_embeddings = list(
                bm25_embedding_model.passage_embed(row["chunk_text"])
            )
            chunk_id = row["chunk_id"]
            embeddings = row["embeddings"]
            metadata = dict(row)
            del (
                metadata["chunk_id"],
                metadata["embeddings"],
            )  # Remove unnecessary fields from metadata

        qdrant_client.upsert(
            collection_name=collection_name,
            points=[
                models.PointStruct(
                    id=chunk_id,
                    vector={
                        "BAAI/bge-m3": embeddings,
                        "bm25": bm25_embeddings[0].as_object(),
                    },
                    payload=metadata,
                )
            ],
        )

        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting data into Qdrant: {e}")
