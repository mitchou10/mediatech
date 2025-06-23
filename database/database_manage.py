import psycopg2
from psycopg2.extras import RealDictCursor
from qdrant_client import QdrantClient, models
import json
from fastembed import SparseTextEmbedding
from tqdm import tqdm
from config import (
    get_logger,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    config_file_path,
)
from utils import generate_embeddings, format_model_name

logger = get_logger(__name__)


def create_tables(model="BAAI/bge-m3", delete_existing: bool = False):
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
        logger.info("Connected to PostgreSQL database")
        probe_vector = generate_embeddings(data="Hey, I'am a probe", model=model)[0]
        embedding_size = len(probe_vector)

        model_name = format_model_name(model)

        # Enabling Pgvector extension
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            conn.commit()

            # Checks if the extension is enabled
            cursor.execute("SELECT * FROM pg_extension WHERE extname = 'vector';")
            if cursor.fetchone() is None:
                logger.error(
                    "pgvector extension could not be enabled. Please check if it's installed in your PostgreSQL instance."
                )
                return
            logger.info("pgvector extension enabled successfully")
        except Exception as e:
            logger.error(f"Error enabling pgvector extension: {e}")
            return

        with open(config_file_path, "r") as file:
            config = json.load(file)

        # Listing all the tables in the config file
        table_names = []
        for category, data in config.items():
            if category.lower().startswith(
                "service_public"
            ):  # Gathering service public pro and part sheets in one table
                if "SERVICE_PUBLIC" not in table_names:
                    table_names.append("SERVICE_PUBLIC")
                else:
                    pass
            elif category.lower() == "travail_emploi":
                table_names.append("TRAVAIL_EMPLOI")
            else:
                table_names.append(category)

        for table_name in table_names:
            if delete_existing:
                # Drop the table if it exists
                cursor.execute(f"DROP TABLE IF EXISTS {table_name.upper()} CASCADE;")

                conn.commit()
                logger.info(
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
                logger.info(
                    f"Table '{table_name.upper()}' already exists in database {POSTGRES_DB}"
                )
            else:
                # Create table if doesn't exist

                if table_name.lower().endswith("directory"):
                    cursor.execute(f"""
                        CREATE TABLE {table_name.upper()} (
                            chunk_id TEXT PRIMARY KEY,
                            types TEXT,
                            name TEXT,
                            mission_description TEXT,
                            addresses JSONB,
                            phone_numbers TEXT[],
                            mails TEXT[],
                            urls TEXT[],
                            social_medias TEXT[],
                            mobile_applications TEXT[],
                            opening_hours TEXT,
                            contact_forms TEXT[],
                            additional_information TEXT,
                            modification_date TEXT,
                            siret TEXT,
                            siren TEXT,
                            people_in_charge JSONB,
                            organizational_chart TEXT[],
                            hierarchy JSONB,
                            directory_url TEXT,
                            chunk_text TEXT,
                            "embeddings_{model_name}" vector({embedding_size}),
                            UNIQUE(chunk_id)
                        )
                    """)

                elif table_name.lower() == "travail_emploi":
                    cursor.execute(f"""
                        CREATE TABLE TRAVAIL_EMPLOI (
                            chunk_id TEXT PRIMARY KEY,
                            sid TEXT NOT NULL,
                            chunk_index INTEGER NOT NULL,
                            title TEXT,
                            surtitre TEXT,
                            source TEXT,
                            introduction TEXT,
                            date TEXT,
                            url TEXT,
                            context TEXT[],
                            text TEXT,
                            chunk_text TEXT,
                            "embeddings_{model_name}" vector({embedding_size}),
                            UNIQUE(chunk_id)
                        )
                    """)
                elif table_name.lower() == "service_public":
                    cursor.execute(f"""
                        CREATE TABLE SERVICE_PUBLIC (
                            chunk_id TEXT PRIMARY KEY,
                            sid TEXT NOT NULL,
                            chunk_index INTEGER NOT NULL,
                            audience TEXT,
                            theme TEXT,
                            title TEXT,
                            surtitre TEXT,
                            source TEXT,
                            introduction TEXT,
                            url TEXT,
                            related_questions JSONB,
                            web_services JSONB,
                            context TEXT[],
                            text TEXT,
                            chunk_text TEXT,
                            "embeddings_{model_name}" vector({embedding_size}),
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
                            "embeddings_{model_name}" vector({embedding_size}),
                            UNIQUE(chunk_id)
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
                            "embeddings_{model_name}" vector({embedding_size}),
                            UNIQUE(chunk_id)
                        )
                    """)

                elif table_name.lower() == "dole":
                    cursor.execute(f"""
                        CREATE TABLE DOLE (
                            chunk_id TEXT PRIMARY KEY,
                            cid TEXT NOT NULL,
                            chunk_number INTEGER NOT NULL,
                            category TEXT,
                            content_type TEXT,
                            title TEXT,
                            number TEXT,
                            wording TEXT,
                            creation_date TEXT,
                            article_number INTEGER,
                            article_title TEXT,
                            article_synthesis TEXT,
                            article_text TEXT,
                            chunk_text TEXT,
                            "embeddings_{model_name}" vector({embedding_size}),
                            UNIQUE(chunk_id)
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
                            "embeddings_{model_name}" vector({embedding_size}),
                            UNIQUE(chunk_id)
                        )
                    """)

                # Create index for vector similarity search
                try:
                    cursor.execute(f"""
                        CREATE INDEX ON {table_name.upper()} USING hnsw ("embeddings_{model_name}" vector_cosine_ops)
                        WITH (m = 16, ef_construction = 128);
                    """)
                except Exception as e:
                    logger.error(
                        f"Error creating HNSW index on {table_name.upper()} table: {e}"
                    )
                    raise

                conn.commit()
                logger.info(
                    f"Table '{table_name.upper()}' created successfully in database {POSTGRES_DB}"
                )

    except Exception as e:
        logger.error(f"Error creating tables in PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()


def insert_data(data: list, table_name: str, model="BAAI/bge-m3"):
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

        model_name = format_model_name(model)
        source_cid = data[0][1]

        if table_name.upper() in [
            "LEGI",
            "CNIL",
            "CONSTIT",
            "DOLE",
        ]:  # Only for data having a cid
            # Delete the existing data for the same cid in order to avoid duplicates and outdated data
            delete_query = f"DELETE FROM {table_name.upper()} WHERE cid = %s"
            cursor.execute(delete_query, (source_cid,))

        if table_name.lower().endswith("directory"):
            insert_query = f"""
                INSERT INTO {table_name.upper()} (chunk_id, types, name, mission_description, addresses, phone_numbers, mails, urls, social_medias, mobile_applications, opening_hours, contact_forms, additional_information, modification_date, siret, siren, people_in_charge, organizational_chart, hierarchy, directory_url, chunk_text, "embeddings_{model_name}")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chunk_id) DO UPDATE SET
                types = EXCLUDED.types,
                name = EXCLUDED.name,
                mission_description = EXCLUDED.mission_description,
                addresses = EXCLUDED.addresses,
                phone_numbers = EXCLUDED.phone_numbers,
                mails = EXCLUDED.mails,
                urls = EXCLUDED.urls,
                social_medias = EXCLUDED.social_medias,
                mobile_applications = EXCLUDED.mobile_applications,
                opening_hours = EXCLUDED.opening_hours,
                contact_forms = EXCLUDED.contact_forms,
                additional_information = EXCLUDED.additional_information,
                modification_date = EXCLUDED.modification_date,
                siret = EXCLUDED.siret,
                siren = EXCLUDED.siren,
                people_in_charge = EXCLUDED.people_in_charge,
                organizational_chart = EXCLUDED.organizational_chart,
                hierarchy = EXCLUDED.hierarchy,
                directory_url = EXCLUDED.directory_url,
                chunk_text = EXCLUDED.chunk_text,
                "embeddings_{model_name}" = EXCLUDED."embeddings_{model_name}";
                """

        elif table_name.lower() == "travail_emploi":
            insert_query = f"""
                INSERT INTO TRAVAIL_EMPLOI (chunk_id, sid, chunk_index, title, surtitre, source, introduction, date, url, context, text, chunk_text, "embeddings_{model_name}")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chunk_id) DO UPDATE SET
                sid = EXCLUDED.sid,
                chunk_index = EXCLUDED.chunk_index,
                title = EXCLUDED.title,
                surtitre = EXCLUDED.surtitre,
                source = EXCLUDED.source,
                introduction = EXCLUDED.introduction,
                date = EXCLUDED.date,
                url = EXCLUDED.url,
                context = EXCLUDED.context,
                text = EXCLUDED.text,
                chunk_text = EXCLUDED.chunk_text,
                "embeddings_{model_name}" = EXCLUDED."embeddings_{model_name}";
            """
        elif table_name.lower() == "service_public":
            insert_query = f"""
                INSERT INTO SERVICE_PUBLIC (chunk_id, sid, chunk_index, audience, theme, title, surtitre, source, introduction, url, related_questions, web_services, context, text, chunk_text, "embeddings_{model_name}")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chunk_id) DO UPDATE SET
                sid = EXCLUDED.sid,
                chunk_index = EXCLUDED.chunk_index,
                audience = EXCLUDED.audience,
                theme = EXCLUDED.theme,
                title = EXCLUDED.title,
                surtitre = EXCLUDED.surtitre,
                source = EXCLUDED.source,
                introduction = EXCLUDED.introduction,
                url = EXCLUDED.url,
                related_questions = EXCLUDED.related_questions,
                web_services = EXCLUDED.web_services,
                context = EXCLUDED.context,
                text = EXCLUDED.text,
                chunk_text = EXCLUDED.chunk_text,
                "embeddings_{model_name}" = EXCLUDED."embeddings_{model_name}";
            """
        elif table_name.lower() == "cnil":
            insert_query = f"""
                INSERT INTO CNIL (chunk_id, cid, chunk_number, nature, etat, nature_delib, titre, titre_complet, numero, date, chunk_text, "embeddings_{model_name}")
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
                "embeddings_{model_name}" = EXCLUDED."embeddings_{model_name}";
            """
        elif table_name.lower() == "constit":
            insert_query = f"""
                INSERT INTO CONSTIT (chunk_id, cid, chunk_number, nature, solution, titre, numero, date_decision, chunk_text, "embeddings_{model_name}")
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
                "embeddings_{model_name}" = EXCLUDED."embeddings_{model_name}";
            """
        elif table_name.lower() == "dole":
            insert_query = f"""
                INSERT INTO DOLE (chunk_id, cid, chunk_number, category, content_type, title, number, wording, creation_date, article_number, article_title, article_synthesis, article_text, chunk_text, "embeddings_{model_name}")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chunk_id) DO UPDATE SET
                cid = EXCLUDED.cid,
                chunk_number = EXCLUDED.chunk_number,
                category = EXCLUDED.category,
                content_type = EXCLUDED.content_type,
                title = EXCLUDED.title,
                number = EXCLUDED.number,
                wording = EXCLUDED.wording,
                creation_date = EXCLUDED.creation_date,
                article_number = EXCLUDED.article_number,
                article_title = EXCLUDED.article_title,
                article_synthesis = EXCLUDED.article_synthesis,
                article_text = EXCLUDED.article_text,
                chunk_text = EXCLUDED.chunk_text,
                "embeddings_{model_name}" = EXCLUDED."embeddings_{model_name}";
            """

        elif table_name.lower() == "legi":
            insert_query = f"""
                INSERT INTO LEGI (chunk_id, cid, chunk_number, nature, etat, titre, titre_complet, sous_titres, numero, date_debut, date_fin, nota, chunk_text, "embeddings_{model_name}")
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
                "embeddings_{model_name}" = EXCLUDED."embeddings_{model_name}";
            """

        else:
            logger.error(f"Unknown table name: {table_name}")
            conn.commit()
            conn.close()
            return
        cursor.executemany(insert_query, data)
        conn.commit()
        conn.close()
        logger.debug("Data inserted into PostgreSQL database")
    except Exception as e:
        logger.error(f"Error inserting data into PostgreSQL: {e}\n{data}")


def postgres_to_qdrant(
    table_name: str,
    qdrant_client: QdrantClient,
    collection_name: str,
    model: str = "BAAI/bge-m3",
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

    probe_vector = generate_embeddings(data="Hey, I'am a probe", model=model)
    embedding_size = len(probe_vector)
    model_name = format_model_name(model)
    bm25_embedding_model = SparseTextEmbedding("Qdrant/bm25")  # For hybrid search

    if delete_existing:
        # Drop the collection if it exists
        try:
            qdrant_client.delete_collection(collection_name=collection_name)
            logger.info(f"Collection '{collection_name}' deleted successfully")
        except Exception as e:
            logger.error(f"Error deleting collection '{collection_name}': {e}")

    # Create the Qdrant collection if it doesn't exist
    qdrant_client.recreate_collection(
        collection_name=collection_name,
        vectors_config={
            model: models.VectorParams(
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
            embeddings = row[f"embeddings_{model_name}"]
            metadata = dict(row)
            del (
                metadata[f"embeddings_{model_name}"],
            )  # Remove unnecessary fields from metadata

        qdrant_client.upsert(
            collection_name=collection_name,
            points=[
                models.PointStruct(
                    id=chunk_id,
                    vector={
                        model: embeddings,
                        "bm25": bm25_embeddings[0].as_object(),
                    },
                    payload=metadata,
                )
            ],
        )

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error inserting data into Qdrant: {e}")


def remove_data(table_name: str, column: str, value: str):
    """
    Remove data from a PostgreSQL table based on a specific column and value.

    Args:
        table_name (str): Name of the PostgreSQL table to remove data from.
        column (str): Column name to filter the rows to be removed.
        value (str): Value in the specified column to match for removal.

    Raises:
        Exception: Any error encountered during database operations is logged.
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

        delete_query = f"DELETE FROM {table_name.upper()} WHERE {column} = %s"
        cursor.execute(delete_query, (value,))
        conn.commit()
        conn.close()
        logger.info(
            f"Data removed from {table_name.upper()} table where {column} = {value} (if exists)"
        )
    except Exception as e:
        logger.error(f"Error removing data from PostgreSQL: {e}")
