# myapp/db_utils.py
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.runtime.migration import MigrationContext
import logging
from pathlib import Path
import os
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DEFAULT_PATH = Path(__file__).parent.parent / "alembic.ini"


def get_alembic_config(db_url: str | None = None, path: Path = DEFAULT_PATH) -> Config:
    """
    Charge la configuration Alembic et remplace l'URL de base de données si besoin.
    """
    if not path.exists():
        raise FileNotFoundError(f"Alembic config file not found at {path}")
    cfg_path = str(path)
    alembic_cfg = Config(cfg_path)

    alembic_cfg.set_main_option("script_location", "migrations")

    # Définit explicitement sqlalchemy.url pour éviter les erreurs d'interpolation
    if db_url:
        alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    else:
        env_url = os.getenv("DATABASE_URL")
        if env_url:
            alembic_cfg.set_main_option("sqlalchemy.url", env_url)
        else:
            # valeur vide pour éviter que configparser tente une interpolation sur %(DATABASE_URL)s
            alembic_cfg.set_main_option("sqlalchemy.url", "")

    return alembic_cfg


def is_migration_needed(db_url: str, revision: str = "head") -> bool:
    """
    Vérifie rapidement si des migrations sont nécessaires.
    """
    try:
        cfg = get_alembic_config(db_url)

        # Créer une connexion temporaire
        engine = create_engine(db_url)

        with engine.connect() as connection:
            context = MigrationContext.configure(connection)
            current_rev = context.get_current_revision()

            # Obtenir la révision head
            script = ScriptDirectory.from_config(cfg)
            head_rev = script.get_current_head()

            logger.debug(f"Current revision: {current_rev}, Head revision: {head_rev}")

            # Si current_rev est None, la base n'est pas initialisée
            if current_rev is None:
                logger.debug("Base de données non initialisée, migration nécessaire")
                return True

            # Si les révisions sont différentes, migration nécessaire
            needs_migration = current_rev != head_rev
            logger.debug(f"Migration nécessaire: {needs_migration}")
            return needs_migration

    except Exception as e:
        logger.debug(f"Erreur lors de la vérification, migration nécessaire: {e}")
        return True  # En cas d'erreur, on assume qu'une migration est nécessaire


def upgrade_db(db_url: str | None = None, revision: str = "head"):
    """
    Applique les migrations jusqu'à la révision spécifiée (par défaut 'head').
    Vérifie d'abord si une migration est nécessaire.
    """
    if not db_url:
        db_url = os.getenv("DATABASE_URL", "sqlite:///./dev.db")

    # Vérification rapide
    if not is_migration_needed(db_url, revision):
        logger.info("✅ Base de données déjà à jour, aucune migration nécessaire")
        return

    logger.info("🔄 Migration nécessaire, démarrage...")
    cfg = get_alembic_config(db_url)
    logger.debug(f"Using Alembic config file at {cfg.config_file_name}")
    logger.debug(f"Upgrading database to revision {revision}")
    effective_url = db_url or os.getenv("DATABASE_URL") or "<not set>"
    logger.debug(f"Database URL: {effective_url}")
    try:
        command.upgrade(cfg, revision)
        logger.info(f"✅ Base de données migrée jusqu'à {revision}")
    except Exception:
        logger.warning(
            "⚠️ Échec de l'exécution d'Alembic, fallback : création des tables via SQLAlchemy"
        )
        raise


def downgrade_db(db_url: str | None = None, revision: str = "base"):
    """
    Reviens en arrière jusqu'à la révision spécifiée (par défaut 'base').
    db_url doit être passé en premier (positionnel ou nommé).
    """
    cfg = get_alembic_config(db_url)
    command.downgrade(cfg, revision)
    logger.info(f"🔻 Base de données rétrogradée à {revision}")


def reset_db(db_url: str | None = None):
    """
    Rétablit une base propre en refaisant toutes les migrations depuis zéro.
    db_url doit être passé en premier (positionnel ou nommé).
    """
    downgrade_db(db_url, "base")
    upgrade_db(db_url, "head")
    logger.info("🔄 Base de données réinitialisée avec succès.")
