import os
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./dev.db")
ASYNC_DATABASE_URL = DATABASE_URL

ECHO = os.getenv("SQL_ECHO", "False").lower() in ("1", "true", "yes")

engine = create_engine(
    DATABASE_URL,
    echo=ECHO,
    pool_pre_ping=True,
)


SessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=engine, expire_on_commit=False
)

Base = declarative_base()


@contextmanager
def get_db():
    """
    Contexte sync pour obtenir une session (compatibilité ascendante).
    Usage:
        with get_db() as db:
            ... utiliser db (Session) ...
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
