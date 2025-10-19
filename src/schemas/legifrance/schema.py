from datetime import datetime
from src.connectors.db.sql import Base
from sqlalchemy import Column, String, DateTime, Text, JSON


class LegiFrance(Base):
    __tablename__ = "legifrance"

    id = Column(String, primary_key=True, index=True)
    cid = Column(String, index=True, nullable=False)
    nature = Column(String, nullable=True)
    category = Column(String, nullable=True)
    ministere = Column(String, nullable=True)
    etat_juridique = Column(String, nullable=True)
    title = Column(String, nullable=True)
    full_title = Column(Text, nullable=True)
    subtitles = Column(Text, nullable=True)
    number = Column(String, nullable=True)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    nota = Column(Text, nullable=True)
    links = Column(JSON, nullable=True)  # JSON serialized list
    text_content = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.now, onupdate=datetime.now
    )
    status = Column(String, nullable=True)
