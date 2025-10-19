from datetime import datetime
from src.connectors.db.sql import Base
from sqlalchemy import Column, String, DateTime


class ExtractionRecord(Base):
    __tablename__ = "extraction_records"

    id = Column(String, primary_key=True)
    uri = Column(String, unique=True, index=True)
    output_path = Column(String, nullable=True)
    status = Column(String)
    error_message = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.now, onupdate=datetime.now
    )
