from datetime import datetime
from src.connectors.db.sql import Base


from sqlalchemy import Column, String, DateTime


class ProcessRecord(Base):
    __tablename__ = "process_records"

    id = Column(String, primary_key=True, index=True)
    uri = Column(String, unique=True, index=True)
    doc_id = Column(String, index=True, nullable=True)

    status = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=True, default=datetime.now)
    updated_at = Column(
        DateTime, nullable=True, default=datetime.now, onupdate=datetime.now
    )
