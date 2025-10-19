from src.connectors.db.sql import Base
from sqlalchemy import Column, String, DateTime
from datetime import datetime


class DownloadRecord(Base):
    __tablename__ = "download_records"

    id = Column(String, primary_key=True, index=True)
    file_name = Column(String, index=True, nullable=False)
    url = Column(String, unique=True, index=True, nullable=False)
    download_date = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.now, onupdate=datetime.now, nullable=False
    )
