from datetime import datetime
from uuid import uuid4
from pydantic import BaseModel, Field


class DownloadRecordModel(BaseModel):
    id: str = Field(..., description="Unique identifier for the download record")
    file_name: str = Field(..., description="Name of the downloaded file")
    url: str = Field(..., description="URL from which the file was downloaded")
    download_date: datetime = Field(
        default_factory=datetime.now,
        description="Date and time when the file was downloaded",
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        description="Date and time when the record was last updated",
    )

    class Config:
        from_attributes = True


class DownloadRecordCreate(BaseModel):
    id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for the download record",
    )
    file_name: str = Field(..., description="Name of the downloaded file")
    url: str = Field(..., description="URL from which the file was downloaded")
    download_date: datetime = Field(
        default_factory=datetime.now,
        description="Date and time when the file was downloaded",
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        description="Date and time when the record was last updated",
    )

    class Config:
        from_attributes = True


class DownloadRecordUpdate(BaseModel):
    updated_at: datetime = Field(
        default_factory=datetime.now,
        description="Date and time when the record was last updated",
    )
    file_name: str | None = Field(
        default=None,
        description="Name of the downloaded file",
    )
    download_date: datetime | None = Field(
        default_factory=datetime.now,
        description="Date and time when the file was downloaded",
    )
    updated_at: datetime | None = Field(
        default_factory=datetime.now,
        description="Date and time when the record was last updated",
    )

    class Config:
        from_attributes = True
