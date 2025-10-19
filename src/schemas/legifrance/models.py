from uuid import uuid4
from datetime import datetime
from pydantic import BaseModel, Field


class LegifranceModel(BaseModel):
    id: str = Field(..., description="Database ID")  # noqa
    cid: str = Field(
        ..., description="Unique identifier for the legal document"  # noqa
    )  # noqa
    nature: str | None = Field(None, description="Nature of the legal document")  # noqa
    category: str | None = Field(
        None, description="Category of the legal document"  # noqa
    )  # noqa
    ministere: str | None = Field(None, description="Ministry responsible")  # noqa
    etat_juridique: str | None = Field(None, description="Legal status")  # noqa
    title: str | None = Field(None, description="Title of the legal document")  # noqa
    full_title: str | None = Field(
        None, description="Full title of the legal document"  # noqa
    )  # noqa
    subtitles: str | None = Field(
        None, description="Subtitles of the legal document"  # noqa
    )  # noqa
    number: str | None = Field(None, description="Document number")  # noqa
    start_date: datetime | None = Field(
        None, description="Start date of validity"
    )  # noqa
    end_date: datetime | None = Field(None, description="End date of validity")  # noqa
    nota: str | None = Field(
        None, description="Notes associated with the document"  # noqa
    )  # noqa
    links: list[str] | None = Field(None, description="List of related links")  # noqa
    text_content: str | None = Field(
        None, description="Text content of the document"  # noqa
    )  # noqa
    status: str | None = Field(None, description="Processing status")  # noqa
    created_at: datetime = Field(
        default_factory=datetime.now, description="Record creation timestamp"  # noqa
    )  # noqa
    updated_at: datetime = Field(
        default_factory=datetime.now, description="Record last update timestamp"  # noqa
    )  # noqa

    class Config:
        from_attributes = True


class LegifranceModelUpdate(BaseModel):
    cid: str | None = Field(
        None, description="Unique identifier for the legal document"
    )
    nature: str | None = Field(None, description="Nature of the legal document")
    category: str | None = Field(None, description="Category of the legal document")
    ministere: str | None = Field(None, description="Ministry responsible")
    etat_juridique: str | None = Field(None, description="Legal status")
    title: str | None = Field(None, description="Title of the legal document")
    full_title: str | None = Field(None, description="Full title of the legal document")
    subtitles: str | None = Field(None, description="Subtitles of the legal document")
    number: str | None = Field(None, description="Document number")
    start_date: datetime | None = Field(None, description="Start date of validity")
    end_date: datetime | None = Field(None, description="End date of validity")
    nota: str | None = Field(None, description="Notes associated with the document")
    links: list[str] | None = Field(None, description="List of related links")
    text_content: str | None = Field(None, description="Text content of the document")
    status: str | None = Field(None, description="Processing status")
    created_at: datetime | None = Field(
        default_factory=datetime.now, description="Record creation timestamp"
    )
    updated_at: datetime | None = Field(
        default_factory=datetime.now, description="Record last update timestamp"
    )

    class Config:
        from_attributes = True


class LegiFranceCreateModel(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()), description="Database ID")
    cid: str = Field(..., description="Unique identifier for the legal document")
    nature: str | None = Field(None, description="Nature of the legal document")
    category: str | None = Field(None, description="Category of the legal document")
    ministere: str | None = Field(None, description="Ministry responsible")
    etat_juridique: str | None = Field(None, description="Legal status")
    title: str | None = Field(None, description="Title of the legal document")
    full_title: str | None = Field(None, description="Full title of the legal document")
    subtitles: str | None = Field(None, description="Subtitles of the legal document")
    number: str | None = Field(None, description="Document number")
    start_date: datetime | None = Field(None, description="Start date of validity")
    end_date: datetime | None = Field(None, description="End date of validity")
    nota: str | None = Field(None, description="Notes associated with the document")
    links: list[str] | None = Field(None, description="List of related links")
    text_content: str | None = Field(None, description="Text content of the document")
    status: str | None = Field(None, description="Processing status")
    created_at: datetime = Field(
        default_factory=datetime.now, description="Record creation timestamp"
    )
    updated_at: datetime = Field(
        default_factory=datetime.now, description="Record last update timestamp"
    )

    class Config:
        from_attributes = True
