from uuid import uuid4
from datetime import datetime
from pydantic import BaseModel, Field


class ProcessRecordModel(BaseModel):
    id: str = Field(..., description="Unique identifier for the process record")
    uri: str = Field(..., description="URI of the process record")  # noqa
    doc_id: str | None = Field(
        None, description="Document ID associated with the process record"  # noqa
    )
    status: str = Field(..., description="Current status of the process")  # noqa
    created_at: datetime = Field(
        default_factory=datetime.now,
        description="Timestamp when the record was created",
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        description="Timestamp when the record was last updated",
    )

    class Config:
        from_attributes = True


class ProcessRecordCreateModel(BaseModel):
    id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for the process record",
    )
    uri: str = Field(..., description="URI of the process record")
    doc_id: str | None = Field(
        None, description="Document ID associated with the process record"
    )
    status: str = Field(..., description="Current status of the process")

    class Config:
        from_attributes = True


class ProcessRecordUpdateModel(BaseModel):
    doc_id: str | None = Field(
        None, description="Document ID associated with the process record"
    )
    status: str | None = Field(None, description="Current status of the process")

    class Config:
        from_attributes = True
