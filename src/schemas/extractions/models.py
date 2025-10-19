from datetime import datetime
from uuid import uuid4
from pydantic import BaseModel, Field


class ExtractionModel(BaseModel):
    id: str = Field(..., description="The unique identifier of the extraction model")
    uri: str = Field(..., description="The URI of the extraction data")
    output_path: str | None = Field(
        None, description="The output path where extracted data is stored"  # noqa
    )
    status: str = Field(..., description="The status of the extraction process")  # noqa
    error_message: str | None = Field(
        None, description="Error message if the extraction failed"  # noqa
    )
    created_at: datetime = Field(
        default_factory=datetime.now, description="The creation timestamp of the record"
    )
    updated_at: datetime = Field(
        default_factory=datetime.now,
        description="The last update timestamp of the record",
    )

    class Config:
        from_attributes = True


class ExtractionCreateModel(BaseModel):
    id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="The unique identifier of the extraction model",
    )
    uri: str = Field(..., description="The URI of the extraction data")
    output_path: str | None = Field(
        None, description="The output path where extracted data is stored"
    )
    status: str = Field(..., description="The status of the extraction process")
    error_message: str | None = Field(
        None, description="Error message if the extraction failed"
    )


class ExtractionUpdateModel(BaseModel):
    output_path: str | None = Field(
        None, description="The output path where extracted data is stored"
    )
    status: str | None = Field(None, description="The status of the extraction process")
    error_message: str | None = Field(
        None, description="Error message if the extraction failed"
    )
