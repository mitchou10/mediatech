from src.connectors.db.sql import DATABASE_URL
from uuid import uuid4
from migrations.utils import upgrade_db
from src.schemas.extractions.table import extraction_table
from src.schemas.extractions.models import (
    ExtractionModel,
    ExtractionUpdateModel,
)


upgrade_db(DATABASE_URL)


def test_create_extraction_record() -> ExtractionModel:
    uri = f"http://example.com/data-{str(uuid4())}.csv"
    status = "pending"
    record = extraction_table.create_record(uri=uri, status=status)
    assert record.uri == uri
    assert record.status == status
    return record


def test_get_extraction_record_by_uri():
    created_extraction = test_create_extraction_record()
    uri = created_extraction.uri
    record = extraction_table.get_record_by_uri(uri=uri)
    assert record is not None
    assert record.uri == uri


def test_update_extraction_record():
    created_extraction = test_create_extraction_record()
    record_id = created_extraction.id

    updated_record_data = ExtractionUpdateModel(
        status="completed", output_path="/data/output.csv"
    )
    updated_record = extraction_table.update_record(
        record_id=record_id,
        updated_record=updated_record_data,
    )
    assert updated_record is not None
    assert updated_record.status == "completed"
    assert updated_record.output_path == "/data/output.csv"


def test_delete_extraction_record():
    created_extraction = test_create_extraction_record()
    record_id = created_extraction.id

    delete_result = extraction_table.delete_record(record_id=record_id)
    assert delete_result is True

    record = extraction_table.get_record_by_uri(uri=created_extraction.uri)
    assert record is None
