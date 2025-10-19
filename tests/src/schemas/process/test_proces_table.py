from uuid import uuid4
from src.connectors.db.sql import DATABASE_URL
from migrations.utils import upgrade_db
from src.schemas.process.table import process_table
from src.schemas.process.models import (
    ProcessRecordModel,
    ProcessRecordCreateModel,
    ProcessRecordUpdateModel,
)

upgrade_db(DATABASE_URL)


def test_create_process_record() -> ProcessRecordModel:
    uri = f"https://example.com/document-{str(uuid4())}.pdf"
    record = ProcessRecordCreateModel(
        uri=uri,
        status="pending",
    )
    created_record = process_table.create_record(
        uri=record.uri,
        status=record.status,
    )
    assert created_record.uri == uri
    assert created_record.status == "pending"
    return created_record


def test_get_record_by_uri():
    created_record = test_create_process_record()

    record = process_table.get_record_by_uri(uri=created_record.uri)
    assert record is not None
    assert record.uri == created_record.uri


def test_get_record_by_id():
    created_record = test_create_process_record()

    record = process_table.get_record_by_id(record_id=created_record.id)
    assert record is not None
    assert record.id == created_record.id


def test_update_process_record():
    created_record = test_create_process_record()

    updated_record_data = ProcessRecordUpdateModel(status="completed")
    updated_record = process_table.update_record(
        record_id=created_record.id,
        updated_record=updated_record_data,
    )
    assert updated_record is not None
    assert updated_record.status == "completed"


def test_delete_process_record():
    created_record = test_create_process_record()

    delete_result = process_table.delete_record(record_id=created_record.id)
    assert delete_result is True

    record = process_table.get_record_by_id(record_id=created_record.id)
    assert record is None


def test_delete_nonexistent_record():
    delete_result = process_table.delete_record(record_id=str(uuid4()))
    assert delete_result is False
