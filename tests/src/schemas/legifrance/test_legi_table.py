from uuid import uuid4
from src.connectors.db.sql import DATABASE_URL
from migrations.utils import upgrade_db
from src.schemas.legifrance.table import legifrance_table
from src.schemas.legifrance.models import (
    LegifranceModel,
    LegifranceModelUpdate,
    LegiFranceCreateModel,
)

upgrade_db(DATABASE_URL)


def test_create_legifrance_record() -> LegifranceModel:
    cid = f"CID-{str(uuid4())}"
    record = LegiFranceCreateModel(
        cid=cid,
        title="Test Legal Document",
        status="new",
    )
    created_record = legifrance_table.create_record(
        legifrance_data=record,
    )
    assert created_record.cid == cid
    assert created_record.title == "Test Legal Document"
    assert created_record.status == "new"
    return created_record


def test_get_record_by_cid():
    created_record = test_create_legifrance_record()

    record = legifrance_table.get_record_by_cid(cid=created_record.cid)
    assert record is not None
    assert record.cid == created_record.cid


def test_get_record_by_id():
    created_record = test_create_legifrance_record()

    record = legifrance_table.get_record_by_id(record_id=created_record.id)
    assert record is not None
    assert record.id == created_record.id


def test_update_legifrance_record():
    created_record = test_create_legifrance_record()

    updated_record_data = LegifranceModelUpdate(title="Updated Legal Document")
    updated_record = legifrance_table.update_record(
        record_id=created_record.id,
        updated_record=updated_record_data,
    )
    assert updated_record is not None
    assert updated_record.title == "Updated Legal Document"


def test_delete_legifrance_record():
    created_record = test_create_legifrance_record()

    delete_result = legifrance_table.delete_record(record_id=created_record.id)
    assert delete_result is True

    record = legifrance_table.get_record_by_id(record_id=created_record.id)
    assert record is None
