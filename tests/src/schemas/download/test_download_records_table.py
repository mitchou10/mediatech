from uuid import uuid4
from src.connectors.db.sql import DATABASE_URL
from migrations.utils import upgrade_db
from src.schemas.download.table import download_record_table
from src.schemas.download.models import (
    DownloadRecordModel,
    DownloadRecordCreate,
    DownloadRecordUpdate,
)

upgrade_db(DATABASE_URL)


def test_create_download_record() -> DownloadRecordModel:
    url = f"https://example.com/test_file-{str(uuid4())}.tar.gz"
    record = DownloadRecordCreate(
        file_name="test_file.tar.gz",
        url=url,
    )
    created_record = download_record_table.create_record(
        file_name=record.file_name, url=record.url
    )
    assert created_record.file_name == "test_file.tar.gz"
    assert created_record.url == url
    return created_record


def test_get_record_by_url():
    created_record = test_create_download_record()

    record = download_record_table.get_record_by_url(url=created_record.url)
    assert record is not None
    assert record.url == created_record.url


def test_get_record_by_id():
    created_record = test_create_download_record()

    record = download_record_table.get_url_by_id(record_id=created_record.id)
    assert record is not None
    assert record.id == created_record.id


def test_update_download_record():
    created_record = test_create_download_record()

    updated_record_data = DownloadRecordUpdate(file_name="updated_test_file.tar.gz")
    updated_record = download_record_table.update_record(
        record_id=created_record.id,
        updated_record=updated_record_data,
    )
    assert updated_record is not None
    assert updated_record.file_name == "updated_test_file.tar.gz"


def test_delete_download_record():
    created_record = test_create_download_record()

    delete_result = download_record_table.delete_record(record_id=created_record.id)
    assert delete_result is True

    record = download_record_table.get_url_by_id(record_id=created_record.id)
    assert record is None


def test_delete_record_by_url():
    created_record = test_create_download_record()

    delete_result = download_record_table.delete_by_url(url=created_record.url)
    assert delete_result is True

    record = download_record_table.get_record_by_url(url=created_record.url)
    assert record is None
