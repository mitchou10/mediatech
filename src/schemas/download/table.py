from src.connectors.db.sql import get_db
from src.schemas.download.schema import DownloadRecord
from src.schemas.download.models import (
    DownloadRecordCreate,
    DownloadRecordModel,
    DownloadRecordUpdate,
)


class DownloadTable:

    def __init__(self):
        self.db_session = get_db

    def create_record(self, file_name: str, url: str) -> DownloadRecordModel:
        with self.db_session() as db:
            record_create = DownloadRecordCreate(
                file_name=file_name,
                url=url,
            )
            db_record = DownloadRecord(**record_create.model_dump())
            db.add(db_record)
            db.commit()
            db.refresh(db_record)
            return DownloadRecordModel.model_validate(db_record)

    def get_url_by_id(self, record_id: str) -> DownloadRecordModel | None:
        with self.db_session() as db:
            record = (
                db.query(DownloadRecord).filter(DownloadRecord.id == record_id).first()
            )
            if record:
                return DownloadRecordModel.model_validate(record)
            return None

    def get_record_by_url(self, url: str) -> DownloadRecordModel | None:
        with self.db_session() as db:
            record = db.query(DownloadRecord).filter(DownloadRecord.url == url).first()
            if record:
                return DownloadRecordModel.model_validate(record)
            return None

    def update_record(
        self, record_id: str, updated_record: DownloadRecordUpdate
    ) -> DownloadRecordModel | None:
        with self.db_session() as db:
            record = (
                db.query(DownloadRecord).filter(DownloadRecord.id == record_id).first()
            )
            if not record:
                return None
            for field, value in updated_record.dict(exclude_unset=True).items():
                setattr(record, field, value)
            db.commit()
            db.refresh(record)
        return DownloadRecordModel.model_validate(record)

    def delete_record(self, record_id: str) -> bool:
        with self.db_session() as db:
            record = (
                db.query(DownloadRecord).filter(DownloadRecord.id == record_id).first()
            )
            if not record:
                return False
            db.delete(record)
            db.commit()
            return True

    def delete_by_url(self, url: str) -> bool:
        with self.db_session() as db:
            record = db.query(DownloadRecord).filter(DownloadRecord.url == url).first()
            if not record:
                return False
            db.delete(record)
            db.commit()
            return True

    def list_all_records(
        self, offset: int = 0, limit: int = 100
    ) -> list[DownloadRecordModel]:
        with self.db_session() as db:
            records = db.query(DownloadRecord).offset(offset).limit(limit).all()
            return [DownloadRecordModel.model_validate(record) for record in records]


download_record_table = DownloadTable()
