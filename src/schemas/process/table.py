from src.connectors.db.sql import get_db
from src.schemas.process.schema import ProcessRecord
from src.schemas.process.models import (
    ProcessRecordUpdateModel,
    ProcessRecordCreateModel,
    ProcessRecordModel,
)


class ProcessTable:
    def __init__(self):
        self.db = get_db

    def create_record(
        self, uri: str, status: str, doc_id: str = None
    ) -> ProcessRecordModel:
        with self.db() as db:
            new_record = ProcessRecordCreateModel(uri=uri, status=status, doc_id=doc_id)
            new_record = ProcessRecord(**new_record.model_dump())
            db.add(new_record)
            db.commit()
            db.refresh(new_record)
        return ProcessRecordModel.model_validate(new_record)

    def get_record_by_uri(self, uri: str) -> ProcessRecordModel | None:
        with self.db() as db:
            record = db.query(ProcessRecord).filter(ProcessRecord.uri == uri).first()
        if record:
            return ProcessRecordModel.model_validate(record)
        return None

    def get_record_by_id(self, record_id: str) -> ProcessRecordModel | None:
        with self.db() as db:
            record = (
                db.query(ProcessRecord).filter(ProcessRecord.id == record_id).first()
            )
        if record:
            return ProcessRecordModel.model_validate(record)
        return None

    def update_record(
        self, record_id: str, updated_record: ProcessRecordUpdateModel
    ) -> ProcessRecordModel | None:
        with self.db() as db:
            record = (
                db.query(ProcessRecord).filter(ProcessRecord.id == record_id).first()
            )
            if not record:
                return None
            for field, value in updated_record.model_dump(exclude_unset=True).items():
                setattr(record, field, value)

            db.commit()
            db.refresh(record)
            return ProcessRecordModel.model_validate(record)

    def delete_record(self, record_id: str) -> bool:
        with self.db() as db:
            record = (
                db.query(ProcessRecord).filter(ProcessRecord.id == record_id).first()
            )
            if not record:
                return False
            db.delete(record)
        db.commit()
        return True


process_table = ProcessTable()
