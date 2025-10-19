from src.connectors.db.sql import get_db
from src.schemas.extractions.schema import ExtractionRecord
from src.schemas.extractions.models import (
    ExtractionModel,
    ExtractionCreateModel,
    ExtractionUpdateModel,
)


class ExtractionTable:
    def __init__(self):
        self.db_session = get_db

    def create_record(self, uri: str, status: str) -> ExtractionModel:
        with self.db_session() as db:
            created_record = ExtractionCreateModel(
                uri=uri,
                status=status,
            )
            db_record = ExtractionRecord(**created_record.model_dump())
            db.add(db_record)
            db.commit()
            db.refresh(db_record)
            return ExtractionModel.model_validate(db_record)

    def get_record_by_uri(self, uri: str) -> ExtractionModel | None:
        with self.db_session() as db:
            record = (
                db.query(ExtractionRecord).filter(ExtractionRecord.uri == uri).first()
            )
            return ExtractionModel.model_validate(record) if record else None

    def update_record(
        self, record_id: str, updated_record: ExtractionUpdateModel
    ) -> ExtractionModel | None:
        with self.db_session() as db:
            record = (
                db.query(ExtractionRecord)
                .filter(ExtractionRecord.id == record_id)
                .first()
            )
            if not record:
                return None
            for field, value in updated_record.dict(exclude_unset=True).items():
                setattr(record, field, value)
            db.commit()
            db.refresh(record)
        return ExtractionModel.model_validate(record) if record else None

    def delete_record(self, record_id: str) -> bool:
        with self.db_session() as db:
            record = (
                db.query(ExtractionRecord)
                .filter(ExtractionRecord.id == record_id)
                .first()
            )
            if not record:
                return False
            db.delete(record)
            db.commit()
            return True


extraction_table = ExtractionTable()
