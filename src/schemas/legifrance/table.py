from src.connectors.db.sql import get_db
from src.schemas.legifrance.schema import LegiFrance
from src.schemas.legifrance.models import (
    LegifranceModel,
    LegiFranceCreateModel,
    LegifranceModelUpdate,
)


class LegiFranceTable:
    def __init__(self):
        self.db = get_db

    def create_record(self, legifrance_data: LegiFranceCreateModel) -> LegifranceModel:
        with self.db() as db:
            db_record = LegiFrance(**legifrance_data.model_dump())
            db.add(db_record)
            db.commit()
            db.refresh(db_record)
            return LegifranceModel.model_validate(db_record)

    def get_record_by_cid(self, cid: str) -> LegifranceModel | None:
        with self.db() as db:
            result = db.query(LegiFrance).filter(LegiFrance.cid == cid).first()
            if not result:
                return None
            return LegifranceModel.model_validate(result)

    def get_record_by_id(self, record_id: str) -> LegifranceModel | None:
        with self.db() as db:
            result = db.query(LegiFrance).filter(LegiFrance.id == record_id).first()
            if not result:
                return None
            return LegifranceModel.model_validate(result)

    def update_record(
        self, record_id: str, updated_record: LegifranceModelUpdate
    ) -> LegifranceModel | None:
        with self.db() as db:
            db_record = db.query(LegiFrance).filter(LegiFrance.id == record_id).first()
            if not db_record:
                return None
            for field, value in updated_record.model_dump(exclude_unset=True).items():
                setattr(db_record, field, value)
            db.commit()
            db.refresh(db_record)
            return LegifranceModel.model_validate(db_record)

    def delete_record(self, record_id: str) -> bool:
        with self.db() as db:
            db_record = db.query(LegiFrance).filter(LegiFrance.id == record_id).first()
            if not db_record:
                return False
            db.delete(db_record)
            db.commit()
            return True


legifrance_table = LegiFranceTable()
