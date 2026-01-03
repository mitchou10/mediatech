from src.exports.base import BaseExporter
import os
import pandas as pd
from datasets import Dataset
from datetime import datetime


class ServicePublicExporter(BaseExporter):
    def process(self, user_id: str, dataset_name: str = "service_public", *args: tuple, **kwargs: dict) -> None:
        paquet_file = f"data/{dataset_name}/data/{dataset_name}_full_documents.parquet"
        df = pd.read_parquet(paquet_file)
        repo_id = f"{user_id}/{dataset_name}-full-documents"
        print(df.head())
        dataset = Dataset.from_pandas(df)
        commit_message = f"Data update on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        dataset.push_to_hub(repo_id=repo_id, split="train", create_pr=False,
                            num_proc=4, revision="main", commit_message=commit_message, max_shard_size="500MB")
        print(f"Uploaded dataset to Hugging Face Hub at {repo_id}.")


class ServiceParticulierExporter(ServicePublicExporter):
    def process(self, user_id: str, dataset_name: str = "service_public_part", *args: tuple, **kwargs: dict) -> None:
        super().process(user_id=user_id, dataset_name=dataset_name, *args, **kwargs)


class ServiceProExporter(ServicePublicExporter):
    def process(self, user_id: str, dataset_name: str = "service_public_pro", *args: tuple, **kwargs: dict) -> None:
        super().process(user_id=user_id, dataset_name=dataset_name, *args, **kwargs)


if __name__ == "__main__":
    obj_part = ServiceParticulierExporter()
    obj_part.process(user_id="hulk10")
    obj_pro = ServiceProExporter()
    obj_pro.process(user_id="hulk10")
