from src.exports.base import BaseExporter
import os
import pandas as pd

from datasets import Dataset


from datetime import datetime


class TravailEmploiExporter(BaseExporter):
    def process(self, user_id: str, dataset_name: str = "travail_emploi", *args: tuple, **kwargs: dict) -> None:
        paquet_file = f"data/{dataset_name}/data/{dataset_name}_full_documents.parquet"
        df = pd.read_parquet(paquet_file)
        repo_id = f"{user_id}/{dataset_name}-full-documents"
        dataset = Dataset.from_pandas(df)
        commit_message = f"Data update on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        dataset.push_to_hub(repo_id=repo_id, split="train", create_pr=False,
                            num_proc=kwargs.get("num_proc", 1), revision="main", commit_message=commit_message,
                            max_shard_size=kwargs.get("max_shard_size", "64MB"))
        print(f"Uploaded dataset to Hugging Face Hub at {repo_id}.")
