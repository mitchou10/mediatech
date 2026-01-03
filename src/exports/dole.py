from src.exports.base import BaseExporter
from datasets import Dataset, load_dataset
import pandas as pd
from datetime import datetime
import os


class DoleExporter(BaseExporter):
    def process(self, user_id: str, dataset_name: str = "dole", *args: tuple, **kwargs: dict) -> None:
        print(
            f"Processing Dole dataset for user {user_id} and dataset {dataset_name}.")
        parquet_file = f"data/{dataset_name}/data/{dataset_name}_full_documents.parquet"
        df = pd.DataFrame()
        for base in os.listdir(parquet_file):
            parquet_file_path = os.path.join(parquet_file, base)
            if os.path.isdir(parquet_file_path):
                df = pd.read_parquet(parquet_file_path)
                print(f"{base}: {len(df)} records")
                df = pd.concat([df, pd.read_parquet(
                    parquet_file_path)], ignore_index=True)

        print(f"Dataframe loaded with {len(df)} records.")
        repo_id = f"{user_id}/{dataset_name}-full-documents"
        dataset_dole = load_dataset(repo_id, split="train")
        dataset_dole_df = dataset_dole.to_pandas()
        df: pd.DataFrame = pd.concat([df, dataset_dole_df]
                                     ).drop_duplicates().reset_index(drop=True)

        dataset = Dataset.from_pandas(df)
        commit_message = f"Data update on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        dataset.push_to_hub(repo_id=repo_id, split="train", create_pr=False,
                            num_proc=4, revision="main", commit_message=commit_message, max_shard_size="500MB")
        print(f"Uploaded dataset to Hugging Face Hub at {repo_id}.")


if __name__ == "__main__":
    obj = DoleExporter()
    obj.process(user_id="hulk10")
