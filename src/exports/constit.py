from src.exports.base import BaseExporter
from datasets import Dataset, load_dataset
import pandas as pd
from datetime import datetime


class ConstitExporter(BaseExporter):
    def process(self, user_id: str, dataset_name: str = "constit", *args: tuple, **kwargs: dict) -> None:
        print(
            f"Processing Constit dataset for user {user_id} and dataset {dataset_name}.")
        parquet_file = f"data/{dataset_name}/data/{dataset_name}_full_documents.parquet"
        df = pd.read_parquet(parquet_file)
        print(f"Dataframe loaded with {len(df)} records.")

        repo_id = f"{user_id}/{dataset_name}-full-documents"
        try:
            dataset_constit = load_dataset(repo_id, split="train")
            dataset_constit_df = dataset_constit.to_pandas()
            df: pd.DataFrame = pd.concat([df, dataset_constit_df]
                                         ).drop_duplicates().reset_index(drop=True)
        except Exception as e:
            print(e)
        dataset = Dataset.from_pandas(df)
        commit_message = f"Data update on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        dataset.push_to_hub(repo_id=repo_id, split="train", create_pr=False,
                            num_proc=kwargs.get("num_proc", 4), revision="main",
                            commit_message=commit_message,
                            max_shard_size=kwargs.get("max_shard_size", "64MB"))
        print(f"Uploaded dataset to Hugging Face Hub at {repo_id}.")


if __name__ == "__main__":
    obj = ConstitExporter()
    obj.process(user_id="hulk10")
