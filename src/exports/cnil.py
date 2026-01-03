from src.exports.base import BaseExporter
import pandas as pd
from datasets import Dataset, load_dataset
from datetime import datetime


class CNILExporter(BaseExporter):
    def process(self, user_id: str, dataset_name: str = "cnil", *args: tuple, **kwargs: dict) -> None:

        parquet_file = f"data/{dataset_name}/data/{dataset_name}_full_documents.parquet"
        df = pd.read_parquet(parquet_file)
        repo_id = f"{user_id}/{dataset_name}-full-documents"
        dataset_cnil = load_dataset(repo_id, split="train")
        dataset_cnil_df = dataset_cnil.to_pandas()
        df: pd.DataFrame = pd.concat([df, dataset_cnil_df]
                                     ).drop_duplicates().reset_index(drop=True)
        dataset = Dataset.from_pandas(df)
        commit_message = f"Data update on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        dataset.push_to_hub(repo_id=repo_id, split="train", create_pr=False,
                            num_proc=4, revision="main", commit_message=commit_message, max_shard_size="500MB")
        print(f"Uploaded dataset to Hugging Face Hub at {repo_id}.")


if __name__ == "__main__":
    obj = CNILExporter()
    obj.process(user_id="hulk10")
