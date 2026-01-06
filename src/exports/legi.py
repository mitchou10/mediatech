from src.exports.base import BaseExporter
import pandas as pd
from datasets import Dataset, load_dataset, concatenate_datasets
from datetime import datetime
import time
import os
import pyarrow.parquet as pq
import shutil


class LegiExporter(BaseExporter):
    def process(self, user_id: str, dataset_name: str = "legi", *args: tuple, **kwargs: dict) -> None:
        repo_id = f"{user_id}/{dataset_name}-full-documents"
        parquet_file = f"data/{dataset_name}/data/{dataset_name}_full_documents.parquet"

        # Optimisation 1: Lecture par chunks
        chunk_size = kwargs.get("chunk_size", 10000)

        try:
            # Charger le dataset existant en streaming
            existing_dataset = load_dataset(
                repo_id, split="train", streaming=True)
        except Exception as e:
            existing_dataset = None
            print(f"No existing dataset found: {e}")

        def gen_from_parquet():
            """Générateur qui lit le parquet par chunks"""
            if os.path.isdir(parquet_file):
                # Si c'est un dossier partitionné, lire avec PyArrow
                dataset = pq.ParquetDataset(parquet_file)
                for batch in dataset.to_batches(batch_size=chunk_size):
                    df_batch = batch.to_pandas()
                    for _, row in df_batch.iterrows():
                        yield row.to_dict()
                    del df_batch  # Libérer la mémoire
            else:
                # Si c'est un fichier unique, lire par chunks
                parquet_file_obj = pd.read_parquet(
                    parquet_file, engine='pyarrow')
                for start in range(0, len(parquet_file_obj), chunk_size):
                    end = min(start + chunk_size, len(parquet_file_obj))
                    chunk_df = parquet_file_obj.iloc[start:end]
                    for _, row in chunk_df.iterrows():
                        yield row.to_dict()
                    del chunk_df  # Libérer la mémoire
                del parquet_file_obj  # Libérer la mémoire

        # Créer le nouveau dataset depuis le générateur
        new_dataset = Dataset.from_generator(gen_from_parquet)

        # Optimisation 2: Concaténation efficace si dataset existant
        if existing_dataset:
            print("Merging with existing dataset...")
            existing_dataset_materialized = existing_dataset.to_iterable_dataset()
            final_dataset = concatenate_datasets(
                [existing_dataset_materialized, new_dataset])
        else:
            final_dataset = new_dataset

        commit_message = f"Data update on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        # Optimisation 3: Upload avec paramètres optimisés
        final_dataset.push_to_hub(
            repo_id=repo_id,
            split="train",
            create_pr=False,
            # Réduire pour éviter la surcharge
            num_proc=kwargs.get("num_proc", 2),
            revision="main",
            commit_message=commit_message,
            # Augmenter pour moins de fichiers
            max_shard_size=kwargs.get("max_shard_size", "128MB"),
            embed_external_files=False  # Éviter l'embarquement de fichiers externes
        )
        print(f"Uploaded dataset to Hugging Face Hub at {repo_id}.")


# Alternative plus efficace avec streaming complet
class LegiExporterStreaming(BaseExporter):
    def process(self, user_id: str, dataset_name: str = "legi", *args: tuple, **kwargs: dict) -> None:
        repo_id = f"{user_id}/{dataset_name}-full-documents"
        parquet_path = f"data/{dataset_name}/data/{dataset_name}_full_documents.parquet"

        # Upload direct du parquet sans conversion
        from huggingface_hub import HfApi
        api = HfApi()
        print(79 * "=")
        print(f"Preparing to upload parquet files to {repo_id}...")
        if os.path.isdir(parquet_path):
            print(
                f"Uploading partitioned parquet files from {parquet_path}...")
            # Upload de tous les fichiers parquet du dossier
            for base_name in os.listdir(parquet_path):
                file_path = os.path.join(parquet_path, base_name)
                if os.path.isdir(file_path):
                    api.upload_folder(
                        folder_path=file_path,
                        path_in_repo=f"data/{base_name}",
                        repo_id=repo_id,
                        repo_type="dataset",
                        commit_message=f"Data update on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    )
                    time.sleep(1)  # Pause pour éviter les limites d'API

                    shutil.rmtree(file_path)  # Nettoyer après upload
                    print(f"Uploaded and removed {file_path}")

        print(f"Uploaded parquet files directly to {repo_id}")
