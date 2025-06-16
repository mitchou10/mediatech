#!/bin/bash

# Load environment variables from .env file
export $(grep -v '^#' .env | xargs)

# Wait for PostgreSQL to be ready
until pg_isready -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

# Run the Python script
python main.py download_files
python main.py create_tables --model BAAI/bge-m3
python main.py process_files --all --model BAAI/bge-m3
python main.py export_tables
python main.py upload_dataset --input data/parquet/state_administrations_directory.parquet --dataset-name state-administrations-directory
python main.py upload_dataset --input data/parquet/service_public.parquet --dataset-name service-public
python main.py upload_dataset --input data/parquet/travail_emploi.parquet --dataset-name travail-emploi 