#!/bin/bash

# Load environment variables from .env file
export $(grep -v '^#' .env | xargs)

# Wait for PostgreSQL to be ready
until pg_isready -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

# Run the Python scripts
python main.py download_files
python main.py create_tables --model BAAI/bge-m3
python main.py process_files --all --folder data/unprocessed --model BAAI/bge-m3
python main.py export_tables
python main.py upload_dataset --all 
