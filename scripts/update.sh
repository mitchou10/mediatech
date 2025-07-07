#!/bin/bash

# Load environment variables from .env file
export $(grep -v '^#' .env | xargs)

# Wait for PostgreSQL to be ready
until pg_isready -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

# Run the Python scripts
bibliotheque create_tables --model BAAI/bge-m3
bibliotheque download_and_process_files --all --model BAAI/bge-m3
bibliotheque export_tables
bibliotheque upload_dataset --all 
