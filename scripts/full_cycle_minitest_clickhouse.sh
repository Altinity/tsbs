#!/bin/bash

BULK_DATA_DIR="/tmp/bulk_data_clickhouse_minitest"

# Ensure data dir is in place
mkdir -p "${BULK_DATA_DIR}"

echo "Step 1. Generate data"
BULK_DATA_DIR="${BULK_DATA_DIR}" \
EXE_FILE_NAME="${GOPATH}/bin/tsbs_generate_data" \
FORMATS="clickhouse" \
USE_CASE=cpu-only \
SCALE=10 \
SEED=123 \
    ./generate_data.sh

echo "Step 2. Generate queries"
BULK_DATA_DIR="${BULK_DATA_DIR}" \
EXE_FILE_NAME="${GOPATH}/bin/tsbs_generate_queries" \
FORMATS="clickhouse" \
USE_CASE=cpu-only \
SCALE=10 \
SEED=123 \
QUERY_TYPES="lastpoint cpu-max-all-1 high-cpu-1" \
    ./generate_queries.sh

echo "Step 3. Load data into ClickHouse"
BULK_DATA_DIR="${BULK_DATA_DIR}" \
EXE_FILE_NAME="${GOPATH}/bin/tsbs_load_clickhouse" \
DATABASE_NAME="benchmark" \
DATABASE_HOST="127.0.0.1" \
DATABASE_PORT="9000" \
NUM_WORKERS=1 \
    ./load_clickhouse.sh

echo "Step 4. Run queries"
BULK_DATA_DIR="${BULK_DATA_DIR}" \
EXE_FILE_NAME="${GOPATH}/bin/tsbs_run_queries_clickhouse" \
DATABASE_NAME="benchmark" \
DATABASE_HOSTS="127.0.0.1" \
DATABASE_PORT="9000" \
NUM_WORKERS=1 \
MAX_QUERIES=100 \
    ./run_queries_clickhouse.sh
