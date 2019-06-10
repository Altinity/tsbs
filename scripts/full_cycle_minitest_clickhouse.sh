#!/bin/bash

BULK_DATA_DIR="/tmp/bulk_data/"

# Ensure build data dir is in place
mkdir -p "${BULK_DATA_DIR}"

# Generate data
$GOPATH/bin/tsbs_generate_data -format clickhouse -use-case cpu-only -scale 10 -seed 123 -file "${BULK_DATA_DIR}/clickhouse_data"

# Generate some queries
$GOPATH/bin/tsbs_generate_queries -format clickhouse -use-case cpu-only -scale 10 -seed 123 -query-type lastpoint     -file "${BULK_DATA_DIR}/clickhouse_query_lastpoint"
$GOPATH/bin/tsbs_generate_queries -format clickhouse -use-case cpu-only -scale 10 -seed 123 -query-type cpu-max-all-1 -file "${BULK_DATA_DIR}/clickhouse_query_cpu-max-all-1"
$GOPATH/bin/tsbs_generate_queries -format clickhouse -use-case cpu-only -scale 10 -seed 123 -query-type high-cpu-1    -file "${BULK_DATA_DIR}/clickhouse_query_high-cpu-1"

# Load data generated earlier into ClickHouse
$GOPATH/bin/tsbs_load_clickhouse --db-name=benchmark --host=127.0.0.1 --workers=1 --file="${BULK_DATA_DIR}/clickhouse_data"

# Run some queries
$GOPATH/bin/tsbs_run_queries_clickhouse --db-name=benchmark --hosts=127.0.0.1 --workers=1 --max-queries=100 --file="${BULK_DATA_DIR}/clickhouse_query_lastpoint"
$GOPATH/bin/tsbs_run_queries_clickhouse --db-name=benchmark --hosts=127.0.0.1 --workers=1 --max-queries=100 --file="${BULK_DATA_DIR}/clickhouse_query_cpu-max-all-1"
$GOPATH/bin/tsbs_run_queries_clickhouse --db-name=benchmark --hosts=127.0.0.1 --workers=1 --max-queries=100 --file="${BULK_DATA_DIR}/clickhouse_query_high-cpu-1"
