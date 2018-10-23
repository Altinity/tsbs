#!/bin/bash

# Ensure runner is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_run_queries_timescaledb)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_run_queries_timescaledb not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Queries folder
BULK_DATA_DIR=${BULK_DATA_DIR:-"/tmp/bulk_queries"}

LIMIT=${LIMIT:-"0"}
NUM_WORKERS=${NUM_WORKERS:-$(grep -c ^processor /proc/cpuinfo)}  # match # of cores - worker per core

for FULL_DATA_FILE_NAME in ${BULK_DATA_DIR}/queries_timescaledb*; do
    # $FULL_DATA_FILE_NAME:  /full/path/to/file_with.ext
    # $DATA_FILE_NAME:       file_with.ext
    # $DIR:                  /full/path/to
    # $EXTENSION:            ext
    # NO_EXT_DATA_FILE_NAME: file_with

    DATA_FILE_NAME=$(basename -- "${FULL_DATA_FILE_NAME}")
    DIR=$(dirname "${FULL_DATA_FILE_NAME}")
    EXTENSION="${DATA_FILE_NAME##*.}"
    NO_EXT_DATA_FILE_NAME="${DATA_FILE_NAME%.*}"

    # Several options on how to name results file
    #OUT_FULL_FILE_NAME="${DIR}/result_${DATA_FILE_NAME}"
    OUT_FULL_FILE_NAME="${DIR}/result_${NO_EXT_DATA_FILE_NAME}.out"
    #OUT_FULL_FILE_NAME="${DIR}/${NO_EXT_DATA_FILE_NAME}.out"

    if [ "${EXTENSION}" == "gz" ]; then
        GUNZIP="gunzip"
    else
        GUNZIP="cat"
    fi

    echo "Running ${DATA_FILE_NAME}"
    cat $FULL_DATA_FILE_NAME \
        | $GUNZIP \
        | $EXE_FILE_NAME \
            -limit $LIMIT \
            -workers $NUM_WORKERS \
        | tee $OUT_FULL_FILE_NAME
done
