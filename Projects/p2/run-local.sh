#!/bin/bash


set -e

# Parse command line arguments
WORK_DIR=./tmp
INPUT_FILE=EVENTS_100.csv
while [[ $# -gt 0 ]]; do
  case $1 in
    --work-dir)
      WORK_DIR=$2
      shift
      ;;
    --input-file)
      INPUT_FILE=$2
      shift
      ;;
    *)
      echo "error: unrecognized argument $1"
      exit 1
      ;;
  esac
  shift
done

# Wrapper function to print the command being run
function run {
  echo "$ $@"
  "$@"
}

# Extract the data files
echo '>> Extracting data'
run python src/data_extractor.py \
  -w $WORK_DIR \
  -i $INPUT_FILE
echo ''

# Preprocess the datasets
echo '>> Preprocessing'
run python src/preprocess.py \
  -w $WORK_DIR \
  -i $INPUT_FILE
echo ''

# Train and evaluate the model
echo '>> Training'
run python src/trainer.py \
  -w $WORK_DIR
echo ''

# Get the model path
EXPORT_DIR=$WORK_DIR/model/export/final
if [[ $EXPORT_DIR == gs://* ]]; then
  MODEL_DIR=$(gsutil ls -d "$EXPORT_DIR/*" | sort -r | head -n 1)
else
  MODEL_DIR=$(ls -d -1 $EXPORT_DIR/* | sort -r | head -n 1)
fi
echo "Model: $MODEL_DIR"
echo ''

# Make batch predictions on SDF files
echo '>> Batch prediction'
run python src/predict.py \
  -w $WORK_DIR \
  -m $MODEL_DIR \
  batch \
  -i $INPUT_FILE \
  -o $WORK_DIR/predictions

# Display some predictions
if [[ $WORK_DIR == gs://* ]]; then
  gsutil cat $WORK_DIR/predictions/* | head -n 10
else
  head -n 10 $WORK_DIR/predictions/*
fi
