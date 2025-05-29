#!/bin/bash

spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2g \
  --driver-memory 2g \
  opt/airflow/scripts/spark_processing.py
