#!/usr/bin/env bash

set -e

AIRFLOW_VERSION=$(airflow version | awk -F. '{print $1}')
echo "Detected Airflow major version: $AIRFLOW_VERSION"

#pip install --no-cache-dir -r /opt/airflow/requirements.txt

export PYTHONPATH=$PYTHONPATH:/opt/airflow/scripts

echo "Running airflow db migrate..."
airflow db migrate

# Admin kullanıcıyı oluştur

airflow users create \
  --username "${_AIRFLOW_USERNAME}" \
  --firstname "${_AIRFLOW_FIRSTNAME}" \
  --lastname "${_AIRFLOW_LASTNAME}" \
  --role Admin \
  --email "${_AIRFLOW_EMAIL}" \
  --password "${_AIRFLOW_PASSWORD}"


echo "Starting Airflow webserver..."
exec airflow webserver

echo "connections add spark"

airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark-master' \
    --conn-port '7077'
