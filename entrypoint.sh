#!/usr/bin/env bash

set -e

AIRFLOW_VERSION=$(airflow version | awk -F. '{print $1}')
echo "Detected Airflow major version: $AIRFLOW_VERSION"

pip install --no-cache-dir -r /opt/airflow/requirements.txt

export PYTHONPATH=$PYTHONPATH:/opt/airflow/scripts

echo "Running airflow db upgrade..."
airflow db upgrade

# Admin kullanıcıyı oluştur (eğer yoksa)
echo "Checking for existing admin user..."
airflow users list | grep -w "${_AIRFLOW_USERNAME}" > /dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "Creating admin user..."
  airflow users create \
    --username "${_AIRFLOW_USERNAME}" \
    --firstname "${_AIRFLOW_FIRSTNAME}" \
    --lastname "${_AIRFLOW_LASTNAME}" \
    --role Admin \
    --email "${_AIRFLOW_EMAIL}" \
    --password "${_AIRFLOW_PASSWORD}"
else
  echo "Admin user '${_AIRFLOW_USERNAME}' already exists. Skipping creation."
fi

echo "Starting Airflow webserver..."
exec airflow webserver
