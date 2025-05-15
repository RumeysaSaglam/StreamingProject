#!/usr/bin/env bash

set -e

AIRFLOW_VERSION=$(airflow version | awk -F. '{print $1}')
echo "Detected Airflow major version: $AIRFLOW_VERSION"

echo "Running airflow db upgrade.."
airflow db upgrade

echo "Starting Airflow webserver..."
exec airflow webserver
