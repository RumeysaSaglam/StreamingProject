#!/usr/bin/env bash

set -e

# Airflow versiyonunu kontrol et
AIRFLOW_VERSION=$(airflow version | awk -F. '{print $1}')
echo "Detected Airflow major version: $AIRFLOW_VERSION"

# Veritabanı migrate
echo "Running airflow db migrate.."
airflow db migrate

# Web sunucusunu başlat
echo "Starting Airflow API server..."
exec airflow api-server
