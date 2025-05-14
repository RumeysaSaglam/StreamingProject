#!/usr/bin/env bash

set -e

# Airflow versiyonunu kontrol et
AIRFLOW_VERSION=$(airflow version | cut -d. -f1)

echo "Detected Airflow major version: $AIRFLOW_VERSION"

# Veritabanı upgrade
echo "Running airflow db upgrade..."
airflow db migrate


# Sadece Airflow 2.x için kullanıcı oluştur
# if [ "$AIRFLOW_VERSION" = "2" ]; then
#   echo "Creating admin user..."
#   airflow users create \
#     --username admin \
#     --firstname Admin \
#     --lastname User \
#     --role Admin \
#     --email admin@example.com \
#     --password admin"""
# else
#   echo "Skipping user creation for Airflow $AIRFLOW_VERSION"
# fi

# Webserver başlat
echo "Starting webserver..."
exec airflow webserver
