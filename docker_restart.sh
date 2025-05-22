#!/bin/bash

# Tüm konteynerleri durdur
echo "Stopping all containers..."
docker compose down

# Kafka volumelerini kaldır
echo "Removing Kafka volumes..."

#Tüm volume'leri sil
#docker volume rm $(docker volume ls -q)

#Kafkaya ait volume'leri sil
#docker volume ls -q | grep kafka | xargs docker volume rm

docker volume rm streamingproject_kafka_data_broker_1 streamingproject_kafka_data_broker_2

echo "Building Dockerfile..."
docker compose build

# Yeniden başlat
echo "Starting containers..."
docker compose up -d

# Konteyner durumunu kontrol et
echo "Checking container status..."
sleep 5
docker compose ps