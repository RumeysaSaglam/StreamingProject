#!/bin/bash

# Gerekli dizinleri oluştur
mkdir -p producer
mkdir -p src
mkdir -p jars

echo "JAR files installing"

# PostgreSQL JDBC driver
if [ ! -f "jars/postgresql-42.7.0.jar" ]; then
    curl -L -o jars/postgresql-42.7.0.jar https://jdbc.postgresql.org/download/postgresql-42.7.0.jar
fi


# Docker Compose ile servisleri başlat
echo "Docker service"
docker-compose up -d zookeeper kafka postgres

sleep 30

# Kafka topic oluştur
echo "Kafka topic create"
docker exec kafka kafka-topics --create --topic user-data --bootstrap-server localhost:9094 --partitions 1 --replication-factor 1

# Spark servisleri başlat
echo "Spark service"
docker-compose up -d spark-master spark-worker

sleep 20

# Producer'ı başlat
echo "Producer starting"
docker-compose up -d producer

echo "All services started!"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - PostgreSQL: localhost:5432 "
echo ""
echo "Run the following command to start the spark streaming job:"
echo "docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.7.0 /app/streaming_consumer.py"