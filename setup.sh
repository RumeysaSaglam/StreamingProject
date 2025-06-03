#!/bin/bash

echo "🚀 Stream Pipeline kurulumu başlıyor..."

# Gerekli dizinleri oluştur
mkdir -p producer
mkdir -p spark-app
mkdir -p jars

echo "📦 Gerekli JAR dosyalarını indiriliyor..."

# PostgreSQL JDBC driver
if [ ! -f "jars/postgresql-42.7.0.jar" ]; then
    curl -L -o jars/postgresql-42.7.0.jar https://jdbc.postgresql.org/download/postgresql-42.7.0.jar
fi

# Kafka JAR (Spark ile gelen sürümü kullanacağız)
echo "✅ JAR dosyaları hazır"

# Docker Compose ile servisleri başlat
echo "🐳 Docker servisleri başlatılıyor..."
docker-compose up -d zookeeper kafka postgres

echo "⏳ Kafka ve PostgreSQL'in hazır olması bekleniyor..."
sleep 30

# Kafka topic oluştur
echo "📝 Kafka topic oluşturuluyor..."
docker exec kafka kafka-topics --create --topic user-data --bootstrap-server localhost:9094 --partitions 1 --replication-factor 1

# Spark servisleri başlat
echo "⚡ Spark servisleri başlatılıyor..."
docker-compose up -d spark-master spark-worker

echo "⏳ Spark'ın hazır olması bekleniyor..."
sleep 20

# Producer'ı başlat
echo "📡 Producer başlatılıyor..."
docker-compose up -d producer

echo "✅ Tüm servisler başlatıldı!"
echo ""
echo "🌐 Erişim URL'leri:"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - PostgreSQL: localhost:5432 (user: postgres, password: password, db: userdata)"
echo ""
echo "🔥 Spark streaming job'ını başlatmak için şu komutu çalıştırın:"
echo "docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.7.0 /app/streaming_consumer.py"
echo ""
echo "📊 Verileri kontrol etmek için:"
echo "docker exec -it postgres psql -U postgres -d userdata -c 'SELECT COUNT(*) FROM users;'"