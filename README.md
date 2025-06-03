# StreamingProject# Stream Pipeline - Docker + PySpark + Kafka + PostgreSQL

Bu proje, randomuser.me API'sinden kullanıcı verilerini alıp, Kafka üzerinden stream ederek PostgreSQL'e yazan bir pipeline'dır.

## 🏗️ Mimari

```
RandomUser API → Kafka Producer → Kafka → Spark Streaming → PostgreSQL
```

## 📁 Proje Yapısı

```
.
├── docker-compose.yml          # Tüm servislerin yapılandırması
├── init.sql                   # PostgreSQL tablo oluşturma scripti
├── setup.sh                  # Otomatik kurulum scripti
├── Dockerfile.producer        # Producer için Dockerfile
├── producer/
│   ├── requirements.txt       # Python bağımlılıkları
│   └── producer.py           # Kafka producer scripti
├── spark-app/
│   └── streaming_consumer.py  # PySpark streaming uygulaması
└── jars/                     # JDBC driver'lar (otomatik indirilir)
```

## 🚀 Hızlı Başlangıç

### 1. Otomatik Kurulum (Önerilen)

```bash
chmod +x setup.sh
./setup.sh
```

### 2. Manuel Kurulum

```bash
# 1. Dizinleri oluştur
mkdir -p producer spark-app jars

# 2. PostgreSQL JDBC driver'ını indir
curl -L -o jars/postgresql-42.7.0.jar https://jdbc.postgresql.org/download/postgresql-42.7.0.jar

# 3. Servisleri başlat
docker-compose up -d

# 4. Kafka topic oluştur
docker exec kafka kafka-topics --create --topic user-data --bootstrap-server localhost:9094 --partitions 1 --replication-factor 1
```

## ▶️ Pipeline'ı Çalıştırma

### Spark Streaming Job'ını Başlat

```bash
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.7.0 \
  /app/streaming_consumer.py
```

## 🔍 Monitoring & Kontrol

### Spark UI
```
http://localhost:8080
```

### PostgreSQL'deki Verileri Kontrol Et
```bash
docker exec -it pgdb psql -U postgres -d postgres -c "SELECT COUNT(*) FROM users;"
docker exec -it pgdb psql -U postgres -d postgres -c "SELECT * FROM users LIMIT 5;"
```

### Kafka Topic'ini Kontrol Et
```bash
docker exec kafka kafka-console-consumer --topic user-data --from-beginning --bootstrap-server localhost:9094
```

### Logları İzle
```bash
# Producer logları
docker logs -f kafka-producer

# Spark logları
docker logs -f spark-master
```

## 🛠️ Servis Detayları

| Servis | Port | Kullanıcı/Şifre | Açıklama |
|--------|------|----------------|----------|
| Kafka | 9092 | - | Message broker |
| PostgreSQL | 5432 | postgres/password | Veritabanı |
| Spark Master | 8080 | - | Spark UI |
| Zookeeper | 2181 | - | Kafka coordination |

## 📊 Veri Akışı

1. **Producer**: Her 5 saniyede bir randomuser.me API'sinden veri çeker
2. **Kafka**: Veriyi `user-data` topic'inde saklar
3. **Spark Streaming**: Kafka'dan veriyi okur, parse eder ve PostgreSQL'e yazar
4. **PostgreSQL**: Tüm kullanıcı verilerini `users` tablosunda saklar

## 🗃️ PostgreSQL Tablo Yapısı

`users` tablosu şu alanları içerir:
- Kişisel bilgiler (ad, soyad, cinsiyet, yaş)
- İletişim bilgileri (email, telefon)
- Adres bilgileri (sokak, şehir, ülke, koordinatlar)
- Login bilgileri (kullanıcı adı, hash'ler)
- Profil fotoğrafları

## 🔧 Troubleshooting

### Pipeline Çalışmıyor mu?

1. **Servislerin durumunu kontrol et:**
```bash
docker-compose ps
```

2. **Kafka topic'i var mı kontrol et:**
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9094
```

3. **PostgreSQL bağlantısını test et:**
```bash
docker exec -it pgdb psql -U postgres -d postgres -c "\dt"
```

### Spark Job Başlamıyor mu?

JAR dosyalarının doğru yüklendiğinden emin ol:
```bash
ls -la jars/
```

## 🛑 Pipeline'ı Durdurma

```bash
docker-compose down
docker-compose down -v  # Volume'ları da sil
```

## 📈 Performans Ayarları

Producer frekansını değiştirmek için `producer/producer.py` dosyasındaki `time.sleep(5)` değerini düzenleyin.

Spark worker sayısını artırmak için `docker-compose.yml` dosyasında yeni worker servisleri ekleyin.