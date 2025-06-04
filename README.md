# Stream Pipeline - Docker + PySpark + Kafka + PostgreSQL

Bu proje, randomuser.me API'sinden kullanıcı verilerini alıp, Kafka üzerinden stream ederek PostgreSQL'e yazan bir pipeline'dır.

## Mimari

```
RandomUser API → Kafka Producer → Kafka → Spark Streaming → PostgreSQL
```

## Proje Yapısı

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
└── jars/                     # JDBC driver'lar
```


### 1. İlk çalıştırma için Kurulum 

```bash
chmod +x setup.sh
./setup.sh
```

## Pipeline'ı Çalıştırma

### Docker servislerini başlatmak için .sh çalıştırılmalı

```bash
./docker_restart.sh
```

### Spark Streaming Job'ını Başlatmak için aşağıdaki komut çalıştırılmalı

```bash
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.7.0 \
  /app/streaming_consumer.py
```

## Monitoring & Kontrol

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


## Veri Akışı

1. **Producer**: Her 5 saniyede bir randomuser.me API'sinden veri çeker
2. **Kafka**: Veriyi `user-data` topic'inde saklar
3. **Spark Streaming**: Kafka'dan veriyi okur, parse eder ve PostgreSQL'e yazar
4. **PostgreSQL**: Tüm kullanıcı verilerini `users` tablosunda saklar

## PostgreSQL Tablo Yapısı

`users` tablosu şu alanları içerir:
- Kişisel bilgiler (ad, soyad, cinsiyet, yaş)
- İletişim bilgileri (email, telefon)
- Adres bilgileri (sokak, şehir, ülke, koordinatlar)
- Login bilgileri (kullanıcı adı, hash'ler)
- Profil fotoğrafları

## Troubleshooting

### Pipeline Çalışmıyor ise

1. **Servislerin durumunu kontrol et:**
```bash
docker-compose ps -a
```

2. **Kafka topic'i var mı kontrol et:**
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9094
```

3. **PostgreSQL bağlantısını test et:**
```bash
docker exec -it pgdb psql -U postgres -d postgres -c "\dt"
```


## Pipeline'ı Durdurma

```bash
docker-compose down
docker-compose down -v  # Volume'ları da sil
```