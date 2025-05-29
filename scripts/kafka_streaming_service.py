import requests
import json
import time
import hashlib
from confluent_kafka import Producer

# Constants and configuration
API_ENDPOINT = "https://randomuser.me/api/?results=1"
# Docker Compose'daki Kafka servis adınıza göre düzenleyin
KAFKA_BOOTSTRAP_SERVERS = ['kafka_broker:19092']  # Logdan görünen adres
KAFKA_TOPIC = "names_topic"  
PAUSE_INTERVAL = 10  
STREAMING_DURATION = 120

def retrieve_user_data():
    """Retrieves user data from randomuser.me API with error handling."""
    url = "https://randomuser.me/api/?results=1"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Data retrieval failed: {e}")
        # Fallback mock data
        return {
            "results": [{
                "name": {
                    "title": "Mr",
                    "first": "Fallback", 
                    "last": "User"
                },
                "gender": "male",
                "location": {
                    "street": {"number": 123, "name": "Main St"},
                    "city": "Unknown City",
                    "country": "Unknown Country",
                    "postcode": "00000",
                    "coordinates": {"latitude": "0.0", "longitude": "0.0"}
                },
                "email": "fallback@example.com"
            }]
        }

def transform_user_data(api_response: dict) -> dict:
    """Formats the fetched user data for Kafka streaming."""
    # API response'dan ilk kullanıcıyı al
    user_data = api_response['results'][0]
    
    return {
        "name": f"{user_data['name']['title']}. {user_data['name']['first']} {user_data['name']['last']}",
        "gender": user_data["gender"],
        "address": f"{user_data['location']['street']['number']}, {user_data['location']['street']['name']}",  
        "city": user_data['location']['city'],
        "nation": user_data['location']['country'],  
        "zip": encrypt_zip(user_data['location']['postcode']),  
        "latitude": float(user_data['location']['coordinates']['latitude']),
        "longitude": float(user_data['location']['coordinates']['longitude']),
        "email": user_data["email"],
        "timestamp": int(time.time())  # Timestamp ekledim
    }

def encrypt_zip(zip_code):  
    """Hashes the zip code using MD5 and returns its integer representation."""
    zip_str = str(zip_code)
    hash_hex = hashlib.md5(zip_str.encode()).hexdigest()
    # Hash'i daha küçük sayıya çevir (mod ile)
    return int(hash_hex, 16) % (10**10)  # 10 haneli sayı

def configure_kafka(servers=KAFKA_BOOTSTRAP_SERVERS):
    """Creates and returns a Kafka producer instance."""
    settings = {
        'bootstrap.servers': ','.join(servers),
        'client.id': 'user_data_producer',
        'acks': '1',  # Sadece leader'dan onay bekle (daha hızlı)
        'retries': 5,   # Hata durumunda retry
        'retry.backoff.ms': 2000,
        'delivery.timeout.ms': 60000,  # 60 saniye timeout
        'request.timeout.ms': 30000,   # 30 saniye request timeout
        'socket.timeout.ms': 10000,    # 10 saniye socket timeout
        'message.timeout.ms': 60000,   # 60 saniye message timeout
        'api.version.request': True,   # API versiyonunu otomatik belirle
        'log_level': 0  # Debug logları için
    }
    return Producer(settings)

def publish_to_kafka(producer, topic, data):
    """Sends data to a Kafka topic."""
    try:
        # Key olarak email kullan (partitioning için)
        key = data.get('email', 'unknown').encode('utf-8')
        value = json.dumps(data, ensure_ascii=False).encode('utf-8')
        
        producer.produce(
            topic=topic, 
            key=key,
            value=value, 
            callback=delivery_status
        )
        producer.flush()  # Mesajın gönderilmesini garanti et
    except Exception as e:
        print(f"Failed to send message to Kafka: {e}")

def delivery_status(err, msg):
    """Reports the delivery status of the message to Kafka."""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Message delivered to {msg.topic()} [Partition: {msg.partition()}, Offset: {msg.offset()}]')

def initiate_stream():
    """Initiates the process to stream user data to Kafka."""
    print("🚀 Starting Kafka streaming...")
    print(f"📡 Kafka servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📝 Topic: {KAFKA_TOPIC}")
    print(f"⏱️  Interval: {PAUSE_INTERVAL}s, Duration: {STREAMING_DURATION}s")
    print("-" * 50)
    
    # Kafka bağlantısını test et
    print("🔍 Testing Kafka connection...")
    
    try:
        kafka_producer = configure_kafka()
        
        # Test mesajı gönder
        test_msg = {"test": "connection", "timestamp": int(time.time())}
        kafka_producer.produce(
            topic=KAFKA_TOPIC,
            value=json.dumps(test_msg).encode('utf-8'),
            callback=lambda err, msg: print(f"🧪 Test message: {'✅ Success' if err is None else f'❌ Failed: {err}'}")
        )
        kafka_producer.flush(timeout=10)  # 10 saniye bekle
        
        message_count = 0
        
        for i in range(STREAMING_DURATION // PAUSE_INTERVAL):
            print(f"\n📊 Fetching user data #{i+1}...")
            
            # API'den veri çek
            raw_data = retrieve_user_data()
            
            # Veriyi dönüştür
            kafka_formatted_data = transform_user_data(raw_data)
            
            # Kafka'ya gönder (timeout ile)
            publish_to_kafka(kafka_producer, KAFKA_TOPIC, kafka_formatted_data)
            message_count += 1
            
            print(f"👤 User: {kafka_formatted_data['name']}")
            print(f"📧 Email: {kafka_formatted_data['email']}")
            print(f"🌍 Location: {kafka_formatted_data['city']}, {kafka_formatted_data['nation']}")
            
            if i < (STREAMING_DURATION // PAUSE_INTERVAL) - 1:  # Son iterasyon değilse bekle
                print(f"⏳ Waiting {PAUSE_INTERVAL} seconds...")
                time.sleep(PAUSE_INTERVAL)
        
        print(f"\n✅ Streaming completed! Sent {message_count} messages to Kafka.")
        
    except KeyboardInterrupt:
        print("\n⚠️  Streaming interrupted by user.")
    except Exception as e:
        print(f"\n❌ Error during streaming: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("🔄 Closing Kafka producer...")
        if 'kafka_producer' in locals():
            kafka_producer.flush(timeout=5)  # 5 saniye bekle

if __name__ == "__main__":
    initiate_stream()