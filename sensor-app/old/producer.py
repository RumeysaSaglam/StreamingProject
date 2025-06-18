import json
import time
import requests
from kafka import KafkaProducer
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_producer():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9094')
    return KafkaProducer(
        bootstrap_servers=[bootstrap_servers], #Kafka cluster'a bağlanmak için IP ve port bilgisi
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

def fetch_sensor_data():
    try:
        response = requests.get('http://localhost:8000/api/data')
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"API çağrısında hata: {e}")
        return None

def main():
    producer = create_producer()
    topic = 'sensor_data'
    
    logger.info("Producer başlatıldı.")
    
    while True:
        try:
            user_data = fetch_user_data()
            if user_data:
                # Kafka'ya gönder
                future = producer.send(topic, value=user_data)
                result = future.get(timeout=10)
                logger.info(f"Veri gönderildi: {result}")
            
            time.sleep(5)
            
        except KeyboardInterrupt:
            logger.info("Producer durduruluyor...")
            break
        except Exception as e:
            logger.error(f"Hata oluştu: {e}")
            time.sleep(5)
    
    producer.close()

if __name__ == "__main__":
    main()