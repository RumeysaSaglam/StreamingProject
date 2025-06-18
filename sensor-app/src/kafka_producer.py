import json
import logging
from confluent_kafka import Producer
from typing import Dict, Any
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, get_partition_for_sensor_type, is_critical_value

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SensorDataProducer:
    def __init__(self):
        self.producer = None
        self._initialize_producer()
    
    def _initialize_producer(self):
        """Initialize Kafka producer with proper configuration."""
        try:
            self.producer = Producer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'client.id': 'sensor-producer',
                'acks': 'all',  # Wait for all replicas to acknowledge
                'retries': 3,
                'retry.backoff.ms': 300,
                'request.timeout.ms': 30000,
                'max.in.flight.requests.per.connection': 1,
                'max.request.size': 1200000000,
                'batch.size' :16384,
                'linger.ms': 0  

            })
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def _delivery_callback(self, err, msg):
        """Delivery callback for produced messages."""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to topic: {msg.topic()}, "
                       f"partition: {msg.partition()}, offset: {msg.offset()}")
    
    def send_sensor_data(self, sensor_data: Dict[str, Any]) -> bool:
        """
        Send sensor data to appropriate Kafka topic based on criticality.
        
        Args:
            sensor_data: Dictionary containing sensor information
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        try:
            sensor_type = sensor_data['type']
            value = sensor_data['value']
            sensor_id = sensor_data['sensor_id']
            
            # Determine if the value is critical
            critical = is_critical_value(sensor_type, value)
            
            if critical:
                # Send to sensor-alerts topic
                topic = KAFKA_TOPICS['SENSOR_ALERTS']
                partition = None  # Let Kafka handle partition assignment for alerts
                key = f"alert_{sensor_id}"
                
                # Add criticality information to the data
                alert_data = sensor_data.copy()
                alert_data['is_critical'] = True
                alert_data['alert_type'] = self._get_alert_type(sensor_type, value)
                
                logger.info(f"Sending CRITICAL data to {topic}: {sensor_id} = {value}")
            else:
                # Send to sensor-data topic with specific partition
                topic = KAFKA_TOPICS['SENSOR_DATA']
                partition = get_partition_for_sensor_type(sensor_type)
                key = f"sensor_{sensor_id}"
                alert_data = sensor_data.copy()
                alert_data['is_critical'] = False
                
                logger.info(f"Sending normal data to {topic} (partition {partition}): {sensor_id} = {value}")
            
            # Send message to Kafka
            self.producer.produce(
                topic=topic,
                value=json.dumps(alert_data),
                key=key,
                partition=partition,
                callback=self._delivery_callback
            )
            
            # Flush to ensure message is sent
            self.producer.flush(timeout=10)
            
            return True
            
        except Exception as e:
            logger.error(f"Error while sending message: {e}")
            return False
    
    def _get_alert_type(self, sensor_type: str, value: float) -> str:
        """Determine the type of alert based on sensor type and value."""
        if sensor_type == 'temperature':
            return 'high_temperature'
        elif sensor_type == 'humidity':
            return 'humidity_out_of_range'
        elif sensor_type == 'traffic':
            return 'high_traffic'
        elif sensor_type == 'air-quality':
            return 'poor_air_quality'
        else:
            return 'unknown_alert'
    
    def send_test_message(self, topic: str, message: Dict[str, Any]) -> bool:
        """Send a test message to specified topic."""
        try:
            self.producer.produce(
                topic=topic,
                value=json.dumps(message),
                callback=self._delivery_callback
            )
            self.producer.flush(timeout=10)
            logger.info(f"Test message sent to {topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to send test message: {e}")
            return False
    
    def close(self):
        """Close the producer and flush any pending messages."""
        if self.producer:
            try:
                self.producer.flush()
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")

# Singleton instance
_producer_instance = None

def get_producer() -> SensorDataProducer:
    """Get or create a singleton producer instance."""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = SensorDataProducer()
    return _producer_instance