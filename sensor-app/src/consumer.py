import json
import threading
import time
import requests
import logging
from confluent_kafka import Consumer, KafkaError
from database import DatabaseManager
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, CONSUMER_GROUPS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global database manager
db_manager = DatabaseManager()

class SensorDataProcessor:
    """Processor for sensor data with analytics calculations."""
    
    @staticmethod
    def process_sensor_data(data):
        """
        Process sensor data and calculate analytics.
        This is where you can implement your specific processing logic.
        """
        try:
            sensor_id = data['sensor_id']
            sensor_type = data['type']
            value = data['value']
            
            # Calculate daily average
            daily_avg = db_manager.calculate_daily_average(sensor_id, sensor_type)
            
            # Calculate hourly count
            hourly_count = db_manager.calculate_hourly_count(sensor_id, sensor_type)
            
            # Calculate anomaly score (simple example)
            anomaly_score = 0.0
            if daily_avg:
                deviation = abs(value - daily_avg) / daily_avg
                anomaly_score = min(deviation, 1.0)  # Cap at 1.0
            
            # Determine status
            status = 'critical' if data.get('is_critical', False) else 'normal'
            if not data.get('is_critical', False) and anomaly_score > 0.3:
                status = 'warning'  # High deviation but not critical
            
            # Smooth processed value (simple moving average simulation)
            processed_value = value
            if daily_avg:
                processed_value = (value * 0.7) + (daily_avg * 0.3)  # Weighted average
            
            processed_result = {
                'processed_value': round(processed_value, 2),
                'status': status,
                'daily_avg': daily_avg,
                'hourly_count': hourly_count,
                'anomaly_score': round(anomaly_score, 3)
            }
            
            logger.debug(f"Processed {sensor_id}: {processed_result}")
            return processed_result
            
        except Exception as e:
            logger.error(f"Error processing sensor data: {e}")
            return {
                'processed_value': data.get('value', 0),
                'status': 'error',
                'daily_avg': None,
                'hourly_count': 0,
                'anomaly_score': 0.0
            }

class ConsumerManager:
    def __init__(self):
        self.running = False
        self.consumer_threads = []
    
    def sensor_data_consumer(self, consumer_id):
        """Consumer for sensor-data topic (Consumer Group 1)."""
        try:
            consumer = Consumer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': CONSUMER_GROUPS['SENSOR_DATA_PROCESSORS'],
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True,
                'auto.commit.interval.ms': 1000,
                'session.timeout.ms': 30000,
                'heartbeat.interval.ms': 10000
            })
            
            consumer.subscribe([KAFKA_TOPICS['SENSOR_DATA']])
            logger.info(f"🔄 Consumer {consumer_id} started for sensor-data topic")
            
            while self.running:
                try:
                    msg = consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.debug(f"End of partition reached {msg.topic()}/{msg.partition()}")
                        else:
                            logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    # Parse message
                    data = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"📥 Consumer {consumer_id} received: {data['sensor_id']} = {data['value']} "
                              f"(partition: {msg.partition()}, offset: {msg.offset()})")
                    
                    # Store raw data in database
                    raw_id = db_manager.insert_raw_data(
                        data, 
                        partition_id=msg.partition(), 
                        offset_id=msg.offset()
                    )
                    
                    # Process the data
                    processed_result = SensorDataProcessor.process_sensor_data(data)
                    
                    # Store processed data in database
                    processed_id = db_manager.insert_processed_data(raw_id, data, processed_result)
                    
                    logger.info(f"✅ Consumer {consumer_id} processed data: raw_id={raw_id}, processed_id={processed_id}")
                    
                except Exception as e:
                    logger.error(f"❌ Error in consumer {consumer_id}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"❌ Unexpected error in consumer {consumer_id}: {e}")
        finally:
            consumer.close()
            logger.info(f"🛑 Consumer {consumer_id} stopped")
    
    def alert_consumer(self, endpoint_type, group_suffix):
        """
        Consumer for sensor-alerts topic.
        endpoint_type: 'notify' or 'email'
        group_suffix: 'NOTIFIERS' or 'EMAILERS'
        """
        try:
            consumer = Consumer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': CONSUMER_GROUPS[f'ALERT_{group_suffix}'],
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True,
                'auto.commit.interval.ms': 1000,
                'fetch.max.bytes': 1200000000,
                'max.partition.fetch.bytes': 1200000000
            })
            
            consumer.subscribe([KAFKA_TOPICS['SENSOR_ALERTS']])
            logger.info(f"🚨 Alert consumer started for {endpoint_type} endpoint (group: {group_suffix})")
            
            while self.running:
                try:
                    msg = consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.debug(f"End of partition reached {msg.topic()}/{msg.partition()}")
                        else:
                            logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    # Parse message
                    data = json.loads(msg.value().decode('utf-8'))
                    logger.warning(f"⚠️  CRITICAL ALERT received: {data['sensor_id']} = {data['value']} "
                                 f"(Type: {data['type']}, Location: {data['location']})")
                    
                    # Forward to appropriate API endpoint
                    api_url = f"http://api:8000/{endpoint_type}"
                    response = requests.post(api_url, json=data, timeout=10)
                    
                    if response.status_code == 200:
                        logger.info(f"📤 Alert forwarded to /{endpoint_type} successfully")
                    else:
                        logger.error(f"❌ Failed to forward alert to /{endpoint_type}: Status {response.status_code}")
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"❌ Network error forwarding alert to {endpoint_type}: {e}")
                except Exception as e:
                    logger.error(f"❌ Error in alert consumer ({endpoint_type}): {e}")
                    continue
        
        except Exception as e:
            logger.error(f"❌ Unexpected error in alert consumer ({endpoint_type}): {e}")
        finally:
            consumer.close()
            logger.info(f"🛑 Alert consumer ({endpoint_type}) stopped")
    
    def start_consumers(self):
        """Start all consumer groups as specified in requirements."""
        logger.info("🎯 Starting Kafka Consumer System")
        logger.info("=" * 50)
        
        # Test Kafka connection
        try:
            test_consumer = Consumer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': 'test-group'
            })
            test_consumer.close()
            logger.info("✅ Kafka connection successful")
        except Exception as e:
            logger.error(f"❌ Cannot connect to Kafka: {e}")
            logger.error("Make sure Kafka is running: docker-compose up -d")
            return
        
        # Test database connection
        try:
            with db_manager.get_connection() as conn:
                logger.info("✅ Database connection successful")
        except Exception as e:
            logger.error(f"❌ Cannot connect to database: {e}")
            return
        
        self.running = True
        
        # Consumer Group 1: 3 consumers for sensor-data topic
        logger.info("🔄 Starting Consumer Group 1: sensor-data processors (3 consumers)")
        for i in range(3):
            thread = threading.Thread(
                target=self.sensor_data_consumer,
                args=(f"sensor-data-consumer-{i+1}",),
                daemon=True,
                name=f"SensorDataConsumer-{i+1}"
            )
            self.consumer_threads.append(thread)
            thread.start()
            time.sleep(0.5)  # Small delay between starting consumers
        
        # Consumer Group 2: 1 consumer for alerts -> notify endpoint
        logger.info("🚨 Starting Consumer Group 2: alert notifiers (1 consumer)")
        notify_thread = threading.Thread(
            target=self.alert_consumer,
            args=("notify", "NOTIFIERS"),
            daemon=True,
            name="AlertNotifyConsumer"
        )
        self.consumer_threads.append(notify_thread)
        notify_thread.start()
        time.sleep(0.5)
        
        # Consumer Group 3: 1 consumer for alerts -> email endpoint
        logger.info("📧 Starting Consumer Group 3: alert emailers (1 consumer)")
        email_thread = threading.Thread(
            target=self.alert_consumer,
            args=("email", "EMAILERS"),
            daemon=True,
            name="AlertEmailConsumer"
        )
        self.consumer_threads.append(email_thread)
        email_thread.start()
        
        logger.info(f"✅ Started {len(self.consumer_threads)} consumer threads")
        logger.info("📊 Consumer Group Configuration:")
        logger.info(f"   • Group 1 ({CONSUMER_GROUPS['SENSOR_DATA_PROCESSORS']}): 3 consumers processing sensor-data")
        logger.info(f"   • Group 2 ({CONSUMER_GROUPS['ALERT_NOTIFIERS']}): 1 consumer for notifications")
        logger.info(f"   • Group 3 ({CONSUMER_GROUPS['ALERT_EMAILERS']}): 1 consumer for emails")
        logger.info("=" * 50)
        logger.info("Press Ctrl+C to stop all consumers...")
        
        # Keep main thread alive
        try:
            while self.running:
                # Print status every 2 minutes
                time.sleep(120)
                active_threads = sum(1 for t in self.consumer_threads if t.is_alive())
                logger.info(f"📈 Status: {active_threads}/{len(self.consumer_threads)} consumers active")
                
        except KeyboardInterrupt:
            logger.info("🛑 Stopping all consumers...")
            self.stop_consumers()
    
    def stop_consumers(self):
        """Stop all consumer threads."""
        self.running = False
        logger.info("⏳ Waiting for consumer threads to finish...")
        
        # Wait for all threads to finish
        for thread in self.consumer_threads:
            if thread.is_alive():
                thread.join(timeout=5)
        
        logger.info("✅ All consumers stopped successfully")

def main():
    """Main function to run the consumer system."""
    consumer_manager = ConsumerManager()
    
    try:
        consumer_manager.start_consumers()
    except Exception as e:
        logger.error(f"❌ Consumer system failed: {e}")
    finally:
        consumer_manager.stop_consumers()

if __name__ == "__main__":
    main()