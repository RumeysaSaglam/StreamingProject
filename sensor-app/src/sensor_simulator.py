import time
import threading
import requests
import random
import logging
from datetime import datetime
from config import SENSOR_CONFIGS, SENSOR_LOCATIONS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SensorSimulator:
    def __init__(self, api_url="http://api:8000"):
        self.api_url = api_url
        self.running = False
        self.threads = []
    
    def generate_sensor_data(self, sensor_type, sensor_id, location):
        """Generate random sensor data based on type and configured ranges."""
        config_key = sensor_type.replace('-', '_')
        config = SENSOR_CONFIGS.get(config_key)
        
        if not config:
            logger.error(f"No configuration found for sensor type: {sensor_type}")
            return None
        
        # Generate random value within range
        min_val = config['min_value']
        max_val = config['max_value']
        
        # Add some randomness - occasionally generate critical values for testing
        if random.random() < 0.1:  # 10% chance of critical value
            if sensor_type == 'temperature':
                value = random.uniform(46, 55)  # Critical temperature
            elif sensor_type == 'humidity':
                if random.choice([True, False]):
                    value = random.uniform(0, 19)  # Too low
                else:
                    value = random.uniform(76, 100)  # Too high
            elif sensor_type == 'traffic':
                value = random.uniform(351, 500)  # High traffic
            elif sensor_type == 'air-quality':
                value = random.uniform(151, 500)  # Poor air quality
            else:
                value = random.uniform(min_val, max_val)
        else:
            # Normal range with some variation
            if sensor_type == 'temperature':
                value = random.uniform(15, 35)  # Normal range
            elif sensor_type == 'humidity':
                value = random.uniform(30, 60)  # Normal range
            elif sensor_type == 'traffic':
                value = random.uniform(100, 300)  # Normal range
            elif sensor_type == 'air-quality':
                value = random.uniform(0, 100)  # Good air quality
            else:
                value = random.uniform(min_val, max_val)
        
        return {
            "sensor_id": sensor_id,
            "type": sensor_type,
            "location": location,
            "value": round(value, 2),
            "timestamp": datetime.now().isoformat()
        }
    
    def sensor_thread(self, sensor_type, sensor_id, location):
        """Thread function for individual sensor data generation."""
        logger.info(f"🚀 Started sensor thread: {sensor_id} in {location}")
        
        while self.running:
            try:
                # Generate sensor data
                data = self.generate_sensor_data(sensor_type, sensor_id, location)
                
                if data is None:
                    logger.error(f"Failed to generate data for {sensor_id}")
                    continue
                
                # Send data to API
                response = requests.post(
                    f"{self.api_url}/publish",
                    json=data,
                    timeout=5
                )
                
                if response.status_code == 200:
                    status_icon = "🔥" if data['value'] > 45 and sensor_type == 'temperature' else "✅"
                    logger.info(f"{status_icon} {sensor_id}: {data['value']} {SENSOR_CONFIGS[sensor_type.replace('-', '_')]['unit']}")
                else:
                    logger.error(f"❌ Failed to send data for {sensor_id}: Status {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Network error for {sensor_id}: {e}")
            except Exception as e:
                logger.error(f"❌ Unexpected error in {sensor_id}: {e}")
            
            # Wait for next iteration
            time.sleep(5)  # Generate data every 5 seconds
        
        logger.info(f"🛑 Stopped sensor thread: {sensor_id}")
    
    def start_simulation(self):
        """Start all sensor simulation threads."""
        logger.info("🎯 Starting Kafka Sensor Simulation System")
        logger.info("=" * 50)
        
        # Test API connection first
        try:
            response = requests.get(f"{self.api_url}/health", timeout=5)
            if response.status_code != 200:
                logger.error("❌ API health check failed. Make sure the API is running!")
                return
            logger.info("✅ API connection successful")
        except Exception as e:
            logger.error(f"❌ Cannot connect to API at {self.api_url}: {e}")
            logger.error("Make sure to start the API first: uvicorn api:app --host 0.0.0.0 --port 8000")
            return
        
        self.running = True
        sensor_types = ['temperature', 'humidity', 'traffic', 'air-quality']
        
        # Create sensors for each type
        for sensor_type in sensor_types:
            for i in range(5):  # 5 sensors per type (20 total)
                sensor_id = f"{sensor_type}_{i+1:03d}"
                location = random.choice(SENSOR_LOCATIONS)
                
                # Create and start thread
                thread = threading.Thread(
                    target=self.sensor_thread,
                    args=(sensor_type, sensor_id, location),
                    daemon=True,
                    name=f"Sensor-{sensor_id}"
                )
                
                self.threads.append(thread)
                thread.start()
                
                # Small delay between starting threads
                time.sleep(0.1)
        
        logger.info(f"🔄 Started {len(self.threads)} sensor threads")
        logger.info("📊 Sensor Distribution:")
        for sensor_type in sensor_types:
            config = SENSOR_CONFIGS[sensor_type.replace('-', '_')]
            logger.info(f"   • {sensor_type}: 5 sensors ({config['unit']})")
        
        logger.info("🌍 Locations: " + ", ".join(SENSOR_LOCATIONS))
        logger.info("⏰ Data generation interval: 5 seconds")
        logger.info("🚨 ~10% of data will be critical values for testing")
        logger.info("=" * 50)
        logger.info("Press Ctrl+C to stop simulation...")
        
        # Keep main thread alive
        try:
            while self.running:
                # Print status every minute
                time.sleep(60)
                active_threads = sum(1 for t in self.threads if t.is_alive())
                logger.info(f"📈 Status: {active_threads}/{len(self.threads)} sensors active")
                
        except KeyboardInterrupt:
            logger.info("🛑 Stopping simulation...")
            self.stop_simulation()
    
    def stop_simulation(self):
        """Stop all sensor threads."""
        self.running = False
        logger.info("⏳ Waiting for threads to finish...")
        
        # Wait for all threads to finish
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=2)
        
        logger.info("✅ Simulation stopped successfully")

def main():
    """Main function to run the sensor simulator."""
    simulator = SensorSimulator()
    
    try:
        simulator.start_simulation()
    except Exception as e:
        logger.error(f"❌ Simulation failed: {e}")
    finally:
        simulator.stop_simulation()

if __name__ == "__main__":
    main()