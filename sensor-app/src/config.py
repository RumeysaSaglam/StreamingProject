import os
from typing import Dict, Any

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPICS = {
    'SENSOR_DATA': 'sensor-data',
    'SENSOR_ALERTS': 'sensor-alerts'
}

# Database Configuration
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'pass')
}

# Sensor Configuration
SENSOR_CONFIGS = {
    'temperature': {
        'min_value': -10.0,
        'max_value': 50.0,
        'critical_threshold': 45.0,
        'unit': '°C',
        'partition': 0
    },
    'humidity': {
        'min_value': 0.0,
        'max_value': 100.0,
        'critical_threshold_low': 20.0,
        'critical_threshold_high': 75.0,
        'unit': '%',
        'partition': 1
    },
    'traffic': {
        'min_value': 0.0,
        'max_value': 500.0,
        'critical_threshold': 350.0,
        'unit': 'vehicles/hour',
        'partition': 2
    },
    'air-quality': {
        'min_value': 0.0,
        'max_value': 500.0,
        'critical_threshold': 150.0,
        'unit': 'AQI',
        'partition': 3
    }
}

# Sensor locations
SENSOR_LOCATIONS = [
    'Istanbul', 'Ankara', 'Izmir', 'Bursa', 'Antalya',
    'Adana', 'Konya', 'Gaziantep', 'Mersin', 'Diyarbakir'
]

# API Configuration
API_HOST = os.getenv('API_HOST', '0.0.0.0')
API_PORT = int(os.getenv('API_PORT', '8000'))

# Consumer Group Configuration
CONSUMER_GROUPS = {
    'SENSOR_DATA_PROCESSORS': 'sensor-data-processors',
    'ALERT_NOTIFIERS': 'alert-notifiers',
    'ALERT_EMAILERS': 'alert-emailers'
}

# Data Generation Configuration
DATA_GENERATION_INTERVAL = 5  # seconds
SENSOR_COUNT_PER_TYPE = 5

def is_critical_value(sensor_type: str, value: float) -> bool:
    """Check if sensor value is critical based on type and thresholds."""
    config = SENSOR_CONFIGS.get(sensor_type.replace('-', '_'))
    if not config:
        return False
    
    if sensor_type == 'humidity':
        return value < config['critical_threshold_low'] or value > config['critical_threshold_high']
    else:
        return value > config.get('critical_threshold', float('inf'))

def get_partition_for_sensor_type(sensor_type: str) -> int:
    """Get the partition number for a specific sensor type."""
    config = SENSOR_CONFIGS.get(sensor_type.replace('-', '_'))
    return config.get('partition', 0) if config else 0