# Kafka Sensor Data Processing System

A real-time sensor data processing system using Kafka, FastAPI, and PostgreSQL. This system simulates IoT sensors, processes data through Kafka topics, and provides analytics insights.

## 🏗️ Architecture

```
Sensor Simulator → FastAPI → Kafka Topics → Consumer Groups → PostgreSQL
                     ↓
                Analytics API
```

## 📋 Features

- **Real-time sensor data simulation** (Temperature, Humidity, Traffic, Air Quality)
- **Kafka-based message processing** with multiple topics and consumer groups
- **Critical alert system** with automatic notifications
- **PostgreSQL database** for data persistence and analytics
- **RESTful API** with comprehensive endpoints
- **Real-time analytics** and monitoring dashboard

## 🛠️ Prerequisites

- Python 3.8+
- Docker & Docker Compose
- Git

## 🚀 Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd kafka-sensor-system
pip install -r requirements.txt
```

### 2. Start Infrastructure

```bash
# Start Kafka, Zookeeper, and PostgreSQL
docker-compose up -d

# Wait for services to start
sleep 15

# Verify topics were created
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 3. Start Application Components

**Terminal 1: Start API Server**
```bash
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

**Terminal 2: Start Consumer System**
```bash
python consumer.py
```

**Terminal 3: Start Sensor Simulator**
```bash
python sensor_simulator.py
```

### 4. Access the System

- **API Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health
- **Analytics**: http://localhost:8000/analysis

## 📊 System Components

### Kafka Topics

- **sensor-data** (4 partitions): Normal sensor readings
  - Partition 0: Temperature sensors
  - Partition 1: Humidity sensors  
  - Partition 2: Traffic sensors
  - Partition 3: Air quality sensors

- **sensor-alerts** (2 partitions): Critical alerts

### Consumer Groups

1. **sensor-data-processors** (3 consumers): Process normal sensor data
2. **alert-notifiers** (1 consumer): Handle notification alerts
3. **alert-emailers** (1 consumer): Handle email alerts

### Sensor Types & Thresholds

| Sensor Type | Unit | Normal Range | Critical Threshold |
|-------------|------|--------------|-------------------|
| Temperature | °C | -10 to 50 | > 45°C |
| Humidity | % | 0 to 100 | < 20% or > 75% |
| Traffic | vehicles/hour | 0 to 500 | > 350 |
| Air Quality | AQI | 0 to 500 | > 150 |

## 🔌 API Endpoints

### Data Publishing
- `POST /publish` - Publish sensor data
- `GET /health` - System health check

### Analytics
- `GET /analysis` - Comprehensive analytics
- `GET /sensors/{sensor_id}/history` - Sensor history

### Alert Processing
- `POST /notify` - Process notifications (internal)
- `POST /email` - Process email alerts (internal)

## 🗄️ Database Schema

### raw_data table
- Stores all consumed messages with partition/offset info
- Indexed by sensor_id, type, timestamp

### processed_data table  
- Stores processed results with analytics
- Includes daily averages, hourly counts, anomaly scores
- Status tracking (normal/warning/critical)

## 📈 Analytics Features

- **Real-time metrics**: Active sensors, data points, alert counts
- **Sensor summaries**: By type and location
- **Anomaly detection**: Based on historical averages
- **Status monitoring**: Normal/Warning/Critical states

## 🔧 Configuration

Key configuration in `config.py`:

```python
# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Database settings  
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'sensordb',
    'user': 'postgres',
    'password': 'pass'
}

# Sensor locations
SENSOR_LOCATIONS = ['Istanbul', 'Ankara', 'Izmir', ...]
```

## 🐳 Docker Services

```yaml
services:
  - zookeeper:2181
  - kafka:9092  
  - postgres:5432
```

## 🧪 Testing

### Manual Testing
```bash
# Test API health
curl http://localhost:8000/health

# Test data publishing
curl -X POST http://localhost:8000/publish \
  -H "Content-Type: application/json" \
  -d '{
    "sensor_id": "temp_