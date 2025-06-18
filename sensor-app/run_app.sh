#!/bin/bash

# Sensor Data Processing System Launcher
echo "🚀 Starting Kafka Sensor Data Processing System"
echo "=" * 50

# Check Python installation
PYTHON_CMD=""
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then 
    PYTHON_CMD="python"
else
    echo "❌ Python not found! Please install Python 3.7+"
    exit 1
fi

echo "✅ Using Python: $PYTHON_CMD"

# Check Docker installation
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found! Please install Docker"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose not found! Please install Docker Compose"
    exit 1
fi

echo "✅ Docker and Docker Compose found"

# Check if required files exist
required_files=("docker-compose.yml" "init.sql" "api.py" "consumer.py" "sensor_simulator.py")
for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ Required file $file not found!"
        exit 1
    fi
done

echo "✅ All required files found"

# Cleanup existing containers
echo "🧹 Cleaning up existing containers..."
docker stop $(docker ps -aq --filter "name=kafka") 2>/dev/null || true
docker stop $(docker ps -aq --filter "name=zookeeper") 2>/dev/null || true  
docker stop $(docker ps -aq --filter "name=sensor-postgres") 2>/dev/null || true
docker rm $(docker ps -aq --filter "name=kafka") 2>/dev/null || true
docker rm $(docker ps -aq --filter "name=zookeeper") 2>/dev/null || true
docker rm $(docker ps -aq --filter "name=sensor-postgres") 2>/dev/null || true

# Kill processes on required ports
echo "🔍 Checking for processes on required ports..."
lsof -ti:9092 | xargs kill -9 2>/dev/null || true
lsof -ti:5432 | xargs kill -9 2>/dev/null || true
lsof -ti:8000 | xargs kill -9 2>/dev/null || true

# Install Python dependencies
echo "📦 Installing Python dependencies..."
pip3 install -r requirements.txt 2>/dev/null || pip install -r requirements.txt

# Start Docker services
echo "🐳 Starting Docker services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check if Kafka is ready
echo "🔍 Checking Kafka connection..."
timeout=60
counter=0
while [ $counter -lt $timeout ]; do
    if docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 &> /dev/null; then
        echo "✅ Kafka is ready!"
        break
    fi
    echo "⏳ Waiting for Kafka... ($counter/$timeout)"
    sleep 2
    counter=$((counter + 2))
done

if [ $counter -ge $timeout ]; then
    echo "❌ Kafka failed to start within $timeout seconds"
    docker-compose logs kafka
    exit 1
fi

# Check if PostgreSQL is ready
echo "🔍 Checking PostgreSQL connection..."
counter=0
while [ $counter -lt $timeout ]; do
    if docker exec sensor-postgres pg_isready -U postgres &> /dev/null; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    echo "⏳ Waiting for PostgreSQL... ($counter/$timeout)"
    sleep 2
    counter=$((counter + 2))
done

if [ $counter -ge $timeout ]; then
    echo "❌ PostgreSQL failed to start within $timeout seconds"
    docker-compose logs postgres
    exit 1
fi

# Start the API
echo "🌐 Starting FastAPI server..."
$PYTHON_CMD -m uvicorn api:app --host 0.0.0.0 --port 8000 &
API_PID=$!

# Wait for API to start
sleep 5

# Check if API is running
if ! curl -s http://localhost:8000/health > /dev/null; then
    echo "❌ API failed to start"
    kill $API_PID 2>/dev/null
    exit 1
fi

echo "✅ API is running at http://localhost:8000"

# Print instructions
echo ""
echo "🎯 System Started Successfully!"
echo "=" * 50
echo "📋 API Documentation: http://localhost:8000/docs"
echo "💚 Health Check: http://localhost:8000/health"
echo "📊 Analysis: http://localhost:8000/analysis"
echo ""
echo "🔄 To start consumers, run in another terminal:"
echo "   $PYTHON_CMD consumer.py"
echo ""
echo "📈 To start sensor simulation, run in another terminal:"
echo "   $PYTHON_CMD sensor_simulator.py"
echo ""
echo "🛑 To stop everything:"
echo "   Ctrl+C (this terminal) + docker-compose down"
echo "=" * 50

# Keep the script running
wait $API_PID