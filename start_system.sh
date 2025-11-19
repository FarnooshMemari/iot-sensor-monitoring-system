#!/bin/bash

# IoT Sensor Monitoring System - Startup Script
# This script starts all services in the correct order

set -e

echo "🚀 Starting IoT Sensor Monitoring System..."
echo "========================================"

# Function to check if a service is healthy
check_service() {
    local service_name=$1
    local max_attempts=30
    local attempt=1
    
    echo "⏳ Waiting for $service_name to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker compose ps $service_name | grep -q "healthy\|running"; then
            echo "✅ $service_name is ready!"
            return 0
        fi
        
        echo "   Attempt $attempt/$max_attempts - $service_name not ready yet..."
        sleep 5
        ((attempt++))
    done
    
    echo "❌ $service_name failed to start within expected time"
    return 1
}

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop first."
    exit 1
fi

# Build the Python services
echo "🔨 Building Python services..."
docker compose build

# Start infrastructure services first
echo "🏗️  Starting infrastructure services..."
docker compose up -d zookeeper kafka postgres

# Wait for infrastructure to be ready
echo "⏳ Waiting for infrastructure services..."
sleep 20

# Check Kafka topics and create if necessary
echo "📋 Setting up Kafka topics..."
docker compose exec kafka kafka-topics --bootstrap-server localhost:29092 --list | grep -q "sensor_readings" || \
docker compose exec kafka kafka-topics --bootstrap-server localhost:29092 --create --topic sensor_readings --partitions 3 --replication-factor 1

# Initialize database
echo "🗄️  Initializing database..."
docker compose up --no-deps db-init
docker compose rm -f db-init

# Start application services
echo "🌐 Starting application services..."
docker compose up -d iot-consumer iot-producer iot-dashboard

# Check service status
echo "🔍 Checking service status..."
sleep 10

echo ""
echo "📊 Service Status:"
echo "=================="
docker compose ps

echo ""
echo "🎉 Deployment Complete!"
echo "======================"
echo ""
echo "📈 Dashboard URL: http://localhost:8501"
echo "📊 Kafka UI: You can connect to kafka at localhost:9092"
echo "🗄️  PostgreSQL: localhost:5432 (user: postgres, password: postgres, db: streaming_db)"
echo ""
echo "📝 To view logs:"
echo "   docker compose logs -f iot-producer    # Producer logs"
echo "   docker compose logs -f iot-consumer    # Consumer logs"  
echo "   docker compose logs -f iot-dashboard   # Dashboard logs"
echo "   docker compose logs -f                 # All service logs"
echo ""
echo "🛑 To stop all services:"
echo "   docker compose down"
echo ""
echo "🔄 To restart a specific service:"
echo "   docker compose restart <service-name>"
echo ""

# Wait a moment for services to fully start
sleep 5

echo "🚨 Checking for any failed services..."
failed_services=$(docker compose ps --filter "status=exited" --format "table {{.Service}}" | tail -n +2)

if [ -n "$failed_services" ]; then
    echo "❌ The following services failed to start:"
    echo "$failed_services"
    echo ""
    echo "📋 Run 'docker compose logs <service-name>' to check the logs"
else
    echo "✅ All services are running successfully!"
fi

echo ""
echo "🎊 Your IoT Sensor Monitoring System is ready!"
echo "   Open http://localhost:8501 to view the dashboard"