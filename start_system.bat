@echo off
REM IoT Sensor Monitoring System - Windows Startup Script

echo 🚀 Starting IoT Sensor Monitoring System...
echo ========================================

REM Check if Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker is not running. Please start Docker Desktop first.
    pause
    exit /b 1
)

REM Build the Python services
echo 🔨 Building Python services...
docker compose build

REM Start infrastructure services first
echo 🏗️  Starting infrastructure services...
docker compose up -d zookeeper kafka postgres

REM Wait for infrastructure
echo ⏳ Waiting for infrastructure services...
timeout /t 20 /nobreak

REM Initialize database
echo 🗄️  Initializing database...
docker compose up --no-deps db-init
docker compose rm -f db-init

REM Start application services
echo 🌐 Starting application services...
docker compose up -d iot-consumer iot-producer iot-dashboard

REM Check service status
echo 🔍 Checking service status...
timeout /t 10 /nobreak

echo.
echo 📊 Service Status:
echo ==================
docker compose ps

echo.
echo 🎉 Deployment Complete!
echo ======================
echo.
echo 📈 Dashboard URL: http://localhost:8501
echo 📊 Kafka UI: You can connect to kafka at localhost:9092
echo 🗄️  PostgreSQL: localhost:5432 (user: postgres, password: postgres, db: streaming_db)
echo.
echo 📝 To view logs:
echo    docker compose logs -f iot-producer    # Producer logs
echo    docker compose logs -f iot-consumer    # Consumer logs
echo    docker compose logs -f iot-dashboard   # Dashboard logs
echo    docker compose logs -f                 # All service logs
echo.
echo 🛑 To stop all services:
echo    docker compose down
echo.
echo 🎊 Your IoT Sensor Monitoring System is ready!
echo    Open http://localhost:8501 to view the dashboard
echo.
pause