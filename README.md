# IoT Sensor Monitoring System

A real-time streaming data pipeline that simulates IoT environmental sensors and provides live visualization through an interactive dashboard.

![IoT Dashboard Screenshot](dashboard-screenshot.png)

## Architecture
```
IoT Sensors → Kafka → Consumer → PostgreSQL → Streamlit Dashboard
```

## Features
- **5 Sensor Locations**: Downtown Office, Manufacturing Floor, Warehouse A, Server Room, Cafeteria
- **Real-time Data**: Temperature, humidity, air quality, pressure, light levels
- **Live Dashboard**: Auto-refreshing charts with anomaly detection
- **Scalable Processing**: Kafka streaming with batch database writes

## Quick Start

```bash
# Build and start all services
docker compose build
docker compose up -d

# Check status
docker compose ps
```

## Access Points
- **Dashboard**: http://localhost:8501
- **PostgreSQL**: localhost:5432 (postgres/postgres/streaming_db)
- **Kafka**: localhost:9092

## Project Structure
```
├── docker-compose.yml          # Service orchestration
├── src/
│   ├── producer/iot_producer.py    # Data generation
│   ├── consumer/iot_consumer.py    # Data processing
│   ├── database/init_db.py         # Database setup
│   └── dashboard/iot_dashboard.py  # Visualization
└── requirements.txt
```

## Key Commands
```bash
# View logs
docker compose logs -f iot-producer
docker compose logs -f iot-consumer

# Stop all services
docker compose down
```

## Bonus Extensions
- **Apache Flink**: Real-time aggregations and complex event processing
- **Machine Learning**: Anomaly detection and predictive modeling

---
