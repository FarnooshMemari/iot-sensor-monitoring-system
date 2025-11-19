"""
IoT Sensor Data Producer
Generates synthetic sensor readings and streams them to Kafka
"""

import json
import time
import random
import uuid
from datetime import datetime, timezone
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IoTSensorProducer:
    def __init__(self, kafka_servers=["localhost:9092"], topic_name="sensor_readings"):
        self.topic_name = topic_name
        self.producer = None
        self.kafka_servers = kafka_servers

        # Sensor locations and their typical ranges
        self.locations = {
            "Downtown Office": {
                "temp_range": (68, 76),
                "humidity_range": (30, 60),
                "aqi_range": (25, 75),
                "pressure_range": (1010, 1025),
                "light_range": (300, 800),
            },
            "Manufacturing Floor": {
                "temp_range": (72, 85),
                "humidity_range": (40, 80),
                "aqi_range": (35, 95),
                "pressure_range": (1008, 1022),
                "light_range": (800, 1200),
            },
            "Warehouse A": {
                "temp_range": (60, 80),
                "humidity_range": (35, 75),
                "aqi_range": (20, 60),
                "pressure_range": (1012, 1028),
                "light_range": (200, 600),
            },
            "Server Room": {
                "temp_range": (65, 72),
                "humidity_range": (20, 45),
                "aqi_range": (15, 35),
                "pressure_range": (1015, 1025),
                "light_range": (100, 300),
            },
            "Cafeteria": {
                "temp_range": (70, 78),
                "humidity_range": (45, 65),
                "aqi_range": (30, 70),
                "pressure_range": (1010, 1023),
                "light_range": (400, 900),
            },
        }

        self.connect_to_kafka()

    def connect_to_kafka(self):
        """Establish connection to Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                retry_backoff_ms=1000,
            )
            logger.info(f"Connected to Kafka servers: {self.kafka_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def generate_sensor_reading(self, location_name, sensor_id):
        """Generate a single sensor reading for a given location"""
        location_config = self.locations[location_name]

        # Add some realistic variation with occasional spikes
        temp_base = random.uniform(*location_config["temp_range"])

        # Simulate realistic sensor behavior
        reading = {
            "sensor_id": sensor_id,
            "location_name": location_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": round(temp_base + random.gauss(0, 1), 2),
            "humidity": round(
                random.uniform(*location_config["humidity_range"]) + random.gauss(0, 2),
                2,
            ),
            "air_quality_index": max(
                0,
                min(
                    500,
                    int(
                        random.uniform(*location_config["aqi_range"])
                        + random.gauss(0, 5)
                    ),
                ),
            ),
            "pressure": round(
                random.uniform(*location_config["pressure_range"])
                + random.gauss(0, 0.5),
                2,
            ),
            "light_level": round(
                random.uniform(*location_config["light_range"]) + random.gauss(0, 10), 2
            ),
        }

        # Simulate occasional anomalies (5% chance)
        if random.random() < 0.05:
            anomaly_type = random.choice(["temp_spike", "humidity_spike", "aqi_spike"])
            if anomaly_type == "temp_spike":
                reading["temperature"] += random.uniform(5, 15)
            elif anomaly_type == "humidity_spike":
                reading["humidity"] += random.uniform(10, 20)
            elif anomaly_type == "aqi_spike":
                reading["air_quality_index"] += random.randint(50, 100)

        return reading

    def send_reading(self, reading):
        """Send a single reading to Kafka"""
        try:
            future = self.producer.send(
                self.topic_name,
                key=f"{reading['location_name']}_{reading['sensor_id']}",
                value=reading,
            )

            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            logger.debug(
                f"Sent reading for {reading['location_name']} - "
                f"Topic: {record_metadata.topic}, "
                f"Partition: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            logger.error(f"Failed to send reading: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending reading: {e}")
            return False

    def simulate_sensors(self, duration_minutes=60, readings_per_minute=10):
        """
        Simulate multiple sensors sending data

        Args:
            duration_minutes: How long to run the simulation
            readings_per_minute: Number of readings per minute across all sensors
        """
        logger.info(f"Starting sensor simulation for {duration_minutes} minutes")
        logger.info(f"Target rate: {readings_per_minute} readings per minute")

        # Create sensor IDs for each location
        sensors = {}
        for location in self.locations.keys():
            sensors[location] = [
                f"sensor_{location.replace(' ', '_').lower()}_{i:02d}"
                for i in range(1, 4)
            ]  # 3 sensors per location

        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        readings_sent = 0

        try:
            while time.time() < end_time:
                cycle_start = time.time()

                # Send readings for this cycle
                for _ in range(readings_per_minute):
                    # Randomly select location and sensor
                    location = random.choice(list(self.locations.keys()))
                    sensor_id = random.choice(sensors[location])

                    # Generate and send reading
                    reading = self.generate_sensor_reading(location, sensor_id)
                    if self.send_reading(reading):
                        readings_sent += 1

                        if readings_sent % 50 == 0:
                            logger.info(f"Sent {readings_sent} readings...")

                # Wait for next cycle (ensure we don't exceed target rate)
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0, 60 - cycle_duration)  # 60 seconds per cycle

                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user")
        except Exception as e:
            logger.error(f"Error during simulation: {e}")
        finally:
            logger.info(f"Simulation completed. Total readings sent: {readings_sent}")
            self.close()

    def close(self):
        """Close the Kafka producer connection"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer connection closed")


def main():
    """Main function to run the IoT sensor producer"""
    import argparse

    parser = argparse.ArgumentParser(description="IoT Sensor Data Producer")
    parser.add_argument(
        "--kafka-servers",
        default="localhost:9092",
        help="Comma-separated list of Kafka servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="sensor_readings",
        help="Kafka topic name (default: sensor_readings)",
    )
    parser.add_argument(
        "--duration", type=int, default=60, help="Duration in minutes (default: 60)"
    )
    parser.add_argument(
        "--rate", type=int, default=10, help="Readings per minute (default: 10)"
    )

    args = parser.parse_args()

    # Parse Kafka servers
    kafka_servers = [server.strip() for server in args.kafka_servers.split(",")]

    try:
        # Create and start producer
        producer = IoTSensorProducer(kafka_servers=kafka_servers, topic_name=args.topic)
        producer.simulate_sensors(
            duration_minutes=args.duration, readings_per_minute=args.rate
        )

    except Exception as e:
        logger.error(f"Producer failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
