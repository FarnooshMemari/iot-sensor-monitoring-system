"""
IoT Sensor Data Consumer
Consumes sensor readings from Kafka and stores them in PostgreSQL
"""

import json
import os
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import threading
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IoTSensorConsumer:
    def __init__(self, kafka_servers=["localhost:9092"], topic_name="sensor_readings"):
        self.topic_name = topic_name
        self.kafka_servers = kafka_servers
        self.consumer = None
        self.engine = None
        self.running = False

        # Statistics tracking
        self.stats = {
            "total_processed": 0,
            "total_saved": 0,
            "total_errors": 0,
            "start_time": None,
            "last_processed": None,
        }

        # Batch processing
        self.batch_size = 100
        self.batch_timeout = 10  # seconds
        self.batch_data = []
        self.last_batch_time = time.time()

        self.connect_to_kafka()
        self.connect_to_database()

    def connect_to_kafka(self):
        """Establish connection to Kafka"""
        try:
            self.consumer = KafkaConsumer(
                self.topic_name,
                bootstrap_servers=self.kafka_servers,
                auto_offset_reset="latest",  # Start from latest messages
                enable_auto_commit=True,
                group_id="iot_sensor_consumer_group",
                value_deserializer=lambda m: (
                    json.loads(m.decode("utf-8")) if m else None
                ),
                consumer_timeout_ms=1000,  # 1 second timeout
                max_poll_records=500,
            )
            logger.info(
                f"Connected to Kafka topic '{self.topic_name}' on servers: {self.kafka_servers}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def connect_to_database(self):
        """Establish connection to PostgreSQL"""
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "streaming_db")
        db_user = os.getenv("DB_USER", "postgres")
        db_password = os.getenv("DB_PASSWORD", "postgres")

        connection_string = (
            f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )

        try:
            self.engine = create_engine(
                connection_string,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def validate_reading(self, reading):
        """Validate sensor reading data"""
        required_fields = [
            "sensor_id",
            "location_name",
            "timestamp",
            "temperature",
            "humidity",
            "air_quality_index",
            "pressure",
            "light_level",
        ]

        # Check required fields
        for field in required_fields:
            if field not in reading:
                logger.warning(f"Missing required field: {field}")
                return False

        # Validate data types and ranges
        try:
            # Temperature should be reasonable (-50 to 150 Celsius)
            temp = float(reading["temperature"])
            if not -50 <= temp <= 150:
                logger.warning(f"Temperature out of range: {temp}")
                return False

            # Humidity should be 0-100%
            humidity = float(reading["humidity"])
            if not 0 <= humidity <= 100:
                logger.warning(f"Humidity out of range: {humidity}")
                return False

            # AQI should be 0-500
            aqi = int(reading["air_quality_index"])
            if not 0 <= aqi <= 500:
                logger.warning(f"AQI out of range: {aqi}")
                return False

            # Parse timestamp
            datetime.fromisoformat(reading["timestamp"].replace("Z", "+00:00"))

        except (ValueError, TypeError) as e:
            logger.warning(f"Data validation error: {e}")
            return False

        return True

    def save_reading_to_db(self, reading):
        """Save a single sensor reading to the database"""
        insert_sql = """
        INSERT INTO sensor_readings 
        (sensor_id, location_name, timestamp, temperature, humidity, 
         air_quality_index, pressure, light_level)
        VALUES 
        (:sensor_id, :location_name, :timestamp, :temperature, :humidity,
         :air_quality_index, :pressure, :light_level)
        """

        try:
            with self.engine.connect() as connection:
                connection.execute(text(insert_sql), reading)
                connection.commit()
                return True
        except SQLAlchemyError as e:
            logger.error(f"Database error saving reading: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error saving reading: {e}")
            return False

    def save_batch_to_db(self, batch_data):
        """Save a batch of sensor readings to the database"""
        if not batch_data:
            return 0

        insert_sql = """
        INSERT INTO sensor_readings 
        (sensor_id, location_name, timestamp, temperature, humidity, 
         air_quality_index, pressure, light_level)
        VALUES 
        (:sensor_id, :location_name, :timestamp, :temperature, :humidity,
         :air_quality_index, :pressure, :light_level)
        """

        saved_count = 0
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(insert_sql), batch_data)
                connection.commit()
                saved_count = len(batch_data)
                logger.info(f"Saved batch of {saved_count} readings to database")

        except SQLAlchemyError as e:
            logger.error(f"Database error saving batch: {e}")
            # Try saving individually as fallback
            for reading in batch_data:
                if self.save_reading_to_db(reading):
                    saved_count += 1
        except Exception as e:
            logger.error(f"Unexpected error saving batch: {e}")

        return saved_count

    def process_batch(self):
        """Process accumulated batch data"""
        if not self.batch_data:
            return

        batch_to_process = self.batch_data.copy()
        self.batch_data.clear()
        self.last_batch_time = time.time()

        saved_count = self.save_batch_to_db(batch_to_process)

        self.stats["total_saved"] += saved_count
        self.stats["total_errors"] += len(batch_to_process) - saved_count

    def should_process_batch(self):
        """Check if batch should be processed"""
        return (
            len(self.batch_data) >= self.batch_size
            or time.time() - self.last_batch_time >= self.batch_timeout
        )

    def start_consuming(self):
        """Start consuming messages from Kafka"""
        logger.info("Starting to consume sensor readings...")
        self.running = True
        self.stats["start_time"] = time.time()

        try:
            while self.running:
                try:
                    # Poll for messages
                    message_batch = self.consumer.poll(timeout_ms=1000)

                    if message_batch:
                        for topic_partition, messages in message_batch.items():
                            for message in messages:
                                if not self.running:
                                    break

                                try:
                                    reading = message.value
                                    self.stats["total_processed"] += 1
                                    self.stats["last_processed"] = time.time()

                                    # Validate reading
                                    if not self.validate_reading(reading):
                                        self.stats["total_errors"] += 1
                                        continue

                                    # Add to batch
                                    self.batch_data.append(reading)

                                    # Process batch if conditions met
                                    if self.should_process_batch():
                                        self.process_batch()

                                except Exception as e:
                                    logger.error(f"Error processing message: {e}")
                                    self.stats["total_errors"] += 1

                    # Process remaining batch if timeout reached
                    if self.batch_data and self.should_process_batch():
                        self.process_batch()

                except KafkaError as e:
                    logger.error(f"Kafka error: {e}")
                    time.sleep(5)  # Wait before retrying
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    time.sleep(5)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        finally:
            # Process any remaining batch data
            if self.batch_data:
                self.process_batch()
            self.stop()

    def stop(self):
        """Stop consuming and close connections"""
        logger.info("Stopping consumer...")
        self.running = False

        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")

        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")

        self.print_stats()

    def print_stats(self):
        """Print consumption statistics"""
        if self.stats["start_time"]:
            duration = time.time() - self.stats["start_time"]
            rate = self.stats["total_processed"] / duration if duration > 0 else 0

            logger.info("=== Consumer Statistics ===")
            logger.info(f"Duration: {duration:.1f} seconds")
            logger.info(f"Total processed: {self.stats['total_processed']}")
            logger.info(f"Total saved: {self.stats['total_saved']}")
            logger.info(f"Total errors: {self.stats['total_errors']}")
            logger.info(f"Processing rate: {rate:.2f} messages/second")
            logger.info(
                f"Success rate: {(self.stats['total_saved']/max(1, self.stats['total_processed']))*100:.1f}%"
            )


def start_stats_thread(consumer):
    """Start a thread to periodically print statistics"""

    def print_periodic_stats():
        while consumer.running:
            time.sleep(30)  # Print stats every 30 seconds
            if consumer.stats["start_time"]:
                duration = time.time() - consumer.stats["start_time"]
                rate = (
                    consumer.stats["total_processed"] / duration if duration > 0 else 0
                )
                logger.info(
                    f"Processed: {consumer.stats['total_processed']}, "
                    f"Saved: {consumer.stats['total_saved']}, "
                    f"Rate: {rate:.1f}/sec"
                )

    stats_thread = threading.Thread(target=print_periodic_stats, daemon=True)
    stats_thread.start()
    return stats_thread


def main():
    """Main function to run the IoT sensor consumer"""
    import argparse

    parser = argparse.ArgumentParser(description="IoT Sensor Data Consumer")
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
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for database inserts (default: 100)",
    )

    args = parser.parse_args()

    # Parse Kafka servers
    kafka_servers = [server.strip() for server in args.kafka_servers.split(",")]

    try:
        # Create and start consumer
        consumer = IoTSensorConsumer(kafka_servers=kafka_servers, topic_name=args.topic)
        consumer.batch_size = args.batch_size

        # Start statistics thread
        stats_thread = start_stats_thread(consumer)

        # Start consuming
        consumer.start_consuming()

    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
