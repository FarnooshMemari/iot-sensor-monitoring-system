"""
Database initialization script for IoT sensor streaming system.
Creates tables for storing sensor readings and locations.
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_database_connection():
    """Create connection to PostgreSQL database"""
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'streaming_db')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def create_tables():
    """Create all necessary tables for the IoT sensor system"""
    
    engine = create_database_connection()
    
    # SQL for creating sensor locations table
    locations_sql = """
    CREATE TABLE IF NOT EXISTS sensor_locations (
        id SERIAL PRIMARY KEY,
        location_name VARCHAR(100) NOT NULL UNIQUE,
        latitude DECIMAL(10, 8),
        longitude DECIMAL(11, 8),
        building_type VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # SQL for creating sensor readings table
    readings_sql = """
    CREATE TABLE IF NOT EXISTS sensor_readings (
        id SERIAL PRIMARY KEY,
        sensor_id VARCHAR(50) NOT NULL,
        location_name VARCHAR(100) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        temperature DECIMAL(5, 2),
        humidity DECIMAL(5, 2),
        air_quality_index INTEGER,
        pressure DECIMAL(7, 2),
        light_level DECIMAL(8, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (location_name) REFERENCES sensor_locations(location_name)
    );
    """
    
    # SQL for creating indexes for better query performance
    indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_readings_timestamp ON sensor_readings(timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_readings_location ON sensor_readings(location_name);",
        "CREATE INDEX IF NOT EXISTS idx_readings_sensor_id ON sensor_readings(sensor_id);",
        "CREATE INDEX IF NOT EXISTS idx_readings_timestamp_location ON sensor_readings(timestamp, location_name);"
    ]
    
    try:
        with engine.connect() as connection:
            # Create tables
            logger.info("Creating sensor_locations table...")
            connection.execute(text(locations_sql))
            
            logger.info("Creating sensor_readings table...")
            connection.execute(text(readings_sql))
            
            # Create indexes
            logger.info("Creating indexes...")
            for index_sql in indexes_sql:
                connection.execute(text(index_sql))
            
            connection.commit()
            logger.info("All tables and indexes created successfully!")
            
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

def insert_sample_locations():
    """Insert sample sensor locations"""
    
    engine = create_database_connection()
    
    sample_locations = [
        ("Downtown Office", 40.7589, -73.9851, "office"),
        ("Manufacturing Floor", 40.7505, -73.9934, "industrial"),
        ("Warehouse A", 40.7549, -73.9840, "warehouse"),
        ("Server Room", 40.7614, -73.9776, "datacenter"),
        ("Cafeteria", 40.7580, -73.9855, "commercial"),
    ]
    
    insert_sql = """
    INSERT INTO sensor_locations (location_name, latitude, longitude, building_type)
    VALUES (:location_name, :latitude, :longitude, :building_type)
    ON CONFLICT (location_name) DO NOTHING;
    """
    
    try:
        with engine.connect() as connection:
            for location in sample_locations:
                connection.execute(text(insert_sql), {
                    'location_name': location[0],
                    'latitude': location[1],
                    'longitude': location[2],
                    'building_type': location[3]
                })
            
            connection.commit()
            logger.info(f"Inserted {len(sample_locations)} sample locations")
            
    except Exception as e:
        logger.error(f"Error inserting sample locations: {e}")
        raise

def main():
    """Main function to initialize database"""
    try:
        logger.info("Starting database initialization...")
        create_tables()
        insert_sample_locations()
        logger.info("Database initialization completed successfully!")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

if __name__ == "__main__":
    main()