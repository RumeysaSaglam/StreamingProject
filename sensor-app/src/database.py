import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from .config import DATABASE_CONFIG

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.connection_config = DATABASE_CONFIG
        self._ensure_tables_exist()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_config)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def _ensure_tables_exist(self):
        """Create tables if they don't exist."""
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS raw_sensor_data (
            id SERIAL PRIMARY KEY,
            sensor_id VARCHAR(50) NOT NULL,
            sensor_type VARCHAR(50) NOT NULL,
            location VARCHAR(100) NOT NULL,
            value DECIMAL(10,2) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            is_critical BOOLEAN DEFAULT FALSE,
            partition_id INTEGER,
            offset_id BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS processed_sensor_data (
            id SERIAL PRIMARY KEY,
            raw_data_id INTEGER REFERENCES raw_sensor_data(id),
            sensor_id VARCHAR(50) NOT NULL,
            sensor_type VARCHAR(50) NOT NULL,
            location VARCHAR(100) NOT NULL,
            original_value DECIMAL(10,2) NOT NULL,
            processed_value DECIMAL(10,2) NOT NULL,
            status VARCHAR(20) NOT NULL,
            daily_avg DECIMAL(10,2),
            hourly_count INTEGER,
            anomaly_score DECIMAL(5,3),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_raw_sensor_id ON raw_sensor_data(sensor_id);
        CREATE INDEX IF NOT EXISTS idx_raw_timestamp ON raw_sensor_data(timestamp);
        CREATE INDEX IF NOT EXISTS idx_processed_sensor_id ON processed_sensor_data(sensor_id);
        CREATE INDEX IF NOT EXISTS idx_processed_timestamp ON processed_sensor_data(processed_at);
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_tables_sql)
                    conn.commit()
                    logger.info("Database tables ensured")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def insert_raw_data(self, sensor_data: Dict[str, Any], partition_id: int = None, offset_id: int = None) -> int:
        """Insert raw sensor data and return the ID."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO raw_sensor_data 
                        (sensor_id, sensor_type, location, value, timestamp, is_critical, partition_id, offset_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        sensor_data['sensor_id'],
                        sensor_data['type'],
                        sensor_data['location'],
                        sensor_data['value'],
                        sensor_data['timestamp'],
                        sensor_data.get('is_critical', False),
                        partition_id,
                        offset_id
                    ))
                    
                    raw_id = cursor.fetchone()[0]
                    conn.commit()
                    return raw_id
                    
        except Exception as e:
            logger.error(f"Error inserting raw data: {e}")
            raise
    
    def insert_processed_data(self, raw_data_id: int, sensor_data: Dict[str, Any], 
                            processed_result: Dict[str, Any]) -> int:
        """Insert processed sensor data and return the ID."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO processed_sensor_data 
                        (raw_data_id, sensor_id, sensor_type, location, original_value, 
                         processed_value, status, daily_avg, hourly_count, anomaly_score)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        raw_data_id,
                        sensor_data['sensor_id'],
                        sensor_data['type'],
                        sensor_data['location'],
                        sensor_data['value'],
                        processed_result['processed_value'],
                        processed_result['status'],
                        processed_result['daily_avg'],
                        processed_result['hourly_count'],
                        processed_result['anomaly_score']
                    ))
                    
                    processed_id = cursor.fetchone()[0]
                    conn.commit()
                    return processed_id
                    
        except Exception as e:
            logger.error(f"Error inserting processed data: {e}")
            raise
    
    def calculate_daily_average(self, sensor_id: str, sensor_type: str) -> Optional[float]:
        """Calculate daily average for a sensor."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT AVG(value) as daily_avg
                        FROM raw_sensor_data 
                        WHERE sensor_id = %s AND sensor_type = %s 
                        AND timestamp >= CURRENT_DATE
                    """, (sensor_id, sensor_type))
                    
                    result = cursor.fetchone()
                    return float(result[0]) if result[0] else None
                    
        except Exception as e:
            logger.error(f"Error calculating daily average: {e}")
            return None
    
    def calculate_hourly_count(self, sensor_id: str, sensor_type: str) -> int:
        """Calculate hourly data count for a sensor."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT COUNT(*) as hourly_count
                        FROM raw_sensor_data 
                        WHERE sensor_id = %s AND sensor_type = %s 
                        AND timestamp >= NOW() - INTERVAL '1 hour'
                    """, (sensor_id, sensor_type))
                    
                    result = cursor.fetchone()
                    return int(result[0]) if result[0] else 0
                    
        except Exception as e:
            logger.error(f"Error calculating hourly count: {e}")
            return 0
    
    def get_analysis_data(self) -> Dict[str, Any]:
        """Get comprehensive analysis data."""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Total and active sensors
                    cursor.execute("""
                        SELECT 
                            COUNT(DISTINCT sensor_id) as total_sensors,
                            COUNT(DISTINCT CASE WHEN timestamp >= NOW() - INTERVAL '1 hour' 
                                  THEN sensor_id END) as active_sensors
                        FROM raw_sensor_data
                    """)
                    sensor_counts = cursor.fetchone()
                    
                    # Critical alerts count
                    cursor.execute("""
                        SELECT COUNT(*) as critical_count
                        FROM raw_sensor_data 
                        WHERE is_critical = true AND timestamp >= NOW() - INTERVAL '24 hours'
                    """)
                    critical_count = cursor.fetchone()['critical_count']
                    
                    # Sensor types summary
                    cursor.execute("""
                        SELECT sensor_type, COUNT(*) as count, AVG(value) as avg_value
                        FROM raw_sensor_data 
                        WHERE timestamp >= NOW() - INTERVAL '24 hours'
                        GROUP BY sensor_type
                    """)
                    sensor_types = cursor.fetchall()
                    
                    # Location summary
                    cursor.execute("""
                        SELECT location, COUNT(*) as count
                        FROM raw_sensor_data 
                        WHERE timestamp >= NOW() - INTERVAL '24 hours'
                        GROUP BY location
                    """)
                    locations = cursor.fetchall()
                    
                    # Recent data points
                    cursor.execute("""
                        SELECT COUNT(*) as recent_count
                        FROM raw_sensor_data 
                        WHERE timestamp >= NOW() - INTERVAL '1 hour'
                    """)
                    recent_count = cursor.fetchone()['recent_count']
                    
                    # Alerts by location
                    cursor.execute("""
                        SELECT location, COUNT(*) as alert_count
                        FROM raw_sensor_data 
                        WHERE is_critical = true AND timestamp >= NOW() - INTERVAL '24 hours'
                        GROUP BY location
                    """)
                    alerts_by_location = cursor.fetchall()
                    
                    return {
                        'total_sensors': sensor_counts['total_sensors'],
                        'active_sensors': sensor_counts['active_sensors'],
                        'critical_alerts_count': critical_count,
                        'sensor_types_summary': {row['sensor_type']: {
                            'count': row['count'],
                            'avg_value': float(row['avg_value']) if row['avg_value'] else 0
                        } for row in sensor_types},
                        'location_summary': {row['location']: row['count'] for row in locations},
                        'recent_data_points': recent_count,
                        'average_values_by_type': {row['sensor_type']: float(row['avg_value']) 
                                                 if row['avg_value'] else 0 for row in sensor_types},
                        'alerts_by_location': {row['location']: row['alert_count'] 
                                             for row in alerts_by_location}
                    }
                    
        except Exception as e:
            logger.error(f"Error getting analysis data: {e}")
            raise
    
    def get_sensor_history(self, sensor_id: str, hours: int = 24) -> List[Dict[str, Any]]:
        """Get historical data for a specific sensor."""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT sensor_id, sensor_type, location, value, timestamp, is_critical
                        FROM raw_sensor_data 
                        WHERE sensor_id = %s AND timestamp >= NOW() - INTERVAL '%s hours'
                        ORDER BY timestamp DESC
                        LIMIT 1000
                    """, (sensor_id, hours))
                    
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
                    
        except Exception as e:
            logger.error(f"Error getting sensor history: {e}")
            return []