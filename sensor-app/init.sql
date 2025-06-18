-- Sensor Data Processing System Database Schema
-- This script initializes the PostgreSQL database with required tables

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Table for storing raw sensor data as received from Kafka
CREATE TABLE IF NOT EXISTS raw_data (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL,
    location VARCHAR(100) NOT NULL,
    value DECIMAL(10,2) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    partition_id INTEGER,
    offset_id BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Table for storing processed sensor data with analytics
CREATE TABLE IF NOT EXISTS processed_data (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL,
    location VARCHAR(100) NOT NULL,
    original_value DECIMAL(10,2) NOT NULL,
    processed_value DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'normal',
    raw_data_id INTEGER REFERENCES raw_data(id),
    daily_avg DECIMAL(10,2),
    hourly_count INTEGER DEFAULT 0,
    anomaly_score DECIMAL(5,3) DEFAULT 0.0,
    processing_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_raw_data_sensor_id ON raw_data(sensor_id);
CREATE INDEX IF NOT EXISTS idx_raw_data_timestamp ON raw_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_data_type ON raw_data(type);
CREATE INDEX IF NOT EXISTS idx_raw_data_location ON raw_data(location);

CREATE INDEX IF NOT EXISTS idx_processed_data_sensor_id ON processed_data(sensor_id);
CREATE INDEX IF NOT EXISTS idx_processed_data_timestamp ON processed_data(processing_timestamp);
CREATE INDEX IF NOT EXISTS idx_processed_data_type ON processed_data(type);
CREATE INDEX IF NOT EXISTS idx_processed_data_status ON processed_data(status);
CREATE INDEX IF NOT EXISTS idx_processed_data_location ON processed_data(location);

-- Sample data for testing (optional)
INSERT INTO raw_data (sensor_id, type, location, value, timestamp) VALUES
    ('temp_001', 'temperature', 'Istanbul', 25.5, NOW() - INTERVAL '1 hour'),
    ('hum_001', 'humidity', 'Ankara', 65.2, NOW() - INTERVAL '2 hours'),
    ('traffic_001', 'traffic', 'Izmir', 180.0, NOW() - INTERVAL '30 minutes')
ON CONFLICT DO NOTHING;

-- Create a view for easy analytics
CREATE OR REPLACE VIEW sensor_analytics AS
SELECT 
    p.sensor_id,
    p.type,
    p.location,
    COUNT(*) as total_readings,
    AVG(p.processed_value) as avg_value,
    MIN(p.processed_value) as min_value,
    MAX(p.processed_value) as max_value,
    COUNT(CASE WHEN p.status = 'critical' THEN 1 END) as critical_count,
    COUNT(CASE WHEN p.status = 'warning' THEN 1 END) as warning_count,
    MAX(p.processing_timestamp) as last_reading
FROM processed_data p
WHERE p.processing_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY p.sensor_id, p.type, p.location;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;

-- Print completion message
DO $$
BEGIN
    RAISE NOTICE 'Database schema initialized successfully!';
    RAISE NOTICE 'Tables created: raw_data, processed_data';
    RAISE NOTICE 'View created: sensor_analytics';
    RAISE NOTICE 'Indexes created for optimal performance';
END $$;