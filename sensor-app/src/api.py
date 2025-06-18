from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import logging
from datetime import datetime
import uuid
from typing import Dict, Any

from models import (
    SensorData, CriticalAlert, NotificationStructure, 
    EmailStructure, AnalysisResponse
)
from kafka_producer import get_producer
from database import DatabaseManager
from config import is_critical_value, SENSOR_CONFIGS


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Kafka Sensor Data Processing System",
    description="Real-time sensor data processing with Kafka and PostgreSQL",
    version="1.0.0"
)

# Initialize database manager
db_manager = DatabaseManager()

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    logger.info("Starting Kafka Sensor Data Processing API...")
    try:
        # Test database connection
        with db_manager.get_connection() as conn:
            logger.info("Database connection established successfully")
        
        # Test Kafka producer
        producer = get_producer()
        logger.info("Kafka producer initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("Shutting down API...")
    try:
        producer = get_producer()
        producer.close()
        logger.info("Services shut down successfully")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Kafka Sensor Data Processing System",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "POST /publish": "Send sensor data to Kafka topics",
            "POST /notify": "Prepare notification structure for critical alerts",
            "POST /email": "Prepare email structure for critical alerts",
            "GET /analysis": "Get analytical insights from processed data",
            "GET /health": "Check system health"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Check database connection
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                db_status = "healthy"
        
        # Check Kafka producer
        producer = get_producer()
        kafka_status = "healthy" if producer.producer else "unhealthy"
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "database": db_status,
                "kafka": kafka_status
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")

@app.post("/publish")
async def publish_sensor_data(sensor_data: SensorData):
    """
    Receive sensor data and route it to appropriate Kafka topics.
    
    - If data is within normal range: routes to 'sensor-data' topic
    - If data is critical: routes to 'sensor-alerts' topic
    """
    try:
        logger.info(f"Received sensor data: {sensor_data.sensor_id} = {sensor_data.value}")
        
        # Convert Pydantic model to dictionary
        data_dict = sensor_data.dict()
        
        # Send to Kafka
        producer = get_producer()
        success = producer.send_sensor_data(data_dict)
        
        if success:
            # Determine if critical
            is_critical = is_critical_value(sensor_data.type.value, sensor_data.value)
            status = "critical" if is_critical else "normal"
            
            return {
                "status": "success",
                "message": f"Sensor data published successfully",
                "sensor_id": sensor_data.sensor_id,
                "value": sensor_data.value,
                "alert_level": status,
                "timestamp": sensor_data.timestamp
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to publish data to Kafka")
            
    except Exception as e:
        logger.error(f"Error publishing sensor data: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/notify")
async def create_notification(alert_data: dict):
    """
    Process critical alert data and create notification structure.
    This endpoint is called by alert consumers.
    """
    try:
        logger.info(f"Creating notification for critical alert: {alert_data.get('sensor_id')}")
        
        # Create notification structure
        notification = NotificationStructure(
            alert_id=str(uuid.uuid4()),
            sensor_id=alert_data['sensor_id'],
            message=f"CRITICAL ALERT: {alert_data['type']} sensor {alert_data['sensor_id']} "
                   f"in {alert_data['location']} reported value {alert_data['value']}",
            priority="HIGH",
            timestamp=datetime.now().isoformat(),
            location=alert_data['location'],
            value=alert_data['value'],
            threshold=_get_threshold_for_sensor(alert_data['type'], alert_data['value'])
        )
        
        # Here you would typically send this to a notification service
        # For now, we'll just log it
        logger.warning(f"🚨 NOTIFICATION CREATED: {notification.message}")
        
        return {
            "status": "success",
            "notification": notification.dict(),
            "message": "Notification structure created successfully"
        }
        
    except Exception as e:
        logger.error(f"Error creating notification: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create notification: {str(e)}")

@app.post("/email")
async def create_email(alert_data: dict):
    """
    Process critical alert data and create email structure.
    This endpoint is called by alert consumers.
    """
    try:
        logger.info(f"Creating email for critical alert: {alert_data.get('sensor_id')}")
        
        # Create alert object
        alert = CriticalAlert(
            sensor_id=alert_data['sensor_id'],
            type=alert_data['type'],
            location=alert_data['location'],
            value=alert_data['value'],
            threshold=_get_threshold_for_sensor(alert_data['type'], alert_data['value']),
            timestamp=alert_data.get('timestamp', datetime.now().isoformat()),
            severity="critical"
        )
        
        # Create email structure
        email = EmailStructure(
            to="admin@company.com",
            subject=f"CRITICAL SENSOR ALERT - {alert_data['type'].upper()} in {alert_data['location']}",
            body=f"""
CRITICAL SENSOR ALERT

Sensor ID: {alert_data['sensor_id']}
Type: {alert_data['type']}
Location: {alert_data['location']}
Current Value: {alert_data['value']}
Threshold: {_get_threshold_for_sensor(alert_data['type'], alert_data['value'])}
Timestamp: {alert_data.get('timestamp', datetime.now().isoformat())}

This sensor has exceeded critical thresholds and requires immediate attention.

System: Kafka Sensor Monitoring
            """.strip(),
            alert_data=alert,
            timestamp=datetime.now().isoformat()
        )
        
        # Here you would typically send this email via SMTP
        # For now, we'll just log it
        logger.warning(f"📧 EMAIL PREPARED: {email.subject}")
        
        return {
            "status": "success",
            "email": email.dict(),
            "message": "Email structure created successfully"
        }
        
    except Exception as e:
        logger.error(f"Error creating email: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create email: {str(e)}")

@app.get("/analysis", response_model=AnalysisResponse)
async def get_analysis():
    """
    Get comprehensive analytical insights from processed sensor data.
    """
    try:
        logger.info("Generating analysis report...")
        
        # Get analysis data from database
        analysis_data = db_manager.get_analysis_data()
        
        return AnalysisResponse(**analysis_data)
        
    except Exception as e:
        logger.error(f"Error generating analysis: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to generate analysis: {str(e)}")

@app.get("/sensors/{sensor_id}/history")
async def get_sensor_history(sensor_id: str, hours: int = 24):
    """Get historical data for a specific sensor."""
    try:
        history = db_manager.get_sensor_history(sensor_id, hours)
        
        return {
            "sensor_id": sensor_id,
            "hours": hours,
            "data_points": len(history),
            "history": history
        }
        
    except Exception as e:
        logger.error(f"Error getting sensor history: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get sensor history: {str(e)}")

def _get_threshold_for_sensor(sensor_type: str, value: float) -> float:
    """Helper function to get threshold value for a sensor type."""
    config_key = sensor_type.replace('-', '_')
    config = SENSOR_CONFIGS.get(config_key, {})
    
    if sensor_type == 'humidity':
        # For humidity, return the appropriate threshold based on value
        if value < config.get('critical_threshold_low', 20):
            return config.get('critical_threshold_low', 20)
        else:
            return config.get('critical_threshold_high', 75)
    else:
        return config.get('critical_threshold', 0)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)