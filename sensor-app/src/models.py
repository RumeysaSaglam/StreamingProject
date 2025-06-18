from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum

class SensorType(str, Enum):
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    TRAFFIC = "traffic"
    AIR_QUALITY = "air-quality"

class SensorData(BaseModel):
    sensor_id: str = Field(..., description="Unique identifier for the sensor")
    type: SensorType = Field(..., description="Type of sensor")
    location: str = Field(..., description="Physical location of the sensor")
    value: float = Field(..., description="Sensor reading value")
    timestamp: str = Field(..., description="ISO format timestamp")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class CriticalAlert(BaseModel):
    sensor_id: str
    type: SensorType
    location: str
    value: float
    threshold: float
    timestamp: str
    severity: str = "critical"

class NotificationStructure(BaseModel):
    alert_id: str
    sensor_id: str
    message: str
    priority: str
    timestamp: str
    location: str
    value: float
    threshold: float

class EmailStructure(BaseModel):
    to: str = "admin@company.com"
    subject: str
    body: str
    alert_data: CriticalAlert
    timestamp: str

class AnalysisResponse(BaseModel):
    total_sensors: int
    active_sensors: int
    critical_alerts_count: int
    sensor_types_summary: Dict[str, Any]
    location_summary: Dict[str, Any]
    recent_data_points: int
    average_values_by_type: Dict[str, float]
    alerts_by_location: Dict[str, int]

class ProcessedDataSummary(BaseModel):
    sensor_id: str
    type: str
    location: str
    latest_value: float
    daily_avg: Optional[float] = None
    hourly_count: Optional[int] = None
    status: str
    last_updated: datetime
    anomaly_score: Optional[float] = None