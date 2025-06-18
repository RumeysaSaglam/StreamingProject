# app.py
from fastapi import FastAPI, Request
from threading import Thread
from datetime import datetime
import random
import time
import requests

app = FastAPI()

# Gelen verileri burada tutacağız
received_data = []

@app.post("/api/publish")
async def publish(request: Request):
    data = await request.json()
    print("Gelen veri:", data)
    received_data.append(data)
    return {"status": "ok"}

@app.get("/api/data")
def get_data():
    return received_data

def simulate_sensor_data():
    sensors = [
        {"id": "temp_1", "type": "temperature", "location": "Sivas"},
        {"id": "hum_1", "type": "humidity", "location": "Ankara"},
        {"id": "traf_1", "type": "traffic", "location": "Istanbul"},
        {"id": "air_1", "type": "air-quality", "location": "Yalova"},
    ]

    time.sleep(2)  # API ayağa kalksın

    while True:
        for sensor in sensors:
            value = random.uniform(20, 40)
            data = {
                "sensor_id": sensor["id"],
                "type": sensor["type"],
                "location": sensor["location"],
                "value": value,
                "timestamp": datetime.now().isoformat()
            }
            try:
                requests.post("http://localhost:8000/api/publish", json=data)
                print("Gönderildi:", data)
            except Exception as e:
                print("Gönderim hatası:", e)

        time.sleep(10)

@app.on_event("startup")
def start_simulation():
    Thread(target=simulate_sensor_data, daemon=True).start()
