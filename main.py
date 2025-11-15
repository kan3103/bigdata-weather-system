import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from prediction.preprocessing.spark import (
    WeatherKafkaService,
    build_spark_session,
)

try:
    import uvicorn
except ImportError:
    uvicorn = None


app = FastAPI(title="Weather Streaming API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "duLieuKhuVuc")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
STREAM_TRIGGER = os.getenv("WEATHER_STREAM_TRIGGER", "30 seconds")
STREAM_TABLE = os.getenv("WEATHER_STREAM_TABLE", "weather_events")

spark_session = build_spark_session(SPARK_MASTER_URL)
weather_service = WeatherKafkaService(
    spark_session,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    starting_offsets=KAFKA_STARTING_OFFSETS,
    enable_streaming=True,
    stream_table=STREAM_TABLE,
    stream_trigger=STREAM_TRIGGER,
)


@app.on_event("shutdown")
def shutdown() -> None:
    if weather_service:
        weather_service.stop()
    if spark_session:
        spark_session.stop()


@app.get("/")
def read_root() -> dict:
    return {
        "message": "Weather service is up",
        "spark_master": SPARK_MASTER_URL,
        "kafka_topic": KAFKA_TOPIC,
    }


@app.get("/weather")
def list_weather(limit: Optional[int] = Query(default=None, gt=0)) -> dict:
    records = weather_service.list_latest(limit=limit)
    return {"count": len(records), "results": records}


@app.get("/weather/{location_key}")
def get_weather(location_key: str) -> dict:
    record = weather_service.get_location(location_key)
    if not record:
        raise HTTPException(
            status_code=404,
            detail=f"No weather data available for location key '{location_key}'",
        )
    return record

@app.get("/weather/average_day/{location_key}/{date}")
def get_weather_average_day(location_key: str, date: str) -> dict:
    record = weather_service.get_weather_average_day(location_key, date)
    if not record:
        raise HTTPException(
            status_code=404,
            detail=f"No weather data available for location key '{location_key}' and date '{date}'",
        )
    return record

@app.get("/weather/days/{location_key}")
def get_weather_days(location_key: str) -> dict:
    record = weather_service.get_weather_days(location_key)
    if not record:
        raise HTTPException(
            status_code=404,
            detail=f"No weather data available for location key '{location_key}'",
        )
    return {"location": location_key, "days": record}

@app.get("/weather/recent_with_step/{location_key}")
def get_recent_weather_with_step(location_key: str, hours: int = 24, step: int = 1) -> dict:
    records = weather_service.get_recent_history_with_step(location_key, hours=hours, step=step)
    return {"count": len(records), "results": records}

if __name__ == "__main__":
    if uvicorn is None:
        raise RuntimeError(
            "uvicorn is required to run this module as a script. "
            "Install it with `pip install uvicorn`."
        )
    uvicorn.run(app, host="0.0.0.0", port=8000)