import argparse
import math
import os
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import torch
import torch.nn as nn
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQuery

# Model definition (same as in train.py)
class WeatherForecastModel(nn.Module):
    def __init__(
        self,
        input_dim: int,
        hidden_dim: int,
        num_layers: int,
        num_weather_codes: int,
        dropout: float,
    ) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_dim,
            hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.dropout = nn.Dropout(dropout)
        self.weather_head = nn.Linear(hidden_dim, num_weather_codes)
        self.temperature_head = nn.Linear(hidden_dim, 1)

    def forward(self, inputs: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        lstm_out, _ = self.lstm(inputs)
        last_state = lstm_out[:, -1, :]
        last_state = self.dropout(last_state)
        weather_logits = self.weather_head(last_state)
        temperature_pred = self.temperature_head(last_state).squeeze(-1)
        return weather_logits, temperature_pred


def _time_of_day_features(timestamp_str: Optional[str]) -> Tuple[float, float, float, float]:
    """Extract time of day features from timestamp string."""
    if not timestamp_str:
        return 0.0, 1.0, 0.0, 1.0
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except ValueError:
        try:
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return 0.0, 1.0, 0.0, 1.0

    seconds_since_midnight = (
        dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1_000_000
    )
    hour_angle = 2 * math.pi * (seconds_since_midnight / 86400.0)
    minute_angle = 2 * math.pi * (dt.minute / 60.0)

    return (
        math.sin(hour_angle),
        math.cos(hour_angle),
        math.sin(minute_angle),
        math.cos(minute_angle),
    )


# Load model with metadata
_model = None
_model_metadata = None

def _load_model():
    """Lazy load model and metadata."""
    global _model, _model_metadata
    if _model is not None:
        return _model, _model_metadata
    
    model_path = "prediction/models/weather_lstm.pt"
    if not os.path.exists(model_path):
        # Try alternative path
        model_path = os.path.join(os.path.dirname(__file__), "..", "models", "weather_lstm.pt")
    
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found: {model_path}")
    
    checkpoint = torch.load(model_path, map_location="cpu")
    _model_metadata = {
        "feature_columns": checkpoint.get("feature_columns", _SEQUENCE_FEATURE_COLUMNS),
        "weathercode_to_idx": checkpoint.get("weathercode_to_idx", {}),
    }
    
    # Build idx_to_weathercode mapping
    idx_to_weathercode = {idx: code for code, idx in _model_metadata["weathercode_to_idx"].items()}
    _model_metadata["idx_to_weathercode"] = idx_to_weathercode
    
    # Determine input_dim from feature_columns
    base_features = _model_metadata["feature_columns"]
    # Remove time features if present
    time_features = ["hour_sin", "hour_cos", "minute_sin", "minute_cos"]
    base_features = [f for f in base_features if f not in time_features]
    input_dim = len(base_features) + len(time_features)  # base + time features
    
    # Default model parameters (should match training)
    num_weather_codes = len(_model_metadata["weathercode_to_idx"]) or 10
    
    _model = WeatherForecastModel(
        input_dim=input_dim,
        hidden_dim=128,
        num_layers=2,
        num_weather_codes=num_weather_codes,
        dropout=0.2,
    )
    _model.load_state_dict(checkpoint["model_state_dict"])
    _model.eval()
    
    return _model, _model_metadata

_KAFKA_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0-preview3,"
    "org.apache.kafka:kafka-clients:3.7.0"
)
_DEFAULT_IVY_PATH = "/opt/ivy-cache"
_DEFAULT_STREAM_TABLE = "weather_events"
_DEFAULT_STREAM_TRIGGER = "30 seconds"
_PAYLOAD_SCHEMA = T.MapType(T.StringType(), T.StringType())
_SEQUENCE_FEATURE_COLUMNS = [
    "temperature",
    "windspeed",
    "winddirection",
    "humidity",
    "rain",
    "visibility",
    "pressure",
    "precipitation",
    "weathercode",
    "is_day",
    "interval",
]


def build_spark_session(
    master_url: str,
    ivy_path: str = _DEFAULT_IVY_PATH,
) -> SparkSession:
    builder = (
        SparkSession.builder.appName("WeatherKafkaStreaming")
        .master(master_url)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
    )

    builder = builder.config("spark.jars.packages", _KAFKA_PACKAGES)
    builder = builder.config("spark.jars.ivy", ivy_path)

    return builder.getOrCreate()


def _read_kafka_snapshot(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "earliest",
) -> DataFrame:
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )


def _parse_kafka_dataframe(kafka_df: DataFrame) -> DataFrame:
    parsed_df = (
        kafka_df.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value_str"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("payload", F.from_json("value_str", _PAYLOAD_SCHEMA))
        .drop("value_str")
    )

    enriched_df = parsed_df.select(
        F.col("key"),
        F.coalesce(F.col("payload").getItem("location_name"), F.col("key")).alias(
            "location"
        ),
        F.col("payload"),
        F.coalesce(
            F.col("payload").getItem("time"),
            F.col("payload").getItem("timestamp"),
        ).alias("event_time_raw"),
        F.col("kafka_timestamp"),
    )

    enriched_df = enriched_df.withColumn(
        "event_timestamp",
        F.coalesce(
            F.to_timestamp("event_time_raw"),
            F.to_timestamp(
                F.regexp_replace("event_time_raw", "[TZ]", " "),
                "yyyy-MM-dd HH:mm:ss",
            ),
            F.to_timestamp(
                F.regexp_replace("event_time_raw", "[TZ]", " "),
                "yyyy-MM-dd HH:mm",
            ),
            F.col("kafka_timestamp"),
        ),
    ).drop("event_time_raw")

    enriched_df = (
        enriched_df.withColumn(
            "temperature", F.col("payload").getItem("temperature").cast("double")
        )
        .withColumn(
            "windspeed", F.col("payload").getItem("windspeed").cast("double")
        )
        .withColumn(
            "winddirection", F.col("payload").getItem("winddirection").cast("double")
        )
        .withColumn("humidity", F.col("payload").getItem("humidity").cast("double"))
        .withColumn("rain", F.col("payload").getItem("rain").cast("double"))
        .withColumn("visibility", F.col("payload").getItem("visibility").cast("double"))
        .withColumn("pressure", F.col("payload").getItem("pressure").cast("double"))
        .withColumn(
            "precipitation", F.col("payload").getItem("precipitation").cast("double")
        )
        .withColumn("weathercode", F.col("payload").getItem("weathercode").cast("int"))
        .withColumn("interval", F.col("payload").getItem("interval").cast("int"))
        .withColumn(
            "is_day",
            F.col("payload")
            .getItem("is_day")
            .cast("int"),
        )
        .withColumn("latitude", F.col("payload").getItem("latitude").cast("double"))
        .withColumn("longitude", F.col("payload").getItem("longitude").cast("double"))
        .withColumn("error_message", F.col("payload").getItem("message"))
    )

    enriched_df = enriched_df.filter(
        F.coalesce(F.length(F.trim(F.col("error_message"))), F.lit(0)) == 0
    ).drop("error_message")

    return enriched_df


def _prepare_latest_dataframe(kafka_df: DataFrame) -> DataFrame:
    parsed_df = _parse_kafka_dataframe(kafka_df)

    window_spec = Window.partitionBy("location").orderBy(
        F.col("event_timestamp").desc(), F.col("kafka_timestamp").desc()
    )

    return (
        parsed_df.withColumn("row_number", F.row_number().over(window_spec))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def start_weather_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    *,
    table_name: str = _DEFAULT_STREAM_TABLE,
    trigger: str = _DEFAULT_STREAM_TRIGGER,
    starting_offsets: str = "earliest",
) -> StreamingQuery:
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    structured_stream = _parse_kafka_dataframe(kafka_stream)

    query = (
        structured_stream.writeStream.outputMode("append")
        .format("memory")
        .queryName(table_name)
        .trigger(processingTime=trigger)
        .start()
    )

    return query


def _coerce_value(value: Optional[str]) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        lowered = text.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        try:
            if "." in text:
                number = float(text)
                return int(number) if number.is_integer() else number
            return int(text)
        except ValueError:
            return text
    return value


def _normalise_payload(payload: Optional[Dict[str, str]]) -> Dict[str, Any]:
    if not payload:
        return {}
    return {
        key: _coerce_value(value)
        for key, value in payload.items()
        if key
        and key.lower() not in {"message", "error", "error_message"}
    }


def _drop_none(data: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallow copy of `data` without keys that have None values."""
    return {key: value for key, value in data.items() if value is not None}


def _row_to_record(row) -> Dict[str, Any]:
    payload = _normalise_payload(row.payload)
    payload.pop("message", None)
    location_name = payload.pop("location_name", None)
    reported_time = payload.pop("time", None)

    record = {
        "key": row.key,
        "location": location_name or row.location,
        "event_timestamp": (
            row.event_timestamp.isoformat() if row.event_timestamp else None
        ),
        "kafka_timestamp": (
            row.kafka_timestamp.isoformat() if row.kafka_timestamp else None
        ),
        "reported_time": reported_time,
        "details": payload,
    }

    return record


def _row_to_sequence_entry(row) -> Dict[str, Any]:
    def _as_float(value: Optional[Any]) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    entry: Dict[str, Any] = {
        "event_timestamp": row.event_timestamp.isoformat()
        if getattr(row, "event_timestamp", None)
        else None
    }
    for feature in _SEQUENCE_FEATURE_COLUMNS:
        value = getattr(row, feature, None)
        if feature == "is_day" and value is not None:
            entry[feature] = int(value)
        else:
            entry[feature] = _as_float(value)
    return entry


def load_latest_weather_dataframe(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "earliest",
) -> DataFrame:
    kafka_df = _read_kafka_snapshot(
        spark, bootstrap_servers, topic, starting_offsets=starting_offsets
    )
    return _prepare_latest_dataframe(kafka_df)


def collect_latest_records(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "earliest",
) -> List[Dict[str, Any]]:
    df = load_latest_weather_dataframe(
        spark, bootstrap_servers, topic, starting_offsets=starting_offsets
    )
    rows = df.collect()
    return [_row_to_record(row) for row in rows]


def get_latest_record_for_location(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    location_key: str,
    starting_offsets: str = "earliest",
) -> Optional[Dict[str, Any]]:
    records = collect_latest_records(
        spark, bootstrap_servers, topic, starting_offsets=starting_offsets
    )
    key_lower = location_key.lower()
    for record in records:
        location_lower = record["location"].lower() if record["location"] else ""
        key_name_lower = record["key"].lower() if record["key"] else ""
        if key_lower in {location_lower, key_name_lower}:
            return record
    return None


class WeatherKafkaService:
    def __init__(
        self,
        spark: SparkSession,
        bootstrap_servers: str,
        topic: str,
        *,
        starting_offsets: str = "earliest",
        enable_streaming: bool = True,
        stream_table: str = _DEFAULT_STREAM_TABLE,
        stream_trigger: str = _DEFAULT_STREAM_TRIGGER,
    ) -> None:
        self.spark = spark
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.starting_offsets = starting_offsets
        self.enable_streaming = enable_streaming
        self.stream_table = stream_table
        self.stream_trigger = stream_trigger
        self.streaming_query: Optional[StreamingQuery] = None

        if self.enable_streaming:
            self.streaming_query = start_weather_stream(
                spark,
                bootstrap_servers,
                topic,
                table_name=stream_table,
                trigger=stream_trigger,
                starting_offsets=starting_offsets,
            )

    def stop(self) -> None:
        if self.streaming_query is not None:
            self.streaming_query.stop()
            self.streaming_query = None

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _stream_df(self) -> Optional[DataFrame]:
        if not self.enable_streaming:
            return None
        try:
            return self.spark.table(self.stream_table)
        except AnalysisException:
            return None

    def _filter_stream_by_location(
        self, df: DataFrame, location_key: str
    ) -> DataFrame:
        key_lower = location_key.lower()
        return df.filter(
            F.lower(F.col("location")).eqNullSafe(key_lower)
            | F.lower(F.col("key")).eqNullSafe(key_lower)
        )

    def _latest_rows_from_stream(
        self, df: DataFrame, limit: Optional[int]
    ) -> List[Dict[str, Any]]:
        window_spec = Window.partitionBy("location").orderBy(
            F.col("event_timestamp").desc(), F.col("kafka_timestamp").desc()
        )
        latest_df = df.withColumn("row_number", F.row_number().over(window_spec)).filter(
            F.col("row_number") == 1
        ).drop("row_number")

        if limit:
            latest_df = latest_df.limit(limit)

        return [_row_to_record(row) for row in latest_df.collect()]

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def list_latest(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        stream_df = self._stream_df()
        if stream_df is not None:
            return self._latest_rows_from_stream(stream_df, limit)

        df = load_latest_weather_dataframe(
            self.spark,
            self.bootstrap_servers,
            self.topic,
            starting_offsets=self.starting_offsets,
        )
        if limit:
            df = df.limit(limit)
        return [_row_to_record(row) for row in df.collect()]

    def get_location(self, location_key: str) -> Optional[Dict[str, Any]]:
        stream_df = self._stream_df()
        if stream_df is not None:
            filtered_df = self._filter_stream_by_location(stream_df, location_key)
            rows = (
                filtered_df.orderBy(F.col("event_timestamp").desc())
                .limit(1)
                .collect()
            )
            if rows:
                return _row_to_record(rows[0])
            return None

        return get_latest_record_for_location(
            self.spark,
            self.bootstrap_servers,
            self.topic,
            location_key,
            starting_offsets=self.starting_offsets,
        )

    def get_recent_history(
        self,
        location_key: str,
        hours: int = 24,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        stream_df = self._stream_df()
        if stream_df is not None:
            filtered_df = self._filter_stream_by_location(stream_df, location_key)
            if filtered_df.take(1):
                latest_timestamp = filtered_df.agg(
                    F.max("event_timestamp").alias("latest_timestamp")
                ).collect()[0]["latest_timestamp"]

                if latest_timestamp:
                    window_start = latest_timestamp - timedelta(hours=hours)
                    recent_df = filtered_df.filter(
                        F.col("event_timestamp") >= F.lit(window_start)
                    ).orderBy(F.col("event_timestamp").desc())
                    if limit:
                        recent_df = recent_df.limit(limit)
                    return [_row_to_record(row) for row in recent_df.collect()]
            return []

        # Fallback to batch processing
        fallback_records = self.list_latest()
        matching = [
            record
            for record in fallback_records
            if record["location"] and record["location"].lower() == location_key.lower()
        ]
        return matching[: limit or len(matching)]

    def get_recent_history_with_step(
        self,
        location_key: str,
        hours: int = 24,
        step: int = 1,
    ) -> List[Dict[str, Any]]:
        stream_df = self._stream_df()
        if stream_df is not None:
            filtered_df = self._filter_stream_by_location(stream_df, location_key)
            if filtered_df.take(1):
                latest_timestamp = filtered_df.agg(
                    F.max("event_timestamp").alias("latest_timestamp")
                ).collect()[0]["latest_timestamp"]

                if latest_timestamp:
                    if hours == 24 and step == 1:
                        current_date = datetime.now().date()
                        current_date_str = current_date.strftime("%Y-%m-%d")
                        
                        today_df = filtered_df.filter(
                            F.date_format("event_timestamp", "yyyy-MM-dd") == current_date_str
                        )
                        
                        if not today_df.take(1):
                            return []
                        
                        # Nhóm theo giờ và tính trung bình
                        aggregated_metrics_df = (
                            today_df.groupBy(F.hour("event_timestamp").alias("hour"))
                            .agg(
                                F.min("event_timestamp").alias("start_timestamp"),
                                F.max("event_timestamp").alias("end_timestamp"),
                                F.avg("temperature").alias("average_temperature"),
                                F.avg("windspeed").alias("average_windspeed"),
                                F.avg("winddirection").alias("average_winddirection"),
                                F.avg("humidity").alias("average_humidity"),
                                F.avg("rain").alias("average_rain"),
                                F.avg("visibility").alias("average_visibility"),
                                F.avg("pressure").alias("average_pressure"),
                                F.avg("precipitation").alias("average_precipitation"),
                                F.first("latitude").alias("latitude"),
                                F.first("longitude").alias("longitude"),
                                F.first("interval").alias("interval"),
                                F.first("is_day").alias("is_day"),
                            )
                            .orderBy("hour")
                        )
                        
                        weathercode_counts_df = (
                            today_df.groupBy(
                                F.hour("event_timestamp").alias("hour"), 
                                "weathercode"
                            )
                            .agg(F.count("*").alias("weathercode_count"))
                        )

                        weathercode_ranked_df = (
                            weathercode_counts_df.withColumn(
                                "rn",
                                F.row_number().over(
                                    Window.partitionBy("hour").orderBy(
                                        F.col("weathercode_count").desc(),
                                        F.col("weathercode").asc_nulls_last(),
                                    )
                                ),
                            )
                            .filter(F.col("rn") == 1)
                            .select("hour", "weathercode")
                        )

                        aggregated_df = (
                            aggregated_metrics_df.join(
                                weathercode_ranked_df, on="hour", how="left"
                            ).orderBy("hour")
                        )

                        return [
                            _drop_none(
                                {
                                    "key": location_key,
                                    "location": location_key,
                                    "event_timestamp": (
                                        row.start_timestamp.isoformat()
                                        if row.start_timestamp
                                        else None
                                    ),
                                    "kafka_timestamp": (
                                        row.start_timestamp.isoformat()
                                        if row.start_timestamp
                                        else None
                                    ),
                                    "reported_time": (
                                        row.start_timestamp.isoformat()
                                        if row.start_timestamp
                                        else None
                                    ),
                                    "details": (
                                        detail_dict
                                        if (detail_dict := _drop_none(
                                            {
                                                "temperature": row.average_temperature,
                                                "windspeed": row.average_windspeed,
                                                "winddirection": row.average_winddirection,
                                                "humidity": row.average_humidity,
                                                "rain": row.average_rain,
                                                "visibility": row.average_visibility,
                                                "pressure": row.average_pressure,
                                                "precipitation": row.average_precipitation,
                                                "weathercode": row.weathercode,
                                                "latitude": row.latitude,
                                                "longitude": row.longitude,
                                                "interval": row.interval,
                                                "is_day": row.is_day,
                                            }
                                        ))
                                        else None
                                    ),
                                }
                            )
                            for row in aggregated_df.collect()
                        ]
                    
                    elif hours == 168 and step == 24:
                        current_date = datetime.now().date()
                        six_days_ago = current_date - timedelta(days=6)
                        six_days_ago_str = six_days_ago.strftime("%Y-%m-%d")
                        current_date_str = current_date.strftime("%Y-%m-%d")
                        
                        # Lọc chỉ lấy dữ liệu trong 7 ngày gần nhất
                        last_7_days_df = filtered_df.filter(
                            (F.date_format("event_timestamp", "yyyy-MM-dd") >= six_days_ago_str) &
                            (F.date_format("event_timestamp", "yyyy-MM-dd") <= current_date_str)
                        )
                        
                        if not last_7_days_df.take(1):
                            return []
                        
                        # Nhóm theo ngày và tính trung bình
                        aggregated_metrics_df = (
                            last_7_days_df.groupBy(
                                F.date_format("event_timestamp", "yyyy-MM-dd").alias("day")
                            )
                            .agg(
                                F.min("event_timestamp").alias("start_timestamp"),
                                F.max("event_timestamp").alias("end_timestamp"),
                                F.avg("temperature").alias("average_temperature"),
                                F.avg("windspeed").alias("average_windspeed"),
                                F.avg("winddirection").alias("average_winddirection"),
                                F.avg("humidity").alias("average_humidity"),
                                F.avg("rain").alias("average_rain"),
                                F.avg("visibility").alias("average_visibility"),
                                F.avg("pressure").alias("average_pressure"),
                                F.avg("precipitation").alias("average_precipitation"),
                                F.first("latitude").alias("latitude"),
                                F.first("longitude").alias("longitude"),
                                F.first("interval").alias("interval"),
                                F.first("is_day").alias("is_day"),
                            )
                            .orderBy("day")
                        )
                        
                        weathercode_counts_df = (
                            last_7_days_df.groupBy(
                                F.date_format("event_timestamp", "yyyy-MM-dd").alias("day"),
                                "weathercode"
                            )
                            .agg(F.count("*").alias("weathercode_count"))
                        )

                        weathercode_ranked_df = (
                            weathercode_counts_df.withColumn(
                                "rn",
                                F.row_number().over(
                                    Window.partitionBy("day").orderBy(
                                        F.col("weathercode_count").desc(),
                                        F.col("weathercode").asc_nulls_last(),
                                    )
                                ),
                            )
                            .filter(F.col("rn") == 1)
                            .select("day", "weathercode")
                        )

                        aggregated_df = (
                            aggregated_metrics_df.join(
                                weathercode_ranked_df, on="day", how="left"
                            ).orderBy("day")
                        )

                        return [
                            _drop_none(
                                {
                                    "key": location_key,
                                    "location": location_key,
                                    "event_timestamp": (
                                        row.start_timestamp.isoformat()
                                        if row.start_timestamp
                                        else None
                                    ),
                                    "kafka_timestamp": (
                                        row.start_timestamp.isoformat()
                                        if row.start_timestamp
                                        else None
                                    ),
                                    "reported_time": (
                                        row.start_timestamp.isoformat()
                                        if row.start_timestamp
                                        else None
                                    ),
                                    "details": (
                                        detail_dict
                                        if (detail_dict := _drop_none(
                                            {
                                                "temperature": row.average_temperature,
                                                "windspeed": row.average_windspeed,
                                                "winddirection": row.average_winddirection,
                                                "humidity": row.average_humidity,
                                                "rain": row.average_rain,
                                                "visibility": row.average_visibility,
                                                "pressure": row.average_pressure,
                                                "precipitation": row.average_precipitation,
                                                "weathercode": row.weathercode,
                                                "latitude": row.latitude,
                                                "longitude": row.longitude,
                                                "interval": row.interval,
                                                "is_day": row.is_day,
                                            }
                                        ))
                                        else None
                                    ),
                                }
                            )
                            for row in aggregated_df.collect()
                        ]
                    
                    # Default case: Giữ nguyên logic cũ cho các trường hợp khác
                    else:
                        hours *= 12
                        step *= 12
                        window_start = latest_timestamp - timedelta(hours=hours)
                        recent_df = (
                            filtered_df.filter(
                                F.col("event_timestamp") >= F.lit(window_start)
                            )
                            .orderBy(F.col("event_timestamp").desc())
                            .limit(hours)
                        )

                        if recent_df.take(1):
                            partition_window = Window.orderBy(
                                F.col("event_timestamp").desc()
                            )
                            indexed_df = recent_df.withColumn(
                                "idx", F.row_number().over(partition_window)
                            )
                            bucketed_df = indexed_df.withColumn(
                                "bucket", F.floor((F.col("idx") - 1) / F.lit(step))
                            )
                            
                            aggregated_metrics_df = (
                                bucketed_df.groupBy("bucket")
                                .agg(
                                    F.min("event_timestamp").alias("start_timestamp"),
                                    F.max("event_timestamp").alias("end_timestamp"),
                                    F.avg("temperature").alias("average_temperature"),
                                    F.avg("windspeed").alias("average_windspeed"),
                                    F.avg("winddirection").alias("average_winddirection"),
                                    F.avg("humidity").alias("average_humidity"),
                                    F.avg("rain").alias("average_rain"),
                                    F.avg("visibility").alias("average_visibility"),
                                    F.avg("pressure").alias("average_pressure"),
                                    F.avg("precipitation").alias("average_precipitation"),
                                    F.first("latitude").alias("latitude"),
                                    F.first("longitude").alias("longitude"),
                                    F.first("interval").alias("interval"),
                                    F.first("is_day").alias("is_day"),
                                )
                            )

                            weathercode_counts_df = (
                                bucketed_df.groupBy("bucket", "weathercode")
                                .agg(F.count("*").alias("weathercode_count"))
                            )

                            weathercode_ranked_df = (
                                weathercode_counts_df.withColumn(
                                    "rn",
                                    F.row_number().over(
                                        Window.partitionBy("bucket").orderBy(
                                            F.col("weathercode_count").desc(),
                                            F.col("weathercode").asc_nulls_last(),
                                        )
                                    ),
                                )
                                .filter(F.col("rn") == 1)
                                .select("bucket", "weathercode")
                            )

                            aggregated_df = (
                                aggregated_metrics_df.join(
                                    weathercode_ranked_df, on="bucket", how="left"
                                ).orderBy("bucket")
                            )

                            return [
                                _drop_none(
                                    {
                                        "key": location_key,
                                        "location": location_key,
                                        "event_timestamp": (
                                            row.start_timestamp.isoformat()
                                            if row.start_timestamp
                                            else None
                                        ),
                                        "kafka_timestamp": (
                                            row.start_timestamp.isoformat()
                                            if row.start_timestamp
                                            else None
                                        ),
                                        "reported_time": (
                                            row.start_timestamp.isoformat()
                                            if row.start_timestamp
                                            else None
                                        ),
                                        "details": (
                                            detail_dict
                                            if (detail_dict := _drop_none(
                                                {
                                                    "temperature": row.average_temperature,
                                                    "windspeed": row.average_windspeed,
                                                    "winddirection": row.average_winddirection,
                                                    "humidity": row.average_humidity,
                                                    "rain": row.average_rain,
                                                    "visibility": row.average_visibility,
                                                    "pressure": row.average_pressure,
                                                    "precipitation": row.average_precipitation,
                                                    "weathercode": row.weathercode,
                                                    "latitude": row.latitude,
                                                    "longitude": row.longitude,
                                                    "interval": row.interval,
                                                    "is_day": row.is_day,
                                                }
                                            ))
                                            else None
                                        ),
                                    }
                                )
                                for row in aggregated_df.collect()
                            ]

            return []

        # Fallback to snapshot processing
        kafka_df = _read_kafka_snapshot(
            self.spark,
            self.bootstrap_servers,
            self.topic,
            starting_offsets=self.starting_offsets,
        )
        parsed_df = _parse_kafka_dataframe(kafka_df)
        filtered_df = self._filter_stream_by_location(parsed_df, location_key).orderBy(
            F.col("event_timestamp").desc()
        )

        if filtered_df.rdd.isEmpty():
            return []

        # Case 1: hours=24, step=1 - Lấy theo giờ trong ngày hiện tại
        if hours == 24 and step == 1:
            current_date = datetime.now().date()
            current_date_str = current_date.strftime("%Y-%m-%d")
            
            # Lọc chỉ lấy dữ liệu trong ngày hiện tại
            today_df = filtered_df.filter(
                F.date_format("event_timestamp", "yyyy-MM-dd") == current_date_str
            )
            
            if today_df.rdd.isEmpty():
                return []
            
            # Nhóm theo giờ và tính trung bình
            aggregated_metrics_df = (
                today_df.groupBy(F.hour("event_timestamp").alias("hour"))
                .agg(
                    F.min("event_timestamp").alias("start_timestamp"),
                    F.max("event_timestamp").alias("end_timestamp"),
                    F.avg("temperature").alias("average_temperature"),
                    F.avg("windspeed").alias("average_windspeed"),
                    F.avg("winddirection").alias("average_winddirection"),
                    F.avg("humidity").alias("average_humidity"),
                    F.avg("rain").alias("average_rain"),
                    F.avg("visibility").alias("average_visibility"),
                    F.avg("pressure").alias("average_pressure"),
                    F.avg("precipitation").alias("average_precipitation"),
                    F.first("latitude").alias("latitude"),
                    F.first("longitude").alias("longitude"),
                    F.first("interval").alias("interval"),
                    F.first("is_day").alias("is_day"),
                )
                .orderBy("hour")
            )
            
            weathercode_counts_df = (
                today_df.groupBy(
                    F.hour("event_timestamp").alias("hour"), 
                    "weathercode"
                )
                .agg(F.count("*").alias("weathercode_count"))
            )

            weathercode_ranked_df = (
                weathercode_counts_df.withColumn(
                    "rn",
                    F.row_number().over(
                        Window.partitionBy("hour").orderBy(
                            F.col("weathercode_count").desc(),
                            F.col("weathercode").asc_nulls_last(),
                        )
                    ),
                )
                .filter(F.col("rn") == 1)
                .select("hour", "weathercode")
            )

            aggregated_df = (
                aggregated_metrics_df.join(
                    weathercode_ranked_df, on="hour", how="left"
                ).orderBy("hour")
            )

            return [
                _drop_none(
                    {
                        "key": location_key,
                        "location": location_key,
                        "event_timestamp": (
                            row.start_timestamp.isoformat()
                            if row.start_timestamp
                            else None
                        ),
                        "kafka_timestamp": (
                            row.start_timestamp.isoformat()
                            if row.start_timestamp
                            else None
                        ),
                        "reported_time": (
                            row.start_timestamp.isoformat()
                            if row.start_timestamp
                            else None
                        ),
                        "details": (
                            detail_dict
                            if (detail_dict := _drop_none(
                                {
                                    "temperature": row.average_temperature,
                                    "windspeed": row.average_windspeed,
                                    "winddirection": row.average_winddirection,
                                    "humidity": row.average_humidity,
                                    "rain": row.average_rain,
                                    "visibility": row.average_visibility,
                                    "pressure": row.average_pressure,
                                    "precipitation": row.average_precipitation,
                                    "weathercode": row.weathercode,
                                    "latitude": row.latitude,
                                    "longitude": row.longitude,
                                    "interval": row.interval,
                                    "is_day": row.is_day,
                                }
                            ))
                            else None
                        ),
                    }
                )
                for row in aggregated_df.collect()
            ]
        
        # Case 2: hours=168 (24*7), step=24 - Lấy theo ngày trong 7 ngày gần nhất
        elif hours == 168 and step == 24:
            # Lấy 7 ngày gần nhất từ bây giờ (hôm nay + 6 ngày trước)
            current_date = datetime.now().date()
            six_days_ago = current_date - timedelta(days=6)
            six_days_ago_str = six_days_ago.strftime("%Y-%m-%d")
            current_date_str = current_date.strftime("%Y-%m-%d")
            
            # Lọc chỉ lấy dữ liệu trong 7 ngày gần nhất
            last_7_days_df = filtered_df.filter(
                (F.date_format("event_timestamp", "yyyy-MM-dd") >= six_days_ago_str) &
                (F.date_format("event_timestamp", "yyyy-MM-dd") <= current_date_str)
            )
            
            if last_7_days_df.rdd.isEmpty():
                return []
            
            # Nhóm theo ngày và tính trung bình
            aggregated_metrics_df = (
                last_7_days_df.groupBy(
                    F.date_format("event_timestamp", "yyyy-MM-dd").alias("day")
                )
                .agg(
                    F.min("event_timestamp").alias("start_timestamp"),
                    F.max("event_timestamp").alias("end_timestamp"),
                    F.avg("temperature").alias("average_temperature"),
                    F.avg("windspeed").alias("average_windspeed"),
                    F.avg("winddirection").alias("average_winddirection"),
                    F.avg("humidity").alias("average_humidity"),
                    F.avg("rain").alias("average_rain"),
                    F.avg("visibility").alias("average_visibility"),
                    F.avg("pressure").alias("average_pressure"),
                    F.avg("precipitation").alias("average_precipitation"),
                    F.first("latitude").alias("latitude"),
                    F.first("longitude").alias("longitude"),
                    F.first("interval").alias("interval"),
                    F.first("is_day").alias("is_day"),
                )
                .orderBy("day")
            )
            
            weathercode_counts_df = (
                last_7_days_df.groupBy(
                    F.date_format("event_timestamp", "yyyy-MM-dd").alias("day"),
                    "weathercode"
                )
                .agg(F.count("*").alias("weathercode_count"))
            )

            weathercode_ranked_df = (
                weathercode_counts_df.withColumn(
                    "rn",
                    F.row_number().over(
                        Window.partitionBy("day").orderBy(
                            F.col("weathercode_count").desc(),
                            F.col("weathercode").asc_nulls_last(),
                        )
                    ),
                )
                .filter(F.col("rn") == 1)
                .select("day", "weathercode")
            )

            aggregated_df = (
                aggregated_metrics_df.join(
                    weathercode_ranked_df, on="day", how="left"
                ).orderBy("day")
            )

            return [
                _drop_none(
                    {
                        "key": location_key,
                        "location": location_key,
                        "event_timestamp": (
                            row.start_timestamp.isoformat()
                            if row.start_timestamp
                            else None
                        ),
                        "kafka_timestamp": (
                            row.start_timestamp.isoformat()
                            if row.start_timestamp
                            else None
                        ),
                        "reported_time": (
                            row.start_timestamp.isoformat()
                            if row.start_timestamp
                            else None
                        ),
                        "details": (
                            detail_dict
                            if (detail_dict := _drop_none(
                                {
                                    "temperature": row.average_temperature,
                                    "windspeed": row.average_windspeed,
                                    "winddirection": row.average_winddirection,
                                    "humidity": row.average_humidity,
                                    "rain": row.average_rain,
                                    "visibility": row.average_visibility,
                                    "pressure": row.average_pressure,
                                    "precipitation": row.average_precipitation,
                                    "weathercode": row.weathercode,
                                    "latitude": row.latitude,
                                    "longitude": row.longitude,
                                    "interval": row.interval,
                                    "is_day": row.is_day,
                                }
                            ))
                            else None
                        ),
                    }
                )
                for row in aggregated_df.collect()
            ]
        
        # Default case: Giữ nguyên logic cũ cho các trường hợp khác
        else:
            hours *= 12
            step *= 12
            recent_df = filtered_df.limit(hours)
            partition_window = Window.orderBy(F.col("event_timestamp").desc())
            indexed_df = recent_df.withColumn("idx", F.row_number().over(partition_window))
            bucketed_df = indexed_df.withColumn(
                "bucket", F.floor((F.col("idx") - 1) / F.lit(step))
            )

            aggregated_metrics_df = (
                bucketed_df.groupBy("bucket")
                .agg(
                    F.min("event_timestamp").alias("start_timestamp"),
                    F.max("event_timestamp").alias("end_timestamp"),
                    F.avg("temperature").alias("average_temperature"),
                    F.avg("windspeed").alias("average_windspeed"),
                    F.avg("winddirection").alias("average_winddirection"),
                    F.avg("humidity").alias("average_humidity"),
                    F.avg("rain").alias("average_rain"),
                    F.avg("visibility").alias("average_visibility"),
                    F.avg("pressure").alias("average_pressure"),
                    F.avg("precipitation").alias("average_precipitation"),
                    F.first("latitude").alias("latitude"),
                    F.first("longitude").alias("longitude"),
                    F.first("interval").alias("interval"),
                    F.first("is_day").alias("is_day"),
                )
            )

            weathercode_counts_df = (
                bucketed_df.groupBy("bucket", "weathercode")
                .agg(F.count("*").alias("weathercode_count"))
            )

            weathercode_ranked_df = (
                weathercode_counts_df.withColumn(
                    "rn",
                    F.row_number().over(
                        Window.partitionBy("bucket").orderBy(
                            F.col("weathercode_count").desc(),
                            F.col("weathercode").asc_nulls_last(),
                        )
                    ),
                )
                .filter(F.col("rn") == 1)
                .select("bucket", "weathercode")
            )

            aggregated_df = (
                aggregated_metrics_df.join(weathercode_ranked_df, on="bucket", how="left")
                .orderBy("bucket")
            )

            return [
                _drop_none(
                    {
                        "key": location_key,
                        "location": location_key,
                        "event_timestamp": (
                            row.start_timestamp.isoformat() if row.start_timestamp else None
                        ),
                        "kafka_timestamp": (
                            row.start_timestamp.isoformat() if row.start_timestamp else None
                        ),
                        "reported_time": (
                            row.start_timestamp.isoformat() if row.start_timestamp else None
                        ),
                        "details": (
                            detail_dict
                            if (detail_dict := _drop_none(
                                {
                                    "temperature": row.average_temperature,
                                    "windspeed": row.average_windspeed,
                                    "winddirection": row.average_winddirection,
                                    "humidity": row.average_humidity,
                                    "rain": row.average_rain,
                                    "visibility": row.average_visibility,
                                    "pressure": row.average_pressure,
                                    "precipitation": row.average_precipitation,
                                    "weathercode": row.weathercode,
                                    "latitude": row.latitude,
                                    "longitude": row.longitude,
                                    "interval": row.interval,
                                    "is_day": row.is_day,
                                }
                            ))
                            else None
                        ),
                    }
                )
                for row in aggregated_df.collect()
            ]

    def get_sequence_for_location(
        self, location_key: str, length: int = 24
    ) -> List[Dict[str, Any]]:
        if length <= 0:
            return []

        def _collect_sequence(df: DataFrame) -> List[Dict[str, Any]]:
            rows = (
                df.orderBy(
                    F.col("event_timestamp").desc(), F.col("kafka_timestamp").desc()
                )
                .limit(length)
                .collect()
            )
            if not rows:
                return []
            return [
                _row_to_sequence_entry(row)
                for row in reversed(rows)  # chronological ascending
            ]

        stream_df = self._stream_df()
        if stream_df is not None:
            filtered_df = self._filter_stream_by_location(stream_df, location_key)
            if filtered_df.take(1):
                sequence = _collect_sequence(filtered_df)
                if sequence:
                    return sequence

        kafka_df = _read_kafka_snapshot(
            self.spark,
            self.bootstrap_servers,
            self.topic,
            starting_offsets=self.starting_offsets,
        )
        parsed_df = _parse_kafka_dataframe(kafka_df)
        filtered_df = self._filter_stream_by_location(parsed_df, location_key)
        return _collect_sequence(filtered_df)

    def get_weather_days(self, location_key: str) -> List[str]:
        stream_df = self._stream_df()
        if stream_df is not None:
            filtered_df = self._filter_stream_by_location(stream_df, location_key)
            if not filtered_df.take(1):
                return []
            days_df = filtered_df.select(
                F.date_format("event_timestamp", "yyyy-MM-dd").alias("day")
            ).distinct()
            return [row.day for row in days_df.orderBy("day").collect()]

        records = self.list_latest()
        return sorted(
            {
                record["event_timestamp"][:10]
                for record in records
                if record["location"]
                and record["location"].lower() == location_key.lower()
                and record["event_timestamp"]
            }
        )

    def get_weather_average_day(self, location_key: str, date_iso: str) -> Dict[str, Any]:
        stream_df = self._stream_df()
        if stream_df is not None:
            filtered_df = self._filter_stream_by_location(stream_df, location_key).filter(
                F.date_format("event_timestamp", "yyyy-MM-dd") == date_iso
            )
            if not filtered_df.take(1):
                return {}

            averages = filtered_df.agg(
                F.avg("temperature").alias("average_temperature"),
                F.avg("windspeed").alias("average_windspeed"),
                F.avg("winddirection").alias("average_winddirection"),
            ).collect()[0]

            return {
                "location": location_key,
                "date": date_iso,
                "average_temperature": averages["average_temperature"],
                "average_windspeed": averages["average_windspeed"],
                "average_winddirection": averages["average_winddirection"],
            }

        # Fallback to snapshot data
        kafka_df = _read_kafka_snapshot(
            self.spark,
            self.bootstrap_servers,
            self.topic,
            starting_offsets=self.starting_offsets,
        )
        parsed_df = _parse_kafka_dataframe(kafka_df)
        filtered_df = self._filter_stream_by_location(parsed_df, location_key).filter(
            F.date_format("event_timestamp", "yyyy-MM-dd") == date_iso
        )
        if filtered_df.rdd.isEmpty():
            return {}

        averages = filtered_df.agg(
            F.avg("temperature").alias("average_temperature"),
            F.avg("windspeed").alias("average_windspeed"),
            F.avg("winddirection").alias("average_winddirection"),
        ).collect()[0]

        return {
            "location": location_key,
            "date": date_iso,
            "average_temperature": averages["average_temperature"],
            "average_windspeed": averages["average_windspeed"],
            "average_winddirection": averages["average_winddirection"],
        }

    def predict_weather(self, location_key: str, steps: int = 1) -> Dict[str, Any]:
        """Predict next weather steps for a location based on recent sequence.
        Predictions are made at 5-minute intervals, then aggregated to hourly intervals.
        
        Args:
            location_key: Location identifier
            steps: Number of future steps to predict (default: 1)
        """
        if steps < 1:
            return {"error": "Steps must be at least 1"}
        
        try:
            model, metadata = _load_model()
        except Exception as e:
            return {"error": f"Failed to load model: {str(e)}"}
        
        # Get sequence of last 24 steps
        sequence = self.get_sequence_for_location(location_key, length=24)
        if not sequence or len(sequence) < 24:
            return {"error": f"Insufficient data for location '{location_key}'. Need at least 24 data points, got {len(sequence) if sequence else 0}."}
        
        # Build feature tensor from sequence
        base_feature_columns = metadata["feature_columns"]
        time_features = ["hour_sin", "hour_cos", "minute_sin", "minute_cos"]
        base_features = [f for f in base_feature_columns if f not in time_features]
        
        # Convert weathercode to index mapping
        idx_to_weathercode = metadata.get("idx_to_weathercode", {})
        weathercode_to_idx = {v: k for k, v in idx_to_weathercode.items()} if idx_to_weathercode else {}
        
        def _build_feature_array_from_sequence(seq: List[Dict]) -> np.ndarray:
            """Build feature array from a sequence of dictionaries."""
            feature_rows = []
            for seq_step in seq:
                row = []
                # Add base features
                for col in base_features:
                    value = seq_step.get(col)
                    if value is None:
                        row.append(float('nan'))
                    else:
                        row.append(float(value))
                
                # Add time features
                timestamp = seq_step.get("event_timestamp")
                hour_sin, hour_cos, minute_sin, minute_cos = _time_of_day_features(timestamp)
                row.extend([hour_sin, hour_cos, minute_sin, minute_cos])
                feature_rows.append(row)
            
            seq_array = np.array(feature_rows, dtype=np.float32)
            if np.isnan(seq_array).any():
                # Fill NaN: forward fill then backward fill using numpy
                for col_idx in range(seq_array.shape[1]):
                    col = seq_array[:, col_idx]
                    mask = np.isnan(col)
                    if mask.any():
                        # Forward fill
                        valid_idx = np.where(~mask)[0]
                        if len(valid_idx) > 0:
                            # Use first valid value for all NaN at the beginning
                            first_valid = valid_idx[0]
                            col[:first_valid] = col[first_valid]
                            # Forward fill the rest
                            for i in range(first_valid + 1, len(col)):
                                if np.isnan(col[i]):
                                    col[i] = col[i-1]
                        else:
                            # All NaN, fill with 0
                            col[:] = 0.0
                # Final check: fill any remaining NaN with 0
                seq_array = np.nan_to_num(seq_array, nan=0.0)
            return seq_array
        
        def _aggregate_to_hourly(predictions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            """Aggregate 5-minute predictions to hourly intervals.
            Temperature: average, Weathercode: most frequent (mode).
            """
            if not predictions:
                return []
            
            # Group predictions by hour
            hourly_groups: Dict[str, List[Dict[str, Any]]] = {}
            
            for pred in predictions:
                timestamp = pred.get("event_timestamp")
                if not timestamp:
                    continue
                
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    # Create hour key: YYYY-MM-DD HH:00:00
                    hour_key = dt.replace(minute=0, second=0, microsecond=0).isoformat()
                    
                    if hour_key not in hourly_groups:
                        hourly_groups[hour_key] = []
                    hourly_groups[hour_key].append(pred)
                except:
                    continue
            
            # Aggregate each hour group
            aggregated = []
            for hour_key in sorted(hourly_groups.keys()):
                group = hourly_groups[hour_key]
                
                # Calculate average temperature
                temps = [p.get("temperature") for p in group if p.get("temperature") is not None]
                avg_temp = sum(temps) / len(temps) if temps else None
                
                # Calculate most frequent weathercode (mode)
                weathercodes = [p.get("weathercode") for p in group if p.get("weathercode") is not None]
                if weathercodes:
                    weathercode_counts = Counter(weathercodes)
                    most_common_weathercode = weathercode_counts.most_common(1)[0][0]
                else:
                    most_common_weathercode = None
                
                # Use the first timestamp in the hour as the representative timestamp
                first_timestamp = group[0].get("event_timestamp") if group else hour_key
                
                aggregated.append({
                    "event_timestamp": first_timestamp,
                    "temperature": round(avg_temp, 2) if avg_temp is not None else None,
                    "weathercode": int(most_common_weathercode) if most_common_weathercode is not None else None,
                    "steps_in_hour": len(group),
                })
            
            return aggregated
        
        try:
            # Start with original sequence
            current_sequence = sequence[-24:].copy()
            raw_predictions = []  # Store raw 5-minute predictions
            
            # Get last entry for location info
            last_entry = sequence[-1]
            last_timestamp = last_entry.get("event_timestamp")
            
            # Predict each step (5-minute intervals)
            # We need to predict enough steps to cover the requested hours
            # Each hour has 12 steps (5 minutes * 12 = 60 minutes)
            # So if steps=1 (1 hour), we need to predict 12 steps
            # If steps=2 (2 hours), we need to predict 24 steps, etc.
            steps_5min = steps * 12  # Convert hours to 5-minute steps
            
            for step_idx in range(steps_5min):
                # Build feature array from current sequence
                seq_array = _build_feature_array_from_sequence(current_sequence)
                
                # Convert to tensor: (batch=1, sequence_length=24, input_dim)
                input_tensor = torch.from_numpy(seq_array).unsqueeze(0)
                
                # Predict
                with torch.no_grad():
                    weather_logits, temperature_pred = model(input_tensor)
                    predicted_weather_idx = weather_logits.argmax(dim=-1).item()
                    predicted_temperature = temperature_pred.item()
                
                # Convert weathercode index back to actual weathercode
                predicted_weathercode = idx_to_weathercode.get(predicted_weather_idx, predicted_weather_idx)
                
                # Calculate timestamp for this prediction
                if last_timestamp:
                    try:
                        last_dt = datetime.fromisoformat(last_timestamp.replace('Z', '+00:00'))
                        # Assuming 5-minute intervals
                        pred_dt = last_dt + timedelta(minutes=5 * (step_idx + 1))
                        pred_timestamp = pred_dt.isoformat()
                    except:
                        pred_timestamp = None
                else:
                    pred_timestamp = None
                
                raw_predictions.append({
                    "step": step_idx + 1,
                    "event_timestamp": pred_timestamp,
                    "temperature": round(predicted_temperature, 2),
                    "weathercode": int(predicted_weathercode),
                })
                
                if step_idx < steps_5min - 1:
                    new_entry = current_sequence[-1].copy()
                    new_entry["temperature"] = predicted_temperature
                    new_entry["weathercode"] = predicted_weathercode
                    if pred_timestamp:
                        new_entry["event_timestamp"] = pred_timestamp
                    current_sequence = current_sequence[1:] + [new_entry]
            
            hourly_predictions = _aggregate_to_hourly(raw_predictions)
            
            return {
                "key": location_key,
                "location": location_key,
                "steps": steps,
                "predictions": hourly_predictions,
                "based_on": {
                    "sequence_length": len(sequence),
                    "last_timestamp": last_timestamp,
                },
                "details": _drop_none({
                    "latitude": last_entry.get("latitude"),
                    "longitude": last_entry.get("longitude"),
                }),
            }
        except Exception as e:
            return {"error": f"Prediction failed: {str(e)}"}


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Inspect weather data from Kafka")
    parser.add_argument("--spark-master", default="spark://spark-master:7077")
    parser.add_argument("--kafka-bootstrap-servers", default="kafka:19092")
    parser.add_argument("--kafka-topic", default="duLieuKhuVuc")
    parser.add_argument(
        "--kafka-starting-offsets",
        default="earliest",
        help="Kafka starting offsets (default: earliest)",
    )
    parser.add_argument("--output-dir", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--features", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--window-size", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--input", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--location", help="Location key to inspect")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of records shown when listing all locations",
    )
    args = parser.parse_args()

    spark = build_spark_session(args.spark_master)
    service = WeatherKafkaService(
        spark,
        args.kafka_bootstrap_servers,
        args.kafka_topic,
        starting_offsets=args.kafka_starting_offsets,
        enable_streaming=False,
    )

    try:
        if args.location:
            record = service.get_location(args.location)
            if record:
                print(record)
            else:
                print(f"No record found for location '{args.location}'")
        else:
            for entry in service.list_latest(limit=args.limit):
                print(entry)
    finally:
        spark.stop()


if __name__ == "__main__":
    _cli()
