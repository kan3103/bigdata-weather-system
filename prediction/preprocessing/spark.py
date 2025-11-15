import argparse
from datetime import timedelta
from typing import Any, Dict, List, Optional

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQuery


_KAFKA_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0-preview3,"
    "org.apache.kafka:kafka-clients:3.7.0"
)
_DEFAULT_IVY_PATH = "/opt/ivy-cache"
_DEFAULT_STREAM_TABLE = "weather_events"
_DEFAULT_STREAM_TRIGGER = "30 seconds"
_PAYLOAD_SCHEMA = T.MapType(T.StringType(), T.StringType())


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
    )

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
    return {key: _coerce_value(value) for key, value in payload.items()}


def _drop_none(data: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallow copy of `data` without keys that have None values."""
    return {key: value for key, value in data.items() if value is not None}


def _row_to_record(row) -> Dict[str, Any]:
    payload = _normalise_payload(row.payload)
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
        hours *= 12
        step *= 12
        stream_df = self._stream_df()
        if stream_df is not None:
            filtered_df = self._filter_stream_by_location(stream_df, location_key)
            if filtered_df.take(1):
                latest_timestamp = filtered_df.agg(
                    F.max("event_timestamp").alias("latest_timestamp")
                ).collect()[0]["latest_timestamp"]

                if latest_timestamp:
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
                    "bucket": row.bucket,
                    "start_timestamp": (
                        row.start_timestamp.isoformat() if row.start_timestamp else None
                    ),
                    "end_timestamp": (
                        row.end_timestamp.isoformat() if row.end_timestamp else None
                    ),
                    "average_temperature": row.average_temperature,
                    "average_windspeed": row.average_windspeed,
                    "average_winddirection": row.average_winddirection,
                    "average_humidity": row.average_humidity,
                    "average_rain": row.average_rain,
                    "average_visibility": row.average_visibility,
                "average_pressure": row.average_pressure,
                "average_precipitation": row.average_precipitation,
                    "weathercode": row.weathercode,
                }
            )
            for row in aggregated_df.collect()
        ]

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
    # Compatibility arguments used by legacy batch jobs
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
