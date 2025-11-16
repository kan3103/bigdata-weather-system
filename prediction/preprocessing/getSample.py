import argparse
import os
from pathlib import Path
from typing import List

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import spark as spark_module

_FEATURE_COLUMNS: List[str] = [
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

_TARGET_FEATURE_COLUMNS: List[str] = [
    "temperature",
    "weathercode",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export the full Kafka weather history and build location-based sequences "
            "ready for LSTM-style training."
        )
    )
    parser.add_argument(
        "--spark-master",
        default=os.getenv("SPARK_MASTER_URL", "local[*]"),
        help="Spark master URL (default: local[*] or value from SPARK_MASTER_URL env).",
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
        help="Kafka bootstrap servers (default: kafka:19092 or KAFKA_BOOTSTRAP_SERVERS env).",
    )
    parser.add_argument(
        "--kafka-topic",
        default=os.getenv("KAFKA_TOPIC", "duLieuKhuVuc"),
        help="Kafka topic to read from (default: duLieuKhuVuc or KAFKA_TOPIC env).",
    )
    parser.add_argument(
        "--kafka-starting-offsets",
        default=os.getenv("KAFKA_STARTING_OFFSETS", "earliest"),
        help=(
            "Kafka starting offsets for the snapshot. Use 'earliest', 'latest', or a JSON map "
            "per the Spark Kafka integration guide (default: earliest)."
        ),
    )
    parser.add_argument(
        "--sequence-length",
        type=int,
        default=int(os.getenv("SEQUENCE_LENGTH", "24")),
        help="Number of consecutive records per sequence window (default: 24).",
    )
    parser.add_argument(
        "--forecast-length",
        type=int,
        default=int(os.getenv("FORECAST_LENGTH", "1")),
        help="Number of future steps to include in the target sequence (default: 1).",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("OUTPUT_PATH", "/data/sample_weather.json"),
        help="Destination directory/file for the JSON output (default: /data/sample_weather.json).",
    )
    parser.add_argument(
        "--coalesce",
        type=int,
        default=int(os.getenv("OUTPUT_COALESCE", "1")),
        help="Number of output partitions (default: 1).",
    )
    return parser.parse_args()


def _validate_args(args: argparse.Namespace) -> None:
    if args.sequence_length <= 0:
        raise ValueError("sequence_length must be a positive integer.")
    if args.forecast_length <= 0:
        raise ValueError("forecast_length must be a positive integer.")


def _load_full_history(
    spark_session,
    *,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str,
) -> DataFrame:
    kafka_df = spark_module._read_kafka_snapshot(  # pylint: disable=protected-access
        spark_session,
        bootstrap_servers,
        topic,
        starting_offsets=starting_offsets,
    )
    parsed_df = spark_module._parse_kafka_dataframe(  # pylint: disable=protected-access
        kafka_df
    )
    parsed_df = _drop_error_messages(parsed_df)

    base_columns = [
        "key",
        "location",
        "event_timestamp",
        "kafka_timestamp",
        "latitude",
        "longitude",
        *_FEATURE_COLUMNS,
    ]

    df = (
        parsed_df.select(*base_columns)
        .filter(F.col("event_timestamp").isNotNull())
        .dropna(subset=["location"])
    )

    # Keep the most recent record per location/event_timestamp pair.
    dedupe_window = Window.partitionBy("location", "event_timestamp").orderBy(
        F.col("kafka_timestamp").desc()
    )
    df = (
        df.withColumn("dedupe_rank", F.row_number().over(dedupe_window))
        .filter(F.col("dedupe_rank") == 1)
        .drop("dedupe_rank")
    )

    return df


def _drop_error_messages(df: DataFrame) -> DataFrame:
    """Remove rows whose payload contains the API error message."""
    return df.filter(
        ~F.lower(F.coalesce(F.col("payload").getItem("message"), F.lit(""))).contains(
            "lỗi khi gọi api"
        )
    )


def _build_sequences(
    df: DataFrame,
    *,
    sequence_length: int,
    forecast_length: int,
) -> DataFrame:
    order_window = (
        Window.partitionBy("location")
        .orderBy(F.col("event_timestamp").asc(), F.col("kafka_timestamp").asc())
    )
    sequence_window = order_window.rowsBetween(-(sequence_length - 1), 0)

    sequence_entry = F.struct(
        F.col("event_timestamp").cast("string").alias("event_timestamp"),
        *[F.col(col).alias(col) for col in _FEATURE_COLUMNS],
    )

    df_with_seq = df.withColumn(
        "sequence", F.collect_list(sequence_entry).over(sequence_window)
    ).withColumn("sequence_length", F.size("sequence"))

    future_structs = [
        F.struct(
            F.lead("event_timestamp", step).over(order_window)
            .cast("string")
            .alias("event_timestamp"),
            *[
                F.lead(col, step).over(order_window).alias(col)
                for col in _TARGET_FEATURE_COLUMNS
            ],
        )
        for step in range(1, forecast_length + 1)
    ]

    df_with_targets = (
        df_with_seq.withColumn("target_sequence", F.array(*future_structs))
        .withColumn("target", F.element_at("target_sequence", 1))
    )

    valid_sequences = df_with_targets.filter(
        (F.col("sequence_length") == sequence_length)
        & (F.size("sequence") == sequence_length)
        & (
            F.element_at(F.col("target_sequence"), forecast_length).getField(
                "event_timestamp"
            ).isNotNull()
        )
    )

    start_event = F.element_at(F.col("sequence"), 1)
    end_event = F.element_at(F.col("sequence"), F.size("sequence"))

    final_df = valid_sequences.select(
        F.col("key").alias("location_key"),
        F.col("location"),
        start_event.getField("event_timestamp").alias("start_timestamp"),
        end_event.getField("event_timestamp").alias("end_timestamp"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("sequence"),
        F.col("target"),
        F.col("target_sequence"),
    )

    return final_df


def main() -> None:
    args = _parse_args()
    _validate_args(args)

    spark = spark_module.build_spark_session(args.spark_master)
    try:
        history_df = _load_full_history(
            spark,
            bootstrap_servers=args.kafka_bootstrap_servers,
            topic=args.kafka_topic,
            starting_offsets=args.kafka_starting_offsets,
        )

        sequences_df = _build_sequences(
            history_df,
            sequence_length=args.sequence_length,
            forecast_length=args.forecast_length,
        )

        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df_to_write = sequences_df.coalesce(max(args.coalesce, 1))
        df_to_write.write.mode("overwrite").json(str(output_path))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()