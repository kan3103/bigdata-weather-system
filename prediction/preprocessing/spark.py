import argparse
import os
from pyspark.sql import SparkSession, functions as F, Window


def main(args):
    spark = (
        SparkSession.builder.appName("TestSpark")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    print(f"Spark job started with input file: {args.input}")

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(args.input)
    )

    trimmed_columns = [col.strip() for col in df.columns]
    df = df.toDF(*trimmed_columns)

    df = df.replace("-9999", None)

    if "datetime_utc" not in df.columns:
        raise ValueError("Input CSV must contain the column 'datetime_utc'")

    df = df.withColumn(
        "timestamp", F.to_timestamp("datetime_utc", "yyyyMMdd-HH:mm")
    ).dropna(subset=["timestamp"])

    features = [f.strip() for f in args.features.split(",")]

    missing_features = [col for col in features if col not in df.columns]
    if missing_features:
        raise ValueError(f"The following columns do not exist in the data: {missing_features}")

    df = df.select(["timestamp"] + features).orderBy("timestamp")

    df = df.na.fill(0)

    window_size = int(args.window_size)
    window_spec = Window.orderBy("timestamp")
    for i in range(1, window_size + 1):
        for col in features:
            df = df.withColumn(
                f"{col}_lag_{i}", F.lag(col, i).over(window_spec)
            )

    df = df.na.drop()

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    df.write.mode("overwrite").parquet(output_dir)

    print(f"Done! Preprocessed data saved to: {output_dir}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--features", required=True)
    parser.add_argument("--window-size", type=int, default=24)
    args = parser.parse_args()
    main(args)
