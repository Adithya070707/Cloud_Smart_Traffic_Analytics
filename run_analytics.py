"""Run the traffic aggregations with Apache Spark and save CSV outputs."""

import argparse
import csv
import os
from pathlib import Path
import tempfile

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = PROJECT_DIR / "data" / "traffic_data.csv"
DEFAULT_OUTPUT_DIR = PROJECT_DIR / "output"
REQUIRED_COLUMNS = {
    "timestamp",
    "location",
    "vehicle_count",
    "avg_speed_kmh",
    "road_capacity",
}


def write_enriched_csv(dataframe, output_path):
    with tempfile.TemporaryDirectory(
        prefix=".traffic_enriched_", dir=output_path.parent
    ) as temporary_directory:
        temporary_output = Path(temporary_directory) / output_path.name
        with temporary_output.open("w", newline="", encoding="utf-8") as output_file:
            writer = csv.writer(output_file)
            writer.writerow(dataframe.columns)
            for row in dataframe.toLocalIterator():
                writer.writerow(row)
        temporary_output.replace(output_path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Input CSV (defaults to data/traffic_data.csv).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for analytics CSVs (defaults to output/).",
    )
    parser.add_argument(
        "--s3-bucket",
        default=os.environ.get("AWS_S3_BUCKET"),
        help="Optional S3 bucket for input and analytics outputs (or AWS_S3_BUCKET).",
    )
    parser.add_argument(
        "--s3-input-key",
        default="traffic/traffic_data.csv",
        help="Input object key when --s3-bucket is set.",
    )
    parser.add_argument(
        "--s3-output-prefix",
        default="analytics",
        help="S3 prefix for analytics outputs when --s3-bucket is set.",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_DEFAULT_REGION"),
        help="Optional AWS region (or AWS_DEFAULT_REGION).",
    )
    args = parser.parse_args()

    if not args.s3_bucket and not args.input.is_file():
        parser.error(f"Input CSV does not exist: {args.input}. Run generate_data.py first.")

    s3 = boto3.client("s3", region_name=args.region) if args.s3_bucket else None
    temporary_directory = tempfile.TemporaryDirectory() if s3 else None
    input_path = args.input
    if s3 and temporary_directory:
        input_path = Path(temporary_directory.name) / "traffic_data.csv"
        try:
            s3.download_file(args.s3_bucket, args.s3_input_key, str(input_path))
        except (BotoCoreError, ClientError) as error:
            temporary_directory.cleanup()
            parser.exit(
                1,
                f"Unable to download s3://{args.s3_bucket}/{args.s3_input_key}: {error}\n",
            )

    spark = (
        SparkSession.builder.appName("CloudSmartTrafficAnalytics")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        raw = spark.read.option("header", True).option("inferSchema", True).csv(str(input_path))
        missing_columns = REQUIRED_COLUMNS.difference(raw.columns)
        if missing_columns:
            parser.error(f"Input CSV is missing required columns: {', '.join(sorted(missing_columns))}")

        enriched = (
            raw.withColumn("timestamp", F.expr("try_to_timestamp(timestamp)"))
            .withColumn("location", F.trim(F.col("location")))
            .withColumn("vehicle_count", F.expr("try_cast(vehicle_count AS BIGINT)"))
            .withColumn("avg_speed_kmh", F.expr("try_cast(avg_speed_kmh AS DOUBLE)"))
            .withColumn("road_capacity", F.expr("try_cast(road_capacity AS BIGINT)"))
            .filter(
                F.col("timestamp").isNotNull()
                & F.col("location").isNotNull()
                & (F.length("location") > 0)
                & (F.col("vehicle_count") >= 0)
                & (F.col("avg_speed_kmh") >= 0)
                & (F.col("road_capacity") > 0)
            )
            .withColumn("hour", F.hour("timestamp"))
            .withColumn(
                "congestion_index",
                F.round(F.col("vehicle_count") / F.col("road_capacity"), 3),
            )
            .withColumn(
                "congestion_level",
                F.when(F.col("congestion_index") >= 0.9, "High")
                .when(F.col("congestion_index") >= 0.65, "Medium")
                .otherwise("Low"),
            )
        )

        if not enriched.take(1):
            parser.error("The input CSV contains no valid traffic records.")

        location_stats = (
            enriched.groupBy("location")
            .agg(
                F.count("*").alias("record_count"),
                F.round(F.avg("vehicle_count"), 1).alias("avg_vehicle_count"),
                F.round(F.avg("avg_speed_kmh"), 1).alias("avg_speed_kmh"),
                F.round(F.avg("congestion_index"), 3).alias("avg_congestion_index"),
            )
            .withColumn(
                "avg_congestion_level",
                F.when(F.col("avg_congestion_index") >= 0.9, "High")
                .when(F.col("avg_congestion_index") >= 0.65, "Medium")
                .otherwise("Low"),
            )
            .orderBy(F.desc("avg_congestion_index"))
        )
        hourly_stats = (
            enriched.groupBy("hour")
            .agg(
                F.count("*").alias("record_count"),
                F.round(F.avg("vehicle_count"), 1).alias("avg_vehicle_count"),
                F.round(F.avg("avg_speed_kmh"), 1).alias("avg_speed_kmh"),
            )
            .orderBy("hour")
        )
        daily_stats = (
            enriched.withColumn("date", F.to_date("timestamp"))
            .groupBy("date")
            .agg(
                F.count("*").alias("record_count"),
                F.round(F.avg("vehicle_count"), 1).alias("avg_vehicle_count"),
                F.round(F.avg("avg_speed_kmh"), 1).alias("avg_speed_kmh"),
            )
            .orderBy("date")
        )
        congestion_counts = (
            enriched.groupBy("congestion_level")
            .agg(F.count("*").alias("record_count"))
        )
        total_records = enriched.agg(F.count("*").alias("total_record_count"))
        congestion_stats = (
            congestion_counts.crossJoin(total_records)
            .withColumn(
                "percentage",
                F.round(
                    F.col("record_count") * 100 / F.col("total_record_count"),
                    1,
                ),
            )
            .drop("total_record_count")
            .orderBy(
                F.when(F.col("congestion_level") == "High", 0)
                .when(F.col("congestion_level") == "Medium", 1)
                .otherwise(2)
            )
        )

        args.output_dir.mkdir(parents=True, exist_ok=True)
        outputs = {
            "location_stats.csv": location_stats,
            "hourly_stats.csv": hourly_stats,
            "daily_stats.csv": daily_stats,
            "congestion_stats.csv": congestion_stats,
            "traffic_enriched.csv": enriched.orderBy("timestamp"),
        }
        for filename, dataframe in outputs.items():
            output_path = args.output_dir / filename
            if filename == "traffic_enriched.csv":
                write_enriched_csv(dataframe, output_path)
            else:
                dataframe.toPandas().to_csv(output_path, index=False)
            print(f"Wrote {output_path}")
            if s3:
                key = "/".join(
                    part.strip("/")
                    for part in (args.s3_output_prefix, filename)
                    if part.strip("/")
                )
                s3.upload_file(str(output_path), args.s3_bucket, key)
                print(f"Uploaded s3://{args.s3_bucket}/{key}")
    except (BotoCoreError, ClientError) as error:
        parser.exit(1, f"Unable to transfer analytics to S3: {error}\n")
    finally:
        spark.stop()
        if temporary_directory:
            temporary_directory.cleanup()


if __name__ == "__main__":
    main()
