"""Upload the generated traffic CSV to an S3 bucket."""

import argparse
import os
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_SOURCE = PROJECT_DIR / "data" / "traffic_data.csv"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bucket",
        default=os.environ.get("AWS_S3_BUCKET"),
        help="S3 bucket name (or set AWS_S3_BUCKET).",
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE,
        help="CSV file to upload (defaults to data/traffic_data.csv).",
    )
    parser.add_argument(
        "--key",
        default="traffic/traffic_data.csv",
        help="Destination object key in the bucket.",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_DEFAULT_REGION"),
        help="Optional AWS region (or AWS_DEFAULT_REGION).",
    )
    args = parser.parse_args()

    if not args.bucket:
        parser.error("Specify --bucket or set the AWS_S3_BUCKET environment variable.")
    if not args.source.is_file():
        parser.error(f"Source CSV does not exist: {args.source}")

    try:
        boto3.client("s3", region_name=args.region).upload_file(
            str(args.source), args.bucket, args.key
        )
    except (BotoCoreError, ClientError) as error:
        parser.exit(1, f"Unable to upload to s3://{args.bucket}/{args.key}: {error}\n")

    print(f"Uploaded {args.source} to s3://{args.bucket}/{args.key}")


if __name__ == "__main__":
    main()
