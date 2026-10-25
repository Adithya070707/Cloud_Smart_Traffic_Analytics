# Cloud Smart Traffic Analytics

A reproducible Hyderabad traffic analytics demo. It generates sample road
observations, processes them with Apache Spark, optionally moves raw and
processed CSVs through Amazon S3, and displays insights in a Streamlit dashboard.

## Requirements

- Python 3.9 or newer
- Java 17 or 21 available on `PATH` (`java -version`) for PySpark 4
- AWS credentials configured with the AWS CLI or another supported AWS SDK
  credential provider only if you use the optional S3 upload

## Setup and run

Run these commands from the `Cloud_Smart_Traffic_Analytics` directory:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python generate_data.py
python run_analytics.py
streamlit run app.py
```

The generated input is saved to `data/traffic_data.csv` and includes timestamps,
Hyderabad locations, vehicle counts, speeds, and road capacities. Spark cleans
invalid rows, computes road/hour/day aggregates, and classifies congestion using
vehicle-count-to-capacity ratios (High >= 0.9, Medium >= 0.65, otherwise Low).
Outputs are `location_stats.csv`, `hourly_stats.csv`, `daily_stats.csv`,
`congestion_stats.csv`, and `traffic_enriched.csv` in `output/`. Spark writes
the enriched records as a bounded-memory stream rather than collecting the full
dataset in Python; the demo exports that CSV as one file for convenient local use.

The data generator defaults to 1,000 reproducible demo records. Use
`python generate_data.py --rows 5000 --seed 7` to choose a different size and
seed. The analytics script accepts `--input` and `--output-dir` to use alternate
paths. The dashboard can read the local outputs or the same analytics from S3.

## Architecture

```text
Generated traffic CSV -> private S3 raw object (optional)
                              |
                       boto3 temporary staging
                              |
                    PySpark cleaning and analysis
                              |
             local analytics CSVs + optional S3 outputs
                              |
                 Streamlit + Plotly dashboard
```

The sample traffic is synthetic and fixed-seed; it is not live camera or
government traffic data. Congestion is estimated from vehicle counts divided by
the demonstration road capacities, so treat it as an educational proxy rather
than a real traffic-management signal. Spark runs locally (`local[*]`); a
distributed Spark cluster and AWS account are not required for the local demo.

## Optional S3 workflow

Create a private S3 bucket and configure AWS credentials with an IAM identity
that can read and write objects in that bucket. Do not place AWS keys in source
files. New buckets block public access by default. Replace the bucket name with
a globally unique name:

```powershell
aws s3 mb s3://your-globally-unique-bucket --region ap-south-1
python generate_data.py
```

Choose one of these upload methods:

```powershell
# AWS CLI
aws s3 cp data\traffic_data.csv s3://your-globally-unique-bucket/traffic/traffic_data.csv
# Or use the included boto3 API script
python upload_to_s3.py --bucket your-globally-unique-bucket --region ap-south-1
```

Then run Spark against the uploaded object:

```powershell
python run_analytics.py --s3-bucket your-globally-unique-bucket --s3-input-key traffic/traffic_data.csv --s3-output-prefix analytics --region ap-south-1
```

The Spark script stages the private S3 input to a temporary local file through
the AWS SDK (avoiding extra Hadoop/S3A connector setup), runs the same Spark
transformations, writes local results, and uploads each result to the requested
S3 prefix. Set `AWS_S3_BUCKET`, `AWS_S3_OUTPUT_PREFIX` (defaults to `analytics`),
and optionally `AWS_DEFAULT_REGION` to configure the dashboard's Amazon S3 mode.
Credentials and region use the standard boto3 provider chain.

## Dashboard and verification

Run `streamlit run app.py` and choose **Local files** or **Amazon S3** in the
sidebar. Charts show road-wise traffic, hourly and daily trends, congestion
distribution, and location-level statistics.

Run the focused checks with:

```powershell
python -m unittest discover -s tests -v
```

The boto3 upload script defaults to `traffic/traffic_data.csv`; override it
with `--key`. AWS credentials and region are resolved by boto3's standard
credential chain.
