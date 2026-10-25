import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from generate_data import ROAD_CAPACITIES, generate_dataset


PROJECT_DIR = Path(__file__).resolve().parents[1]


class DatasetGenerationTests(unittest.TestCase):
    def test_generation_is_repeatable_and_has_expected_schema(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            first = Path(temporary_directory) / "first.csv"
            second = Path(temporary_directory) / "second.csv"

            generate_dataset(12, 7, first)
            generate_dataset(12, 7, second)

            self.assertEqual(first.read_bytes(), second.read_bytes())
            with first.open(newline="", encoding="utf-8") as dataset:
                reader = csv.DictReader(dataset)
                rows = list(reader)

            self.assertEqual(
                reader.fieldnames,
                [
                    "timestamp",
                    "location",
                    "vehicle_count",
                    "avg_speed_kmh",
                    "road_capacity",
                ],
            )
            self.assertEqual(len(rows), 12)
            self.assertTrue(all(row["location"] in ROAD_CAPACITIES for row in rows))


class SparkAnalyticsTests(unittest.TestCase):
    def test_spark_cleans_rows_and_writes_daily_analytics(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            input_path = temporary_path / "traffic.csv"
            output_path = temporary_path / "output"
            input_path.write_text(
                "timestamp,location,vehicle_count,avg_speed_kmh,road_capacity\n"
                "2025-01-06 08:00:00,Ameerpet Junction,90,20,100\n"
                "2025-01-06 09:00:00,Ameerpet Junction,70,30,100\n"
                "2025-01-07 08:00:00,Hitech City,20,50,100\n"
                "not-a-date,Invalid Road,10,40,100\n"
                "2025-01-08 08:00:00,Invalid Road,not-a-number,40,100\n",
                encoding="utf-8",
            )
            result = subprocess.run(
                [
                    sys.executable,
                    str(PROJECT_DIR / "run_analytics.py"),
                    "--input",
                    str(input_path),
                    "--output-dir",
                    str(output_path),
                ],
                capture_output=True,
                text=True,
                timeout=180,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            expected_outputs = {
                "location_stats.csv",
                "hourly_stats.csv",
                "daily_stats.csv",
                "congestion_stats.csv",
                "traffic_enriched.csv",
            }
            self.assertEqual({path.name for path in output_path.iterdir()}, expected_outputs)

            with (output_path / "traffic_enriched.csv").open(
                newline="", encoding="utf-8"
            ) as dataset:
                enriched = list(csv.DictReader(dataset))
            self.assertEqual(len(enriched), 3)
            self.assertEqual(
                {row["congestion_level"] for row in enriched},
                {"High", "Medium", "Low"},
            )

            with (output_path / "location_stats.csv").open(
                newline="", encoding="utf-8"
            ) as dataset:
                locations = list(csv.DictReader(dataset))
            self.assertEqual(len(locations), 2)
            self.assertTrue(
                all(row["avg_congestion_level"] in {"High", "Medium", "Low"} for row in locations)
            )

            with (output_path / "daily_stats.csv").open(
                newline="", encoding="utf-8"
            ) as dataset:
                daily = list(csv.DictReader(dataset))
            self.assertEqual(len(daily), 2)
            self.assertEqual(sum(int(row["record_count"]) for row in daily), 3)


if __name__ == "__main__":
    unittest.main()
