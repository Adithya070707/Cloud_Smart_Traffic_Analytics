"""Generate a deterministic, realistic demo traffic dataset."""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = PROJECT_DIR / "data" / "traffic_data.csv"
ROAD_CAPACITIES = {
    "Ameerpet Junction": 120,
    "Hitech City": 100,
    "Charminar": 90,
    "Jubilee Hills Check Post": 150,
    "Kukatpally Junction": 80,
    "LB Nagar": 180,
}


def generate_dataset(rows, seed, output_path):
    if rows < 1:
        raise ValueError("The number of rows must be at least 1.")

    randomizer = random.Random(seed)
    start = datetime(2025, 1, 6)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as dataset:
        writer = csv.DictWriter(
            dataset,
            fieldnames=[
                "timestamp",
                "location",
                "vehicle_count",
                "avg_speed_kmh",
                "road_capacity",
            ],
        )
        writer.writeheader()

        for _ in range(rows):
            location = randomizer.choice(list(ROAD_CAPACITIES))
            hour = randomizer.randrange(24)
            minute = randomizer.randrange(60)
            timestamp = start + timedelta(
                days=randomizer.randrange(7), hours=hour, minutes=minute
            )
            capacity = ROAD_CAPACITIES[location]
            peak_multiplier = 1.35 if hour in (7, 8, 9, 16, 17, 18) else 0.72
            vehicle_count = round(
                min(
                    capacity * 1.25,
                    max(
                        5,
                        randomizer.gauss(capacity * 0.58 * peak_multiplier, capacity * 0.14),
                    ),
                )
            )
            utilization = vehicle_count / capacity
            speed = max(
                5,
                min(
                    80,
                    randomizer.gauss(68 - utilization * 48, 7),
                ),
            )

            writer.writerow(
                {
                    "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "location": location,
                    "vehicle_count": vehicle_count,
                    "avg_speed_kmh": round(speed, 1),
                    "road_capacity": capacity,
                }
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1000, help="Number of records to generate.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for repeatable data.")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="CSV output path (defaults to data/traffic_data.csv).",
    )
    args = parser.parse_args()

    generate_dataset(args.rows, args.seed, args.output)
    print(f"Generated {args.rows} traffic records: {args.output.resolve()}")


if __name__ == "__main__":
    main()
