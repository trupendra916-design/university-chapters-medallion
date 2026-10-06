import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transforms import (
    flatten_features,
    split_invalid_coordinates,
    apply_city_quality_warning,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_ROOT = (
    PROJECT_ROOT
    / "lake"
    / "bronze"
    / "university_chapters"
)

SILVER_ROOT = (
    PROJECT_ROOT
    / "lake"
    / "silver"
    / "university_chapters"
)

GOLD_ROOT = (
    PROJECT_ROOT
    / "lake"
    / "gold"
    / "university_chapters"
    / "v1"
)

QUARANTINE_ROOT = (
    PROJECT_ROOT
    / "lake"
    / "quarantine"
    / "university_chapters"
)
METRICS_ROOT = (
    PROJECT_ROOT
    / "lake"
    / "metrics"
    / "university_chapters"
)
FIXTURE_PATH = (
    PROJECT_ROOT
    / "fixtures"
    / "university_chapters_bad_rows.json"
)
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="University Chapters Bronze-to-Gold pipeline."
    )

    parser.add_argument(
        "--source",
        choices=["latest-bronze", "fixture"],
        default="latest-bronze",
        help="Input source for the pipeline.",
    )

    return parser.parse_args()

def find_latest_bronze_run():
    """Find the most recent Bronze ingestion run."""

    if not BRONZE_ROOT.exists():
        raise FileNotFoundError(
            f"Bronze directory does not exist: {BRONZE_ROOT}"
        )

    runs = sorted(
        path
        for path in BRONZE_ROOT.iterdir()
        if path.is_dir()
    )

    if not runs:
        raise FileNotFoundError(
            "No Bronze ingestion runs were found."
        )

    return runs[-1]


def read_run_id(run_directory):
    """Read the run_id from the Bronze manifest."""

    manifest_path = run_directory / "manifest.json"

    with manifest_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        manifest = json.load(file)

    return manifest["run_id"]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("UniversityChaptersPipeline")
        .master("local[*]")
        .config(
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.RawLocalFileSystem",
        )
        .config(
            "spark.hadoop.fs.permissions.umask-mode",
            "022",
        )
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )


def process_data(spark, raw, run_id):
    """
    Run the Silver and DQ transformations.
    """

    # ---------------------------------------------------------
    # Flatten + state filter + deduplication
    # ---------------------------------------------------------

    silver_base = flatten_features(raw)

    # ---------------------------------------------------------
    # DQ-Q1: Invalid coordinates
    # ---------------------------------------------------------

    valid, quarantine = split_invalid_coordinates(
        silver_base,
        run_id,
    )

    # ---------------------------------------------------------
    # DQ-W1: Missing / unknown city
    # ---------------------------------------------------------

    silver = apply_city_quality_warning(valid)

    return silver, quarantine

def calculate_and_write_metrics(raw, silver, quarantine, run_id):
    """Calculate and persist per-run DQ metrics."""

    rows_in = (
        raw
        .select(F.explode_outer("features").alias("feature"))
        .filter(F.col("feature").isNotNull())
        .count()
    )

    rows_quarantined = quarantine.count()

    rows_warned = (
        silver
        .filter(F.col("dq_status") == "WARNING")
        .count()
    )

    rows_ok = (
        silver
        .filter(F.col("dq_status") == "OK")
        .count()
    )

    metrics = {
        "run_id": run_id,
        "rows_in": rows_in,
        "rows_quarantined": rows_quarantined,
        "rows_warned": rows_warned,
        "rows_ok": rows_ok,
    }

    METRICS_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    metrics_path = METRICS_ROOT / f"{run_id}.json"

    with metrics_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            metrics,
            file,
            indent=2,
        )

    return metrics_path, metrics
def write_quarantine(quarantine, run_id):
    """Write hard-failure records to quarantine."""

    output_path = QUARANTINE_ROOT / run_id

    (
        quarantine
        .write
        .mode("overwrite")
        .json(str(output_path))
    )

    return output_path


def write_silver(silver, run_id):
    """Write the processed Silver dataset."""

    output_path = SILVER_ROOT / run_id

    (
        silver
        .write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    return output_path


def write_gold(silver):
    """Write the stable Gold data product."""

    gold = silver.select(
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "dq_status",
        "dq_warnings",
    )

    (
        gold
        .write
        .mode("overwrite")
        .parquet(str(GOLD_ROOT))
    )

    return GOLD_ROOT


def main():
    args = parse_arguments()

    print("Starting University Chapters pipeline...")
    print(f"Source: {args.source}")

    if args.source == "fixture":
        response_path = FIXTURE_PATH
        run_id = "fixture_dq_test"

        if not response_path.exists():
            raise FileNotFoundError(
                f"Fixture file does not exist: {response_path}"
            )
        
    else:
        bronze_run = find_latest_bronze_run()
        run_id = read_run_id(bronze_run)
        response_path = bronze_run / "response.json"

    print(f"Run ID: {run_id}")
    print(f"Reading: {response_path}")

    spark = create_spark_session()

    try:
        # -----------------------------------------------------
        # Read Bronze
        # -----------------------------------------------------

        raw = (
            spark.read
            .option("multiLine", "true")
            .json(str(response_path))
        )

        print("\nRaw Bronze schema:")
        raw.printSchema()

        # -----------------------------------------------------
        # Transform + DQ
        # -----------------------------------------------------

        silver, quarantine = process_data(
            spark,
            raw,
            run_id,
        )
        metrics_path, metrics = calculate_and_write_metrics(
    raw,
    silver,
    quarantine,
    run_id,
)

        print("\n========== DQ METRICS ==========")
        print(json.dumps(metrics, indent=2))
        print(f"\nMetrics written to:\n{metrics_path}")
        
        # -----------------------------------------------------
        # Write quarantine
        # -----------------------------------------------------

        quarantine_path = write_quarantine(
            quarantine,
            run_id,
        )

        print(
            f"\nQuarantine written to:\n"
            f"{quarantine_path}"
        )

        # -----------------------------------------------------
        # Write Silver
        # -----------------------------------------------------

        silver_path = write_silver(
            silver,
            run_id,
        )

        print(
            f"\nSilver written to:\n"
            f"{silver_path}"
        )

        # -----------------------------------------------------
        # Write Gold
        # -----------------------------------------------------

        gold_path = write_gold(silver)

        print(
            f"\nGold written to:\n"
            f"{gold_path}"
        )

        # -----------------------------------------------------
        # Display results
        # -----------------------------------------------------

        print("\n========== SILVER ==========")

        silver.show(
            truncate=False
        )

        print("\n========== QUARANTINE ==========")

        quarantine.show(
            truncate=False
        )

        print("\n========== GOLD ==========")

        (
            silver
            .select(
                "chapter_id",
                "chapter_name",
                "city",
                "state",
                "longitude",
                "latitude",
                "dq_status",
                "dq_warnings",
            )
            .show(truncate=False)
        )

    finally:
        spark.stop()

    print("\nPipeline completed successfully.")

if __name__ == "__main__":
    main()