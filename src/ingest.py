import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests


API_URL = (
    "https://services2.arcgis.com/5I7u4SJE1vUr79JC/"
    "arcgis/rest/services/UniversityChapters_Public/"
    "FeatureServer/0/query"
)

BRONZE_ROOT = Path("lake/bronze/university_chapters")


def fetch_data():
    """Fetch raw data from the ArcGIS FeatureServer."""

    params = {
        "where": "State IN ('CA','OR','WA')",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "json",
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def create_run_id():
    """Create a unique UTC-based run ID."""

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    unique_id = uuid.uuid4().hex[:8]

    return f"{timestamp}_{unique_id}"


def save_bronze(raw_response, run_id):
    """Save the raw API response and ingestion manifest."""

    run_directory = BRONZE_ROOT / run_id
    run_directory.mkdir(parents=True, exist_ok=False)

    response_path = run_directory / "response.json"
    manifest_path = run_directory / "manifest.json"

    with response_path.open("w", encoding="utf-8") as file:
        json.dump(raw_response, file, ensure_ascii=False, indent=2)

    manifest = {
        "run_id": run_id,
        "source": API_URL,
        "query": {
            "where": "State IN ('CA','OR','WA')",
            "outFields": "*",
            "returnGeometry": True,
            "format": "json",
        },
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "response_file": "response.json",
    }

    with manifest_path.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, ensure_ascii=False, indent=2)


def main():
    print("Starting Bronze ingestion...")

    raw_response = fetch_data()

    run_id = create_run_id()

    save_bronze(raw_response, run_id)

    print(f"Bronze ingestion completed.")
    print(f"Run ID: {run_id}")


if __name__ == "__main__":
    main()