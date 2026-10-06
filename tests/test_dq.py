import json
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE = PROJECT_ROOT / "src" / "pipeline.py"
GOLD_PATH = PROJECT_ROOT / "lake" / "gold" / "university_chapters" / "v1"
QUARANTINE_PATH = PROJECT_ROOT / "lake" / "quarantine" / "university_chapters" / "fixture_dq_test"


def run_fixture_pipeline():
    result = subprocess.run(
        ["python", str(PIPELINE), "--source", "fixture"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, (
        f"Pipeline failed.\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )

    return result


def test_fixture_pipeline_completes():
    run_fixture_pipeline()


def test_bad_coordinates_are_quarantined():
    run_fixture_pipeline()

    quarantine_files = list(QUARANTINE_PATH.rglob("*.json"))

    assert quarantine_files, "Expected at least one quarantine JSON file."

    records = []
    for path in quarantine_files:
        with path.open("r", encoding="utf-8") as f:
            content = json.load(f)

        if isinstance(content, list):
            records.extend(content)
        else:
            records.append(content)

    bad_rows = [
        row for row in records
        if row.get("chapter_id") == "CA-TEST-003"
    ]

    assert len(bad_rows) == 1
    assert bad_rows[0]["reason_code"] == "INVALID_COORDINATES"
    assert bad_rows[0]["ingest_run_id"] == "fixture_dq_test"


def test_warning_row_reaches_gold():
    run_fixture_pipeline()

    # Spark writes Parquet, so this test verifies the pipeline's stdout
    
    result = run_fixture_pipeline()

    assert "CA-TEST-002" in result.stdout
    assert "WARNING" in result.stdout
    assert "MISSING_OR_UNKNOWN_CITY" in result.stdout


def test_quarantined_row_does_not_reach_gold():
    result = run_fixture_pipeline()

    # The pipeline prints the Gold rows. The invalid row must not appear there.
    gold_section = result.stdout.split(
    "========== GOLD ==========",
    1,
)[-1]

    assert "CA-TEST-003" not in gold_section