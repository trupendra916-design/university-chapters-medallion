# University Chapters Medallion Pipeline

This project is a small data pipeline built with **Python and PySpark**.

It takes university chapter data from an **ArcGIS API**, stores the raw data, cleans and checks it, and finally creates a clean Gold dataset.

## What the project does

The pipeline follows this flow:

```text
ArcGIS API
    ↓
Bronze
    ↓
Silver
    ↓
Data Quality Checks
    ↓
 ┌───────────────┐
 ↓               ↓
Quarantine      Gold
```

### Bronze

Bronze keeps the original API response as raw JSON.

This means we always have the original data available if we need to check or rerun something.

### Silver

Silver converts the API response into simple rows.

It keeps these fields:

- chapter ID
- university chapter name
- city
- state
- longitude
- latitude

It also filters the data to:

- California (`CA`)
- Oregon (`OR`)
- Washington (`WA`)

Duplicate `chapter_id` values are removed.

### Data Quality

The pipeline has two data-quality rules.

**DQ-Q1 — Invalid coordinates**

If longitude or latitude is missing or outside the valid geographic range, the record is rejected and sent to Quarantine.

**DQ-W1 — Missing city**

If the city is empty, NULL, or `UNKNOWN`, the record is kept but marked as a warning.

For example:

```text
dq_status = WARNING
dq_warnings = ["MISSING_OR_UNKNOWN_CITY"]
```

## Gold Dataset

The final Gold dataset is stored at:

```text
lake/gold/university_chapters/v1
```

It contains:

```text
chapter_id
chapter_name
city
state
longitude
latitude
dq_status
dq_warnings
```

Invalid coordinate records do not reach Gold.

## Test Data

A small test fixture is included:

```text
fixtures/university_chapters_bad_rows.json
```

It contains:

- one valid row
- one row with an unknown city
- one row with invalid coordinates

This allows the data-quality rules to be demonstrated even when the live API contains only valid data.

## How to Run

### 1. Activate the virtual environment

```powershell
.venv\Scripts\Activate.ps1
```

### 2. Set Hadoop environment variables

Because this project was tested on Windows, run:

```powershell
$env:HADOOP_HOME = (Resolve-Path ".\hadoop").Path
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
```

### 3. Run the API ingestion

```powershell
python src\ingest.py
```

This downloads the API response and saves it in the Bronze layer.

### 4. Run the pipeline

```powershell
python src\pipeline.py
```

This processes the latest Bronze data and creates Silver, Quarantine, and Gold outputs.

## Test the Data Quality Rules

Run:

```powershell
python src\pipeline.py --source fixture
```

The expected result is:

```text
CA-TEST-001 → OK → Gold
CA-TEST-002 → WARNING → Gold
CA-TEST-003 → INVALID_COORDINATES → Quarantine
```

After running the fixture, restore the real Gold data by running:

```powershell
python src\pipeline.py
```

## Automated Tests

Run:

```powershell
python -m pytest -vv
```

The project currently has four tests covering:

- fixture pipeline completion
- invalid coordinates going to Quarantine
- warning records reaching Gold
- quarantined records not reaching Gold

Current result:

```text
4 passed
```

## Project Structure

```text
university-chapters-medallion/
│
├── README.md
├── requirements.txt
├── .gitignore
├── data_product_contract.md
│
├── docs/
│   └── architecture.md
│
├── fixtures/
│   └── university_chapters_bad_rows.json
│
├── src/
│   ├── ingest.py
│   ├── transforms.py
│   └── pipeline.py
│
├── tests/
│   └── test_dq.py
│
└── lake/
    ├── bronze/
    ├── silver/
    ├── quarantine/
    └── gold/
```

## Final Result

The project demonstrates a simple and reproducible **Bronze → Silver → Gold data pipeline** with clear data-quality handling.

The live pipeline successfully processes the university chapter data, and the automated test suite passes all four tests.