# University Chapters Pipeline Architecture

## What this project does

This project takes university chapter data from an ArcGIS API and processes it using PySpark.

The data goes through three main stages:

```text
API
 ↓
Bronze
 ↓
Silver
 ↓
Gold
```

There is also a separate **Quarantine** area for bad records.

---

## 1. Bronze

Bronze stores the data exactly as it comes from the API.

For example:

```text
lake/bronze/university_chapters/<run_id>/
```

The reason for keeping the raw data is simple: if something goes wrong later, we still have the original data.

---

## 2. Silver

Silver is where the raw API data is cleaned and changed into a simple table.

The pipeline:

- gets the chapter information from the API response
- extracts the chapter name, city, state, and coordinates
- keeps only CA, OR, and WA
- removes duplicate chapter IDs
- checks the data quality

The Silver data is stored here:

```text
lake/silver/university_chapters/<run_id>/
```

---

## 3. Data Quality

The project has two data-quality checks.

### Invalid coordinates

If latitude or longitude is missing or outside the valid range, the record is considered bad.

That record goes to **Quarantine**.

```text
reason_code = INVALID_COORDINATES
```

It does not go into Gold.

### Missing city

If the city is empty, NULL, or `UNKNOWN`, the record is not removed.

Instead, it gets:

```text
dq_status = WARNING
```

and:

```text
dq_warnings = ["MISSING_OR_UNKNOWN_CITY"]
```

The record can still go into Gold.

---

## 4. Quarantine

Quarantine is used for records that fail the hard data-quality check.

The records are stored separately:

```text
lake/quarantine/university_chapters/<run_id>/
```

This means bad records are not lost. We can look at them later and understand why they were rejected.

---

## 5. Gold

Gold contains the final data that can be used by downstream users.

It is stored here:

```text
lake/gold/university_chapters/v1/
```

Gold contains:

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

Normal records have:

```text
dq_status = OK
```

Records with a missing or unknown city can have:

```text
dq_status = WARNING
```

Records with invalid coordinates are not included in Gold.

---

## 6. Test Data

The project also has a small test file:

```text
fixtures/university_chapters_bad_rows.json
```

It contains three examples:

```text
CA-TEST-001 → valid record
CA-TEST-002 → unknown city
CA-TEST-003 → invalid coordinates
```

This allows us to test both data-quality rules.

---

## 7. Testing

The project uses Pytest.

The tests check that:

- the pipeline runs successfully
- bad coordinates go to Quarantine
- warning records reach Gold
- quarantined records do not reach Gold

The final test result is:

```text
4 passed
```

---

## Simple Summary

In simple terms:

> **Bronze keeps the original data, Silver cleans and checks it, Quarantine keeps bad records, and Gold contains the final usable data.**