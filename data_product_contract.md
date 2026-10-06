# University Chapters Gold Data Product Contract

## Data Product

**Name:** University Chapters

**Version:** v1

**Path:**

`lake/gold/university_chapters/v1/`

## Grain

One row per university chapter.

## Business Key

`chapter_id`

## Schema

| Column | Description |
|---|---|
| `chapter_id` | Unique identifier for the university chapter |
| `chapter_name` | University chapter name |
| `city` | City associated with the chapter |
| `state` | State abbreviation |
| `longitude` | WGS84 longitude |
| `latitude` | WGS84 latitude |
| `dq_status` | Data-quality status: `OK` or `WARNING` |
| `dq_warnings` | Array of data-quality warning codes |

## Allowed States

Only the following states are included:

- CA
- OR
- WA

## Data Quality

### DQ-Q1 — Invalid Coordinates

A record is quarantined when:

- `longitude` is NULL
- `latitude` is NULL
- `longitude < -180`
- `longitude > 180`
- `latitude < -90`
- `latitude > 90`

Quarantine reason:

`INVALID_COORDINATES`

Records failing DQ-Q1 are excluded from Silver and Gold consumer outputs.

### DQ-W1 — Missing or Unknown City

A record receives a warning when `city` is:

- NULL
- empty
- `UNKNOWN`, case-insensitive

The warning code is:

`MISSING_OR_UNKNOWN_CITY`

Warning records remain in Silver and Gold.

## DQ Status

| Condition | `dq_status` | Gold |
|---|---|---|
| All hard and soft checks pass | `OK` | Yes |
| City is missing/unknown | `WARNING` | Yes |
| Invalid coordinates | `QUARANTINE` | No |

## Source

ArcGIS FeatureServer University Chapters public layer.

The upstream source view already filters records to `Status = ACTIVE`; the pipeline does not duplicate this filter.

## Geometry

Source geometry is point geometry using WGS84.

ArcGIS geometry mapping:

- `x` → longitude
- `y` → latitude