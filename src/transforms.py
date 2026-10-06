from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def flatten_features(raw: DataFrame) -> DataFrame:
    """
    Flatten the raw ArcGIS FeatureServer response into
    one row per chapter.
    """

    rows = (
        raw
        .select(F.explode("features").alias("feature"))
        .select(
            "feature.attributes",
            "feature.geometry",
        )
    )

    silver = (
        rows
        .select(
            F.col("attributes.ChapterID").alias("chapter_id"),
            F.col("attributes.University_Chapter").alias("chapter_name"),
            F.col("attributes.City").alias("city"),
            F.col("attributes.State").alias("state"),
            F.col("geometry.x").alias("longitude"),
            F.col("geometry.y").alias("latitude"),
        )
        .filter(
            F.col("state").isin("CA", "OR", "WA")
        )
    )

    # One row per chapter_id.
    window = Window.partitionBy("chapter_id").orderBy(
        F.col("chapter_id")
    )

    silver = (
        silver
        .withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop("_row_number")
    )

    return silver


def split_invalid_coordinates(
    silver: DataFrame,
    ingest_run_id: str,
) -> tuple[DataFrame, DataFrame]:
    """
    Apply DQ-Q1.

    Records with NULL or out-of-range coordinates are
    quarantined and removed from the valid dataset.
    """

    invalid_coordinates = (
        F.col("longitude").isNull()
        | F.col("latitude").isNull()
        | (F.col("longitude") < -180)
        | (F.col("longitude") > 180)
        | (F.col("latitude") < -90)
        | (F.col("latitude") > 90)
    )

    quarantine = (
        silver
        .filter(invalid_coordinates)
        .withColumn(
            "reason_code",
            F.lit("INVALID_COORDINATES"),
        )
        .withColumn(
            "ingest_run_id",
            F.lit(ingest_run_id),
        )
    )

    valid = silver.filter(~invalid_coordinates)

    return valid, quarantine


def apply_city_quality_warning(silver: DataFrame) -> DataFrame:
    """
    Apply DQ-W1.

    NULL, empty, or UNKNOWN city values receive a WARNING,
    but remain in the dataset.
    """

    bad_city = (
        F.col("city").isNull()
        | (F.trim(F.col("city")) == "")
        | (F.upper(F.trim(F.col("city"))) == "UNKNOWN")
    )

    return (
        silver
        .withColumn(
            "dq_status",
            F.when(
                bad_city,
                F.lit("WARNING"),
            ).otherwise(
                F.lit("OK"),
            ),
        )
        .withColumn(
            "dq_warnings",
            F.when(
                bad_city,
                F.array(
                    F.lit("MISSING_OR_UNKNOWN_CITY"),
                ),
            ).otherwise(
                F.array().cast("array<string>"),
            ),
        )
    )