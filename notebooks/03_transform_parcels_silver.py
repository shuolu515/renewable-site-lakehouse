# Databricks notebook source
"""Parse parcel Bronze records into typed Silver and quarantine tables."""

# COMMAND ----------

import json
import math
import re

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("ingestion_id", "98c5af3d-dc45-481f-bebf-9a6d7979672e")
dbutils.widgets.text("minimum_area_m2", "20000")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
ingestion_id = dbutils.widgets.get("ingestion_id")
minimum_area_m2 = float(dbutils.widgets.get("minimum_area_m2"))


def validate_identifier(value: str) -> str:
    """Allow only simple Unity Catalog identifiers in generated SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value!r}")
    return value


for identifier in (catalog, bronze_schema, silver_schema):
    validate_identifier(identifier)

if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f-]{27}", ingestion_id):
    raise ValueError("ingestion_id must be a lowercase UUID")
if minimum_area_m2 <= 0:
    raise ValueError("minimum_area_m2 must be positive")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

bronze_table = f"{catalog}.{bronze_schema}.parcels_raw"
silver_table = f"{catalog}.{silver_schema}.parcels"
quarantine_table = f"{catalog}.{silver_schema}.invalid_parcels"

bronze_batch = spark.table(bronze_table).filter(F.col("ingestion_id") == ingestion_id)
bronze_count = bronze_batch.count()
if bronze_count == 0:
    raise ValueError(f"No Bronze parcel rows found for ingestion {ingestion_id}")

print(f"Bronze input: {bronze_table} ({bronze_count} rows)")
print(f"Silver output: {silver_table}")
print(f"Quarantine output: {quarantine_table}")

# COMMAND ----------


def normalize_geometry_json(raw_geometry: str) -> str | None:
    """Convert string-valued GeoJSON coordinates to JSON numbers."""
    try:
        geometry = json.loads(raw_geometry)

        def convert(value):
            if isinstance(value, list):
                return [convert(item) for item in value]
            if isinstance(value, str) and value.strip().startswith("["):
                return convert(json.loads(value))
            return float(value)

        geometry["coordinates"] = convert(geometry["coordinates"])
        return json.dumps(geometry, separators=(",", ":"))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


normalize_geometry_json_udf = F.udf(normalize_geometry_json, StringType())


def geometry_bbox_center(raw_geometry: str) -> list[float] | None:
    """Return [latitude, longitude] for the centre of a geometry bounding box."""
    try:
        normalized_geometry = normalize_geometry_json(raw_geometry)
        geometry = json.loads(normalized_geometry)
        points = []

        def collect(value):
            if (
                isinstance(value, list)
                and len(value) >= 2
                and all(isinstance(item, (int, float)) for item in value[:2])
            ):
                points.append((float(value[0]), float(value[1])))
                return
            if isinstance(value, list):
                for item in value:
                    collect(item)

        collect(geometry["coordinates"])
        if not points:
            return None
        longitudes = [point[0] for point in points]
        latitudes = [point[1] for point in points]
        return [
            (min(latitudes) + max(latitudes)) / 2,
            (min(longitudes) + max(longitudes)) / 2,
        ]
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


geometry_bbox_center_udf = F.udf(
    geometry_bbox_center,
    ArrayType(DoubleType()),
)


def transform_parcels(bronze: DataFrame) -> DataFrame:
    """Parse source fields and attach deterministic data-quality errors."""

    properties = "$.properties"
    parcel_id = F.trim(F.get_json_object("raw_feature_json", f"{properties}.localId"))
    area_m2 = F.get_json_object("raw_feature_json", f"{properties}.areaValue").cast("double")
    area_uom = F.get_json_object("raw_feature_json", f"{properties}.areaValue_uom")
    position = F.trim(F.get_json_object("raw_feature_json", f"{properties}.pos"))
    position_parts = F.split(position, r"\s+")
    source_centroid_lat = F.element_at(position_parts, 1).cast("double")
    source_centroid_lon = F.element_at(position_parts, 2).cast("double")
    raw_geometry = F.get_json_object("raw_feature_json", "$.geometry")
    geometry_json = normalize_geometry_json_udf(raw_geometry)
    fallback_center = geometry_bbox_center_udf(raw_geometry)
    centroid_lat = F.coalesce(source_centroid_lat, F.element_at(fallback_center, 1))
    centroid_lon = F.coalesce(source_centroid_lon, F.element_at(fallback_center, 2))

    parsed = bronze.select(
        "ingestion_id",
        parcel_id.alias("parcel_id"),
        F.col("feature_id").alias("bronze_feature_id"),
        F.get_json_object("raw_feature_json", f"{properties}.nationalCadastralReference").alias(
            "national_cadastral_reference"
        ),
        F.get_json_object("raw_feature_json", f"{properties}.label").alias("parcel_label"),
        area_m2.alias("source_area_m2"),
        area_uom.alias("source_area_uom"),
        F.col("geometry_type"),
        geometry_json.alias("geometry_json"),
        centroid_lat.alias("centroid_lat"),
        centroid_lon.alias("centroid_lon"),
        "source_id",
        "source_license",
        "source_license_url",
        "payload_sha256",
        "source_file",
        "source_completed_at",
        F.col("loaded_at").alias("bronze_loaded_at"),
        "raw_feature_json",
    )
    parsed = parsed.withColumn(
        "geometry_is_valid",
        F.expr("st_isvalid(st_geomfromgeojson(geometry_json))"),
    )

    duplicate_window = Window.partitionBy("ingestion_id", "parcel_id")
    checked = parsed.withColumn(
        "parcel_key_count", F.count(F.lit(1)).over(duplicate_window)
    ).withColumn(
        "quality_errors_raw",
        F.array(
            F.when(
                F.col("parcel_id").isNull() | (F.length("parcel_id") == 0),
                F.lit("missing_parcel_id"),
            ),
            F.when(
                F.col("parcel_id") != F.col("bronze_feature_id"),
                F.lit("source_key_mismatch"),
            ),
            F.when(F.col("parcel_key_count") > 1, F.lit("duplicate_parcel_id")),
            F.when(F.col("source_area_m2").isNull(), F.lit("missing_area")),
            F.when(F.col("source_area_m2") <= 0, F.lit("non_positive_area")),
            F.when(F.col("source_area_uom") != "m2", F.lit("unexpected_area_unit")),
            F.when(
                ~F.col("geometry_type").isin("Polygon", "MultiPolygon"),
                F.lit("unsupported_geometry_type"),
            ),
            F.when(F.col("geometry_json").isNull(), F.lit("missing_geometry")),
            F.when(
                F.col("geometry_json").isNotNull()
                & (~F.coalesce(F.col("geometry_is_valid"), F.lit(False))),
                F.lit("invalid_geometry_topology"),
            ),
            F.when(
                F.col("centroid_lat").isNull()
                | F.col("centroid_lon").isNull()
                | (~F.col("centroid_lat").between(-90, 90))
                | (~F.col("centroid_lon").between(-180, 180)),
                F.lit("invalid_centroid_coordinates"),
            ),
        ),
    )

    return (
        checked.withColumn(
            "quality_errors",
            F.expr("filter(quality_errors_raw, error -> error is not null)"),
        )
        .withColumn(
            "quality_status",
            F.when(F.size("quality_errors") == 0, F.lit("passed")).otherwise(F.lit("failed")),
        )
        .withColumn(
            "meets_minimum_area",
            F.col("source_area_m2") >= F.lit(minimum_area_m2),
        )
        .withColumn("land_use", F.lit("unknown"))
        .withColumn("transformed_at", F.current_timestamp())
        .drop("quality_errors_raw")
    )


checked_batch = transform_parcels(bronze_batch)

# COMMAND ----------

# Exercise the quality rules with two intentionally invalid in-memory records. These rows are never
# written to a project table.
invalid_json = """{
  "type":"Feature",
  "properties":{"localId":"","areaValue":-1,"areaValue_uom":"ha","pos":"999 999"},
  "geometry":{"type":"Point","coordinates":[8.2,50.1]}
}"""
synthetic_bad = (
    bronze_batch.limit(1)
    .withColumn("feature_id", F.lit("synthetic-source-key"))
    .withColumn("geometry_type", F.lit("Point"))
    .withColumn("raw_feature_json", F.lit(invalid_json))
)
synthetic_result = transform_parcels(synthetic_bad.unionByName(synthetic_bad)).first()
expected_errors = {
    "missing_parcel_id",
    "source_key_mismatch",
    "duplicate_parcel_id",
    "non_positive_area",
    "unexpected_area_unit",
    "unsupported_geometry_type",
    "invalid_centroid_coordinates",
}
assert expected_errors.issubset(set(synthetic_result["quality_errors"]))
normalized_geometry = json.loads(
    normalize_geometry_json('{"type":"Point","coordinates":["8.2","50.1"]}')
)
assert all(isinstance(value, float) for value in normalized_geometry["coordinates"])
normalized_nested_geometry = json.loads(
    normalize_geometry_json('{"type":"MultiPolygon","coordinates":[[["[8.2,50.1]","[8.3,50.2]"]]]}')
)
assert normalized_nested_geometry["coordinates"][0][0][0] == [8.2, 50.1]
calculated_bbox_center = geometry_bbox_center(
    '{"type":"MultiPolygon","coordinates":[[[[8.2,50.1],[8.4,50.3]]]]}'
)
assert calculated_bbox_center is not None
assert all(
    math.isclose(actual, expected, rel_tol=0, abs_tol=1e-9)
    for actual, expected in zip(calculated_bbox_center, [50.2, 8.3], strict=True)
)

# COMMAND ----------

valid_batch = checked_batch.filter(F.col("quality_status") == "passed").select(
    "ingestion_id",
    "parcel_id",
    "national_cadastral_reference",
    "parcel_label",
    "source_area_m2",
    "source_area_uom",
    "geometry_type",
    "geometry_json",
    "centroid_lat",
    "centroid_lon",
    "land_use",
    "meets_minimum_area",
    "source_id",
    "source_license",
    "source_license_url",
    "payload_sha256",
    "source_file",
    "source_completed_at",
    "bronze_loaded_at",
    "quality_status",
    "transformed_at",
)

invalid_batch = checked_batch.filter(F.col("quality_status") == "failed").select(
    "ingestion_id",
    "bronze_feature_id",
    "parcel_id",
    "quality_errors",
    "raw_feature_json",
    "source_id",
    "source_file",
    "payload_sha256",
    "bronze_loaded_at",
    F.col("transformed_at").alias("quarantined_at"),
)

valid_count = valid_batch.count()
invalid_count = invalid_batch.count()
assert valid_count + invalid_count == bronze_count, "Silver accounting does not match Bronze input"

display(valid_batch.limit(10))
display(invalid_batch.limit(10))
display(
    invalid_batch.select(F.explode("quality_errors").alias("quality_error"))
    .groupBy("quality_error")
    .count()
    .orderBy(F.desc("count"), "quality_error")
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
        ingestion_id STRING NOT NULL,
        parcel_id STRING NOT NULL,
        national_cadastral_reference STRING,
        parcel_label STRING,
        source_area_m2 DOUBLE NOT NULL,
        source_area_uom STRING NOT NULL,
        geometry_type STRING NOT NULL,
        geometry_json STRING NOT NULL,
        centroid_lat DOUBLE NOT NULL,
        centroid_lon DOUBLE NOT NULL,
        land_use STRING NOT NULL,
        meets_minimum_area BOOLEAN NOT NULL,
        source_id STRING NOT NULL,
        source_license STRING NOT NULL,
        source_license_url STRING NOT NULL,
        payload_sha256 STRING NOT NULL,
        source_file STRING NOT NULL,
        source_completed_at TIMESTAMP,
        bronze_loaded_at TIMESTAMP NOT NULL,
        quality_status STRING NOT NULL,
        transformed_at TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES ('quality' = 'silver')
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {quarantine_table} (
        ingestion_id STRING NOT NULL,
        bronze_feature_id STRING,
        parcel_id STRING,
        quality_errors ARRAY<STRING> NOT NULL,
        raw_feature_json STRING NOT NULL,
        source_id STRING NOT NULL,
        source_file STRING NOT NULL,
        payload_sha256 STRING NOT NULL,
        bronze_loaded_at TIMESTAMP NOT NULL,
        quarantined_at TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES ('quality' = 'quarantine')
    """
)

# COMMAND ----------

valid_batch.createOrReplaceTempView("valid_parcels_batch")
invalid_batch.createOrReplaceTempView("invalid_parcels_batch")

spark.sql(
    f"""
    MERGE INTO {silver_table} AS target
    USING valid_parcels_batch AS source
      ON target.ingestion_id = source.ingestion_id
     AND target.parcel_id = source.parcel_id
    WHEN MATCHED AND target.geometry_json <> source.geometry_json THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

spark.sql(
    f"""
    MERGE INTO {silver_table} AS target
    USING invalid_parcels_batch AS source
      ON target.ingestion_id = source.ingestion_id
     AND target.parcel_id = source.parcel_id
    WHEN MATCHED THEN DELETE
    """
)

spark.sql(
    f"""
    MERGE INTO {quarantine_table} AS target
    USING valid_parcels_batch AS source
      ON target.ingestion_id = source.ingestion_id
     AND target.bronze_feature_id = source.parcel_id
    WHEN MATCHED THEN DELETE
    """
)

spark.sql(
    f"""
    MERGE INTO {quarantine_table} AS target
    USING invalid_parcels_batch AS source
      ON target.ingestion_id = source.ingestion_id
     AND target.bronze_feature_id = source.bronze_feature_id
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------

run_result = spark.sql(
    f"""
    WITH silver_counts AS (
        SELECT COUNT(*) AS valid_count
        FROM {silver_table}
        WHERE ingestion_id = '{ingestion_id}'
    ), quarantine_counts AS (
        SELECT COUNT(*) AS invalid_count
        FROM {quarantine_table}
        WHERE ingestion_id = '{ingestion_id}'
    )
    SELECT
        {bronze_count} AS bronze_count,
        valid_count,
        invalid_count,
        valid_count + invalid_count AS accounted_count
    FROM silver_counts CROSS JOIN quarantine_counts
    """
)

display(run_result)
result = run_result.first()
assert result["accounted_count"] == bronze_count

# COMMAND ----------

display(
    checked_batch.select(F.explode_outer("quality_errors").alias("quality_error"))
    .filter(F.col("quality_error").isNotNull())
    .groupBy("quality_error")
    .count()
    .orderBy(F.desc("count"), "quality_error")
)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {silver_table}"))
display(spark.sql(f"DESCRIBE HISTORY {quarantine_table}"))
