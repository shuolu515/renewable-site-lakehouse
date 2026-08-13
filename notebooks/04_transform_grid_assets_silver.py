# Databricks notebook source
"""Standardize OSM grid proxies into typed Silver and quarantine tables."""

# COMMAND ----------

import re

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("ingestion_id", "4db321da-72b6-44ae-8d8f-6d3470e85f57")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
ingestion_id = dbutils.widgets.get("ingestion_id")


def validate_identifier(value: str) -> str:
    """Allow only simple Unity Catalog identifiers in generated SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value!r}")
    return value


for identifier in (catalog, bronze_schema, silver_schema):
    validate_identifier(identifier)

if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f-]{27}", ingestion_id):
    raise ValueError("ingestion_id must be a lowercase UUID")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

bronze_table = f"{catalog}.{bronze_schema}.grid_assets_raw"
silver_table = f"{catalog}.{silver_schema}.grid_assets"
quarantine_table = f"{catalog}.{silver_schema}.invalid_grid_assets"

bronze_batch = spark.table(bronze_table).filter(F.col("ingestion_id") == ingestion_id)
bronze_count = bronze_batch.count()
if bronze_count == 0:
    raise ValueError(f"No Bronze grid rows found for ingestion {ingestion_id}")

print(f"Bronze input: {bronze_table} ({bronze_count} rows)")
print(f"Silver output: {silver_table}")
print(f"Quarantine output: {quarantine_table}")

# COMMAND ----------

geometry_schema = T.ArrayType(
    T.StructType(
        [
            T.StructField("lat", T.DoubleType()),
            T.StructField("lon", T.DoubleType()),
        ]
    )
)
bbox_schema = T.ArrayType(T.DoubleType())


def transform_grid_assets(bronze: DataFrame) -> DataFrame:
    """Parse OSM elements and attach deterministic data-quality errors."""

    raw_type = F.get_json_object("raw_element_json", "$.type")
    raw_id = F.get_json_object("raw_element_json", "$.id").cast("long")
    raw_power_tag = F.get_json_object("raw_element_json", "$.tags.power")
    node_lat = F.get_json_object("raw_element_json", "$.lat").cast("double")
    node_lon = F.get_json_object("raw_element_json", "$.lon").cast("double")
    geometry_points = F.from_json(
        F.get_json_object("raw_element_json", "$.geometry"), geometry_schema
    )
    bbox = F.from_json("request_bbox_json", bbox_schema)

    parsed = bronze.select(
        "ingestion_id",
        F.col("element_key").alias("bronze_element_key"),
        F.concat(raw_type, F.lit("/"), raw_id.cast("string")).alias("grid_asset_id"),
        raw_type.alias("parsed_osm_element_type"),
        raw_id.alias("parsed_osm_element_id"),
        raw_power_tag.alias("source_power_tag"),
        F.when(raw_power_tag == "minor_line", F.lit("line"))
        .otherwise(raw_power_tag)
        .alias("asset_type"),
        F.get_json_object("raw_element_json", "$.tags.voltage").alias("voltage_raw"),
        F.get_json_object("raw_element_json", "$.tags.operator").alias("operator_name"),
        F.get_json_object("raw_element_json", "$.tags.name").alias("asset_name"),
        node_lat.alias("node_lat"),
        node_lon.alias("node_lon"),
        geometry_points.alias("geometry_points"),
        bbox.alias("request_bbox"),
        "osm_element_type",
        "osm_element_id",
        "power_tag",
        "capacity_status",
        "source_id",
        "source_license",
        "source_license_url",
        "source_attribution",
        "payload_sha256",
        "source_file",
        "source_completed_at",
        F.col("loaded_at").alias("bronze_loaded_at"),
        "raw_element_json",
    )

    enriched = (
        parsed.withColumn("geometry_point_count", F.size("geometry_points"))
        .withColumn(
            "way_centroid_lat",
            F.expr(
                "aggregate(geometry_points, cast(0.0 as double), "
                "(total, point) -> total + point.lat) / size(geometry_points)"
            ),
        )
        .withColumn(
            "way_centroid_lon",
            F.expr(
                "aggregate(geometry_points, cast(0.0 as double), "
                "(total, point) -> total + point.lon) / size(geometry_points)"
            ),
        )
        .withColumn(
            "centroid_lat",
            F.when(F.col("parsed_osm_element_type") == "node", F.col("node_lat")).otherwise(
                F.col("way_centroid_lat")
            ),
        )
        .withColumn(
            "centroid_lon",
            F.when(F.col("parsed_osm_element_type") == "node", F.col("node_lon")).otherwise(
                F.col("way_centroid_lon")
            ),
        )
        .withColumn(
            "way_coordinates",
            F.transform("geometry_points", lambda point: F.array(point.lon, point.lat)),
        )
        .withColumn(
            "geometry_type",
            F.when(F.col("parsed_osm_element_type") == "node", F.lit("Point"))
            .when(F.col("source_power_tag").isin("line", "minor_line"), F.lit("LineString"))
            .when(F.col("parsed_osm_element_type") == "way", F.lit("Polygon")),
        )
        .withColumn(
            "geometry_json",
            F.when(
                F.col("geometry_type") == "Point",
                F.to_json(
                    F.struct(
                        F.lit("Point").alias("type"),
                        F.array("node_lon", "node_lat").alias("coordinates"),
                    )
                ),
            )
            .when(
                F.col("geometry_type") == "LineString",
                F.to_json(
                    F.struct(
                        F.lit("LineString").alias("type"),
                        F.col("way_coordinates").alias("coordinates"),
                    )
                ),
            )
            .when(
                F.col("geometry_type") == "Polygon",
                F.to_json(
                    F.struct(
                        F.lit("Polygon").alias("type"),
                        F.array("way_coordinates").alias("coordinates"),
                    )
                ),
            ),
        )
        .withColumn(
            "voltage_level_kv",
            F.expr(
                "array_max(transform(split(regexp_replace(voltage_raw, ' ', ''), ';'), "
                "value -> try_cast(value as double) / 1000.0))"
            ),
        )
        .withColumn("request_min_lon", F.element_at("request_bbox", 1))
        .withColumn("request_min_lat", F.element_at("request_bbox", 2))
        .withColumn("request_max_lon", F.element_at("request_bbox", 3))
        .withColumn("request_max_lat", F.element_at("request_bbox", 4))
    )

    duplicate_window = Window.partitionBy("ingestion_id", "grid_asset_id")
    checked = enriched.withColumn(
        "asset_key_count", F.count(F.lit(1)).over(duplicate_window)
    ).withColumn(
        "quality_errors_raw",
        F.array(
            F.when(F.col("grid_asset_id").isNull(), F.lit("missing_grid_asset_id")),
            F.when(
                F.col("grid_asset_id").isNull()
                | (F.col("grid_asset_id") != F.col("bronze_element_key")),
                F.lit("source_key_mismatch"),
            ),
            F.when(F.col("asset_key_count") > 1, F.lit("duplicate_grid_asset_id")),
            F.when(
                ~F.col("parsed_osm_element_type").isin("node", "way"),
                F.lit("unsupported_osm_element_type"),
            ),
            F.when(
                ~F.col("source_power_tag").isin("substation", "transformer", "line", "minor_line"),
                F.lit("unsupported_power_tag"),
            ),
            F.when(
                (F.col("parsed_osm_element_type") == "node")
                & (F.col("node_lat").isNull() | F.col("node_lon").isNull()),
                F.lit("missing_geometry"),
            ),
            F.when(
                (F.col("parsed_osm_element_type") == "way") & (F.col("geometry_point_count") < 2),
                F.lit("insufficient_geometry_points"),
            ),
            F.when(
                F.col("voltage_raw").isNotNull()
                & (F.col("voltage_level_kv").isNull() | (F.col("voltage_level_kv") <= 0)),
                F.lit("invalid_voltage"),
            ),
            F.when(
                F.col("centroid_lat").isNull()
                | F.col("centroid_lon").isNull()
                | (F.col("centroid_lat") < F.col("request_min_lat"))
                | (F.col("centroid_lat") > F.col("request_max_lat"))
                | (F.col("centroid_lon") < F.col("request_min_lon"))
                | (F.col("centroid_lon") > F.col("request_max_lon")),
                F.lit("centroid_outside_request_bbox"),
            ),
            F.when(
                F.col("capacity_status") != "unknown",
                F.lit("unexpected_capacity_status"),
            ),
            F.when(
                (F.col("parsed_osm_element_type") != F.col("osm_element_type"))
                | (F.col("parsed_osm_element_id") != F.col("osm_element_id"))
                | (F.col("source_power_tag") != F.col("power_tag")),
                F.lit("bronze_source_mismatch"),
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
        .withColumn("transformed_at", F.current_timestamp())
        .drop("quality_errors_raw")
    )


checked_batch = transform_grid_assets(bronze_batch)

# COMMAND ----------

# Exercise failure paths in memory without writing synthetic records to project tables.
invalid_json = """{
  "type":"relation","tags":{"power":"plant","voltage":"invalid"},"lat":999,"lon":999
}"""
synthetic_bad = (
    bronze_batch.limit(1)
    .withColumn("element_key", F.lit("node/999"))
    .withColumn("raw_element_json", F.lit(invalid_json))
    .withColumn("capacity_status", F.lit("available"))
)
synthetic_result = transform_grid_assets(synthetic_bad.unionByName(synthetic_bad)).first()
expected_errors = {
    "missing_grid_asset_id",
    "source_key_mismatch",
    "duplicate_grid_asset_id",
    "unsupported_osm_element_type",
    "unsupported_power_tag",
    "invalid_voltage",
    "centroid_outside_request_bbox",
    "unexpected_capacity_status",
    "bronze_source_mismatch",
}
assert expected_errors.issubset(set(synthetic_result["quality_errors"]))

# COMMAND ----------

valid_batch = checked_batch.filter(F.col("quality_status") == "passed").select(
    "ingestion_id",
    "grid_asset_id",
    F.col("parsed_osm_element_type").alias("osm_element_type"),
    F.col("parsed_osm_element_id").alias("osm_element_id"),
    "asset_type",
    "source_power_tag",
    "geometry_type",
    "geometry_json",
    "centroid_lat",
    "centroid_lon",
    "voltage_raw",
    "voltage_level_kv",
    "operator_name",
    "asset_name",
    "capacity_status",
    "source_id",
    "source_license",
    "source_license_url",
    "source_attribution",
    "payload_sha256",
    "source_file",
    "source_completed_at",
    "bronze_loaded_at",
    "quality_status",
    "transformed_at",
)

invalid_batch = checked_batch.filter(F.col("quality_status") == "failed").select(
    "ingestion_id",
    "bronze_element_key",
    "grid_asset_id",
    "quality_errors",
    "raw_element_json",
    "source_id",
    "source_file",
    "payload_sha256",
    "bronze_loaded_at",
    F.col("transformed_at").alias("quarantined_at"),
)

valid_count = valid_batch.count()
invalid_count = invalid_batch.count()
assert valid_count + invalid_count == bronze_count

display(valid_batch.limit(10))
display(invalid_batch.limit(10))

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
        ingestion_id STRING NOT NULL,
        grid_asset_id STRING NOT NULL,
        osm_element_type STRING NOT NULL,
        osm_element_id BIGINT NOT NULL,
        asset_type STRING NOT NULL,
        source_power_tag STRING NOT NULL,
        geometry_type STRING NOT NULL,
        geometry_json STRING NOT NULL,
        centroid_lat DOUBLE NOT NULL,
        centroid_lon DOUBLE NOT NULL,
        voltage_raw STRING,
        voltage_level_kv DOUBLE,
        operator_name STRING,
        asset_name STRING,
        capacity_status STRING NOT NULL,
        source_id STRING NOT NULL,
        source_license STRING NOT NULL,
        source_license_url STRING NOT NULL,
        source_attribution STRING NOT NULL,
        payload_sha256 STRING NOT NULL,
        source_file STRING NOT NULL,
        source_completed_at TIMESTAMP,
        bronze_loaded_at TIMESTAMP NOT NULL,
        quality_status STRING NOT NULL,
        transformed_at TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'quality' = 'silver',
        'data_boundary' = 'OSM grid assets are screening proxies, not capacity evidence'
    )
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {quarantine_table} (
        ingestion_id STRING NOT NULL,
        bronze_element_key STRING NOT NULL,
        grid_asset_id STRING,
        quality_errors ARRAY<STRING> NOT NULL,
        raw_element_json STRING NOT NULL,
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

valid_batch.createOrReplaceTempView("valid_grid_assets_batch")
invalid_batch.createOrReplaceTempView("invalid_grid_assets_batch")

spark.sql(
    f"""
    MERGE INTO {silver_table} AS target
    USING valid_grid_assets_batch AS source
      ON target.ingestion_id = source.ingestion_id
     AND target.grid_asset_id = source.grid_asset_id
    WHEN NOT MATCHED THEN INSERT *
    """
)

spark.sql(
    f"""
    MERGE INTO {quarantine_table} AS target
    USING invalid_grid_assets_batch AS source
      ON target.ingestion_id = source.ingestion_id
     AND target.bronze_element_key = source.bronze_element_key
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
    spark.table(silver_table)
    .filter(F.col("ingestion_id") == ingestion_id)
    .groupBy("asset_type", "source_power_tag", "geometry_type")
    .agg(
        F.count("*").alias("asset_count"),
        F.count("voltage_level_kv").alias("assets_with_voltage"),
    )
    .orderBy(F.desc("asset_count"), "source_power_tag")
)

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
