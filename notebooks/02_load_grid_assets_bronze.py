# Databricks notebook source
"""Load one immutable OSM grid-proxy snapshot into a Bronze Delta table.

The table retains every raw OSM element and its provenance. Public OSM assets are proximity
proxies only, so capacity status remains ``unknown`` throughout the MVP.
"""

# COMMAND ----------

import json
import re

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("landing_schema", "landing")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("volume_name", "raw")
dbutils.widgets.text("ingestion_id", "4db321da-72b6-44ae-8d8f-6d3470e85f57")

catalog = dbutils.widgets.get("catalog")
landing_schema = dbutils.widgets.get("landing_schema")
bronze_schema = dbutils.widgets.get("bronze_schema")
volume_name = dbutils.widgets.get("volume_name")
ingestion_id = dbutils.widgets.get("ingestion_id")


def validate_identifier(value: str) -> str:
    """Allow only simple Unity Catalog identifiers in generated SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value!r}")
    return value


for identifier in (catalog, landing_schema, bronze_schema, volume_name):
    validate_identifier(identifier)

if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f-]{27}", ingestion_id):
    raise ValueError("ingestion_id must be a lowercase UUID")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{landing_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{landing_schema}.{volume_name}")

volume_root = f"/Volumes/{catalog}/{landing_schema}/{volume_name}"
snapshot_directory = f"{volume_root}/openstreetmap_overpass/{ingestion_id}"
raw_json_path = f"{snapshot_directory}/grid_assets.json"
manifest_path = f"{snapshot_directory}/manifest.json"
target_table = f"{catalog}.{bronze_schema}.grid_assets_raw"

dbutils.fs.mkdirs(snapshot_directory)
print(f"Upload directory: {snapshot_directory}")
print(f"Target table: {target_table}")

# COMMAND ----------

uploaded_files = {item.name.rstrip("/") for item in dbutils.fs.ls(snapshot_directory)}
required_files = {"grid_assets.json", "manifest.json"}
missing_files = required_files - uploaded_files
if missing_files:
    raise FileNotFoundError(
        f"Upload {sorted(missing_files)} to {snapshot_directory} before continuing."
    )

# COMMAND ----------

manifest_row = (
    spark.read.option("multiLine", True)
    .json(manifest_path)
    .select(
        "ingestion_id",
        "status",
        "source_id",
        "license",
        "license_url",
        "attribution",
        "contains_osm_contributor_metadata",
        "capacity_status_default",
        "completed_at",
        F.col("request.bbox").alias("request_bbox"),
        F.col("request.max_assets").alias("request_limit"),
        F.col("response.element_count").alias("expected_element_count"),
        F.col("response.sha256").alias("payload_sha256"),
    )
    .first()
)

if manifest_row is None:
    raise ValueError("manifest.json contains no record")

manifest = manifest_row.asDict()
if manifest["ingestion_id"] != ingestion_id:
    raise ValueError("Notebook ingestion_id does not match manifest.json")
if manifest["status"] != "succeeded":
    raise ValueError(f"Only succeeded ingestions can enter Bronze: {manifest['status']!r}")
if manifest["contains_osm_contributor_metadata"] is not False:
    raise ValueError("Bronze load stopped because contributor metadata was not excluded")
if manifest["capacity_status_default"] != "unknown":
    raise ValueError("OSM capacity status must remain unknown")

# COMMAND ----------

payload = spark.read.option("multiLine", True).json(raw_json_path)
request_bbox_json = json.dumps(manifest["request_bbox"])

bronze_batch = payload.select(F.explode("elements").alias("element")).select(
    F.lit(manifest["ingestion_id"]).alias("ingestion_id"),
    F.concat_ws("/", F.col("element.type"), F.col("element.id").cast("string")).alias(
        "element_key"
    ),
    F.col("element.type").alias("osm_element_type"),
    F.col("element.id").cast("long").alias("osm_element_id"),
    F.col("element.tags.power").alias("power_tag"),
    F.to_json("element").alias("raw_element_json"),
    F.lit(manifest["source_id"]).alias("source_id"),
    F.lit(manifest["license"]).alias("source_license"),
    F.lit(manifest["license_url"]).alias("source_license_url"),
    F.lit(manifest["attribution"]).alias("source_attribution"),
    F.lit(request_bbox_json).alias("request_bbox_json"),
    F.lit(manifest["request_limit"]).cast("int").alias("request_limit"),
    F.lit(manifest["payload_sha256"]).alias("payload_sha256"),
    F.lit(raw_json_path).alias("source_file"),
    F.lit("unknown").alias("capacity_status"),
    F.to_timestamp(F.lit(manifest["completed_at"])).alias("source_completed_at"),
    F.current_timestamp().alias("loaded_at"),
)

actual_count = bronze_batch.count()
missing_key_count = bronze_batch.filter(F.col("element_key").isNull()).count()
duplicate_key_count = actual_count - bronze_batch.select("element_key").distinct().count()
invalid_power_count = bronze_batch.filter(
    ~F.col("power_tag").isin("substation", "transformer", "line", "minor_line")
).count()
invalid_capacity_count = bronze_batch.filter(F.col("capacity_status") != "unknown").count()

assert actual_count == manifest["expected_element_count"], (
    f"Element count mismatch: expected {manifest['expected_element_count']}, got {actual_count}"
)
assert missing_key_count == 0, f"Bronze batch contains {missing_key_count} missing element keys"
assert duplicate_key_count == 0, f"Bronze batch contains {duplicate_key_count} duplicate keys"
assert invalid_power_count == 0, f"Bronze batch contains {invalid_power_count} unsupported assets"
assert invalid_capacity_count == 0, "OSM capacity status must remain unknown"

display(bronze_batch.limit(10))

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        ingestion_id STRING NOT NULL,
        element_key STRING NOT NULL,
        osm_element_type STRING NOT NULL,
        osm_element_id BIGINT NOT NULL,
        power_tag STRING NOT NULL,
        raw_element_json STRING NOT NULL,
        source_id STRING NOT NULL,
        source_license STRING NOT NULL,
        source_license_url STRING NOT NULL,
        source_attribution STRING NOT NULL,
        request_bbox_json STRING,
        request_limit INT,
        payload_sha256 STRING NOT NULL,
        source_file STRING NOT NULL,
        capacity_status STRING NOT NULL,
        source_completed_at TIMESTAMP,
        loaded_at TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'quality' = 'bronze',
        'data_boundary' = 'OSM grid assets are screening proxies, not capacity evidence'
    )
    """
)

bronze_batch.createOrReplaceTempView("grid_assets_bronze_batch")

spark.sql(
    f"""
    MERGE INTO {target_table} AS target
    USING grid_assets_bronze_batch AS source
      ON target.ingestion_id = source.ingestion_id
     AND target.element_key = source.element_key
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------

run_result = spark.sql(
    f"""
    SELECT
        ingestion_id,
        COUNT(*) AS bronze_row_count,
        COUNT(DISTINCT element_key) AS distinct_element_count,
        SUM(CASE WHEN capacity_status = 'unknown' THEN 1 ELSE 0 END) AS unknown_capacity_count,
        MIN(loaded_at) AS first_loaded_at,
        MAX(loaded_at) AS last_loaded_at
    FROM {target_table}
    WHERE ingestion_id = '{ingestion_id}'
    GROUP BY ingestion_id
    """
)

display(run_result)
result = run_result.first()
assert result["bronze_row_count"] == manifest["expected_element_count"]
assert result["unknown_capacity_count"] == manifest["expected_element_count"]

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT power_tag, COUNT(*) AS asset_count
        FROM {target_table}
        WHERE ingestion_id = '{ingestion_id}'
        GROUP BY power_tag
        ORDER BY asset_count DESC, power_tag
        """
    )
)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {target_table}"))
