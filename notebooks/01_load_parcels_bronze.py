# Databricks notebook source
"""Load one immutable Hessen parcel snapshot into a Bronze Delta table.

Upload ``parcels.geojson`` and ``manifest.json`` to the expected Unity Catalog volume directory
before running this notebook. The merge key makes rerunning the same ingestion idempotent.
"""

# COMMAND ----------

import re

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("landing_schema", "landing")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("volume_name", "raw")
dbutils.widgets.text("ingestion_id", "bad4b270-a420-4075-b159-2775966077da")

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
snapshot_directory = f"{volume_root}/hessen_alkis_inspire_wfs/{ingestion_id}"
geojson_path = f"{snapshot_directory}/parcels.geojson"
manifest_path = f"{snapshot_directory}/manifest.json"
target_table = f"{catalog}.{bronze_schema}.parcels_raw"

dbutils.fs.mkdirs(snapshot_directory)
print(f"Upload directory: {snapshot_directory}")
print(f"Target table: {target_table}")

# COMMAND ----------

# Fail early with an actionable message when the two local files have not been uploaded yet.
uploaded_files = {item.name.rstrip("/") for item in dbutils.fs.ls(snapshot_directory)}
required_files = {"parcels.geojson", "manifest.json"}
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
        "collection_id",
        "license",
        "license_url",
        "contains_personal_data",
        "completed_at",
        F.col("request.bbox").alias("request_bbox"),
        F.col("request.limit").alias("request_limit"),
        F.col("response.feature_count").alias("expected_feature_count"),
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
if manifest["contains_personal_data"] is not False:
    raise ValueError("Bronze load stopped because the manifest does not confirm a non-PII payload")

# COMMAND ----------

payload = spark.read.option("multiLine", True).json(geojson_path)

bronze_batch = payload.select(F.explode("features").alias("feature")).select(
    F.lit(manifest["ingestion_id"]).alias("ingestion_id"),
    F.coalesce(
        F.col("feature.properties.localId"),
        F.col("feature.properties.gml_id"),
        F.col("feature.properties.nationalCadastralReference"),
    ).alias("feature_id"),
    F.lit(manifest["source_id"]).alias("source_id"),
    F.lit(manifest["collection_id"]).alias("collection_id"),
    F.col("feature.geometry.type").alias("geometry_type"),
    F.to_json("feature").alias("raw_feature_json"),
    F.lit(manifest["license"]).alias("source_license"),
    F.lit(manifest["license_url"]).alias("source_license_url"),
    F.lit(manifest["request_bbox"]).alias("request_bbox"),
    F.lit(manifest["request_limit"]).cast("int").alias("request_limit"),
    F.lit(manifest["payload_sha256"]).alias("payload_sha256"),
    F.lit(geojson_path).alias("source_file"),
    F.to_timestamp(F.lit(manifest["completed_at"])).alias("source_completed_at"),
    F.current_timestamp().alias("loaded_at"),
)

actual_count = bronze_batch.count()
missing_id_count = bronze_batch.filter(F.col("feature_id").isNull()).count()
duplicate_id_count = actual_count - bronze_batch.select("feature_id").distinct().count()

assert actual_count == manifest["expected_feature_count"], (
    f"Feature count mismatch: expected {manifest['expected_feature_count']}, got {actual_count}"
)
assert missing_id_count == 0, f"Bronze batch contains {missing_id_count} missing feature ids"
assert duplicate_id_count == 0, f"Bronze batch contains {duplicate_id_count} duplicate feature ids"

display(bronze_batch.limit(10))

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        ingestion_id STRING NOT NULL,
        feature_id STRING NOT NULL,
        source_id STRING NOT NULL,
        collection_id STRING NOT NULL,
        geometry_type STRING,
        raw_feature_json STRING NOT NULL,
        source_license STRING NOT NULL,
        source_license_url STRING NOT NULL,
        request_bbox STRING,
        request_limit INT,
        payload_sha256 STRING NOT NULL,
        source_file STRING NOT NULL,
        source_completed_at TIMESTAMP,
        loaded_at TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'quality' = 'bronze',
        'data_boundary' = 'public cadastral subset without owner data'
    )
    """
)

bronze_batch.createOrReplaceTempView("parcels_bronze_batch")

spark.sql(
    f"""
    MERGE INTO {target_table} AS target
    USING parcels_bronze_batch AS source
      ON target.ingestion_id = source.ingestion_id
     AND target.feature_id = source.feature_id
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------

run_result = spark.sql(
    f"""
    SELECT
        ingestion_id,
        COUNT(*) AS bronze_row_count,
        COUNT(DISTINCT feature_id) AS distinct_feature_count,
        MIN(loaded_at) AS first_loaded_at,
        MAX(loaded_at) AS last_loaded_at
    FROM {target_table}
    WHERE ingestion_id = '{ingestion_id}'
    GROUP BY ingestion_id
    """
)

display(run_result)
assert run_result.first()["bronze_row_count"] == manifest["expected_feature_count"]

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {target_table}"))
