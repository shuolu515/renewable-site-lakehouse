# Databricks notebook source
"""Build an explainable Gold star schema for renewable-site screening."""

# COMMAND ----------

import re
from datetime import UTC, datetime

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("parcel_ingestion_id", "98c5af3d-dc45-481f-bebf-9a6d7979672e")
dbutils.widgets.text("grid_ingestion_id", "1bd38dbc-8a46-4453-ab9c-1713966d2f54")
dbutils.widgets.text("assessment_date", "")
dbutils.widgets.text("minimum_area_m2", "20000")
dbutils.widgets.text("target_area_m2", "50000")
dbutils.widgets.text("maximum_grid_distance_m", "7000")
dbutils.widgets.text("usable_area_ratio", "0.70")
dbutils.widgets.text("area_m2_per_mwp", "10000")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
parcel_ingestion_id = dbutils.widgets.get("parcel_ingestion_id")
grid_ingestion_id = dbutils.widgets.get("grid_ingestion_id")
assessment_date = dbutils.widgets.get("assessment_date") or datetime.now(UTC).date().isoformat()
minimum_area_m2 = float(dbutils.widgets.get("minimum_area_m2"))
target_area_m2 = float(dbutils.widgets.get("target_area_m2"))
maximum_grid_distance_m = float(dbutils.widgets.get("maximum_grid_distance_m"))
usable_area_ratio = float(dbutils.widgets.get("usable_area_ratio"))
area_m2_per_mwp = float(dbutils.widgets.get("area_m2_per_mwp"))


def validate_identifier(value: str) -> str:
    """Allow only simple Unity Catalog identifiers in generated SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value!r}")
    return value


for identifier in (catalog, silver_schema, gold_schema):
    validate_identifier(identifier)

for run_id in (parcel_ingestion_id, grid_ingestion_id):
    if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f-]{27}", run_id):
        raise ValueError("ingestion identifiers must be lowercase UUIDs")

if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", assessment_date):
    raise ValueError("assessment_date must use YYYY-MM-DD")
if (
    min(
        minimum_area_m2,
        target_area_m2,
        maximum_grid_distance_m,
        area_m2_per_mwp,
    )
    <= 0
):
    raise ValueError("area, distance and power-density parameters must be positive")
if not 0 < usable_area_ratio <= 1:
    raise ValueError("usable_area_ratio must be in the range (0, 1]")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

parcel_silver_table = f"{catalog}.{silver_schema}.parcels"
grid_silver_table = f"{catalog}.{silver_schema}.grid_assets"
parcel_dimension_table = f"{catalog}.{gold_schema}.dim_parcel"
grid_dimension_table = f"{catalog}.{gold_schema}.dim_grid_asset"
source_dimension_table = f"{catalog}.{gold_schema}.dim_data_source"
assessment_fact_table = f"{catalog}.{gold_schema}.fact_site_assessment"

parcels = spark.table(parcel_silver_table).filter(F.col("ingestion_id") == parcel_ingestion_id)
grid_assets = spark.table(grid_silver_table).filter(F.col("ingestion_id") == grid_ingestion_id)

parcel_count = parcels.count()
grid_asset_count = grid_assets.count()
if parcel_count == 0 or grid_asset_count == 0:
    raise ValueError("Both Silver inputs must contain rows for the selected ingestion IDs")

print(f"Parcel Silver input: {parcel_count} rows")
print(f"Grid Silver input: {grid_asset_count} rows")
print(f"Assessment date: {assessment_date}")

# COMMAND ----------

parcel_dimension = parcels.select(
    F.sha2(F.concat_ws("|", "ingestion_id", "parcel_id"), 256).alias("parcel_key"),
    "ingestion_id",
    "parcel_id",
    "national_cadastral_reference",
    "parcel_label",
    F.col("source_area_m2").alias("gross_area_m2"),
    "geometry_type",
    "geometry_json",
    "centroid_lat",
    "centroid_lon",
    "land_use",
    "meets_minimum_area",
    "source_id",
    "quality_status",
    F.current_timestamp().alias("created_at"),
)

grid_dimension = grid_assets.select(
    F.sha2(F.concat_ws("|", "ingestion_id", "grid_asset_id"), 256).alias("grid_asset_key"),
    "ingestion_id",
    "grid_asset_id",
    "osm_element_type",
    "osm_element_id",
    "asset_type",
    "source_power_tag",
    "geometry_type",
    "geometry_json",
    "centroid_lat",
    "centroid_lon",
    "voltage_level_kv",
    "operator_name",
    "asset_name",
    "capacity_status",
    "source_id",
    "quality_status",
    F.current_timestamp().alias("created_at"),
)

source_dimension = (
    parcels.select(
        "source_id",
        F.col("source_license").alias("license"),
        F.col("source_license_url").alias("license_url"),
        F.lit(None).cast("string").alias("attribution"),
        F.lit("not_applicable").alias("capacity_evidence"),
    )
    .unionByName(
        grid_assets.select(
            "source_id",
            F.col("source_license").alias("license"),
            F.col("source_license_url").alias("license_url"),
            F.col("source_attribution").alias("attribution"),
            F.lit("proxy_only").alias("capacity_evidence"),
        )
    )
    .dropDuplicates(["source_id"])
    .withColumn("data_source_key", F.sha2("source_id", 256))
    .withColumn("created_at", F.current_timestamp())
    .select(
        "data_source_key",
        "source_id",
        "license",
        "license_url",
        "attribution",
        "capacity_evidence",
        "created_at",
    )
)

# COMMAND ----------

spatial_check = spark.sql(
    """
    SELECT st_intersects(
        st_geomfromgeojson('{"type":"Point","coordinates":[8.16,50.43]}'),
        st_geomfromgeojson('{"type":"Point","coordinates":[8.16,50.43]}')
    ) AS spatial_functions_available
    """
).first()
if not spatial_check["spatial_functions_available"]:
    raise RuntimeError("Databricks spatial SQL functions are not available on this compute")

parcel_geometries = parcel_dimension.select(
    "parcel_key",
    "gross_area_m2",
    F.expr("st_geomfromgeojson(geometry_json)").alias("parcel_geometry"),
)
invalid_geometry_count = parcel_geometries.filter(~F.expr("st_isvalid(parcel_geometry)")).count()
if invalid_geometry_count:
    raise ValueError(f"{invalid_geometry_count} parcel geometries are topologically invalid")

anchor = parcel_geometries.alias("anchor")
neighbor = parcel_geometries.alias("neighbor")
adjacency_metrics = (
    anchor.crossJoin(neighbor)
    .filter(F.col("anchor.parcel_key") != F.col("neighbor.parcel_key"))
    .filter(F.expr("st_intersects(anchor.parcel_geometry, neighbor.parcel_geometry)"))
    .groupBy(F.col("anchor.parcel_key").alias("parcel_key"))
    .agg(
        F.countDistinct("neighbor.parcel_key").alias("adjacent_parcel_count"),
        F.sum("neighbor.gross_area_m2").alias("adjacent_parcel_area_m2"),
    )
)

parcel_points = (
    parcel_dimension.select(
        "parcel_key",
        "ingestion_id",
        "parcel_id",
        "gross_area_m2",
        "meets_minimum_area",
        F.col("centroid_lat").alias("parcel_lat"),
        F.col("centroid_lon").alias("parcel_lon"),
    )
    .join(adjacency_metrics, "parcel_key", "left")
    .fillna({"adjacent_parcel_count": 0, "adjacent_parcel_area_m2": 0.0})
    .withColumn(
        "potential_combined_area_m2",
        F.round(F.col("gross_area_m2") + F.col("adjacent_parcel_area_m2"), 2),
    )
    .withColumn(
        "potential_land_pool",
        (~F.col("meets_minimum_area"))
        & (F.col("adjacent_parcel_count") > 0)
        & (F.col("potential_combined_area_m2") >= minimum_area_m2),
    )
    .withColumn(
        "candidate_type",
        F.when(F.col("meets_minimum_area"), F.lit("standalone_candidate"))
        .when(F.col("potential_land_pool"), F.lit("land_pool_opportunity"))
        .otherwise(F.lit("below_threshold")),
    )
)
grid_points = grid_dimension.select(
    "grid_asset_key",
    "grid_asset_id",
    "asset_type",
    "capacity_status",
    F.col("centroid_lat").alias("grid_lat"),
    F.col("centroid_lon").alias("grid_lon"),
)

earth_radius_m = 6_371_000.0
candidate_pairs = (
    parcel_points.crossJoin(F.broadcast(grid_points))
    .withColumn("parcel_lat_rad", F.radians("parcel_lat"))
    .withColumn("grid_lat_rad", F.radians("grid_lat"))
    .withColumn("delta_lat_rad", F.radians(F.col("grid_lat") - F.col("parcel_lat")))
    .withColumn("delta_lon_rad", F.radians(F.col("grid_lon") - F.col("parcel_lon")))
    .withColumn(
        "haversine_a",
        F.pow(F.sin(F.col("delta_lat_rad") / 2), 2)
        + F.cos("parcel_lat_rad")
        * F.cos("grid_lat_rad")
        * F.pow(F.sin(F.col("delta_lon_rad") / 2), 2),
    )
    .withColumn(
        "grid_distance_m",
        F.lit(2 * earth_radius_m) * F.asin(F.sqrt(F.least(F.lit(1.0), F.col("haversine_a")))),
    )
)

nearest_window = Window.partitionBy("parcel_key").orderBy(
    F.col("grid_distance_m"), F.col("grid_asset_key")
)
nearest_grid = (
    candidate_pairs.withColumn("distance_rank", F.row_number().over(nearest_window))
    .filter(F.col("distance_rank") == 1)
    .drop("distance_rank")
)

# COMMAND ----------

scored = (
    nearest_grid.withColumn(
        "estimated_usable_area_m2",
        F.round(F.col("gross_area_m2") * F.lit(usable_area_ratio), 2),
    )
    .withColumn(
        "estimated_pv_mwp",
        F.round(F.col("estimated_usable_area_m2") / F.lit(area_m2_per_mwp), 3),
    )
    .withColumn(
        "grid_score",
        F.round(
            F.greatest(
                F.lit(0.0),
                F.lit(100.0) * (F.lit(1.0) - F.col("grid_distance_m") / maximum_grid_distance_m),
            ),
            2,
        ),
    )
    .withColumn(
        "land_score",
        F.round(
            F.least(
                F.lit(100.0),
                F.lit(100.0) * F.col("gross_area_m2") / target_area_m2,
            ),
            2,
        ),
    )
    .withColumn("data_quality_score", F.lit(100.0))
    .withColumn("planning_score", F.lit(50.0))
    .withColumn(
        "total_score",
        F.round(
            F.lit(0.40) * F.col("grid_score")
            + F.lit(0.35) * F.col("land_score")
            + F.lit(0.15) * F.col("data_quality_score")
            + F.lit(0.10) * F.col("planning_score"),
            2,
        ),
    )
    .withColumn(
        "eligible_for_shortlist",
        F.col("meets_minimum_area") & (F.col("grid_distance_m") <= maximum_grid_distance_m),
    )
    .withColumn(
        "red_flags_raw",
        F.array(
            F.when(
                (~F.col("meets_minimum_area")) & (~F.col("potential_land_pool")),
                F.lit("below_minimum_area"),
            ),
            F.when(F.col("potential_land_pool"), F.lit("requires_land_assembly")),
            F.when(
                F.col("grid_distance_m") > maximum_grid_distance_m,
                F.lit("grid_proxy_farther_than_limit"),
            ),
            F.when(
                F.col("capacity_status") == "unknown",
                F.lit("grid_capacity_unknown"),
            ),
            F.lit("planning_status_unknown"),
        ),
    )
    .withColumn("red_flags", F.expr("filter(red_flags_raw, flag -> flag is not null)"))
    .withColumn("assessment_date", F.to_date(F.lit(assessment_date)))
    .withColumn("planning_status", F.lit("unknown"))
    .withColumn("capacity_confidence", F.lit("proxy_only"))
    .withColumn("data_quality_status", F.lit("passed"))
    .withColumn("parcel_ingestion_id", F.lit(parcel_ingestion_id))
    .withColumn("grid_ingestion_id", F.lit(grid_ingestion_id))
    .withColumn(
        "assessment_id",
        F.sha2(
            F.concat_ws("|", "parcel_key", F.lit(grid_ingestion_id), F.lit(assessment_date)),
            256,
        ),
    )
    .withColumn("assessed_at", F.current_timestamp())
)

site_assessment = scored.select(
    "assessment_id",
    "assessment_date",
    "parcel_key",
    F.col("grid_asset_key").alias("nearest_grid_asset_key"),
    "parcel_id",
    F.col("grid_asset_id").alias("nearest_grid_asset_id"),
    F.col("asset_type").alias("nearest_grid_asset_type"),
    "gross_area_m2",
    "estimated_usable_area_m2",
    "estimated_pv_mwp",
    F.round("grid_distance_m", 2).alias("nearest_grid_distance_m"),
    "grid_score",
    "land_score",
    "data_quality_score",
    "planning_score",
    "total_score",
    "eligible_for_shortlist",
    "red_flags",
    "planning_status",
    "capacity_status",
    "capacity_confidence",
    "data_quality_status",
    "parcel_ingestion_id",
    "grid_ingestion_id",
    "assessed_at",
    "adjacent_parcel_count",
    "adjacent_parcel_area_m2",
    "potential_combined_area_m2",
    "potential_land_pool",
    "candidate_type",
)

assert site_assessment.count() == parcel_count
assert site_assessment.select("parcel_key").distinct().count() == parcel_count
assert site_assessment.filter(~F.col("total_score").between(0, 100)).count() == 0
assert site_assessment.filter(F.col("capacity_status") != "unknown").count() == 0
assert (
    site_assessment.filter(
        ~F.col("candidate_type").isin(
            "standalone_candidate", "land_pool_opportunity", "below_threshold"
        )
    ).count()
    == 0
)

display(site_assessment.orderBy(F.desc("total_score")).limit(10))

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {parcel_dimension_table} (
        parcel_key STRING NOT NULL,
        ingestion_id STRING NOT NULL,
        parcel_id STRING NOT NULL,
        national_cadastral_reference STRING,
        parcel_label STRING,
        gross_area_m2 DOUBLE NOT NULL,
        geometry_type STRING NOT NULL,
        geometry_json STRING NOT NULL,
        centroid_lat DOUBLE NOT NULL,
        centroid_lon DOUBLE NOT NULL,
        land_use STRING NOT NULL,
        meets_minimum_area BOOLEAN NOT NULL,
        source_id STRING NOT NULL,
        quality_status STRING NOT NULL,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA TBLPROPERTIES ('quality' = 'gold')
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {grid_dimension_table} (
        grid_asset_key STRING NOT NULL,
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
        voltage_level_kv DOUBLE,
        operator_name STRING,
        asset_name STRING,
        capacity_status STRING NOT NULL,
        source_id STRING NOT NULL,
        quality_status STRING NOT NULL,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA TBLPROPERTIES ('quality' = 'gold')
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {source_dimension_table} (
        data_source_key STRING NOT NULL,
        source_id STRING NOT NULL,
        license STRING NOT NULL,
        license_url STRING NOT NULL,
        attribution STRING,
        capacity_evidence STRING NOT NULL,
        created_at TIMESTAMP NOT NULL
    ) USING DELTA TBLPROPERTIES ('quality' = 'gold')
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {assessment_fact_table} (
        assessment_id STRING NOT NULL,
        assessment_date DATE NOT NULL,
        parcel_key STRING NOT NULL,
        nearest_grid_asset_key STRING NOT NULL,
        parcel_id STRING NOT NULL,
        nearest_grid_asset_id STRING NOT NULL,
        nearest_grid_asset_type STRING NOT NULL,
        gross_area_m2 DOUBLE NOT NULL,
        estimated_usable_area_m2 DOUBLE NOT NULL,
        estimated_pv_mwp DOUBLE NOT NULL,
        nearest_grid_distance_m DOUBLE NOT NULL,
        grid_score DOUBLE NOT NULL,
        land_score DOUBLE NOT NULL,
        data_quality_score DOUBLE NOT NULL,
        planning_score DOUBLE NOT NULL,
        total_score DOUBLE NOT NULL,
        eligible_for_shortlist BOOLEAN NOT NULL,
        red_flags ARRAY<STRING> NOT NULL,
        planning_status STRING NOT NULL,
        capacity_status STRING NOT NULL,
        capacity_confidence STRING NOT NULL,
        data_quality_status STRING NOT NULL,
        parcel_ingestion_id STRING NOT NULL,
        grid_ingestion_id STRING NOT NULL,
        assessed_at TIMESTAMP NOT NULL,
        adjacent_parcel_count BIGINT,
        adjacent_parcel_area_m2 DOUBLE,
        potential_combined_area_m2 DOUBLE,
        potential_land_pool BOOLEAN,
        candidate_type STRING
    ) USING DELTA
    TBLPROPERTIES (
        'quality' = 'gold',
        'data_boundary' = 'Screening output only; grid capacity and planning remain unconfirmed'
    )
    """
)

# COMMAND ----------

fact_schema_extensions = {
    "adjacent_parcel_count": "BIGINT",
    "adjacent_parcel_area_m2": "DOUBLE",
    "potential_combined_area_m2": "DOUBLE",
    "potential_land_pool": "BOOLEAN",
    "candidate_type": "STRING",
}
existing_fact_columns = {field.name for field in spark.table(assessment_fact_table).schema.fields}
for column_name, data_type in fact_schema_extensions.items():
    if column_name not in existing_fact_columns:
        spark.sql(f"ALTER TABLE {assessment_fact_table} ADD COLUMNS ({column_name} {data_type})")

# COMMAND ----------

parcel_dimension.createOrReplaceTempView("gold_parcel_dimension_batch")
grid_dimension.createOrReplaceTempView("gold_grid_dimension_batch")
source_dimension.createOrReplaceTempView("gold_source_dimension_batch")
site_assessment.createOrReplaceTempView("gold_site_assessment_batch")

spark.sql(
    f"""
    MERGE INTO {parcel_dimension_table} AS target
    USING gold_parcel_dimension_batch AS source
      ON target.parcel_key = source.parcel_key
    WHEN NOT MATCHED THEN INSERT *
    """
)
spark.sql(
    f"""
    MERGE INTO {grid_dimension_table} AS target
    USING gold_grid_dimension_batch AS source
      ON target.grid_asset_key = source.grid_asset_key
    WHEN NOT MATCHED THEN INSERT *
    """
)
spark.sql(
    f"""
    MERGE INTO {source_dimension_table} AS target
    USING gold_source_dimension_batch AS source
      ON target.data_source_key = source.data_source_key
    WHEN NOT MATCHED THEN INSERT *
    """
)
spark.sql(
    f"""
    MERGE INTO {assessment_fact_table} AS target
    USING gold_site_assessment_batch AS source
      ON target.assessment_id = source.assessment_id
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------

run_result = spark.sql(
    f"""
    SELECT
        {parcel_count} AS silver_parcel_count,
        COUNT(*) AS fact_row_count,
        COUNT(DISTINCT parcel_key) AS distinct_parcel_count,
        SUM(CASE WHEN candidate_type = 'standalone_candidate' THEN 1 ELSE 0 END)
            AS standalone_candidate_count,
        SUM(CASE WHEN candidate_type = 'land_pool_opportunity' THEN 1 ELSE 0 END)
            AS land_pool_opportunity_count,
        SUM(CASE WHEN eligible_for_shortlist THEN 1 ELSE 0 END) AS shortlist_count,
        ROUND(AVG(total_score), 2) AS average_total_score,
        MIN(total_score) AS minimum_total_score,
        MAX(total_score) AS maximum_total_score
    FROM {assessment_fact_table}
    WHERE parcel_ingestion_id = '{parcel_ingestion_id}'
      AND grid_ingestion_id = '{grid_ingestion_id}'
      AND assessment_date = DATE '{assessment_date}'
    """
)

display(run_result)
result = run_result.first()
assert result["fact_row_count"] == parcel_count
assert result["distinct_parcel_count"] == parcel_count
assert result["standalone_candidate_count"] > 0

# COMMAND ----------

display(
    spark.table(assessment_fact_table)
    .filter(
        (F.col("parcel_ingestion_id") == parcel_ingestion_id)
        & (F.col("grid_ingestion_id") == grid_ingestion_id)
        & (F.col("assessment_date") == F.to_date(F.lit(assessment_date)))
    )
    .groupBy("candidate_type")
    .agg(
        F.count("*").alias("parcel_count"),
        F.round(F.avg("gross_area_m2"), 2).alias("average_parcel_area_m2"),
        F.round(F.avg("nearest_grid_distance_m"), 2).alias("average_grid_distance_m"),
    )
    .orderBy("candidate_type")
)

# COMMAND ----------

display(
    spark.table(assessment_fact_table)
    .filter(
        (F.col("parcel_ingestion_id") == parcel_ingestion_id)
        & (F.col("grid_ingestion_id") == grid_ingestion_id)
        & (F.col("assessment_date") == F.to_date(F.lit(assessment_date)))
    )
    .select(F.explode("red_flags").alias("red_flag"))
    .groupBy("red_flag")
    .count()
    .orderBy(F.desc("count"), "red_flag")
)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {assessment_fact_table}"))
