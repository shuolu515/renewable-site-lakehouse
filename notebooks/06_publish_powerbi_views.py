# Databricks notebook source
"""Publish stable Gold views for the Power BI semantic model."""

# COMMAND ----------

import re

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("parcel_ingestion_id", "98c5af3d-dc45-481f-bebf-9a6d7979672e")
dbutils.widgets.text("grid_ingestion_id", "1bd38dbc-8a46-4453-ab9c-1713966d2f54")

catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")
parcel_ingestion_id = dbutils.widgets.get("parcel_ingestion_id")
grid_ingestion_id = dbutils.widgets.get("grid_ingestion_id")


def validate_identifier(value: str) -> str:
    """Allow only simple Unity Catalog identifiers in generated SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value!r}")
    return value


for identifier in (catalog, gold_schema):
    validate_identifier(identifier)

for run_id in (parcel_ingestion_id, grid_ingestion_id):
    if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f-]{27}", run_id):
        raise ValueError("ingestion identifiers must be lowercase UUIDs")

# COMMAND ----------

fact_table = f"{catalog}.{gold_schema}.fact_site_assessment"
parcel_table = f"{catalog}.{gold_schema}.dim_parcel"
grid_table = f"{catalog}.{gold_schema}.dim_grid_asset"
candidate_view = f"{catalog}.{gold_schema}.vw_powerbi_site_candidates"
red_flag_view = f"{catalog}.{gold_schema}.vw_powerbi_red_flags"

required_tables = (fact_table, parcel_table, grid_table)
missing_tables = [table for table in required_tables if not spark.catalog.tableExists(table)]
if missing_tables:
    raise ValueError(f"Run the Gold notebook first; missing tables: {missing_tables}")

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {candidate_view} AS
    WITH selected_assessments AS (
        SELECT *
        FROM {fact_table}
        WHERE parcel_ingestion_id = '{parcel_ingestion_id}'
          AND grid_ingestion_id = '{grid_ingestion_id}'
          AND assessment_date = (
              SELECT MAX(assessment_date)
              FROM {fact_table}
              WHERE parcel_ingestion_id = '{parcel_ingestion_id}'
                AND grid_ingestion_id = '{grid_ingestion_id}'
          )
    )
    SELECT
        f.assessment_id,
        f.assessment_date,
        f.parcel_key,
        f.parcel_id,
        p.national_cadastral_reference,
        p.parcel_label,
        p.centroid_lat,
        p.centroid_lon,
        p.geometry_type AS parcel_geometry_type,
        p.land_use,
        f.candidate_type,
        f.eligible_for_shortlist,
        f.gross_area_m2,
        f.estimated_usable_area_m2,
        f.estimated_pv_mwp,
        f.adjacent_parcel_count,
        f.adjacent_parcel_area_m2,
        f.potential_combined_area_m2,
        f.potential_land_pool,
        f.nearest_grid_asset_key,
        f.nearest_grid_asset_id,
        f.nearest_grid_asset_type,
        f.nearest_grid_distance_m,
        g.voltage_level_kv,
        g.operator_name AS grid_operator_name,
        g.asset_name AS grid_asset_name,
        f.grid_score,
        f.land_score,
        f.data_quality_score,
        f.planning_score,
        f.total_score,
        f.planning_status,
        f.capacity_status,
        f.capacity_confidence,
        f.data_quality_status,
        SIZE(f.red_flags) AS red_flag_count,
        f.parcel_ingestion_id,
        f.grid_ingestion_id,
        ROW_NUMBER() OVER (
            ORDER BY
                f.eligible_for_shortlist DESC,
                f.total_score DESC,
                f.gross_area_m2 DESC,
                f.parcel_key
        ) AS screening_rank
    FROM selected_assessments AS f
    INNER JOIN {parcel_table} AS p
      ON f.parcel_key = p.parcel_key
    INNER JOIN {grid_table} AS g
      ON f.nearest_grid_asset_key = g.grid_asset_key
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {red_flag_view} AS
    SELECT
        assessment_id,
        assessment_date,
        parcel_key,
        parcel_id,
        candidate_type,
        red_flag
    FROM (
        SELECT
            f.assessment_id,
            f.assessment_date,
            f.parcel_key,
            f.parcel_id,
            f.candidate_type,
            EXPLODE(f.red_flags) AS red_flag
        FROM {fact_table} AS f
        WHERE f.parcel_ingestion_id = '{parcel_ingestion_id}'
          AND f.grid_ingestion_id = '{grid_ingestion_id}'
          AND f.assessment_date = (
              SELECT MAX(assessment_date)
              FROM {fact_table}
              WHERE parcel_ingestion_id = '{parcel_ingestion_id}'
                AND grid_ingestion_id = '{grid_ingestion_id}'
          )
    ) AS exploded_flags
    """
)

# COMMAND ----------

expected_candidate_count = spark.sql(
    f"""
    SELECT COUNT(*) AS row_count
    FROM {fact_table}
    WHERE parcel_ingestion_id = '{parcel_ingestion_id}'
      AND grid_ingestion_id = '{grid_ingestion_id}'
      AND assessment_date = (
          SELECT MAX(assessment_date)
          FROM {fact_table}
          WHERE parcel_ingestion_id = '{parcel_ingestion_id}'
            AND grid_ingestion_id = '{grid_ingestion_id}'
      )
    """
).first()["row_count"]

candidate_result = spark.sql(
    f"""
    SELECT
        COUNT(*) AS candidate_row_count,
        COUNT(DISTINCT parcel_key) AS distinct_parcel_count,
        SUM(CASE WHEN eligible_for_shortlist THEN 1 ELSE 0 END) AS shortlist_count,
        SUM(CASE WHEN candidate_type = 'standalone_candidate' THEN 1 ELSE 0 END)
            AS standalone_candidate_count,
        SUM(CASE WHEN candidate_type = 'land_pool_opportunity' THEN 1 ELSE 0 END)
            AS land_pool_opportunity_count
    FROM {candidate_view}
    """
)

display(candidate_result)
candidate_row = candidate_result.first()
assert candidate_row["candidate_row_count"] == expected_candidate_count
assert candidate_row["distinct_parcel_count"] == expected_candidate_count

display(
    spark.sql(
        f"""
        SELECT candidate_type, COUNT(*) AS parcel_count
        FROM {candidate_view}
        GROUP BY candidate_type
        ORDER BY candidate_type
        """
    )
)

display(
    spark.sql(
        f"""
        SELECT red_flag, COUNT(*) AS occurrence_count
        FROM {red_flag_view}
        GROUP BY red_flag
        ORDER BY occurrence_count DESC, red_flag
        """
    )
)
