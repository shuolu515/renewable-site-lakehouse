# Databricks notebook source
"""Phase 1 smoke test for Spark and Delta Lake.

Run this notebook in Databricks after creating the learning workspace. The table is intentionally
small; it validates the environment rather than business logic.
"""

# COMMAND ----------

rows = [("foundation", "ok", "2026-08-11")]
df = spark.createDataFrame(rows, ["phase", "status", "checked_on"])

# COMMAND ----------

table_name = "renewable_site_lakehouse_foundation_check"
df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

display(spark.table(table_name))

