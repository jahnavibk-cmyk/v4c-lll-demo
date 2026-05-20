# Databricks notebook source
# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "lll_demo")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
print(f"Target: {catalog}.{schema}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_events (
    event_id  STRING,
    event_ts  TIMESTAMP,
    payload   STRING
  ) USING DELTA
""")

# COMMAND ----------

df = spark.range(50).selectExpr(
    "cast(id as string)  as event_id",
    "current_timestamp() as event_ts",
    "concat('demo_', id) as payload"
)
df.write.mode("append").saveAsTable(f"{catalog}.{schema}.bronze_events")
count = spark.table(f"{catalog}.{schema}.bronze_events").count()
print(f"Done. Total rows: {count}")