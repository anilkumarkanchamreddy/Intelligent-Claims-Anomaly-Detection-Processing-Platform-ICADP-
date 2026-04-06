# Databricks notebook source
# MAGIC %sql
# MAGIC Use catalog enterprise;
# MAGIC Use schema insurance;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from enterprise.insurance.policy_inactive limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create Silver Table
# MAGIC CREATE OR REPLACE TABLE enterprise.insurance.policy_silver
# MAGIC (
# MAGIC   policy_id string,
# MAGIC   customer_id string,
# MAGIC   policy_start_date date,
# MAGIC   policy_end_date date,
# MAGIC   sum_insured double,
# MAGIC   ingestion_date timestamp,
# MAGIC   source_file string,
# MAGIC   policy_status string
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create Silver Table
# MAGIC CREATE OR REPLACE TABLE enterprise.insurance.policy_inactive
# MAGIC (
# MAGIC   policy_id string,
# MAGIC   customer_id string,
# MAGIC   policy_start_date date,
# MAGIC   policy_end_date date,
# MAGIC   sum_insured double,
# MAGIC   ingestion_date timestamp,
# MAGIC   source_file string,
# MAGIC   policy_status string
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# STEP 1: Read Bronze Table
df_bronze = spark.table("enterprise.insurance.policy_raw")

# STEP 2: Data Cleaning & Validation
df_clean = df_bronze.filter(
    (F.col("policy_id").isNotNull()) &
    (F.col("customer_id").isNotNull()) &
    (F.col("sum_insured") > 0) &
    (F.col("policy_start_date").isNotNull()) &
    (F.col("policy_end_date").isNotNull())
)

# STEP 3: Deduplication (Latest record per policy_id)
window_spec = Window.partitionBy("policy_id").orderBy(F.col("ingestion_date").desc())

df_dedup = df_clean \
    .withColumn("rn", F.row_number().over(window_spec)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")

# STEP 4: Add Derived Column (policy_status)
df_final = df_dedup.withColumn(
    "policy_status",
    F.when(F.col("policy_end_date") >= F.current_date(), "active")
     .otherwise("inactive")
)

# STEP 5: Select Required Columns
df_final = df_final.select(
    "policy_id",
    "customer_id",
    "policy_start_date",
    "policy_end_date",
    "sum_insured",
    "ingestion_date",
    "source_file",
    "policy_status"
)

df_valid = df_final.filter(F.col("policy_status") == "active")
df_invalid = df_final.filter(F.col("policy_status") == "inactive")


# COMMAND ----------

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(spark, "enterprise.insurance.policy_silver")

silver_table.alias("t").merge(
    df_valid.alias("s"),
    "t.policy_id = s.policy_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

silver_table = DeltaTable.forName(spark, "enterprise.insurance.policy_inactive")

silver_table.alias("t").merge(
    df_invalid.alias("s"),
    "t.policy_id = s.policy_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
