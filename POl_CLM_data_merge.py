# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Create or replace Table enterprise.insurance.claims_policy_silver
# MAGIC (
# MAGIC claim_id STRING,
# MAGIC policy_id STRING,
# MAGIC customer_id STRING,
# MAGIC -- Claims
# MAGIC claim_amount DOUBLE,
# MAGIC claim_date DATE,
# MAGIC claim_type STRING,
# MAGIC hospital_id STRING,
# MAGIC diagnosis_code STRING,
# MAGIC created_timestamp TIMESTAMP,
# MAGIC -- Policy
# MAGIC policy_start_date DATE,
# MAGIC policy_end_date DATE,
# MAGIC sum_insured DOUBLE,
# MAGIC policy_status STRING,
# MAGIC -- Derived
# MAGIC claim_within_policy STRING,
# MAGIC claim_to_sum_ratio DOUBLE,
# MAGIC -- Quality
# MAGIC is_valid_claim BOOLEAN,
# MAGIC is_valid_policy BOOLEAN,
# MAGIC is_joined_successfully BOOLEAN,
# MAGIC -- Lineage
# MAGIC claim_source_file STRING,
# MAGIC policy_source_file STRING,
# MAGIC ingestion_time TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# STEP 1: Read Silver Tables
df_claims = spark.table("enterprise.insurance.claims_silver")
df_policy = spark.table("enterprise.insurance.policy_silver")

# STEP 2: Join (Left Join to retain all claims)
df_joined = df_claims.alias("c").join(
    df_policy.alias("p"),
    on="policy_id",
    how="left"
)

# STEP 3: Derive Business Columns
df_final = df_joined \
    .withColumn(
        "claim_within_policy",
        F.when(
            (F.col("c.claim_date") >= F.col("p.policy_start_date")) &
            (F.col("c.claim_date") <= F.col("p.policy_end_date")),
            "YES"
        ).otherwise("NO")
    ) \
    .withColumn(
        "claim_to_sum_ratio",
        F.when(
            F.col("p.sum_insured").isNotNull(),
            F.col("c.claim_amount") / F.col("p.sum_insured")
        ).otherwise(None)
    )


# COMMAND ----------


# STEP 4: Data Quality Flags
df_final = df_final \
    .withColumn("is_valid_claim", F.col("c.is_valid")) \
    .withColumn(
        "is_valid_policy",
        F.when(F.col("p.policy_id").isNotNull(), True).otherwise(False)
    ) \
    .withColumn(
        "is_joined_successfully",
        F.when(F.col("p.policy_id").isNotNull(), True).otherwise(False)
    )

# df_final.display()

# COMMAND ----------

# STEP 5: Select Final Schema
df_output = df_final.select(
    # Keys
    F.col("c.claim_id").alias("claim_id"),
    F.col("c.policy_id").alias("policy_id"),
    F.col("c.customer_id").alias("customer_id"),

    # Claim fields
    F.col("c.claim_amount").alias("claim_amount"),
    F.col("c.claim_date").alias("claim_date"),
    F.col("c.claim_type").alias("claim_type"),
    F.col("c.hospital_id").alias("hospital_id"),
    F.col("c.diagnosis_code").alias("diagnosis_code"),
    F.col("c.created_timestamp").alias("created_timestamp"),

    # Policy fields
    F.col("p.policy_start_date").alias("policy_start_date"),
    F.col("p.policy_end_date").alias("policy_end_date"),
    F.col("p.sum_insured").alias("sum_insured"),
    F.col("p.policy_status").alias("policy_status"),

    # Derived
    F.col("claim_within_policy"),
    F.col("claim_to_sum_ratio"),

    # Quality flags
    F.col("is_valid_claim"),
    F.col("is_valid_policy"),
    F.col("is_joined_successfully"),

    # Lineage
    F.col("c.source_file").alias("claim_source_file"),
    F.col("p.source_file").alias("policy_source_file"),

    # Audit
    F.current_timestamp().alias("ingestion_time")
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import Window

# Deduplicate df_output to ensure unique claim_ids
# Keep the most recent record per claim_id based on created_timestamp
window_spec = Window.partitionBy("claim_id").orderBy(F.col("created_timestamp").desc())

df_output_deduped = df_output \
    .withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

print(f"Original rows: {df_output.count()}")
print(f"Deduplicated rows: {df_output_deduped.count()}")

# Perform the merge
target = DeltaTable.forName(spark, "enterprise.insurance.claims_policy_silver")

target.alias("t").merge(
    df_output_deduped.alias("s"),
    "t.claim_id = s.claim_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from enterprise.insurance.claims_policy_silver limit 10