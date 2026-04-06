# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# STEP 1: Read Unified Silver Table
df = spark.table("enterprise.insurance.claims_policy_silver")

# STEP 2: Window for Customer-Level Analysis
window_30d = Window.partitionBy("customer_id") \
    .orderBy(F.unix_date("claim_date").cast("long")) \
    .rangeBetween(-30*86400, 0)   # last 30 days

# STEP 3: Calculate Metrics
df_metrics = df \
    .withColumn("avg_claim_amount", F.avg("claim_amount").over(window_30d)) \
    .withColumn("claim_count_30d", F.count("claim_id").over(window_30d))


# COMMAND ----------


# STEP 4: Fraud Detection Rules
df_fraud = df_metrics \
    .withColumn(
        "fraud_flag",
        F.when(F.col("claim_amount") > 3 * F.col("avg_claim_amount"), "HIGH_AMOUNT")
         .when(F.col("claim_count_30d") > 3, "FREQUENT_CLAIMS")
         .when(F.col("claim_within_policy") == "NO", "OUTSIDE_POLICY")
         .otherwise("NORMAL")
    )

# STEP 5: Risk Score (Optional but Powerful)
df_fraud = df_fraud.withColumn(
    "risk_score",
    F.when(F.col("fraud_flag") == "HIGH_AMOUNT", 90)
     .when(F.col("fraud_flag") == "FREQUENT_CLAIMS", 75)
     .when(F.col("fraud_flag") == "OUTSIDE_POLICY", 95)
     .otherwise(10)
)


# COMMAND ----------


# STEP 6: Select Final Gold Schema
df_gold = df_fraud.select(
    "claim_id",
    "customer_id",
    "policy_id",
    "claim_amount",
    "claim_date",
    "avg_claim_amount",
    "claim_count_30d",
    "fraud_flag",
    "risk_score",
    "claim_to_sum_ratio",
    "policy_status",
    "claim_within_policy",
    "ingestion_time"
)


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

window_spec = Window.partitionBy("claim_id").orderBy(F.col("ingestion_time").desc())

df_gold_dedup = df_gold \
    .withColumn("rn", F.row_number().over(window_spec)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")

# COMMAND ----------

from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "enterprise.insurance.claims_gold")

target.alias("t").merge(
    df_gold_dedup.alias("s"),
    "t.claim_id = s.claim_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

# Save df_gold as a temporary or permanent table for reuse in another notebook
df_gold.write.format("delta").mode("overwrite").saveAsTable("enterprise.insurance.claims_gold_temp")

claims_gold_temp_df = spark.table("enterprise.insurance.claims_gold")

claims_gold_temp_df.write.mode("APPEND").parquet("s3://amzn-s3-insurance-claims/claims_gold")
