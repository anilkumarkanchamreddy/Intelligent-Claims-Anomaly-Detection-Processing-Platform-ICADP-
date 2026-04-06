# Databricks notebook source
from pyspark.sql import functions as F

df_gold = spark.table("enterprise.insurance.claims_gold_temp")

df_kpi = df_gold.groupBy("claim_date").agg(
    F.count("claim_id").alias("total_claims"),
    F.sum("claim_amount").alias("total_claim_amount"),
    F.sum(F.when(F.col("fraud_flag") != "NORMAL", 1).otherwise(0)).alias("fraud_cases")
)

df_kpi.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("enterprise.insurance.claims_kpi_gold")

# COMMAND ----------

claims_kpi_gold_df = spark.table("enterprise.insurance.claims_kpi_gold")

claims_kpi_gold_df.write.mode("overwrite").parquet("s3://amzn-s3-insurance-claims/claims_KPI_gold")