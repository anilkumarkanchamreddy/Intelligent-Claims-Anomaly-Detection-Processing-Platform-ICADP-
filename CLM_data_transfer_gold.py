# Databricks notebook source
# Read gold tables from Unity Catalog
claims_gold_temp_df = spark.table("enterprise.insurance.claims_gold")
claims_kpi_gold_df = spark.table("enterprise.insurance.claims_kpi_gold")


# COMMAND ----------

claims_gold_temp_df.write.mode("APPEND").parquet("s3://amzn-s3-insurance-claims/claims_gold")
claims_kpi_gold_df.write.mode("overwrite").parquet("s3://amzn-s3-insurance-claims/claims_KPI_gold")