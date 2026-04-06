# Databricks notebook source
# MAGIC %md
# MAGIC create catalog if not exists Enterprise;
# MAGIC
# MAGIC create schema if not exists Enterprise.Insurance

# COMMAND ----------

# MAGIC %sql
# MAGIC Use catalog enterprise;
# MAGIC use schema insurance;

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql import functions as F

# Define your schema location variable first
schema_path = "s3://amzn-s3-insurance-claims/Claims/checkpoints/schema" 

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
    .load("s3://amzn-s3-insurance-claims/Claims/raw/ingestion_20260331/")
)

# Add source file column
df = df.withColumn("source_file", F.col("_metadata.file_path"))

# The trigger goes in the writeStream, not the readStream
query = (df.writeStream
    .trigger(availableNow=True)
    .format("delta") # or your preferred sink
    .outputMode("append")
    .option("checkpointLocation", "s3://amzn-s3-insurance-claims/Claims/checkpoints/claims_brnz")
    .toTable("insurance.claims_brnz_raw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from insurance.claims_brnz_raw limit 10 