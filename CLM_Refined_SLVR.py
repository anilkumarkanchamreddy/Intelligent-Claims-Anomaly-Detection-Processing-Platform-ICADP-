# Databricks notebook source
# MAGIC %sql
# MAGIC Use catalog enterprise;
# MAGIC Use schema insurance;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# STEP 1: Read Bronze Table as Stream
df = (spark.readStream
    .table("insurance.claims_brnz_raw")
)

# STEP 2: Data Cleaning & Type Casting
df_clean = df \
    .withColumn("claim_amount", F.col("claim_amount").cast("double")) \
    .withColumn("claim_date", F.to_date("claim_date")) \
    .withColumn("created_timestamp", F.to_timestamp("created_timestamp")) \
    .withColumn("ingestion_time", F.current_timestamp())



# COMMAND ----------


# STEP 3: Data Quality Checks
df_validated = df_clean \
    .withColumn(
        "is_valid",
        F.when(F.col("claim_amount") <= 0, False)
         .when(F.col("policy_id").isNull(), False)
         .otherwise(True)
    ) \
    .withColumn(
        "error_reason",
        F.when(F.col("claim_amount") <= 0, "Invalid claim amount")
         .when(F.col("policy_id").isNull(), "Missing policy_id")
         .otherwise(None)
    )



# COMMAND ----------

# STEP 4: Deduplication (Important)
window_spec = Window.partitionBy(
    "customer_id", "claim_date",
).orderBy(F.col("created_timestamp").desc())

df_dedup = df_validated \
    .withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

# STEP 5: Split Valid & Invalid Records
df_valid = df_dedup.filter(F.col("is_valid") == True)
df_invalid = df_dedup.filter(F.col("is_valid") == False)


# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.window import Window

def process_batch(batch_df, batch_id):
    # Deduplication using ROW_NUMBER (works in batch mode)
    window_spec = Window.partitionBy(
        "customer_id", "claim_date"
    ).orderBy(F.col("created_timestamp").desc())
    
    df_dedup = batch_df \
        .withColumn("row_num", F.row_number().over(window_spec)) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")
    
    # Split valid & invalid records
    df_valid = df_dedup.filter(F.col("is_valid") == True)
    df_invalid = df_dedup.filter(F.col("is_valid") == False)
    
    # Write valid data to silver table
    df_valid.write.mode("append").saveAsTable("insurance.claims_silver")
    
    # Write invalid data to rejects table
    df_invalid.write.mode("append").saveAsTable("insurance.claims_rejects")

# STEP 6 & 7: Write streams using foreachBatch
query = (df_validated.writeStream
    .trigger(availableNow=True)
    .foreachBatch(process_batch)
    .option("checkpointLocation", "s3://amzn-s3-insurance-claims/Claims/checkpoints/claims_silver")
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from claims_silver