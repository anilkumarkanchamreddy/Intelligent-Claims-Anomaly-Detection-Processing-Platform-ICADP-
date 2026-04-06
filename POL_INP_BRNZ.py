# Databricks notebook source
# MAGIC %sql
# MAGIC Use catalog enterprise;
# MAGIC use schema insurance;

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %sql
# MAGIC create table if not exists  enterprise.insurance.policy_raw
# MAGIC (
# MAGIC     policy_id string,
# MAGIC     customer_id string,
# MAGIC     policy_start_date date,
# MAGIC     policy_end_date date,
# MAGIC     sum_insured double,
# MAGIC     ingestion_date timestamp,
# MAGIC     source_file string
# MAGIC )

# COMMAND ----------

from datetime import datetime as dt

# 1. Generate the strings
current_utc = dt.utcnow()
load_date_val = current_utc.strftime("%Y%m%d")
ingestion_time_val = current_utc.strftime("%Y%m%d_%H%M")

# 2. Construct the full S3 path
s3_source_path = f"s3://amzn-s3-insurance-claims/Policy/raw/ingestion_{load_date_val}/"

# 3. Create the widgets
dbutils.widgets.text("load_date", load_date_val)
dbutils.widgets.text("ingestion_time", ingestion_time_val)
dbutils.widgets.text("s3_path", s3_source_path)

# 4. Print to verify (This now matches what the SQL will see)
print(f"SQL will use:")
print(f"  load_date: {load_date_val}")
print(f"  ingestion_time: {ingestion_time_val}")
print(f"  s3_path: {s3_source_path}")

# COMMAND ----------

# DBTITLE 1,Cell 3
from datetime import datetime as dt

# Generate current date values directly
current_utc = dt.utcnow()
load_date = current_utc.strftime("%Y%m%d")
ingestion_time = current_utc.strftime("%Y%m%d_%H%M")

# Construct the COPY INTO SQL dynamically
sql_query = f"""
COPY INTO enterprise.insurance.policy_raw
FROM (
    SELECT
        policy_id,
        customer_id,
        TO_DATE(policy_start_date) AS policy_start_date,
        TO_DATE(policy_end_date) AS policy_end_date,
        CAST(sum_insured AS DOUBLE) AS sum_insured,
        TO_TIMESTAMP('{ingestion_time}', 'yyyyMMdd_HHmm') AS ingestion_date,
        _metadata.file_path AS source_file
    FROM 's3://amzn-s3-insurance-claims/Policy/raw/ingestion_{load_date}/'
)
FILEFORMAT = JSON
"""

print(f"Loading data for date: {load_date}")
print(f"Executing COPY INTO from: s3://amzn-s3-insurance-claims/Policy/raw/ingestion_{load_date}/")

# Execute the SQL
spark.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from enterprise.insurance.policy_raw

# COMMAND ----------

