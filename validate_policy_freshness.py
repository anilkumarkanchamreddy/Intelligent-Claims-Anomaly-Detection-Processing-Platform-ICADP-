# Databricks notebook source
from pyspark.sql import functions as F
from datetime import date

# Read policy silver table
df_policy = spark.table("enterprise.insurance.policy_silver")

# Get distinct ingestion_date and greatest value
latest_date = df_policy.select("ingestion_date").distinct().agg(F.max("ingestion_date")).collect()[0][0]
print(f"Max load date: {latest_date.date()}")

today = date.today()

print(f"Today's date: {today}")
print(f"Latest policy ingestion_date: {latest_date}")

# Check if today's data exists
if latest_date is None:
    raise Exception("❌ Policy table is empty")

if latest_date.date() < today:
    raise Exception(f"❌ Policy not updated today. Latest: {latest_date}")

print("✅ Policy data is fresh. Proceeding...")