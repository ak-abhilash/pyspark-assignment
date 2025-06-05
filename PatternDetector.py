# Databricks notebook source
# MAGIC %md
# MAGIC ## Connecting to Postgres

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(
    host="dpg-d10n19e3jp1c7395oh4g-a.oregon-postgres.render.com",
    dbname="devdolphins_temp",
    user="devdolphins_temp_user", 
    password="0PMec0Rom6SrAwuqpUPwxOvVLRaYBY1e",
    port="5432",
    sslmode="require"
)
cur = conn.cursor()

# COMMAND ----------

cur.execute("""
CREATE TABLE IF NOT EXISTS processed_chunks (
    id SERIAL PRIMARY KEY,
    chunk_name TEXT UNIQUE,
    status TEXT DEFAULT 'processed',
    processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

conn.commit()
cur.close()
conn.close()

print("Table created")

# COMMAND ----------

# %pip install psycopg2-binary

# COMMAND ----------

import psycopg2

def is_chunk_processed(chunk_name):
    try:
        conn = psycopg2.connect(
            host="dpg-d10n19e3jp1c7395oh4g-a.oregon-postgres.render.com",
            dbname="devdolphins_temp",
            user="devdolphins_temp_user", 
            password="0PMec0Rom6SrAwuqpUPwxOvVLRaYBY1e",
            port="5432",
            sslmode="require"
        )
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM processed_chunks WHERE chunk_name = %s", (chunk_name,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result is not None
    except Exception as e:
        print(f"Error checking chunk: {e}")
        return False

def mark_chunk_processed(chunk_name):
    try:
        conn = psycopg2.connect(
            host="dpg-d10n19e3jp1c7395oh4g-a.oregon-postgres.render.com",
            dbname="devdolphins_temp",
            user="devdolphins_temp_user", 
            password="0PMec0Rom6SrAwuqpUPwxOvVLRaYBY1e",
            port="5432",
            sslmode="require"
        )
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO processed_chunks (chunk_name) VALUES (%s) ON CONFLICT (chunk_name) DO NOTHING",
            (chunk_name,)
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"Marked {chunk_name} as processed")
    except Exception as e:
        print(f"Error inserting chunk: {e}")


# COMMAND ----------

import pandas as pd
conn = psycopg2.connect(
    host="dpg-d10n19e3jp1c7395oh4g-a.oregon-postgres.render.com",
    dbname="devdolphins_temp",
    user="devdolphins_temp_user", 
    password="0PMec0Rom6SrAwuqpUPwxOvVLRaYBY1e",
    port="5432",
    sslmode="require"
)
cur = conn.cursor()

# Query to check the logs
query = "SELECT * FROM processed_chunks ORDER BY processed_time DESC"
df_chunks = pd.read_sql(query, conn)
cur.close()
conn.close()

display(df_chunks)

# COMMAND ----------

df = spark.read.option("header", "true").csv("/FileStore/tables/CustomerImportance.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

dbutils.fs.ls("s3a://devdolphins-assignment-bucket/")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

s3_base_path = "s3a://devdolphins-assignment-bucket/"
chunk_path = s3_base_path + "transactions_chunks/"
importance_path = "/FileStore/tables/CustomerImportance.csv"

# COMMAND ----------

transactions = spark.read.option("header", "true").csv(chunk_path)
importance = spark.read.option("header", "true").csv(importance_path)

# COMMAND ----------

importance.count()

# COMMAND ----------

transactions.show(10)

# COMMAND ----------

importance.show(10)

# COMMAND ----------

# Clean quotes from all key columns in transactions
transactions = transactions \
    .withColumn("customer", regexp_replace(trim(col("customer")), "'", "")) \
    .withColumn("merchant", regexp_replace(trim(col("merchant")), "'", "")) \
    .withColumn("gender", regexp_replace(trim(lower(col("gender"))), "'", "")) \
    .withColumn("age", regexp_replace(trim(col("age")), "'", "")) 

# Clean quotes from CustomerImportance
importance = importance \
    .withColumnRenamed("Source", "customer") \
    .withColumnRenamed("Target", "merchant") \
    .withColumn("customer", regexp_replace(trim(col("customer")), "'", "")) \
    .withColumn("merchant", regexp_replace(trim(col("merchant")), "'", "")) \
    .withColumn("weight", col("Weight").cast("float"))

# COMMAND ----------

transactions.select("customer", "merchant", "gender", "age").distinct().show(5, truncate=False)
importance.select("customer", "merchant", "weight").distinct().show(5, truncate=False)

# COMMAND ----------

#Casting required columns
transactions = transactions.withColumn("transactionAmount", col("amount").cast("float"))
transactions = transactions.withColumn("gender", lower(col("gender")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## PatId2 - CHILD Detection

# COMMAND ----------

pat2 = transactions.groupBy("customer", "merchant") \
    .agg(
        avg("transactionAmount").alias("avg_txn"),
        count("*").alias("txn_count")
    ).filter((col("avg_txn") < 23) & (col("txn_count") >= 80)) \
    .withColumn("YStartTime", current_timestamp()) \
    .withColumn("detectionTime", current_timestamp()) \
    .withColumn("patternId", lit("PatId2")) \
    .withColumn("ActionType", lit("CHILD")) \
    .select("YStartTime", "detectionTime", "patternId", "ActionType", "customer", "merchant")

# COMMAND ----------

pat2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PatId3 - DEI-NEEDED Detection

# COMMAND ----------

transactions.display()

# COMMAND ----------

gender_counts = transactions.groupBy("merchant", "gender") \
    .agg(countDistinct("customer").alias("count"))

male = gender_counts.filter(col("gender") == "m") \
    .selectExpr("merchant as m_id", "count as male_count")

female = gender_counts.filter(col("gender") == "f") \
    .selectExpr("merchant as m_id", "count as female_count")

pat3 = female.join(male, "m_id") \
    .filter((col("female_count") < col("male_count")) & (col("female_count") > 100)) \
    .withColumn("YStartTime", current_timestamp()) \
    .withColumn("detectionTime", current_timestamp()) \
    .withColumn("patternId", lit("PatId3")) \
    .withColumn("ActionType", lit("DEI-NEEDED")) \
    .withColumn("customer", lit("")) \
    .withColumn("merchant", col("m_id")) \
    .select("YStartTime", "detectionTime", "patternId", "ActionType", "customer", "merchant")

# COMMAND ----------

pat3.display()

# COMMAND ----------

# To verify if there is only one value
female.join(male, "m_id") \
    .filter((col("female_count") < col("male_count")) & (col("female_count") > 100)) \
    .count()

# COMMAND ----------

gender_counts.groupBy("merchant").pivot("gender").sum("count").filter("f > 100 and f < m").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PatId1 - UPGRADE Detection

# COMMAND ----------

importance_clean = importance.withColumnRenamed("Source", "customer") \
                            .withColumnRenamed("Target", "merchant") \
                            .withColumn("weight", col("Weight").cast("float"))

cust_txn = transactions.groupBy("customer", "merchant").agg(count("*").alias("txn_count"))
merchant_totals = transactions.groupBy("merchant").agg(count("*").alias("total_txn"))

cust_imp = cust_txn.join(
    importance_clean.select("customer", "merchant", "weight"),
    on=["customer", "merchant"],
    how="inner"
)

cust_all = cust_imp.join(merchant_totals, on="merchant").filter(col("total_txn") > 50000)

#To calculate percentile ranks
txn_window = Window.partitionBy("merchant").orderBy(col("txn_count").desc())
weight_window = Window.partitionBy("merchant").orderBy(col("weight").asc())

ranked = cust_all.withColumn("txn_rank", percent_rank().over(txn_window)) \
                 .withColumn("weight_rank", percent_rank().over(weight_window))

pat1 = ranked.filter((col("txn_rank") >= 0.99) & (col("weight_rank") <= 0.01)) \
             .withColumn("YStartTime", current_timestamp()) \
             .withColumn("detectionTime", current_timestamp()) \
             .withColumn("patternId", lit("PatId1")) \
             .withColumn("ActionType", lit("UPGRADE")) \
             .select("YStartTime", "detectionTime", "patternId", "ActionType", "customer", "merchant")

# COMMAND ----------

pat1.display()

# COMMAND ----------

# Checks to confirm the results are good
# Test 1: Merchant volume validation
pat1_merchants = pat1.select("merchant").distinct()
transactions.groupBy("merchant").count() \
    .join(pat1_merchants, on="merchant", how="inner") \
    .filter(col("count") <= 50000) \
    .display()

# COMMAND ----------

# Test 2: Transaction rank validation
pat1.join(
    ranked.select("customer", "merchant", "txn_rank"),
    on=["customer", "merchant"],
    how="inner"
).filter(col("txn_rank") < 0.99).display()

# COMMAND ----------

# Test 3: Data consistency check
pat1_check = ranked.filter((col("txn_rank") >= 0.99) & (col("weight_rank") <= 0.01)) \
                   .select("customer", "merchant")

# Select only the same columns from pat1 for comparison
pat1.select("customer", "merchant").subtract(pat1_check).display()

# COMMAND ----------

import math

all_patterns = pat1.unionByName(pat2).unionByName(pat3)
df = all_patterns.rdd.zipWithIndex().toDF(["data", "row_id"]).selectExpr("data.*", "row_id")

# Set batch size
batch_size = 50
total_rows = df.count()
num_batches = math.ceil(total_rows / batch_size)

# Write each batch to S3 as a separate CSV
for batch_id in range(num_batches):
    start = batch_id * batch_size
    end = start + batch_size

    batch = df.filter((df.row_id >= start) & (df.row_id < end)).drop("row_id")
    batch.coalesce(1).write.mode("overwrite").option("header", True)\
        .csv(f"{s3_base_path}pattern_output/batch_{batch_id}.csv")

# COMMAND ----------

print("pat1 count:", pat1.count())
print("pat2 count:", pat2.count())
print("pat3 count:", pat3.count())