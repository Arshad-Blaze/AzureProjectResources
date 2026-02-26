# Databricks notebook source
# MAGIC %md 
# MAGIC Delta Live Tables - Gold Layer

# COMMAND ----------

import dlt 
import pandas as pd
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Expectations - Data Quality Rules

# COMMAND ----------

expect_coaches = {
    "NotNullCheckCode" : "code is not null",
    "AllowTrueCurrent" : "current is True"
}

# COMMAND ----------

expect_nocs = {
    "NotNullCheckCode" : "code is not null"
}

# COMMAND ----------

expect_events = {
    "NotNullCheckEvent" : "event is not null"
}

# COMMAND ----------

# MAGIC %md
# MAGIC Coaches - DLT

# COMMAND ----------

@dlt.table
def source_coaches():
    df = spark.readStream.table("olympics.silver.coaches")
    return df


# COMMAND ----------

@dlt.view

def view_coaches_new():
    df = spark.readStream.table("LIVE.source_coaches")
    df = df.fillna("unknown")
    return df

# COMMAND ----------

@dlt.table
@dlt.expect_all(expect_coaches)

def coaches_new():
    df = spark.readStream.table("LIVE.view_coaches_new")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC NOCS - DLT

# COMMAND ----------

@dlt.view
def source_nocs():
    df = spark.readStream.table("olympics.silver.nocs")
    return df

# COMMAND ----------

@dlt.table
@dlt.expect_all_or_drop(expect_nocs)
def nocs():
    df = spark.readStream.table("LIVE.source_nocs")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC Events - DLT
# MAGIC

# COMMAND ----------

@dlt.view
def source_events():
    df = spark.readStream.table("olympics.silver.events")
    return df

# COMMAND ----------

@dlt.table
@dlt.expect_all(expect_events)
def events():
    df = spark.readStream.table("LIVE.source_events")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC CDC - Apply Changes

# COMMAND ----------

@dlt.view

def source_athletes():
    df = spark.readStream.table("olympics.silver.athletes")
    return df



# COMMAND ----------

dlt.create_streaming_table("athletes")

# COMMAND ----------

dlt.apply_changes(
    target = "athletes",
    source = "source_athletes",
    keys = ["athlete_id"],
    sequence_by="height",
    stored_as_scd_type=1
)