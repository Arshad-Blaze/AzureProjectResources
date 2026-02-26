# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Silver Level - Data Cleaning and Refining

# COMMAND ----------

# Reading CSV Files - nocs.csv

# df = spark.read.csv('abfss://bronze@arshaddatalake.dfs.core.windows.net/nocs/nocs.csv', header=True, inferSchema=True)
df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("abfss://bronze@arshaddatalake.dfs.core.windows.net/nocs/nocs.csv")
display(df)

# COMMAND ----------

# Transformation 1  - Column Drop 
df = df.drop('country')
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
# Transformation 2 - Column Split and dropping unnecesary values 
df = df.withColumn('tag', split('tag', '-')[0])
display(df)

# COMMAND ----------


# Writing Data in Delta Format in Silver

df.write.format("delta")\
.mode("append")\
.option("path","abfss://silver@arshaddatalake.dfs.core.windows.net/nocs")\
.saveAsTable("olympics.silver.nocs_ext")


# COMMAND ----------


# Writing Data as Managed Table in Catalog in Silver Schema

df.write.format("delta")\
.mode("append")\
.saveAsTable("olympics.silver.nocs")
