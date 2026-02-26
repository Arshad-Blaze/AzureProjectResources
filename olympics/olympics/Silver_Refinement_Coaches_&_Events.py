# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Dynamic Data Reading

# COMMAND ----------

# MAGIC %md 
# MAGIC Parameter Creation

# COMMAND ----------

# Parameter for Source Container
dbutils.widgets.text("source","")
# Parameter for Sink / Target Container
dbutils.widgets.text("target","")
# Parameter for Immediate Parent Folder
dbutils.widgets.text("im_par_folder","")

# COMMAND ----------

# MAGIC %md 
# MAGIC Parameter Allocation 

# COMMAND ----------

source = dbutils.widgets.get("source")
target = dbutils.widgets.get("target")
folder = dbutils.widgets.get("im_par_folder")

# COMMAND ----------

df = spark.read.format("parquet").load(f"abfss://{source}@arshaddatalake.dfs.core.windows.net/{folder}")
display(df)

# COMMAND ----------

# df_events = spark.read.format("parquet")\
#             .load("abfss://bronze@arshaddatalake.dfs.core.windows.net/events")
# display(df_events)


# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
        .option("path",f"abfss://{target}@arshaddatalake.dfs.core.windows.net/{folder}")\
        .saveAsTable(f"olympics.{target}.{folder}")