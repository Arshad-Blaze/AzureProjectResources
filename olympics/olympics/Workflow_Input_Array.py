# Databricks notebook source
# MAGIC %md 
# MAGIC Building Arrays as Input Parameters for Workflow Automation

# COMMAND ----------

input_array = [
    {"source" : "bronze",
     "target" : "silver",
     "im_par_folder" : "events"},
    {"source" : "bronze",
     "target" : "silver",
     "im_par_folder" : "coaches"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "Workflow_Input", value = input_array)