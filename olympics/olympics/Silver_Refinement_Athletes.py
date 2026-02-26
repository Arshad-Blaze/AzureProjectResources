# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet Data Reading

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@arshaddatalake.dfs.core.windows.net/athletes")
display(df)

# COMMAND ----------

display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation 1 - Replace Null Values

# COMMAND ----------

df = df.fillna({"birth_place": "No Info", "birth_country": "No Birth Place", "residence_place": "No Info", "residence_country": "No Residence Place"})

df.select("birth_place", "birth_country", "residence_place", "residence_country").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Data Filtering

# COMMAND ----------

df_selective = df.filter(
    (col('current') == True) 
    & (col('name').contains('van '))
    & (col('gender') == 'Male')
)
df_selective.display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Type Casting
# MAGIC

# COMMAND ----------

df = df.withColumn('height', col('height').cast(FloatType()))\
        .withColumn('weight', col('weight').cast(FloatType()))\
        .withColumn('birth_date', col('birth_date').cast(DateType()))
df.select("height", "weight", "birth_date").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Data Sorting

# COMMAND ----------

# df_sort = df.sort(col('height').desc(), col('weight').asc())
# df_sort = df.sort('height', 'weight', ascending=[False, True])
df_sort = df.sort('height', 'weight', ascending=[0,1]).filter(col('weight') > 0)
# df_sort.select('height', 'weight').display()
display(df_sort)


# COMMAND ----------

# MAGIC %md
# MAGIC Column Handling - Regexp

# COMMAND ----------

df_sort = df_sort.withColumn('nationality',regexp_replace(col('nationality'),'United States', 'US',))
df_sort.select('nationality').sort('nationality', ascending=[0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Group By and Aggregation

# COMMAND ----------

df_duplicates = df.groupBy('code').agg(count('code').alias('code_count'))
df_duplicates.filter(col('code_count') > 1).display()


# COMMAND ----------

# MAGIC %md
# MAGIC Column renaming

# COMMAND ----------

df_sort = df_sort.withColumnRenamed('code','athlete_id')
df_sort.display()

# COMMAND ----------

df_sort = df_sort.withColumn('occupation', split('occupation',','))
df_sort.select("occupation").display()

# COMMAND ----------

df_sort.columns

# COMMAND ----------

df_final = df_sort.select(
    'athlete_id',
 'current',
 'name',
 'name_short',
 'name_tv',
 'gender',
 'function',
 'country_code',
 'country',
 'country_long',
 'nationality',
 'nationality_long',
 'nationality_code',
 'height',
 'weight'
)

# COMMAND ----------

df_final.createOrReplaceTempView('athletes_cleaned')
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Window Functions

# COMMAND ----------

from pyspark.sql.window import Window

df_final = df_final.withColumn("running_weight", sum("weight").over(Window.partitionBy("nationality").orderBy('height').rowsBetween(Window.unboundedPreceding, Window.currentRow)))
df_final = df_final.withColumn("cumulative_weight", sum("weight").over(Window.partitionBy("nationality").orderBy('height').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
display(df_final)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC SUM(weight) OVER(PARTITION BY nationality ORDER BY height ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS cumulative_weight,
# MAGIC SUM(weight) OVER(PARTITION BY nationality ORDER BY height ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_weight
# MAGIC FROM athletes_cleaned

# COMMAND ----------

df_final.write.format("delta")\
            .mode("append")\
            .option("path", "abfss://silver@arshaddatalake.dfs.core.windows.net/athletes")\
            .saveAsTable("olympics.silver.athletes")