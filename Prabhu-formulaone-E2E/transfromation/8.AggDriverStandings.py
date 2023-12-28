# Databricks notebook source
# MAGIC %run "/Prabhu-formulaone-E2E/includes/configurations"

# COMMAND ----------

mnt_presentation

# COMMAND ----------

driver_standings_df= spark.read.parquet((f'{mnt_presentation}/race_results'))

# COMMAND ----------

display(driver_standings_df.head(10))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

# COMMAND ----------

driver_standings_agg_df = driver_standings_df.groupBy(col("race_year"),col("driver_name"),col("driver_nationality"),col("team"))\
    .agg( sum(col("points")).alias("total_points")
         , count(when(col("position")==1, True)).alias("wins") )

# COMMAND ----------

display(driver_standings_agg_df.filter(col("race_year") == 2020).head(10))

# COMMAND ----------

W = Window.partitionBy(col("race_year")).orderBy(  col("total_points").desc() , col("wins").desc() )

# COMMAND ----------

driver_standings_agg_window_df = driver_standings_agg_df.withColumn("rank" , rank().over(W) )

# COMMAND ----------

display(driver_standings_agg_window_df.filter(col('race_year')==2010))

# COMMAND ----------


driver_standings_agg_window_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_presentation.driver_standings limit 2

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


