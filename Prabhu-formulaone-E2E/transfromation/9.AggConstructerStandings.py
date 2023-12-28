# Databricks notebook source
# MAGIC %run "/Prabhu-formulaone-E2E/includes/configurations"

# COMMAND ----------

mnt_presentation

# COMMAND ----------

contructer_standings_df= spark.read.parquet((f'{mnt_presentation}/race_results'))

# COMMAND ----------

display(contructer_standings_df.head(10))

# COMMAND ----------

contructer_standings_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

# COMMAND ----------

contructer_standings_agg_df = contructer_standings_df.groupBy(col("race_year"),col("team"))\
    .agg( sum(col("points")).alias("total_points")
         , count(when(col("position")==1, True)).alias("wins") )

# COMMAND ----------

display(contructer_standings_agg_df.filter(col("race_year") == 2020).head(10))

# COMMAND ----------

W = Window.partitionBy(col("race_year")).orderBy(  col("total_points").desc() , col("wins").desc() )

# COMMAND ----------

contructer_standings_agg_window_df = contructer_standings_agg_df.withColumn("rank" , rank().over(W) )

# COMMAND ----------

display(contructer_standings_agg_window_df.filter(col('race_year')==2020))

# COMMAND ----------

contructer_standings_agg_window_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructer_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_presentation.constructer_standings LIMIT 2

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


