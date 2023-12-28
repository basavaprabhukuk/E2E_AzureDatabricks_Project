# Databricks notebook source
# MAGIC %run "/Prabhu_Projects/Prabhu-formulaone-E2E/includes/configurations"

# COMMAND ----------

# MAGIC %run "/Prabhu_Projects/Prabhu-formulaone-E2E/includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("v_data_source","")
v_data_source = dbutils.widgets.get("v_data_source")

# COMMAND ----------

dbutils.widgets.text("v_file_date","2021-03-28")
p_file_date = dbutils.widgets.get("v_file_date")
p_file_date

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

Qualifyings_schema = StructType([ StructField("qualifyId", IntegerType(), False ),
                                   StructField("raceId", IntegerType(), True ),
                                   StructField("driverId", IntegerType() , True ),
                                   StructField("constructorId", IntegerType(), True ),
                                   StructField("number", IntegerType(), True ),
                                   StructField("position", IntegerType(), True ),
                                   StructField("q1", StringType(), True ),
                                   StructField("q2", StringType(), True ),
                                   StructField("q3", StringType(), True )
])

# COMMAND ----------

Qualifyings_df = spark.read.option("multiLine", True).json(f"{mnt_raw}/{p_file_date}/qualifying/",schema=Qualifyings_schema )

# COMMAND ----------


Qualifyings_df_ingestuon_date_df =  add_ingestion_date(Qualifyings_df)

# COMMAND ----------

Qualifyings_transform_df = Qualifyings_df_ingestuon_date_df.withColumnRenamed("qualifyId","qualify_Id")\
                                         .withColumnRenamed("raceId","race_Id")\
                                         .withColumnRenamed("driverId","driver_Id")\
                                         .withColumnRenamed("constructorId","constructor_Id")\
                                         .withColumn("source_system" , lit(v_data_source) )\
                                         .withColumn("file_date", lit(p_file_date))
                                         

# COMMAND ----------

#Qualifyings_transform_df.write.option("path", "dbfs:/mnt/sourceinput/databricks-course/processed/qualifying").mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

overwrite_partition(Qualifyings_transform_df, 'f1_processed', 'qualifying', 'race_Id')

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC Select race_Id, count(race_Id) cnts from  f1_processed.lap_times
# MAGIC group by race_Id

# COMMAND ----------

dbutils.notebook.exit("Success")
