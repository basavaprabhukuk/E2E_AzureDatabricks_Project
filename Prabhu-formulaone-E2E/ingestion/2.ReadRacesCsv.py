# Databricks notebook source
# MAGIC %run "/Prabhu_Projects/Prabhu-formulaone-E2E/includes/configurations"

# COMMAND ----------

# MAGIC %run "/Prabhu_Projects/Prabhu-formulaone-E2E/includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("v_data_source","")
v_data_source = dbutils.widgets.get("v_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

race_schema = StructType([ StructField("raceId",  IntegerType(), False ),
                           StructField("year",  IntegerType(), False ),
                           StructField("round",  IntegerType(), False ),
                           StructField("circuitId",  IntegerType(), False ),
                           StructField("name",  StringType(), False ),
                           StructField("date",  StringType() , False ),
                           StructField("time",  StringType() , False ),
                           StructField("url",  StringType(), False )                       

])

# COMMAND ----------

races_df = spark.read.csv(f"{mnt_raw}/{v_file_date}/races.csv" , header= True ,   schema = race_schema  )


# COMMAND ----------

races_nourl_df = races_df.drop(col("url") )

# COMMAND ----------


races_newcol_df =   add_ingestion_date(races_nourl_df)   

# COMMAND ----------

races_renamecol_df = races_newcol_df.withColumnRenamed("raceId", "race_Id")\
                                    .withColumnRenamed("circuitId","circuit_Id")\
                                    .withColumnRenamed("year","race_year")\
                                    .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

races_timestamp_df= races_renamecol_df.withColumn("race_timestamp",  to_timestamp( concat(col('date'), lit(' ') ,col('time')) , 'yyyy-MM-dd HH:mm:ss') )

# COMMAND ----------

races_final_df = races_timestamp_df.drop(col('date')).drop(col('time')).withColumn("source_system" , lit(v_data_source) )


# COMMAND ----------

races_final_df.write.option("path", "dbfs:/mnt/sourceinput/databricks-course/processed/races").mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

display(spark.read.parquet(f"{mnt_processed}/races").head(2))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


