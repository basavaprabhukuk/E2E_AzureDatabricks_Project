# Databricks notebook source
# MAGIC %run "/Prabhu_Projects/Prabhu-formulaone-E2E/includes/configurations"

# COMMAND ----------

# MAGIC %run "/Prabhu_Projects/Prabhu-formulaone-E2E/includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("v_file_date","2021-03-28")
p_file_date = dbutils.widgets.get("v_file_date")

# COMMAND ----------

drivers_df = spark.read.parquet(f'{mnt_processed}/drivers')\
    .withColumnRenamed("fullname", "driver_name")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

type(drivers_df)

# COMMAND ----------

races_df = spark.read.parquet(f'{mnt_processed}/races')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

races_df_1 = races_df.withColumnRenamed("name", "race_name")\
                     .withColumnRenamed("name", "race_name")\
                     .withColumn( "race_date",to_date(races_df.race_timestamp, 'yyyy-MM-dd' ))
                    



# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

circuits_df = spark.read.parquet(f'{mnt_processed}/circuits').withColumnRenamed("location","circuit_location" )
                      

# COMMAND ----------

constructer_df = spark.read.parquet(f'{mnt_processed}/constructer').withColumnRenamed("name","team" )

# COMMAND ----------

results_df = spark.read.parquet(f'{mnt_processed}/results')\
    .filter(f"file_date = '{p_file_date}'") \
    .withColumnRenamed("grid","grid" )\
    .withColumnRenamed("fastest_lap","fastest_lap" )\
    .withColumnRenamed("time","race_time" )\
    .withColumnRenamed("points","points" )\
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date") 

# COMMAND ----------

Join_circuits_drivers_df =  circuits_df.join(races_df_1 , races_df_1.circuit_Id == circuits_df.circuit_Id, how="inner")\
    

# COMMAND ----------

Final_join_df= results_df.join(Join_circuits_drivers_df , Join_circuits_drivers_df.race_Id ==  results_df.result_race_id)\
                .join(drivers_df , drivers_df.driver_id ==  results_df.driver_Id)\
                .join(constructer_df , constructer_df.constructor_Id ==  results_df.constructor_Id)
    

# COMMAND ----------

final_df = Final_join_df.select( Final_join_df.race_year,  Final_join_df.race_name,Final_join_df.race_date,Final_join_df.circuit_location,Final_join_df.driver_number,Final_join_df.driver_name,Final_join_df.driver_nationality,Final_join_df.team , Final_join_df.grid, Final_join_df.fastest_lap ,Final_join_df.race_time, Final_join_df.points, Final_join_df.position, Final_join_df.race_Id , Final_join_df.result_file_date   ).withColumn("creates_date", current_date())

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

overwrite_partition_presentation(final_df, 'f1_presentation', 'race_results', 'race_Id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC Select race_Id, count(race_Id) cnts from  f1_presentation.race_results
# MAGIC group by race_Id

# COMMAND ----------


