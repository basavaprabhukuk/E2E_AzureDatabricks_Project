# Databricks notebook source
dbutils.widgets.text("v_data_source","")
v_data_source = dbutils.widgets.get("v_data_source")


# COMMAND ----------

dbutils.widgets.text("v_file_date","2021-03-28")
p_file_date = dbutils.widgets.get("v_file_date")

# COMMAND ----------

# MAGIC %run "/Prabhu_Projects/Prabhu-formulaone-E2E/includes/configurations"

# COMMAND ----------

# MAGIC %run "/Prabhu_Projects/Prabhu-formulaone-E2E/includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

circuit_schema = StructType( [StructField("circuitId" , IntegerType() , False ),
                              StructField("circuitRef" , StringType() , True ),
                              StructField("name" , StringType() , True ),
                              StructField("location" , StringType() , True ),
                              StructField("country" , StringType() , True ),
                              StructField("lat" , FloatType() , True ),
                              StructField("lng" , FloatType(), True ),
                              StructField("alt" , IntegerType() , True ),
                              StructField("url" , StringType() , True )
                              
                            ])

# COMMAND ----------

circuit_df = spark.read.option('header',  True).schema(circuit_schema).csv(f'{mnt_raw}/{p_file_date}/circuits.csv')

# COMMAND ----------

circuit_select_df = circuit_df.\
    select(col('circuitId'), col('circuitRef'), col('name'), col('location'),col('country'),col('lat'),col('lng'),col('alt'))

# COMMAND ----------

circuit_renamed_df = circuit_select_df.withColumnRenamed('circuitId', 'circuit_Id')\
    .withColumnRenamed('circuitRef', 'circuit_Ref')\
    .withColumnRenamed('lat', 'latitude')\
    .withColumnRenamed('lng', 'longitude')\
    .withColumnRenamed('alt', 'altitude')\
    .withColumn("file_date", lit(p_file_date))

# COMMAND ----------

circuit_final_df =  add_ingestion_date(circuit_renamed_df)

# COMMAND ----------

circuit_final_df1  =  circuit_final_df.withColumn("source_system" , lit(v_data_source) )

# COMMAND ----------

circuit_final_df1.write.option("path", "dbfs:/mnt/sourceinput/databricks-course/processed/circuits").mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_processed.circuits

# COMMAND ----------


