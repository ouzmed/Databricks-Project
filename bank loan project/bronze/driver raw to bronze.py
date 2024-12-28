# Databricks notebook source
# MAGIC %run "/Workspace/bank loan/setup/commun var"

# COMMAND ----------

# MAGIC %run "/Workspace/bank loan/utils/commun functions"

# COMMAND ----------

#date,customerId,monthly_salary,health_score,current_debt,category
df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(path_location_driver)

# COMMAND ----------

def getSchema():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    schema = StructType([
        StructField("date", StringType()),
        StructField("customerId", StringType()),
        StructField("monthly_salary", IntegerType()),
        StructField("health_score", IntegerType()),
        StructField("current_debt", DoubleType()),
        StructField("category", StringType())
    ])
    return schema

# COMMAND ----------

def readDriver(schema_file, location_file, watermark):
    from pyspark.sql.functions import input_file_name
    return (spark.read
                 .format("csv")
                 .schema(schema_file)
                 .option("header", "true")
                 .option("modifiedAfter", watermark)
                 .load(location_file)
                 .withColumn("InputFile", input_file_name())
                 )

# COMMAND ----------

def saveDriver(df, table_name):

    if df.count()!=0:

        df.write.mode("append").saveAsTable(table_name)
        updateWatermarkTb("customer_driver", getCurrentTimeStamp())

    else:
        print(f"No Data saved to {table_name}")


# COMMAND ----------

def processDriver():

    schema_file = getSchema()
    location_file = path_location_driver
    watermark = getWatrmak("customer_driver")
    df = readDriver(schema_file, location_file, watermark)
    saveDriver(df, "driver_bz")

# COMMAND ----------

processDriver()

# COMMAND ----------

# MAGIC %sql
# MAGIC select InputFile, count(*) from driver_bz
# MAGIC group by InputFile
