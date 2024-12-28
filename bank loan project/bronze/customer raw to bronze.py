# Databricks notebook source
# DBTITLE 1,importing the variables
# MAGIC %run "/Workspace/bank loan/setup/commun var"

# COMMAND ----------

# MAGIC %run "/Workspace/bank loan/utils/commun functions"

# COMMAND ----------

# DBTITLE 1,define the schema
def getSchema():
    schema = """customerId string, firstName string, lastName string, phone string, email string, gender string, address string, is_active string
    """
    return schema

# COMMAND ----------

def getWatrmak(process):
    watermark = spark.table("watermark_tb").filter(f"process_name='{process}'").select("watermark_time").collect()[0][0]
    return watermark

# COMMAND ----------

# DBTITLE 1,reading the raw data from datalake
def readRawData(schema, watermark_value, path_location):
       from pyspark.sql.functions import input_file_name

       return (spark.read
           .format("csv")
           .schema(schema)
           .option("header", "true")
           .option("modifiedAfter", watermark_value)
           .load(path_location)
           .withColumn("InputFile", input_file_name())
       )

# COMMAND ----------

# DBTITLE 1,process the incremental loading using watermark value and merge
def writeData():

    from pyspark.sql.functions import current_timestamp
    schema = getSchema()
    watermark_value = getWatrmak('ingestion_customer')
    df = readRawData(schema, watermark_value, path_location)
    if (df.count() != 0):
        if not TableIsExist('customer_bz'):
            df.write.mode("overwrite").saveAsTable("customer_bz")
        else:
            df.createOrReplaceTempView("customer_vw")
            merge_statement = """Merge into customer_bz target
                                USING customer_vw source
                                ON source.customerId = target.customerId
                                WHEN MATCHED THEN UPDATE SET *
                                WHEN NOT MATCHED THEN INSERT *
                                """
            spark.sql(merge_statement)
        updateWatermarkTb('ingestion_customer', getCurrentTimeStamp())
    
    else:
        print("No new data yet")

# COMMAND ----------

# DBTITLE 1,saving the process data to the bronze table
writeData()

# COMMAND ----------

# MAGIC %sql
# MAGIC select watermark_time from watermark_tb
# MAGIC where process_name='ingestion_customer'

# COMMAND ----------

# DBTITLE 1,checking the bronze table
# MAGIC %sql
# MAGIC select count(*), InputFile
# MAGIC from customer_bz group by InputFile
