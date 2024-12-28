# Databricks notebook source
# MAGIC %run "/Workspace/bank loan/setup/commun var"

# COMMAND ----------

# MAGIC %run "/Workspace/bank loan/utils/commun functions"

# COMMAND ----------

display(spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(path_location_trx).head(2))

# COMMAND ----------

def getSchema():
    schema = """ date string, customerId string, paymentPeriod int, loanAmount double, currencyType string, evaluationChannel string, interest_rate double """
    return schema

def readTrx(schema, location_trx, watermark):
    from pyspark.sql.functions import input_file_name
    return (
        spark.read
             .format("csv")
             .schema(schema)
             .option("header", "true")
             .option("modifiedAfter", watermark)
             .load(location_trx)
             .withColumn("InputFile", input_file_name())
    )

def saveTrx(df, table_name):

    if df.count() !=0:
        print("Processing transactions...", end="")
        df.write.mode("append").saveAsTable(table_name)
        updateWatermarkTb("ingestion_transaction", getCurrentTimeStamp())
        print("Done")
    else:
        print("No new data to load")


# COMMAND ----------

def processTrx():

    schema = getSchema()
    watermark = getWatrmak("ingestion_transaction")
    df = readTrx(getSchema(), path_location_trx, watermark)
    saveTrx(df, "transactions_bz")



# COMMAND ----------

processTrx()

# COMMAND ----------

# MAGIC %sql
# MAGIC select InputFile, count(*) Total
# MAGIC from transactions_bz
# MAGIC group by InputFile
