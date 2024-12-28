# Databricks notebook source
def TableIsExist(table_name):
    try:
        spark.sql(f"""desc table {table_name}""")
        return True
    except:
        return False

# COMMAND ----------

def getCurrentTimeStamp():
    timestamp_df = spark.sql("SELECT current_timestamp() AS current_time")
    current_time = timestamp_df.collect()[0][0]
    return current_time

# COMMAND ----------

def updateWatermarkTb(process, Wvalue):
    spark.sql(f"""
              update watermark_tb
              set watermark_time = '{Wvalue}'
              where process_name = '{process}'""")

# COMMAND ----------

def getWatrmak(process):
    watermark = spark.table("watermark_tb").filter(f"process_name='{process}'").select("watermark_time").collect()[0][0]
    return watermark
