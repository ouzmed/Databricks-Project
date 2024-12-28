# Databricks notebook source
# DBTITLE 1,importing the variables
# MAGIC %run "/Workspace/bank loan/setup/commun var"

# COMMAND ----------

# DBTITLE 1,importing the utils functions
# MAGIC %run "/Workspace/bank loan/utils/commun functions"

# COMMAND ----------

display(spark.read.table("customer_bz"))

# COMMAND ----------

# DBTITLE 1,Transformation of the raw data from the bronze table
def readCustomerData(table_name):
    from pyspark.sql.functions import input_file_name, length, substring, col
    from pyspark.sql.types import LongType

    return (spark.table(f"{table_name}")
                 .withColumn("customerId", substring(col("customerId"), 4, length(col("customerId"))-3).cast(LongType()))
                 .withColumnRenamed("customerId", "customer_id")
                 .withColumnRenamed("lastName", "last_name")
                 .withColumnRenamed("firstName", "first_name")
                 .withColumnRenamed("phone", "phone_number")
                 .withColumnRenamed("address", "customer_address")
                 .withColumnRenamed("is_active", "active_status")
                 )

# COMMAND ----------

def saveCustomerData(df, table_name):
    if not TableIsExist(table_name):
        df.write.mode("overwrite").saveAsTable(table_name)
    else:
        df.createOrReplaceTempView("customer_sl_vw")
        merge_statement = """MERGE INTO customer_sl t
                             USING customer_sl_vw s
                             ON t.customer_id = s.customer_id
                             AND t.InputFile <> s.InputFile
                             WHEN MATCHED THEN UPDATE SET *
                             WHEN NOT MATCHED THEN INSERT *
                          """
        spark.sql(merge_statement)

# COMMAND ----------

# DBTITLE 1,process the data to the silver table
def processDataCustomer():

    print("\nprocessing customer data...", end="")
    df = readCustomerData("customer_bz")
    saveCustomerData(df, "customer_sl")
    
    print("Done")
    
    

# COMMAND ----------

# DBTITLE 1,execute the process function
processDataCustomer()

# COMMAND ----------

# DBTITLE 1,check the silver table
# MAGIC %sql
# MAGIC select InputFile, count(*) from customer_sl
# MAGIC group by InputFile
