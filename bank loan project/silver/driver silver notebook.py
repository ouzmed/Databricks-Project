# Databricks notebook source
# MAGIC %run "/Workspace/bank loan/setup/commun var"

# COMMAND ----------

# MAGIC %run "/Workspace/bank loan/utils/commun functions"

# COMMAND ----------

display(spark.read.table("driver_bz").head(2))

# COMMAND ----------

def ReadCustomerDriver():
    from pyspark.sql.functions import substring, to_date, col
    from pyspark.sql.types import LongType
    return (spark.read
             .table("driver_bz")
             .withColumn("customerId", substring("customerId", 4, 11).cast(LongType()))
             .withColumn("date", to_date(col("date")))
             .withColumnRenamed("customerId", "customer_id")
    )

def transformationDF(df):
    from pyspark.sql.functions import when, col, min
    monthtly_salary_min =df.agg({"monthly_salary": "min"}).collect()[0][0]
    df = df.fillna({"monthly_salary":monthtly_salary_min, "health_score":100, "current_debt":0, "category":"OTHERS"})
    df = df.withColumn("flag", when(col("health_score")<100, "True").otherwise("False"))
    return df

def saveDriverSL(df, table_name):

    if not TableIsExist(table_name):
        df.write.mode("overwrite").saveAsTable(table_name)
    else:
        df.createOrReplaceTempView("driver_sl_vw")
        merge_statement =f"""MERGE INTO {table_name} AS t
                             USING driver_sl_vw AS s
                             ON t.customer_id = s.customer_id
                             AND t.date = s.date
                             WHEN MATCHED THEN UPDATE SET *
                             WHEN NOT MATCHED THEN INSERT *
                          """
        spark.sql(merge_statement)
    print("Done")

def processDriverSL():
    df = ReadCustomerDriver()
    df = transformationDF(df)
    saveDriverSL(df, "driver_sl")


# COMMAND ----------

processDriverSL()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from driver_sl

# COMMAND ----------

# MAGIC %sql
# MAGIC select InputFile, count(*) 
# MAGIC from driver_sl
# MAGIC group by InputFile
