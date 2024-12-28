# Databricks notebook source
# DBTITLE 1,setup
# MAGIC %run "/Workspace/bank loan/setup/commun var"

# COMMAND ----------

# MAGIC %run "/Workspace/bank loan/utils/commun functions"

# COMMAND ----------

def readTrxDat(table_name):
    return (spark
                .read
                .table(table_name)
            )

def transformationTrxDat(df):
    from pyspark.sql.functions import substring, to_date, col
    from pyspark.sql.types import LongType
    df = (df.withColumn("customerId", substring("customerId", 4, 11).cast(LongType()))
            .withColumn("date", to_date(col("date"), "dd/MM/yyyy"))
            .withColumnRenamed("customerId", "customer_id")
            .withColumnRenamed("paymentPeriod", "payment_period")
            .withColumnRenamed("loanAmount", "loan_amount")
            .withColumnRenamed("currencyType", "currency_type")
            .withColumnRenamed("evaluationChannel", "evaluation_channel")
          )
    return df

def saveTrxDat(df, table_name):

    if not TableIsExist(table_name):
        df.write.mode("overwrite").saveAsTable(table_name)
    else:
        df.createOrReplaceTempView("trx_sl_vx")
        merge_statement = f"""MERGE INTO {table_name} t
                                  USING trx_sl_vx s
                                  ON t.customer_id = s.customer_id
                                  WHEN MATCHED THEN UPDATE SET *
                                  WHEN NOT MATCHED THEN INSERT *
                               """
        spark.sql(merge_statement)

    print("Loading done")

def processTrxDat():

    df = readTrxDat("transactions_bz")
    df = transformationTrxDat(df)
    saveTrxDat(df, "transactions_sl")

processTrxDat()

# COMMAND ----------

# MAGIC %sql
# MAGIC select InputFile, count(*) Total
# MAGIC from transactions_sl
# MAGIC group by InputFile

# COMMAND ----------

# MAGIC %sql
# MAGIC select date, count(*) from transactions_sl
# MAGIC group by date
