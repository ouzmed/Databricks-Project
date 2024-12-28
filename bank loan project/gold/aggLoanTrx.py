# Databricks notebook source
# MAGIC %run "/Workspace/bank loan/utils/commun functions"

# COMMAND ----------

df_driver_sl = spark.read.table("driver_sl")
df_trx_sl = spark.read.table("transactions_sl")

# COMMAND ----------

display(df_driver_sl.head(2))
display(df_trx_sl.head(2))

# COMMAND ----------

def aggloanTrx(df1, df2):
    return (
        df1.join(df2, df1.customer_id == df2.customer_id, how="left")
        .select(df1.date, df1.customer_id, df1.payment_period, df1.loan_amount, df1.currency_type, df1.evaluation_channel, df1.interest_rate, df2.monthly_salary, df2.health_score, df2.current_debt, df2.category, df2.flag)   
    )

def saveAggLoanTrx(df, table_name):
    if not TableIsExist(table_name):
        df.write.mode("overwrite").saveAsTable(table_name)
    else:
        df.createOrReplaceTempView("aggloanTrx_vw")
        merge_statement = f"""merge into {table_name} t
                              using aggloanTrx_vw s on t.customer_id = s.customer2
                              when matched then update set *
                              when not matched then insert *
                           """

def writeAggLoanTrx():
    df1 = df_trx_sl
    df2 = df_driver_sl
    df = aggloanTrx(df1, df2)
    saveAggLoanTrx(df, "aggLoanTrx")

# COMMAND ----------

writeAggLoanTrx()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aggLoanTrx
