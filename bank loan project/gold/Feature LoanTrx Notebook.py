# Databricks notebook source
# MAGIC %run "/Workspace/bank loan/utils/commun functions"

# COMMAND ----------

def getAggLoanTrx():
    from pyspark.sql.functions import avg, sum, max, min
    return (
        spark.read
             .table("aggLoanTrx")
             .groupBy("date","payment_period", "currency_type", "evaluation_channel", "category")
             .agg(sum("loan_amount").alias("Total_loan_amount"),
                  avg("loan_amount").alias("Avg_loan_amount"),
                  sum("current_debt").alias("Total_current_debt"),
                  avg("interest_rate").alias("Avg_interest_rate"),
                  max("interest_rate").alias("Max_interest_rate"),
                  min("interest_rate").alias("Min_interest_rate"),
                  avg("health_score").alias("Avg_health_score"),
                  avg("monthly_salary").alias("Avg_monthly_salary"))
    )

def saveFeatureLoanTrx(df, table_name):
    if not TableIsExist(table_name):
        df.write.mode("overwrite").saveAsTable(table_name)
    else:
        df.createOrReplaceTempView("feature_loan_trx_vw")
        merge_statement = f"""merge into {table_name} t
                              using feature_loan_trx_vw s
                              on t.date = s.date and t.payment_period = s.payment_period and t.currency_type = s.currency_type and t.evaluation_channel = s.evaluation_channel and t.category = s.category
                              when matched then update
                                t.Total_loan_amount = t.Total_loan_amount + s.Total_loan_amount, 
                                t.Avg_loan_amount = t.Avg_loan_amount + s.Avg_loan_amount,
                                t.Total_current_debt = t.Total_current_debt + s.Total_current_debt,
                                t.Avg_interest_rate = t.Avg_interest_rate + s.Avg_interest_rate,
                                t.Max_interest_rate = t.Max_interest_rate + s.Max_interest_rate,
                                t.Min_interest_rate = t.Min_interest_rate + s.Min_interest_rate,
                                t.Avg_health_score = t.Avg_health_score + s.Avg_health_score,
                                t.Avg_monthly_salary = t.Avg_monthly_salary + s.Avg_monthly_salary
                              when not matched then insert *
                           """
        spark.sql(merge_statement)

def processFeatureLoanTrx():
    df = getAggLoanTrx()
    saveFeatureLoanTrx(df, "feature_loan_trx")

# COMMAND ----------

processFeatureLoanTrx()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from feature_loan_trx
