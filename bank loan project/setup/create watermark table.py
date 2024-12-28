# Databricks notebook source
# MAGIC %sql
# MAGIC create table watermark_tb (process_name string, watermark_time timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into watermark_tb
# MAGIC values ("ingestion_customer", current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into watermark_tb
# MAGIC values ("customer_driver", current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into watermark_tb
# MAGIC values ("ingestion_transaction", current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from watermark_tb
