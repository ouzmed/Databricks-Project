# Databricks-Project
Bank Loan Analysis Project: 

a- Data sources (ADLS GEN2)
-Customer data
-Customer Drivers
-Loan Transactions

b- Processing using databricks:
- Used the delta tables (bronze, silver, gold)
- Implemented incremental loading and processing of bank customer without duplication by using upsert and loan data
- Used manual watermark table to track incremental file/data processing.
- Used medallion architechture and folder structure.
![arch_loan](https://github.com/user-attachments/assets/e1d2d22b-6e93-499c-a83d-af7b3d4d005c)
