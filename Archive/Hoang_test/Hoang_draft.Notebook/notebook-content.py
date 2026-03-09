# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c9d7507e-938a-4c6d-a042-d8743e386ab5",
# META       "default_lakehouse_name": "lh_bnj_bronze",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
# META         },
# META         {
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         },
# META         {
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         },
# META         {
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "d689dc5c-1a53-4ebb-bde6-ba010ac9f4e7",
# META       "known_warehouses": [
# META         {
# META           "id": "d689dc5c-1a53-4ebb-bde6-ba010ac9f4e7",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "6c4c66d5-647d-4056-a27e-381254b9833d",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "7a446370-de43-42a5-a5c5-82412cbd8fb0",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "032c2344-9ff8-416e-91f5-cb7b3799ae82",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from lh_bnj_silver.xero.silver_invoices
# MAGIC order by invoice_date DESC
# MAGIC limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from lh_bnj_bronze.xero.brz_invoices
# MAGIC order by `records.date` DESC
# MAGIC limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/xero/Data/20260302050005/xero_invoices.json")
# df now is a Spark DataFrame containing JSON data from "Files/xero/Data/20260302050005/xero_invoices.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_test.fact_expenses AS
# MAGIC SELECT *
# MAGIC FROM gold.fact_expenses;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS gold_test.fact_revenue AS
# MAGIC SELECT *
# MAGIC FROM gold.fact_revenue;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * from lh_bnj_metadata.md.etl_jobs

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
