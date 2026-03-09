# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Check inventory health metrics
spark.table(AGG_TABLES["inventory"]).select(
    "invoice_date_key",
    "total_stock_quantity",
    "total_stock_value",
    "expired_stock_quantity",
    "expired_stock_percentage",
    "inventory_turnover_ratio",
    "days_inventory_outstanding",
    "inventory_health_score"
).orderBy(col("invoice_date_key").desc()).show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select * from `WS-ETL-BNJ`.lh_bnj_gold.gold.fact_appointment

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select 
# MAGIC     CASE
# MAGIC         WHEN is_void = TRUE THEN 'Rejected'
# MAGIC         WHEN is_finalized = TRUE THEN 'Approved'
# MAGIC         ELSE 'Pending'
# MAGIC     END AS claim_status 
# MAGIC from `WS-ETL-BNJ`.lh_bnj_gold.gold_test.fact_claims


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select lost_patients, new_patients from `WS-ETL-BNJ`.lh_bnj_gold.gold_test.agg_patient

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
