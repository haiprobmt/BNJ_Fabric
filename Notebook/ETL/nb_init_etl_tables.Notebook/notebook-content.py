# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "48bd1f5e-ef56-4df0-8515-17758bcbd734",
# META       "default_lakehouse_name": "lh_bnj_metadata",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Create schema if needed
# MAGIC CREATE SCHEMA IF NOT EXISTS lh_bnj_metadata.md;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_metadata.md.etl_jobs;
# MAGIC 
# MAGIC -- Create the Delta table
# MAGIC CREATE TABLE IF NOT EXISTS lh_bnj_metadata.md.etl_jobs (
# MAGIC     job_id                 BIGINT,
# MAGIC     job_group_name         STRING,
# MAGIC     job_name               STRING,
# MAGIC     active_flg             STRING,
# MAGIC     src_catalog            STRING,
# MAGIC     src_table              STRING,
# MAGIC     tgt_catalog            STRING,
# MAGIC     tgt_table              STRING,
# MAGIC     job_script             STRING,
# MAGIC     filter_condition       STRING,
# MAGIC     dynamic_integration_id STRING,
# MAGIC     created_dt             TIMESTAMP,
# MAGIC     last_updated_dt        TIMESTAMP,
# MAGIC     batch_group            STRING
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS md.etl_log (
# MAGIC   job_instance_id      STRING,
# MAGIC   job_id               INT,
# MAGIC   batch_id             STRING,
# MAGIC   job_instance_status  STRING,
# MAGIC   job_instance_msg     STRING,
# MAGIC   start_ts             TIMESTAMP,
# MAGIC   end_ts               TIMESTAMP,
# MAGIC   src_row_num          BIGINT,
# MAGIC   tgt_row_num          BIGINT,
# MAGIC   created_dt           TIMESTAMP,
# MAGIC   last_updated_dt      TIMESTAMP,
# MAGIC   source               STRING
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
