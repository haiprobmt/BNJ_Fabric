# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "102823e0-12f1-4ca5-b61b-a2df5d75beb2",
# META       "default_lakehouse_name": "lh_bnj_gold",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         },
# META         {
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE DATABASE IF NOT EXISTS lh_bnj_gold.gold_test;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.dim_date;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.dim_date (
# MAGIC   date_key BIGINT,
# MAGIC   date DATE,
# MAGIC   year INT,
# MAGIC   quarter INT,
# MAGIC   month INT,
# MAGIC   day INT,
# MAGIC   day_of_week INT,
# MAGIC   day_name STRING,
# MAGIC   month_name STRING,
# MAGIC   is_weekend BOOLEAN,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.dim_patient;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.dim_patient (
# MAGIC   patient_key BIGINT,
# MAGIC   patient_id STRING,
# MAGIC   patient_code STRING,
# MAGIC   full_name STRING,
# MAGIC   title STRING,
# MAGIC   nric_masked STRING,
# MAGIC   nric_hash STRING,
# MAGIC   nric_type STRING,
# MAGIC   gender STRING,
# MAGIC   date_of_birth STRING,
# MAGIC   email STRING,
# MAGIC   phone_primary STRING,
# MAGIC   phone_secondary STRING,
# MAGIC   phone_tertiary STRING,
# MAGIC   address STRING,
# MAGIC   unit_number STRING,
# MAGIC   postal_code STRING,
# MAGIC   nationality STRING,
# MAGIC   do_not_disturb BOOLEAN,
# MAGIC   occupation STRING,
# MAGIC   marital_status STRING,
# MAGIC   allergies STRING,
# MAGIC   allergies_select STRING,
# MAGIC   food_allergies STRING,
# MAGIC   food_allergies_select STRING,
# MAGIC   g6pd_status STRING,
# MAGIC   alerts STRING,
# MAGIC   assigned_doctor STRING,
# MAGIC   referred_by STRING,
# MAGIC   notes STRING,
# MAGIC   tags STRING,
# MAGIC   next_of_kin_id STRING,
# MAGIC   created_at STRING,
# MAGIC   updated_at STRING,
# MAGIC   created_by STRING,
# MAGIC   updated_by STRING,
# MAGIC   effective_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.dim_location;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.dim_location (
# MAGIC   location_key BIGINT,
# MAGIC   location_id STRING,
# MAGIC   location_name STRING,
# MAGIC   address STRING,
# MAGIC   postal_code STRING,
# MAGIC   telephone STRING,
# MAGIC   effective_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.dim_corporate;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.dim_corporate (
# MAGIC   corporate_key BIGINT,
# MAGIC   corporate_id STRING,
# MAGIC   corporate_name STRING,
# MAGIC   corporate_notes STRING,
# MAGIC   effective_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.dim_payment_mode;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.dim_payment_mode (
# MAGIC   payment_mode_key BIGINT,
# MAGIC   payment_mode STRING,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.dim_inventory;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.dim_inventory (
# MAGIC   inventory_key BIGINT,
# MAGIC   inventory_id STRING,
# MAGIC   given_id STRING,
# MAGIC   item_name STRING,
# MAGIC   description STRING,
# MAGIC   category STRING,
# MAGIC   inventory_type STRING,
# MAGIC   selling_price DECIMAL(18,2),
# MAGIC   cost_price DECIMAL(18,2),
# MAGIC   min_price DECIMAL(18,2),
# MAGIC   package_original_price DECIMAL(18,2),
# MAGIC   track_stock BOOLEAN,
# MAGIC   unit STRING,
# MAGIC   order_unit STRING,
# MAGIC   pack_size STRING,
# MAGIC   dosage STRING,
# MAGIC   dosage_usage STRING,
# MAGIC   dosage_dose STRING,
# MAGIC   dosage_unit STRING,
# MAGIC   dosage_frequency STRING,
# MAGIC   dosage_duration STRING,
# MAGIC   precautions STRING,
# MAGIC   supplier_id STRING,
# MAGIC   manufacturer STRING,
# MAGIC   is_redeemable BOOLEAN,
# MAGIC   redemption_count INT,
# MAGIC   is_fixed_price BOOLEAN,
# MAGIC   no_discount BOOLEAN,
# MAGIC   has_expiry_after_dispensing BOOLEAN,
# MAGIC   is_hidden BOOLEAN,
# MAGIC   effective_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.fact_invoice_header;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.fact_invoice_header (
# MAGIC   invoice_fact_key BIGINT,
# MAGIC   invoice_id STRING,
# MAGIC   invoice_date_key BIGINT,
# MAGIC   created_date_key BIGINT,
# MAGIC   finalized_date_key BIGINT,
# MAGIC   patient_key BIGINT,
# MAGIC   location_key BIGINT,
# MAGIC   corporate_key BIGINT,
# MAGIC   invoice_number STRING,
# MAGIC   invoice_prefix STRING,
# MAGIC   full_invoice_number STRING,
# MAGIC   session BIGINT,
# MAGIC   invoice_type STRING,
# MAGIC   scheme STRING,
# MAGIC   rate STRING,
# MAGIC   sub_total_amount DECIMAL(18,2),
# MAGIC   tax_amount DECIMAL(18,2),
# MAGIC   adjustment_amount DECIMAL(18,2),
# MAGIC   total_amount DECIMAL(18,2),
# MAGIC   no_gst_flag BOOLEAN,
# MAGIC   status STRING,
# MAGIC   is_finalized BOOLEAN,
# MAGIC   is_void BOOLEAN,
# MAGIC   is_adjustment BOOLEAN,
# MAGIC   is_credit_note BOOLEAN,
# MAGIC   is_debit_note BOOLEAN,
# MAGIC   corp_amount DECIMAL(18,2),
# MAGIC   corp_reference STRING,
# MAGIC   created_on TIMESTAMP,
# MAGIC   finalized_on TIMESTAMP,
# MAGIC   void_on TIMESTAMP,
# MAGIC   created_by STRING,
# MAGIC   void_reason STRING,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.fact_invoice_line;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.fact_invoice_line (
# MAGIC   invoice_line_fact_key BIGINT,
# MAGIC   invoice_fact_key BIGINT,
# MAGIC   invoice_id STRING,
# MAGIC   inventory_key BIGINT,
# MAGIC   invoice_date_key BIGINT,
# MAGIC   patient_key BIGINT,
# MAGIC   location_key BIGINT,
# MAGIC   item_id STRING,
# MAGIC   line_number INT,
# MAGIC   item_name STRING,
# MAGIC   category STRING,
# MAGIC   description STRING,
# MAGIC   batch_number STRING,
# MAGIC   batch_expiry_date DATE,
# MAGIC   batch_cpu DECIMAL(18,2),
# MAGIC   quantity INT,
# MAGIC   unit STRING,
# MAGIC   unit_price DECIMAL(18,2),
# MAGIC   selling_price DECIMAL(18,2),
# MAGIC   cost_price DECIMAL(18,2),
# MAGIC   discount_percentage DECIMAL(5,2),
# MAGIC   discount_amount DECIMAL(18,2),
# MAGIC   line_sub_total DECIMAL(18,2),
# MAGIC   line_total_amount DECIMAL(18,2),
# MAGIC   line_cost_total DECIMAL(18,2),
# MAGIC   line_margin_amount DECIMAL(18,2),
# MAGIC   line_margin_percentage DECIMAL(5,2),
# MAGIC   packageitems STRING,
# MAGIC   track_stock BOOLEAN,
# MAGIC   no_discount BOOLEAN,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.fact_payment;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.fact_payment (
# MAGIC   payment_fact_key BIGINT,
# MAGIC   payment_id BIGINT,
# MAGIC   payment_date_key BIGINT,
# MAGIC   invoice_fact_key BIGINT,
# MAGIC   invoice_id STRING,
# MAGIC   patient_key BIGINT,
# MAGIC   location_key BIGINT,
# MAGIC   payment_mode_key BIGINT,
# MAGIC   corporate_id STRING,
# MAGIC   payment_mode STRING,
# MAGIC   session_id INT,
# MAGIC   payment_amount DECIMAL(18,2),
# MAGIC   reference_number STRING,
# MAGIC   is_voided BOOLEAN,
# MAGIC   void_on TIMESTAMP,
# MAGIC   void_by STRING,
# MAGIC   void_reason STRING,
# MAGIC   created_on TIMESTAMP,
# MAGIC   created_by STRING,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.fact_appointment;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.fact_appointment (
# MAGIC   appointment_fact_key BIGINT,
# MAGIC   appointment_id STRING,
# MAGIC   appointment_date_key BIGINT,
# MAGIC   patient_key BIGINT,
# MAGIC   location_key BIGINT,
# MAGIC   title STRING,
# MAGIC   description STRING,
# MAGIC   appointment_type STRING,
# MAGIC   start_time TIMESTAMP,
# MAGIC   end_time TIMESTAMP,
# MAGIC   duration_minutes INT,
# MAGIC   is_all_day BOOLEAN,
# MAGIC   appointment_status STRING,
# MAGIC   is_first_visit BOOLEAN,
# MAGIC   is_follow_up BOOLEAN,
# MAGIC   email STRING,
# MAGIC   handphone STRING,
# MAGIC   recurrence STRING,
# MAGIC   created_on TIMESTAMP,
# MAGIC   created_by STRING,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
