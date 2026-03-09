# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a",
# META       "default_lakehouse_name": "lh_bnj_silver",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         },
# META         {
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.agg_patient;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lh_bnj_gold.gold_test.agg_patient
# MAGIC (
# MAGIC   location_key                      INT,
# MAGIC   invoice_id                        STRING,
# MAGIC   invoice_date_key                  INT,
# MAGIC   patient_key                       BIGINT,
# MAGIC 
# MAGIC   year_month                        STRING,
# MAGIC   year                              INT,
# MAGIC   month                             INT,
# MAGIC 
# MAGIC   total_current_patients            BIGINT,
# MAGIC   total_current_patients_yoy        BIGINT,
# MAGIC 
# MAGIC   new_patients                      INT,
# MAGIC   returning_patients                INT,
# MAGIC   lost_patients                     INT,
# MAGIC   pending_patients                  INT,
# MAGIC   patients_with_future_appointments INT,
# MAGIC 
# MAGIC   total_followup_appointments       INT,
# MAGIC   avg_followups_per_patient         DOUBLE,
# MAGIC 
# MAGIC   retention_rate_pct                DOUBLE,
# MAGIC   attrition_rate_pct                DOUBLE,
# MAGIC   conversion_rate_pct               DOUBLE,
# MAGIC 
# MAGIC   total_surveys_completed           INT,
# MAGIC   delighted_patients                INT,
# MAGIC   delighted_patients_pct            INT,
# MAGIC   unhappy_patients                  INT,
# MAGIC   unhappy_patients_pct              INT,
# MAGIC   avg_satisfaction_score            INT,
# MAGIC 
# MAGIC   panel_score                       DOUBLE,
# MAGIC   growth_score                      INT,
# MAGIC   retention_score                   DOUBLE,
# MAGIC   attrition_score                   DOUBLE,
# MAGIC   conversion_score                  DOUBLE,
# MAGIC   delight_score                     INT,
# MAGIC   unhappy_score                     INT,
# MAGIC   patient_health_score              DOUBLE,
# MAGIC 
# MAGIC   dw_created_at                     TIMESTAMP,
# MAGIC   dw_updated_at                     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (location_key);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.agg_claims;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lh_bnj_gold.gold_test.agg_claims
# MAGIC (
# MAGIC   location_key                BIGINT,
# MAGIC   corporate_key               BIGINT,
# MAGIC   invoice_id                  STRING,
# MAGIC   invoice_date_key            INT,
# MAGIC 
# MAGIC   total_claims_outstanding    BIGINT,
# MAGIC   claims_submitted            BIGINT,
# MAGIC   claims_approved             BIGINT,
# MAGIC   claims_pending              BIGINT,
# MAGIC   claims_rejected             BIGINT,
# MAGIC 
# MAGIC   claims_submitted_pct        DOUBLE,
# MAGIC   claims_approved_pct         DOUBLE,
# MAGIC   claims_pending_pct          DOUBLE,
# MAGIC   claims_rejected_pct         DOUBLE,
# MAGIC 
# MAGIC   avg_turnaround_days         DOUBLE,
# MAGIC   median_turnaround_days      DOUBLE,
# MAGIC   max_turnaround_days         INT,
# MAGIC 
# MAGIC   approval_score              DOUBLE,
# MAGIC   pending_score               DOUBLE,
# MAGIC   rejection_score             DOUBLE,
# MAGIC 
# MAGIC   dw_created_at               TIMESTAMP,
# MAGIC   dw_updated_at               TIMESTAMP,
# MAGIC 
# MAGIC   claims_health_score         DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (location_key);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.agg_inventory;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lh_bnj_gold.gold_test.agg_inventory
# MAGIC (
# MAGIC   inventory_key               BIGINT,
# MAGIC   invoice_id                  STRING,
# MAGIC   invoice_date_key            INT,
# MAGIC   category                    STRING,
# MAGIC 
# MAGIC   beginning_inventory_value   DECIMAL(38,2),
# MAGIC   ending_inventory_value      DECIMAL(38,2),
# MAGIC   average_inventory_value     DECIMAL(33,6),
# MAGIC   cogs_mtd                    DECIMAL(28,2),
# MAGIC 
# MAGIC   inventory_turnover_ratio    DECIMAL(38,6),
# MAGIC   days_inventory_outstanding  DECIMAL(38,6),
# MAGIC 
# MAGIC   expired_stock_quantity      BIGINT,
# MAGIC   expired_stock_value         DECIMAL(38,2),
# MAGIC   expired_stock_percentage    DOUBLE,
# MAGIC 
# MAGIC   total_stock_quantity        BIGINT,
# MAGIC   total_stock_value           DECIMAL(38,2),
# MAGIC 
# MAGIC   inventory_turnover_score    DOUBLE,
# MAGIC   expired_stock_score         DOUBLE,
# MAGIC   ccc_inventory_score         DOUBLE,
# MAGIC 
# MAGIC   dw_created_at               TIMESTAMP,
# MAGIC   dw_updated_at               TIMESTAMP,
# MAGIC 
# MAGIC   inventory_health_score      DOUBLE
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.agg_cashflow;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS lh_bnj_gold.gold_test.agg_cashflow
# MAGIC (
# MAGIC   -- keys
# MAGIC   location_key                    BIGINT      NOT NULL,
# MAGIC   invoice_id                      STRING      NOT NULL,
# MAGIC   invoice_date_key                INT         NOT NULL,
# MAGIC   inventory_id                    BIGINT,
# MAGIC 
# MAGIC   -- cashflow + CCC metrics
# MAGIC   total_ar_amount                 DECIMAL(18,2),
# MAGIC   current_liabilities             DECIMAL(18,2),
# MAGIC   operating_cash_flow             DECIMAL(18,2),
# MAGIC   operating_cash_flow_ratio       DOUBLE,
# MAGIC   days_inventory_outstanding      DOUBLE,
# MAGIC   days_sales_outstanding          DOUBLE,
# MAGIC   days_payable_outstanding        DOUBLE,
# MAGIC   cash_conversion_cycle_days      DOUBLE,
# MAGIC 
# MAGIC   -- debt metrics
# MAGIC   interest_expense                DECIMAL(18,2),
# MAGIC   principal_repayment             DECIMAL(18,2),
# MAGIC   total_debt_financing_expense    DECIMAL(18,2),
# MAGIC 
# MAGIC   -- component scores
# MAGIC   ar_score                        DOUBLE,
# MAGIC   ap_score                        DOUBLE,
# MAGIC   inventory_score                 DOUBLE,
# MAGIC   interest_score                  DOUBLE,
# MAGIC   principal_score                 DOUBLE,
# MAGIC 
# MAGIC   -- composite cashflow health score
# MAGIC   cashflow_health_score           DOUBLE,
# MAGIC 
# MAGIC   -- audit
# MAGIC   dw_created_at                   TIMESTAMP,
# MAGIC   dw_updated_at                   TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (location_key);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.agg_overall_health;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.agg_overall_health
# MAGIC (
# MAGIC   invoice_id                  STRING,
# MAGIC   invoice_date_key            INT,
# MAGIC   year                        INT,
# MAGIC   month                       INT,
# MAGIC   year_month                  STRING,
# MAGIC 
# MAGIC   financial_health_score      DOUBLE,
# MAGIC   operational_health_score    DOUBLE,
# MAGIC   pl_health_score             DOUBLE,
# MAGIC   ar_ap_health_score          DOUBLE,
# MAGIC   cashflow_health_score       DOUBLE,
# MAGIC   claims_health_score         DOUBLE,
# MAGIC   patient_health_score        DOUBLE,
# MAGIC   inventory_health_score      DOUBLE,
# MAGIC 
# MAGIC   overall_health_score        DOUBLE,
# MAGIC   yoy_health_score            DOUBLE,
# MAGIC 
# MAGIC   invoice_count               BIGINT,
# MAGIC 
# MAGIC   forecasted_year_end_score   DOUBLE,
# MAGIC   target_year_end_score       DOUBLE,
# MAGIC   progress_to_target_pct      DOUBLE,
# MAGIC 
# MAGIC   dw_created_at               TIMESTAMP,
# MAGIC   dw_updated_at               TIMESTAMP,
# MAGIC 
# MAGIC   health_status               STRING,
# MAGIC   health_status_color         STRING,
# MAGIC 
# MAGIC   overall_health_score_yoy        DOUBLE,
# MAGIC   yoy_health_score_prior_year     DOUBLE,
# MAGIC   yoy_health_change              DOUBLE,
# MAGIC   yoy_health_change_pct          DOUBLE,
# MAGIC   yoy_health_score_change        DOUBLE,
# MAGIC   yoy_health_score_change_pct    DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (year, month);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.agg_pl_monthly;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.agg_pl_monthly
# MAGIC (
# MAGIC   year_month              STRING,
# MAGIC   year                    INT,
# MAGIC   month                   INT,
# MAGIC   location_key            BIGINT,
# MAGIC 
# MAGIC   total_revenue           DECIMAL(18,2),
# MAGIC   target_revenue          DECIMAL(18,2),
# MAGIC   revenue_variance        DECIMAL(19,2),
# MAGIC   revenue_variance_pct    DECIMAL(18,6),
# MAGIC 
# MAGIC   cost_of_sales           DECIMAL(18,2),
# MAGIC   target_cogs             DECIMAL(18,2),
# MAGIC   cogs_variance           DECIMAL(19,2),
# MAGIC 
# MAGIC   gross_profit            DECIMAL(18,2),
# MAGIC   gross_profit_margin     DECIMAL(18,6),
# MAGIC   target_gross_profit     DECIMAL(18,2),
# MAGIC 
# MAGIC   operating_expenses      DECIMAL(18,2),
# MAGIC   target_opex             DECIMAL(18,2),
# MAGIC   opex_variance           DECIMAL(18,2),
# MAGIC 
# MAGIC   operating_profit        DECIMAL(18,2),
# MAGIC   operating_profit_margin DECIMAL(18,6),
# MAGIC   target_operating_profit DECIMAL(18,2),
# MAGIC 
# MAGIC   dw_created_at           TIMESTAMP,
# MAGIC   dw_updated_at           TIMESTAMP,
# MAGIC 
# MAGIC   revenue_score           DECIMAL(38,16),
# MAGIC   cogs_score              DECIMAL(35,13),
# MAGIC   gross_profit_score      DECIMAL(38,16),
# MAGIC   opex_score              DECIMAL(35,13),
# MAGIC   operating_profit_score  DECIMAL(38,16),
# MAGIC 
# MAGIC   pl_health_score         DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (year, month);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.agg_financial_health_monthly;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.agg_financial_health_monthly
# MAGIC (
# MAGIC   location_key                   BIGINT,
# MAGIC   year_month                     STRING,
# MAGIC   year                           INT,
# MAGIC   month                          INT,
# MAGIC 
# MAGIC   pl_health_score                DOUBLE,
# MAGIC   ar_ap_health_score             DOUBLE,
# MAGIC   cashflow_health_score          DOUBLE,
# MAGIC   financial_health_score         DOUBLE,
# MAGIC 
# MAGIC   dw_created_at                  TIMESTAMP,
# MAGIC   dw_updated_at                  TIMESTAMP,
# MAGIC 
# MAGIC   health_status                  STRING,
# MAGIC   health_status_color            STRING,
# MAGIC 
# MAGIC   financial_health_score_yoy     DOUBLE,
# MAGIC   financial_health_yoy_change    DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (year, month);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.agg_ar_ap;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.agg_ar_ap
# MAGIC (
# MAGIC   date_key                INT,
# MAGIC   report_date             STRING,
# MAGIC   location_key            BIGINT,
# MAGIC   inventory_key           BIGINT,
# MAGIC   invoice_id              STRING,
# MAGIC   invoice_date_key        INT,
# MAGIC   corporate_id            STRING,
# MAGIC 
# MAGIC   -- AR (decimal 38,6)
# MAGIC   total_ar_amount         DECIMAL(38,6),
# MAGIC   ar_0_30_days_amount     DECIMAL(38,6),
# MAGIC   ar_31_60_days_amount    DECIMAL(38,6),
# MAGIC   ar_61_90_days_amount    DECIMAL(38,6),
# MAGIC   ar_91_180_days_amount   DECIMAL(38,6),
# MAGIC   ar_over_180_days_amount DECIMAL(38,6),
# MAGIC 
# MAGIC   ar_0_30_days_count      BIGINT,
# MAGIC   ar_31_60_days_count     BIGINT,
# MAGIC   ar_61_90_days_count     BIGINT,
# MAGIC   ar_91_180_days_count    BIGINT,
# MAGIC   ar_over_180_days_count  BIGINT,
# MAGIC 
# MAGIC   ar_days_outstanding     DOUBLE,
# MAGIC 
# MAGIC   -- AP (double)
# MAGIC   total_ap_amount         DOUBLE,
# MAGIC   ap_0_30_days_amount     DOUBLE,
# MAGIC   ap_31_60_days_amount    DOUBLE,
# MAGIC   ap_61_90_days_amount    DOUBLE,
# MAGIC   ap_91_180_days_amount   DOUBLE,
# MAGIC   ap_over_180_days_amount DOUBLE,
# MAGIC 
# MAGIC   ap_0_30_days_count      INT,
# MAGIC   ap_31_60_days_count     INT,
# MAGIC   ap_61_90_days_count     INT,
# MAGIC   ap_91_180_days_count    INT,
# MAGIC   ap_over_180_days_count  INT,
# MAGIC 
# MAGIC   ap_days_outstanding     DOUBLE,
# MAGIC   ap_paid_on_time_count   INT,
# MAGIC   ap_paid_early_count     INT,
# MAGIC   ap_paid_late_count      INT,
# MAGIC 
# MAGIC   dw_created_at           TIMESTAMP,
# MAGIC   dw_updated_at           TIMESTAMP,
# MAGIC 
# MAGIC   ar_aging_score          DOUBLE,
# MAGIC   ap_aging_score          DOUBLE,
# MAGIC   ar_ap_health_score      DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date_key);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.dim_date;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.dim_date (
# MAGIC   date_key INT,
# MAGIC   full_date DATE,
# MAGIC   year INT,
# MAGIC   quarter INT,
# MAGIC   quarter_name STRING,
# MAGIC   month INT,
# MAGIC   month_name STRING,
# MAGIC   month_abbr STRING,
# MAGIC   week_of_year INT,
# MAGIC   day_of_month INT,
# MAGIC   day_of_week INT,
# MAGIC   day_of_week_name STRING,
# MAGIC   day_of_year INT,
# MAGIC   is_weekend BOOLEAN,
# MAGIC   is_holiday BOOLEAN,
# MAGIC   holiday_name STRING,
# MAGIC   fiscal_year INT,
# MAGIC   fiscal_quarter INT,
# MAGIC   fiscal_month INT,
# MAGIC   year_month STRING,
# MAGIC   year_quarter STRING,
# MAGIC   date_display STRING
# MAGIC ) USING DELTA;


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
# MAGIC   given_id STRING,
# MAGIC   patient_name STRING,
# MAGIC   nric STRING,
# MAGIC   date_of_birth DATE,
# MAGIC   age INT,
# MAGIC   marital_status STRING,
# MAGIC   sex STRING,
# MAGIC   nationality STRING,
# MAGIC   nric_type STRING,
# MAGIC   title STRING,
# MAGIC   allergies_select STRING,
# MAGIC   allergies STRING,
# MAGIC   food_allergies_select STRING,
# MAGIC   food_allergies STRING,
# MAGIC   g6pd_status STRING,
# MAGIC   alerts STRING,
# MAGIC   email STRING,
# MAGIC   telephone STRING,
# MAGIC   telephone2 STRING,
# MAGIC   telephone3 STRING,
# MAGIC   address STRING,
# MAGIC   postal_code STRING,
# MAGIC   unit_no STRING,
# MAGIC   occupation STRING,
# MAGIC   do_not_disturb BIGINT,
# MAGIC   referred_by STRING,
# MAGIC   assigned_doctor STRING,
# MAGIC   tags STRING,
# MAGIC   recalls STRING,
# MAGIC   notes STRING,
# MAGIC   is_current BOOLEAN,
# MAGIC   effective_from TIMESTAMP,
# MAGIC   effective_to TIMESTAMP,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC ) USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.dim_location;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.dim_location (
# MAGIC   location_key INT,
# MAGIC   location_id STRING,
# MAGIC   location_name STRING,
# MAGIC   location_type STRING,
# MAGIC   address STRING,
# MAGIC   postal_code STRING,
# MAGIC   telephone STRING,
# MAGIC   email STRING,
# MAGIC   is_active BOOLEAN,
# MAGIC   is_current BOOLEAN,
# MAGIC   effective_from TIMESTAMP,
# MAGIC   effective_to TIMESTAMP,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC ) USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_gold.gold_test.dim_payment_mode;
# MAGIC CREATE TABLE lh_bnj_gold.gold_test.dim_payment_mode (
# MAGIC   payment_mode_key INT,
# MAGIC   payment_mode_code STRING,
# MAGIC   payment_mode_name STRING,
# MAGIC   payment_method STRING,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC ) USING DELTA;


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
# MAGIC   corporate_type STRING,
# MAGIC   coverage_percentage DECIMAL(5,2),
# MAGIC   is_active BOOLEAN,
# MAGIC   is_current BOOLEAN,
# MAGIC   effective_from TIMESTAMP,
# MAGIC   effective_to TIMESTAMP,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC ) USING DELTA;


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
# MAGIC   item_name STRING,
# MAGIC   category STRING,
# MAGIC   inventory_type STRING,
# MAGIC   selling_price DECIMAL(18,2),
# MAGIC   cost_price DECIMAL(18,2),
# MAGIC   is_active BOOLEAN,
# MAGIC   is_hidden BOOLEAN,
# MAGIC   supplier_id STRING,
# MAGIC   manufacturer STRING,
# MAGIC   pack_size STRING,
# MAGIC   unit STRING,
# MAGIC   is_current BOOLEAN,
# MAGIC   effective_from TIMESTAMP,
# MAGIC   effective_to TIMESTAMP,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC ) USING DELTA;


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
# MAGIC   invoice_number STRING,
# MAGIC   invoice_prefix STRING,
# MAGIC   invoice_date DATE,
# MAGIC   finalized_on TIMESTAMP,
# MAGIC   patient_key BIGINT,
# MAGIC   location_key INT,
# MAGIC   doctor_id STRING,
# MAGIC   status STRING,
# MAGIC   is_finalized BOOLEAN,
# MAGIC   is_voided BOOLEAN,
# MAGIC   total_amount DECIMAL(18,2),
# MAGIC   tax_amount DECIMAL(18,2),
# MAGIC   sub_total_amount DECIMAL(18,2),
# MAGIC   adjustment_amount DECIMAL(18,2),
# MAGIC   corporate_id STRING,
# MAGIC   corporate_key BIGINT,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC ) USING DELTA;


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
# MAGIC   item_id STRING,
# MAGIC   inventory_key BIGINT,
# MAGIC   item_name STRING,
# MAGIC   quantity DECIMAL(18,2),
# MAGIC   unit_price DECIMAL(18,2),
# MAGIC   line_amount DECIMAL(18,2),
# MAGIC   session_id INT,
# MAGIC   source_system STRING,
# MAGIC   dw_created_at TIMESTAMP,
# MAGIC   dw_updated_at TIMESTAMP
# MAGIC ) USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_gold.FACT_INVOICE (
# MAGIC     invoice_key BIGINT,
# MAGIC     
# MAGIC     -- Dimension keys
# MAGIC     date_key INT,
# MAGIC     due_date_key INT,
# MAGIC     patient_key BIGINT,
# MAGIC     doctor_key BIGINT,
# MAGIC     location_key BIGINT,
# MAGIC     corporate_key BIGINT,
# MAGIC     contact_key BIGINT, -- For Xero contacts
# MAGIC     
# MAGIC     -- Source tracking
# MAGIC     source_system VARCHAR(10), -- 'PLATO' or 'XERO'
# MAGIC     
# MAGIC     -- Natural keys from Plato
# MAGIC     plato_invoice_id VARCHAR(8000),
# MAGIC     plato_invoice_number INT,
# MAGIC     plato_invoice_prefix VARCHAR(8000),
# MAGIC     
# MAGIC     -- Natural keys from Xero
# MAGIC     xero_invoice_id VARCHAR(8000),
# MAGIC     xero_invoice_number VARCHAR(8000),
# MAGIC     xero_invoice_type VARCHAR(50), -- ACCREC (receivable) or ACCPAY (payable)
# MAGIC     
# MAGIC     -- Dates
# MAGIC     invoice_date DATE,
# MAGIC     due_date DATE,
# MAGIC     finalized_on timestamp,
# MAGIC     
# MAGIC     -- Amounts
# MAGIC     sub_total DECIMAL(18,4),
# MAGIC     tax DECIMAL(18,4),
# MAGIC     total DECIMAL(18,4),
# MAGIC     adjustment_amount DECIMAL(18,4),
# MAGIC     amount_paid DECIMAL(18,4),
# MAGIC     amount_due DECIMAL(18,4),
# MAGIC     amount_credited DECIMAL(18,4),
# MAGIC     
# MAGIC     -- Status
# MAGIC     status VARCHAR(50),
# MAGIC     is_finalized BOOLEAN,
# MAGIC     is_voided BOOLEAN,
# MAGIC     is_adjusted BOOLEAN,
# MAGIC     
# MAGIC     -- Additional info
# MAGIC     reference VARCHAR(8000),
# MAGIC     notes VARCHAR(8000),
# MAGIC     currency_code VARCHAR(10),
# MAGIC     
# MAGIC     -- Audit
# MAGIC     created_on timestamp,
# MAGIC     updated_date_utc timestamp,
# MAGIC     silver_loaded_at timestamp,
# MAGIC     dw_created_at timestamp,
# MAGIC     dw_updated_at timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_gold.FACT_PAYMENT (
# MAGIC     payment_key BIGINT,
# MAGIC     
# MAGIC     -- Dimension keys
# MAGIC     date_key INT,
# MAGIC     invoice_key BIGINT,
# MAGIC     patient_key BIGINT,
# MAGIC     location_key BIGINT,
# MAGIC     corporate_key BIGINT,
# MAGIC     account_key BIGINT, -- For Xero bank account
# MAGIC     
# MAGIC     -- Source tracking
# MAGIC     source_system VARCHAR(10),
# MAGIC     
# MAGIC     -- Natural keys from Plato
# MAGIC     plato_payment_id BIGINT,
# MAGIC     plato_invoice_id VARCHAR(8000),
# MAGIC     plato_payment_mode VARCHAR(8000),
# MAGIC     
# MAGIC     -- Natural keys from Xero
# MAGIC     xero_payment_id VARCHAR(8000),
# MAGIC     xero_invoice_id VARCHAR(8000),
# MAGIC     xero_payment_type VARCHAR(50),
# MAGIC     
# MAGIC     -- Dates
# MAGIC     payment_date DATE,
# MAGIC     
# MAGIC     -- Amounts
# MAGIC     amount DECIMAL(18,4),
# MAGIC     bank_amount DECIMAL(18,4),
# MAGIC     
# MAGIC     -- Status
# MAGIC     is_voided BOOLEAN,
# MAGIC     is_reconciled BOOLEAN,
# MAGIC     payment_status VARCHAR(50),
# MAGIC     
# MAGIC     -- Additional info
# MAGIC     reference VARCHAR(8000),
# MAGIC     currency_code VARCHAR(10),
# MAGIC     currency_rate FLOAT,
# MAGIC     
# MAGIC     -- Audit
# MAGIC     created_on TIMESTAMP,
# MAGIC     updated_date_utc TIMESTAMP,
# MAGIC     dw_created_at TIMESTAMP,
# MAGIC     dw_updated_at TIMESTAMP
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_gold.FACT_APPOINTMENT (
# MAGIC     appointment_key BIGINT,
# MAGIC     
# MAGIC     -- Dimension keys
# MAGIC     date_key INT,
# MAGIC     patient_key BIGINT,
# MAGIC     doctor_key BIGINT,
# MAGIC     location_key BIGINT,
# MAGIC     
# MAGIC     -- Natural key
# MAGIC     appointment_id VARCHAR(8000),
# MAGIC     
# MAGIC     -- Dates and times
# MAGIC     appointment_date DATE,
# MAGIC     start_time TIMESTAMP,
# MAGIC     end_time TIMESTAMP,
# MAGIC     duration_minutes INT,
# MAGIC     
# MAGIC     -- Appointment details
# MAGIC     title VARCHAR(8000),
# MAGIC     description VARCHAR(8000),
# MAGIC     appointment_type VARCHAR(8000),
# MAGIC     
# MAGIC     -- Status
# MAGIC     appointment_status VARCHAR(50), -- Scheduled, Completed, Cancelled, No-show
# MAGIC     is_all_day BOOLEAN,
# MAGIC     is_completed BOOLEAN,
# MAGIC     is_no_show BOOLEAN,
# MAGIC     is_cancelled BOOLEAN,
# MAGIC     
# MAGIC     -- Contact info
# MAGIC     email VARCHAR(8000),
# MAGIC     handphone VARCHAR(8000),
# MAGIC     
# MAGIC     -- Audit
# MAGIC     created_on TIMESTAMP,
# MAGIC     last_edited TIMESTAMP,
# MAGIC     silver_loaded_at TIMESTAMP,
# MAGIC     dw_created_at TIMESTAMP,
# MAGIC     dw_updated_at TIMESTAMP
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_gold.FACT_INVENTORY_TRANSACTION (
# MAGIC     inventory_transaction_key BIGINT,
# MAGIC     
# MAGIC     -- Dimension keys
# MAGIC     date_key INT,
# MAGIC     inventory_key BIGINT,
# MAGIC     supplier_key BIGINT,
# MAGIC     location_key BIGINT,
# MAGIC     invoice_key BIGINT, -- Link to sale/purchase invoice
# MAGIC     
# MAGIC     -- Transaction details
# MAGIC     transaction_id VARCHAR(100),
# MAGIC     transaction_type VARCHAR(50), -- 'SALE', 'PURCHASE', 'ADJUSTMENT', 'EXPIRY', 'TRANSFER'
# MAGIC     transaction_date DATE,
# MAGIC     
# MAGIC     -- Quantities
# MAGIC     quantity DECIMAL(18,2),
# MAGIC     quantity_before DECIMAL(18,2),
# MAGIC     quantity_after DECIMAL(18,2),
# MAGIC     
# MAGIC     -- Values
# MAGIC     unit_cost DECIMAL(18,2),
# MAGIC     unit_price DECIMAL(18,2),
# MAGIC     total_cost DECIMAL(18,2),
# MAGIC     total_value DECIMAL(18,2),
# MAGIC     
# MAGIC     -- Additional info
# MAGIC     reference VARCHAR(8000),
# MAGIC     notes VARCHAR(8000),
# MAGIC     expiry_date DATE,
# MAGIC     
# MAGIC     -- Audit
# MAGIC     created_at TIMESTAMP
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_gold.FACT_AR_DAILY (
# MAGIC     ar_key BIGINT,
# MAGIC     
# MAGIC     -- Dimension keys
# MAGIC     snapshot_date_key INT,
# MAGIC     invoice_date_key INT,
# MAGIC     invoice_key BIGINT,
# MAGIC     patient_key BIGINT,
# MAGIC     corporate_key BIGINT,
# MAGIC     
# MAGIC     -- Snapshot date
# MAGIC     snapshot_date DATE NOT NULL,
# MAGIC     
# MAGIC     -- Invoice details
# MAGIC     source_system VARCHAR(10),
# MAGIC     invoice_id VARCHAR(8000),
# MAGIC     invoice_date DATE,
# MAGIC     
# MAGIC     -- Amounts
# MAGIC     invoice_total DECIMAL(18,4),
# MAGIC     amount_paid DECIMAL(18,4),
# MAGIC     amount_due DECIMAL(18,4),
# MAGIC     
# MAGIC     -- Aging
# MAGIC     days_outstanding INT,
# MAGIC     aging_bucket VARCHAR(20), -- '0-30', '31-45', '46-60', '61-90', '>90'
# MAGIC     aging_category VARCHAR(20), -- 'CURRENT', 'OVERDUE_30', 'OVERDUE_60', 'OVERDUE_90', 'OVERDUE_90_PLUS'
# MAGIC     
# MAGIC     -- Status
# MAGIC     is_overdue boolean,
# MAGIC     
# MAGIC     -- Audit
# MAGIC     created_at timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_gold.FACT_AP_DAILY (
# MAGIC     ap_key BIGINT,
# MAGIC     
# MAGIC     -- Dimension keys
# MAGIC     snapshot_date_key INT,
# MAGIC     invoice_date_key INT,
# MAGIC     due_date_key INT,
# MAGIC     invoice_key BIGINT,
# MAGIC     contact_key BIGINT,
# MAGIC     
# MAGIC     -- Snapshot date
# MAGIC     snapshot_date DATE NOT NULL,
# MAGIC     
# MAGIC     -- Invoice details
# MAGIC     source_system VARCHAR(10),
# MAGIC     invoice_id VARCHAR(8000),
# MAGIC     invoice_date DATE,
# MAGIC     due_date DATE,
# MAGIC     
# MAGIC     -- Amounts
# MAGIC     invoice_total DECIMAL(18,4),
# MAGIC     amount_paid DECIMAL(18,4),
# MAGIC     amount_due DECIMAL(18,4),
# MAGIC     
# MAGIC     -- Aging
# MAGIC     days_outstanding INT,
# MAGIC     days_until_due INT,
# MAGIC     aging_bucket VARCHAR(20),
# MAGIC     aging_category VARCHAR(20),
# MAGIC     
# MAGIC     -- Status
# MAGIC     is_overdue BOOLEAN,
# MAGIC     is_due_soon BOOLEAN, -- Due within 7 days
# MAGIC     
# MAGIC     -- Audit
# MAGIC     created_at TIMESTAMP
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_gold.FACT_CASHFLOW_DAILY (
# MAGIC     cashflow_key BIGINT,
# MAGIC     
# MAGIC     -- Dimension key
# MAGIC     date_key INT,
# MAGIC     
# MAGIC     -- Snapshot date
# MAGIC     snapshot_date DATE NOT NULL,
# MAGIC     
# MAGIC     -- Cash movements
# MAGIC     cash_inflow DECIMAL(18,2),           -- Payments received
# MAGIC     cash_outflow DECIMAL(18,2),          -- Payments made
# MAGIC     net_cash_flow DECIMAL(18,2),         -- Inflow - Outflow
# MAGIC     cumulative_cash_flow DECIMAL(18,2),  -- Running total
# MAGIC     
# MAGIC     -- Working capital components
# MAGIC     ar_balance DECIMAL(18,2),
# MAGIC     inventory_value DECIMAL(18,2),
# MAGIC     ap_balance DECIMAL(18,2),
# MAGIC     working_capital DECIMAL(18,2),
# MAGIC     change_in_working_capital DECIMAL(18,2),
# MAGIC     
# MAGIC     -- Operating cash flow
# MAGIC     operating_cash_flow DECIMAL(18,2),
# MAGIC     current_liabilities DECIMAL(18,2),
# MAGIC     operating_cash_flow_ratio DECIMAL(10,4),
# MAGIC     
# MAGIC     -- Cash conversion cycle components
# MAGIC     dio DECIMAL(10,2),  -- Days Inventory Outstanding
# MAGIC     dso DECIMAL(10,2),  -- Days Sales Outstanding
# MAGIC     dpo DECIMAL(10,2),  -- Days Payable Outstanding
# MAGIC     ccc DECIMAL(10,2),  -- Cash Conversion Cycle
# MAGIC     
# MAGIC     -- Debt service (if applicable)
# MAGIC     interest_paid DECIMAL(18,2),
# MAGIC     principal_paid DECIMAL(18,2),
# MAGIC     total_debt_service DECIMAL(18,2),
# MAGIC     debt_service_ratio DECIMAL(10,4),
# MAGIC     
# MAGIC     -- Component scores (0-100)
# MAGIC     ar_score DECIMAL(5,2),
# MAGIC     ap_score DECIMAL(5,2),
# MAGIC     inventory_score DECIMAL(5,2),
# MAGIC     interest_score DECIMAL(5,2),
# MAGIC     principal_score DECIMAL(5,2),
# MAGIC     
# MAGIC     -- Overall health score (0-100)
# MAGIC     cashflow_health_score DECIMAL(5,2),
# MAGIC     health_status VARCHAR(10), -- 'RED', 'YELLOW', 'GREEN'
# MAGIC     
# MAGIC     -- Audit
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_gold.FACT_HEALTH_SCORE (
# MAGIC     health_score_key BIGINT,
# MAGIC     
# MAGIC     -- Dimension key
# MAGIC     date_key INT,
# MAGIC     
# MAGIC     -- Snapshot date
# MAGIC     snapshot_date DATE NOT NULL,
# MAGIC     
# MAGIC     -- Score classification
# MAGIC     metric_category VARCHAR(50), -- 'FINANCIAL', 'OPERATIONAL', 'P&L', 'AR/AP', 'CASHFLOW', 'INVENTORY', 'CLAIMS', 'PATIENT'
# MAGIC     metric_name VARCHAR(100),    -- Specific metric name
# MAGIC     metric_subcategory VARCHAR(50),
# MAGIC     
# MAGIC     -- Values
# MAGIC     raw_value DECIMAL(18,4),
# MAGIC     target_value DECIMAL(18,4),
# MAGIC     variance DECIMAL(18,4),
# MAGIC     variance_pct DECIMAL(10,4),
# MAGIC     
# MAGIC     -- Score
# MAGIC     score DECIMAL(5,2), -- 0-100
# MAGIC     weight DECIMAL(5,4), -- Weight in composite score
# MAGIC     weighted_score DECIMAL(5,2),
# MAGIC     
# MAGIC     -- Status
# MAGIC     traffic_light VARCHAR(10), -- 'RED', 'YELLOW', 'GREEN'
# MAGIC     
# MAGIC     -- Thresholds
# MAGIC     threshold_green_min DECIMAL(18,4),
# MAGIC     threshold_yellow_min DECIMAL(18,4),
# MAGIC     threshold_red_min DECIMAL(18,4),
# MAGIC     
# MAGIC     -- Audit
# MAGIC     created_at TIMESTAMP
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_gold.FACT_COMPOSITE_HEALTH (
# MAGIC     composite_health_key BIGINT,
# MAGIC     
# MAGIC     -- Dimension key
# MAGIC     date_key INT,
# MAGIC     
# MAGIC     -- Snapshot date
# MAGIC     snapshot_date DATE NOT NULL,
# MAGIC     
# MAGIC     -- Component health scores (0-100)
# MAGIC     financial_health_score DECIMAL(5,2),
# MAGIC     operational_health_score DECIMAL(5,2),
# MAGIC     pl_health_score DECIMAL(5,2),
# MAGIC     ar_ap_health_score DECIMAL(5,2),
# MAGIC     cashflow_health_score DECIMAL(5,2),
# MAGIC     inventory_health_score DECIMAL(5,2),
# MAGIC     claims_health_score DECIMAL(5,2),
# MAGIC     patient_health_score DECIMAL(5,2),
# MAGIC     
# MAGIC     -- Overall health score (0-100)
# MAGIC     overall_health_score DECIMAL(5,2),
# MAGIC     health_status VARCHAR(10), -- 'RED', 'YELLOW', 'GREEN'
# MAGIC     
# MAGIC     -- Year-over-year comparison
# MAGIC     yoy_health_score DECIMAL(5,2),
# MAGIC     yoy_change DECIMAL(5,2),
# MAGIC     yoy_change_pct DECIMAL(10,4),
# MAGIC     
# MAGIC     -- Forecasting
# MAGIC     forecasted_year_end_score DECIMAL(5,2),
# MAGIC     progress_to_target_pct DECIMAL(10,4),
# MAGIC     
# MAGIC     -- Audit
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_date
df = spark.sql("""
SELECT 
    CAST(date_format(DateValue,'yyyyMMdd') AS INT) AS date_key,
    DateValue AS full_date,
    day(DateValue) AS day_of_month,
    dayofmonth(DateValue) AS day_of_month,
    dayofweek(DateValue) AS day_of_week,        -- returns 1=Sun ... 7=Sat
    date_format(DateValue,'EEEE') AS day_of_week_name,
    month(DateValue) AS month_number,
    date_format(DateValue,'MMMM') AS month_name,
    date_format(DateValue,'MMM') AS month_abbr,
    quarter(DateValue) AS quarter_number,
    CONCAT('Q', quarter(DateValue)) AS quarter_name,
    year(DateValue) AS year_number,
    date_format(DateValue,'yyyy-MM') AS year_month,
    CONCAT(year(DateValue),'-Q',quarter(DateValue)) AS year_quarter,
    year(DateValue) AS fiscal_year,
    quarter(DateValue) AS fiscal_quarter,
    month(DateValue) AS fiscal_month,
    CASE WHEN date_format(DateValue,'EEEE') IN ('6','7') THEN 1 ELSE 0 END AS is_weekend,
    0 AS is_holiday,
    CASE WHEN date_format(DateValue,'EEEE') NOT IN ('6','7') THEN 1 ELSE 0 END AS is_weekday,
    CASE WHEN DateValue = current_date() THEN 1 ELSE 0 END AS is_current_day,
    CASE WHEN date_format(DateValue,'yyyy-MM') = date_format(current_date(),'yyyy-MM') THEN 1 ELSE 0 END AS is_current_month,
    CASE WHEN quarter(DateValue) = quarter(current_date()) AND year(DateValue) = year(current_date()) THEN 1 ELSE 0 END AS is_current_quarter,
    CASE WHEN year(DateValue) = year(current_date()) THEN 1 ELSE 0 END AS is_current_year
FROM (
    SELECT explode(sequence(to_date('2018-01-01'), to_date('2025-12-31'), interval 1 day)) AS DateValue
) tmp
WHERE NOT EXISTS (
    SELECT 1 FROM DIM_DATE dd WHERE dd.full_date = tmp.DateValue
)
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_patient
df = spark.sql("""SELECT 
        sp.patient_id,
        sp.given_id,
        sp.patient_name,
        sp.title,
        sp.email,
        sp.nric,
        sp.date_of_birth,
        sp.sex,
        sp.nationality,
        sp.marital_status,
        sp.telephone,
        sp.telephone2,
        sp.telephone3,
        sp.address,
        sp.postal_code,
        sp.unit_no,
        sp.allergies_select,
        sp.allergies,
        sp.food_allergies_select,
        sp.food_allergies,
        sp.g6pd_status,
        sp.alerts,
        sp.occupation,
        sp.assigned_doctor,
        sp.referred_by,
        sp.tags,
        'Active' AS patient_status, -- Will update with business logic
        sp.created_on AS first_visit_date,
        sp.last_edited AS last_visit_date,
        1 AS is_current,
        current_timestamp() AS effective_date,
        '9999-12-31' AS end_date,
        sp.created_on,
        sp.last_edited,
        current_timestamp() AS dw_created_at,
        current_timestamp() AS dw_updated_at
    FROM lh_bnj_silver.plato.silver_patient sp
    WHERE NOT EXISTS (
        SELECT 1 
        FROM dim_patient dp 
        WHERE dp.patient_id = sp.patient_id 
        AND dp.is_current = 1
    )"""
)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_corporate
df = spark.sql("""
    MERGE INTO DIM_CORPORATE
    USING lh_bnj_silver.plato.silver_corporate AS source
    ON DIM_CORPORATE.corporate_id = source.corporate_id
    WHEN MATCHED THEN
    UPDATE SET
        corporate_name = source.corporate_name,
        category = source.category,
        is_insurance = source.is_insurance,
        contact_person = source.contact_person,
        email = source.email,
        telephone = source.telephone,
        payment_type = source.payment_type,
        dw_updated_at = current_timestamp()
    WHEN NOT MATCHED THEN
    INSERT (
        corporate_id, given_id, corporate_name, category, is_insurance,
        contact_person, email, telephone, fax, address, url,
        payment_type, default_amount, is_active, created_on, last_edited
    )
    VALUES (
        source.corporate_id, source.given_id, source.corporate_name, source.category,
        source.is_insurance, source.contact_person, source.email, source.telephone,
        source.fax, source.address, source.url, source.payment_type,
        source.default_amount, 1, source.created_on, source.last_edited
    )
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_supplier
df = spark.sql("""
MERGE INTO DIM_SUPPLIER
USING lh_bnj_silver.plato.silver_supplier AS source
ON DIM_SUPPLIER.supplier_id = source.supplier_id
WHEN MATCHED THEN
  UPDATE SET
      supplier_name = source.supplier_name,
      contact_person = source.contact_person,
      email = source.email,
      telephone = source.telephone,
      dw_updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (
      supplier_id, supplier_name, category, contact_person,
      email, handphone, telephone, fax, address, url,
      is_active, created_on, last_edited
  )
  VALUES (
      source.supplier_id, source.supplier_name, source.category, source.contact_person,
      source.email, source.handphone, source.telephone, source.fax, source.address,
      source.url, 1, source.created_on, source.last_edited
  )
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DIM_ACCOUNT
df = spark.sql(
    """
    MERGE INTO DIM_ACCOUNT AS target
    USING lh_bnj_silver.xero.silver_Accounts AS source
    ON target.account_id = source.Accounts_AccountID

    WHEN MATCHED THEN
    UPDATE SET
        target.account_name       = source.Accounts_Name,
        target.description        = source.Accounts_Description,
        target.status             = source.Accounts_Status,
        target.is_active          = CASE WHEN source.Accounts_Status = 'ACTIVE' THEN 1 ELSE 0 END,
        target.updated_date_utc   = source.Accounts_UpdatedDateUTC,
        target.dw_updated_at      = current_timestamp()

    WHEN NOT MATCHED THEN
    INSERT (
        account_id, account_code, account_name, description,
        account_type, account_class, tax_type,
        bank_account_number, bank_account_type, currency_code,
        reporting_code, reporting_code_name, status, is_active,
        updated_date_utc
    )
    VALUES (
        source.Accounts_AccountID,
        source.Accounts_Code,
        source.Accounts_Name,
        source.Accounts_Description,
        source.Accounts_Type,
        source.Accounts_Class,
        source.Accounts_TaxType,
        source.Accounts_BankAccountNumber,
        source.Accounts_BankAccountType,
        source.Accounts_CurrencyCode,
        source.Accounts_ReportingCode,
        source.Accounts_ReportingCodeName,
        source.Accounts_Status,
        CASE WHEN source.Accounts_Status = 'ACTIVE' THEN 1 ELSE 0 END,
        source.Accounts_UpdatedDateUTC
    );
    """
)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql(
    """
    MERGE INTO DIM_CONTACT AS target
    USING lh_bnj_silver.plato.silver_contact AS source
    ON target.contact_id = source.contact_id

    WHEN MATCHED AND target.source_system = 'PLATO' THEN
    UPDATE SET
        target.contact_name = source.contact_name,
        target.person_name = source.person_name,
        target.email = source.email,
        target.telephone = source.telephone,
        target.dw_updated_at = current_timestamp()

    WHEN NOT MATCHED THEN
    INSERT (
        contact_id, contact_name, person_name, email,
        handphone, telephone, fax, address, url, category,
        is_active, source_system, created_on, last_edited
    )
    VALUES (
        source.contact_id, source.contact_name, source.person_name, source.email,
        source.handphone, source.telephone, source.fax, source.address, source.url,
        source.category, 1, 'PLATO', source.created_on, source.last_edited
    );
    """
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql(
    """
    SELECT 
        -- Date keys
        CAST(date_format(si.invoice_date, 'yyyyMMdd') AS INT) AS date_key,
        NULL AS due_date_key,

        -- Dimension keys
        patient_id AS patient_key,

        -- Source
        'PLATO' AS source_system,

        -- Natural keys
        si.invoice_id AS plato_invoice_id,
        si.invoice_number AS plato_invoice_number,
        si.invoice_prefix AS plato_invoice_prefix,

        -- Dates
        si.invoice_date,
        NULL AS due_date,
        si.finalized_on,

        -- Amounts
        si.sub_total,
        si.tax,
        si.total,
        si.adjustment_amount,

        -- Amount paid
        coalesce((
            SELECT SUM(p.amount)
            FROM lh_bnj_silver.plato.silver_payment p
            WHERE p.invoice_id = si.invoice_id
              AND p.is_voided = 0
        ), 0) AS amount_paid,

        -- Amount due
        si.total - coalesce((
            SELECT SUM(p.amount)
            FROM lh_bnj_silver.plato.silver_payment p
            WHERE p.invoice_id = si.invoice_id
              AND p.is_voided = 0
        ), 0) AS amount_due,

        -- Status
        si.status,
        si.is_finalized,
        si.is_voided,
        si.is_adjusted,

        -- Additional
        NULL AS reference,
        si.notes,

        -- Audit
        si.created_on,
        si.silver_loaded_at

    FROM lh_bnj_silver.plato.silver_invoice si
    WHERE si.is_voided = 0
      AND NOT EXISTS (
            SELECT 1 
            FROM FACT_INVOICE fi 
            WHERE fi.plato_invoice_id = si.invoice_id
              AND fi.source_system = 'PLATO'
      )
    """
)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT 
        -- Date key
        CAST(date_format(si.invoice_date, 'yyyyMMdd') AS INT) AS date_key,
        
        -- Dimension keys
        sp.invoice_id AS invoice_key,
        si.patient_id AS patient_key,
        
        -- Source
        'PLATO' AS source_system,
        
        -- Natural keys
        sp.payment_id AS plato_payment_id,
        sp.invoice_id AS plato_invoice_id,
        sp.payment_mode AS plato_payment_mode,
        
        -- Date
        CAST(sp.created_on AS DATE) AS payment_date,
        
        -- Amounts
        sp.amount,
        sp.amount AS bank_amount, -- Plato doesn't separate
        
        -- Status
        CASE WHEN sp.is_voided = 1 THEN 1 ELSE 0 END AS is_voided,
        
        -- Additional
        sp.reference,
        
        -- Audit
        sp.created_on
    FROM lh_bnj_silver.plato.silver_payment sp
    LEFT JOIN lh_bnj_silver.plato.silver_invoice si on si.invoice_id = sp.invoice_id
    -- INNER JOIN lh_bnj_silver.plato.silver_patient spt on spt.patient_id = si.patient_id
    -- WHERE 
    -- CAST(sp.silver_loaded_at AS DATE) >= @LoadDate
    --    NOT EXISTS (
    --        SELECT 1 FROM FACT_PAYMENT fp 
    --        WHERE fp.plato_payment_id = sp.payment_id 
    --        AND fp.source_system = 'PLATO'
    --    );
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
