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
# META         },
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

# MARKDOWN ********************

# # Plato Silver → Gold Fact: `fact_payment`

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '2681'
src_catalog = "plato"
job_group_name = "gold"
src_table = "silver_payment"
tgt_catalog = "gold"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


SILVER_TABLES = {"payment": f"lh_bnj_silver.{src_catalog}.silver_payment"}
GOLD_FACTS = {"payment": f"lh_bnj_gold.{tgt_catalog}.fact_payment"}
GOLD_DIMENSIONS = {"payer": f"lh_bnj_gold.{tgt_catalog}.dim_payer", "location": f"lh_bnj_gold.{tgt_catalog}.dim_location"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_fact_payment():
    """
    Create fact_payment from PLATO payment data.
    Grain: One row per payment transaction.
    
    Silver payment columns available (based on actual PLATO schema):
    - payment_id, invoice_id, payment_amount, payment_method
    - payment_mode_raw (original mode value)
    - created_at (used as payment date since PLATO has no separate date)
    - corporate_id (payer), location
    - void, void_reason, voided_at, voided_by
    - session_number, reference
    
    Note: PLATO payments don't have patient_id directly - must join via invoice
    """
    
    # Read silver payment data
    silver_payment = spark.table(SILVER_TABLES["payment"])
    
    # Read dimension tables
    dim_payer = spark.table(GOLD_DIMENSIONS["payer"]).select("payer_key", "payer_id")
    dim_location = spark.table(GOLD_DIMENSIONS["location"]).select("location_key", "location_id")
    
    # Transform payment data
    # Use created_at as the payment date (PLATO doesn't have separate payment_date)
    fact_payment = silver_payment.select(
        #monotonically_increasing_id().alias("payment_fact_key"),
        col("payment_id"),
        get_date_key(col("created_at")).alias("date_key"),
        col("corporate_id").alias("payer_id"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        col("invoice_id"),
        col("payment_method"),
        col("payment_amount").cast(DecimalType(18, 2)),
        coalesce(col("is_void").cast(BooleanType()), lit(False)).alias("is_void"),
        col("reference"),
        col("session"),
        col("created_at").cast(TimestampType())
    )
    
    # Join with dimensions
    fact_payment = fact_payment \
        .join(dim_payer, "payer_id", "left") \
        .join(dim_location, "location_id", "left")
    
    # Select final columns
    fact_payment = fact_payment.select(
        "payment_id",
        "date_key",
        coalesce(col("payer_key"), lit(-1)).alias("payer_key"),
        coalesce(col("location_key"), lit(1)).alias("location_key"),
        "invoice_id",
        "payment_method",
        "payment_amount",
        "is_void",
        "reference",
        "session",
        "created_at"
    ).withColumn("is_current", lit(True)) \
    .withColumn("end_date", lit(None).cast(DateType())) \
    .withColumn("effective_date", current_date()) \
    .withColumn("dw_created_at", current_timestamp()) \
    .withColumn("dw_updated_at", current_timestamp())
    
    return fact_payment

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start fact_payment")
try:
    src_df = create_fact_payment()
    src_cnt = spark.table(SILVER_TABLES["payment"]).count()
    tgt_cnt = src_df.count()
    # log_data_quality("fact_payment", df, "payment_key")
    merge_dimension(
        src_df,
        GOLD_FACTS["payment"],
        ["payment_id"],
        "payment_fact_key"
    )  
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created fact_payment", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed fact_payment. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
