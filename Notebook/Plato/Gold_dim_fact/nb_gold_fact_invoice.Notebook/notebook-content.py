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

# # Plato Silver → Gold Fact: `fact_invoice`

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '5512'
src_catalog = "plato"
job_group_name = "gold"
src_table = "silver_invoice"
tgt_catalog = "gold"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This cell is generated from runtime parameters. Learn more: https://go.microsoft.com/fwlink/?linkid=2161015
batch_id = "20260303050006"
job_id = "5512"
src_catalog = "plato"
tgt_catalog = "gold_test"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


SILVER_TABLES = {"invoice": f"lh_bnj_silver.{src_catalog}.silver_invoice"}
GOLD_FACTS = {"invoice": "lh_bnj_gold.gold.fact_invoice"}
GOLD_DIMENSIONS = {"patient": f"lh_bnj_gold.{tgt_catalog}.dim_patient", "payer": f"lh_bnj_gold.{tgt_catalog}.dim_payer", "location": f"lh_bnj_gold.{tgt_catalog}.dim_location"}


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

def create_fact_invoice():
    """
    Create fact_invoice from PLATO invoice data.
    Grain: One row per invoice (header level - not line items).
    
    Silver invoice columns available:
    - invoice_id, invoice_number, patient_id, doctor_id, location_id, corporate_id
    - invoice_date, subtotal, tax_amount, total_amount, adjustment_amount
    - gst_applicable, tax_rate, status, status_changed_at
    - is_finalized, finalized_at, finalized_by
    - is_void, void_reason, voided_at, voided_by
    - is_credit_debit_note, cndn_applied_to_invoice
    - scheme, session_number, is_highlighted
    - notes, corporate_notes, invoice_notes
    - manual_time_in, manual_time_out
    - created_at, created_by, updated_at, updated_by
    """
    
    # Read silver invoice data
    silver_invoice = spark.table(SILVER_TABLES["invoice"])
    
    # Read dimension tables for key lookups
    dim_patient = spark.table(GOLD_DIMENSIONS["patient"]).select("patient_key", "patient_id")
    dim_location = spark.table(GOLD_DIMENSIONS["location"]).select("location_key", "location_id")
    dim_payer = spark.table(GOLD_DIMENSIONS["payer"]).select("payer_key", "payer_id")
    
    # Transform invoice data (header level)
    fact_invoice = silver_invoice.select(
        #monotonically_increasing_id().alias("invoice_fact_key"),
        col("invoice_id"),
        col("invoice_number"),
        get_date_key(col("invoice_date")).alias("date_key"),
        col("patient_id"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        col("corporate_id").alias("payer_id"),
        col("doctor_id"),
        
        # Invoice type classification
        when(col("is_credit_debit_note") == True, "Credit/Debit Note")
        .when(col("is_void") == True, "Voided")
        .otherwise("Standard").alias("invoice_type"),
        
        # Amounts (header level)
        col("subtotal").cast(DecimalType(18, 2)).alias("subtotal"),
        col("tax_amount").cast(DecimalType(18, 2)).alias("tax_amount"),
        col("total_amount").cast(DecimalType(18, 2)).alias("total_amount"),
        col("adjustment_amount").cast(DecimalType(18, 2)).alias("adjustment_amount"),
        
        # Calculate net amount (total - adjustments if any)
        (col("total_amount") - coalesce(col("adjustment_amount"), lit(0))).cast(DecimalType(18, 2)).alias("net_amount"),
        
        # GST info
        col("gst_applicable"),
        col("tax_rate"),
        
        # Status flags
        col("status"),
        col("is_finalized"),
        col("is_void"),
        col("is_credit_debit_note"),
        
        # Scheme (CHAS, MediSave, etc.)
        col("scheme"),
        
        # Has corporate/insurance payer
        when(col("corporate_id").isNotNull(), True).otherwise(False).alias("has_payer"),
        
        # Timestamps
        col("created_at").cast(TimestampType()),
        col("finalized_at").cast(TimestampType())
    )
    
    # Join with dimension tables to get surrogate keys
    fact_invoice = fact_invoice \
        .join(dim_patient, "patient_id", "left") \
        .join(dim_location, "location_id", "left") \
        .join(dim_payer, "payer_id", "left")
    
    # Select final columns
    fact_invoice = fact_invoice.select(
        "invoice_id",
        "invoice_number",
        "date_key",
        coalesce(col("patient_key"), lit(-1)).alias("patient_key"),
        coalesce(col("location_key"), lit(1)).alias("location_key"),
        coalesce(col("payer_key"), lit(-1)).alias("payer_key"),
        "doctor_id",
        "invoice_type",
        "subtotal",
        "tax_amount",
        "total_amount",
        "adjustment_amount",
        "net_amount",
        "gst_applicable",
        "tax_rate",
        "status",
        "is_finalized",
        "is_void",
        "is_credit_debit_note",
        "scheme",
        "has_payer",
        "created_at",
        "finalized_at"
    ).withColumn("is_current", lit(True)) \
    .withColumn("end_date", lit(None).cast(DateType())) \
    .withColumn("effective_date", current_date()) \
    .withColumn("dw_created_at", current_timestamp()) \
    .withColumn("dw_updated_at", current_timestamp())
    
    return fact_invoice

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start fact_invoice")
try:
    src_df = create_fact_invoice()
    src_cnt = spark.table(SILVER_TABLES["invoice"]).count()
    tgt_cnt = src_df.count()
    tgt_table = GOLD_FACTS["invoice"]
    # log_data_quality("fact_invoice", df, "invoice_key")
    merge_dimension(
        src_df,
        tgt_table,
        ["invoice_id"],
        "invoice_fact_key"
    )

    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created fact_invoice", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print(f"✅ Created {tgt_table} with {src_cnt} rows")
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed fact_invoice. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
