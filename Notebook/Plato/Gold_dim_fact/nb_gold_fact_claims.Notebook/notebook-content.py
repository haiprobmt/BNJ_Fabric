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

# # Plato Silver → Gold Fact: `fact_claims`

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '6858'
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


SILVER_TABLES = {"invoice": f"lh_bnj_silver.{src_catalog}.silver_invoice"}
GOLD_FACTS = {"claims": f"lh_bnj_gold.{tgt_catalog}.fact_claims"}
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

def create_fact_claims():
    """
    Create fact_claims for Claims Health score.
    Claims are invoices with corporate payers (insurance) or schemes (CHAS, MediSave).
    Grain: One row per claim invoice.
    
    PLATO doesn't have separate claim tracking - claims are identified by:
    - corporate_id is not null (insurance/corporate payer)
    - OR scheme is not null (CHAS, MediSave, PG, etc.)
    
    Note: Detailed claim submission/response tracking would require 
    additional data from insurance portals (not in PLATO).
    """
    
    # Read silver invoice data
    silver_invoice = spark.table(SILVER_TABLES["invoice"])
    
    # Filter to claims: invoices with corporate payer OR scheme
    claims_df = silver_invoice.filter(
        (col("corporate_id").isNotNull()) | 
        (col("scheme").isNotNull() & (col("scheme") != ""))
    )
    
    # Read dimension tables
    dim_patient = spark.table(GOLD_DIMENSIONS["patient"]).select("patient_key", "patient_id")
    dim_payer = spark.table(GOLD_DIMENSIONS["payer"]).select("payer_key", "payer_id")
    dim_location = spark.table(GOLD_DIMENSIONS["location"]).select("location_key", "location_id")
    
    # Transform to claims fact
    fact_claims = claims_df.select(
        #monotonically_increasing_id().alias("claim_fact_key"),
        col("invoice_id").alias("claim_id"),  # Use invoice_id as claim identifier
        col("invoice_id"),
        get_date_key(col("invoice_date")).alias("date_key"),
        col("patient_id"),
        col("corporate_id").alias("payer_id"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        
        # Claim amount is the total invoice amount
        col("total_amount").cast(DecimalType(18, 2)).alias("claim_amount"),
        
        # Scheme info (CHAS, MediSave, PG, etc.)
        col("scheme"),
        
        # Claim type based on scheme
        when(upper(col("scheme")).contains("CHAS"), "CHAS")
        .when(upper(col("scheme")).contains("MEDI"), "MediSave")
        .when(upper(col("scheme")).contains("PG"), "Pioneer Generation")
        .when(upper(col("scheme")).contains("FLEXI"), "Flexi-MediSave")
        .when(col("corporate_id").isNotNull(), "Corporate/Insurance")
        .otherwise("Other").alias("claim_type"),
        
        # Invoice status (proxy for claim status since PLATO doesn't track claims separately)
        col("status"),
        col("is_finalized"),
        col("is_void"),
        
        # Timestamps
        col("invoice_date").alias("claim_date"),
        col("created_at").cast(TimestampType())
    )
    
    # Join with dimensions
    fact_claims = fact_claims \
        .join(dim_patient, "patient_id", "left") \
        .join(dim_payer, "payer_id", "left") \
        .join(dim_location, "location_id", "left")
    
    # Select final columns
    fact_claims = fact_claims.select(
        "claim_id",
        "invoice_id",
        "date_key",
        coalesce(col("patient_key"), lit(-1)).alias("patient_key"),
        coalesce(col("payer_key"), lit(-1)).alias("payer_key"),
        coalesce(col("location_key"), lit(1)).alias("location_key"),
        "claim_amount",
        "scheme",
        "claim_type",
        "status",
        "is_finalized",
        "is_void",
        "claim_date",
        "created_at"
    ).withColumn("is_current", lit(True)) \
    .withColumn("end_date", lit(None).cast(DateType())) \
    .withColumn("effective_date", current_date()) \
    .withColumn("dw_created_at", current_timestamp()) \
    .withColumn("dw_updated_at", current_timestamp())
    return fact_claims

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


try:
    src_df = create_fact_claims()
    src_cnt = spark.table(SILVER_TABLES["invoice"]).count()
    tgt_cnt = src_df.count()
    # log_data_quality("fact_claims", df, "claims_key")
    merge_dimension(
        src_df,
        GOLD_FACTS["claims"],
        ["claim_id"],
        "claim_fact_key"
    )   
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created fact_claims", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed fact_claims. {safe_exception_text(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
