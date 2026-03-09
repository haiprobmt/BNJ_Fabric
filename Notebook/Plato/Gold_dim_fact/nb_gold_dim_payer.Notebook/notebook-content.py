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
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
# META         },
# META         {
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         },
# META         {
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold: dim_inventory


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '6029'
src_catalog = "plato"
job_group_name = "gold"
src_table = "silver_corporate"
tgt_catalog = "gold"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


SILVER_TABLES = {"corporate": f"lh_bnj_silver.{src_catalog}.silver_corporate"}
GOLD_DIMENSIONS = {"payer": f"lh_bnj_gold.{tgt_catalog}.dim_payer"}

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

def create_dim_payer():
    """
    Create payer dimension from silver_corporate table.
    Includes insurance companies and corporate accounts.
    
    Silver corporate columns:
    - corporate_id, corporate_code, corporate_name, corporate_type
    - category, contact_person, email, phone, fax
    - address, website
    - payment_type, credit_amount
    - scheme, discounts, specials, restricted_items
    - notes, invoice_notes, other_info
    - is_insurance, is_active
    - created_at, updated_at
    """
    
    # Read silver corporate data
    silver_corporate = spark.table(SILVER_TABLES["corporate"])
    
    # Transform to dimension
    dim_payer = silver_corporate.select(
        col("corporate_id").alias("payer_id"),
        col("corporate_code").alias("payer_code"),
        col("corporate_name").alias("payer_name"),
        col("corporate_type").alias("payer_type"),
        col("contact_person"),
        col("phone"),
        col("email"),
        col("address"),
        col("payment_type"),
        col("credit_amount"),
        col("scheme"),
        col("is_insurance"),
        coalesce(col("is_active"), lit(True)).alias("is_active")
    ).dropDuplicates(["payer_id"])
    
    return dim_payer


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start dim_payer")
try:
    src_df = create_dim_payer()
    src_cnt = spark.table(SILVER_TABLES["corporate"]).count()
    tgt_cnt = src_df.count()
    log_data_quality("dim_payer", src_df, "payer_id")

    merge_dimension(
        src_df,
        GOLD_DIMENSIONS["payer"],
        ["payer_id"],
        "payer_key"
    )

    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created dim_payer", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed dim_payer. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
