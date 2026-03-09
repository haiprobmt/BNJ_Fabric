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
job_id = '8181'
src_catalog = "plato"
job_group_name = "gold"
src_table = "silver_supplier"
tgt_catalog = "gold"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


SILVER_TABLES = {"supplier": f"lh_bnj_silver.{src_catalog}.silver_supplier"}
GOLD_DIMENSIONS = {"supplier": f"lh_bnj_gold.{tgt_catalog}.dim_supplier"}

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

def create_dim_supplier():
    """
    Create supplier dimension from silver_supplier table.
    
    Silver supplier columns:
    - supplier_id, supplier_name, supplier_category
    - contact_person, email, phone, mobile, fax
    - address, website, notes, other_info
    - is_active, created_at, updated_at
    """
    
    # Read silver supplier data
    silver_supplier = spark.table(SILVER_TABLES["supplier"])
    
    # Transform to dimension
    dim_supplier = silver_supplier.select(
        #monotonically_increasing_id().alias("supplier_key"),
        col("supplier_id"),
        col("supplier_name"),
        col("supplier_category"),
        col("contact_person"),
        col("phone"),
        col("mobile"),
        col("email"),
        col("address"),
        col("website"),
        coalesce(col("is_active"), lit(True)).alias("is_active"),
        current_date().alias("effective_date"),
        lit(None).cast(DateType()).alias("end_date"),
        lit(True).alias("is_current"),
        current_timestamp().alias("dw_created_at"),
        current_timestamp().alias("dw_updated_at")
    ).dropDuplicates(["supplier_id"])
    
    return dim_supplier

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    src_df = create_dim_supplier()
    src_cnt = spark.table(SILVER_TABLES["supplier"]).count()
    tgt_cnt = src_df.count()
    log_data_quality("dim_supplier", src_df, "supplier_id")

    merge_dimension(
        src_df,
        GOLD_DIMENSIONS["supplier"],
        ["supplier_id"],
        "supplier_key"
    )    
    
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created dim_supplier", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed dim_supplier. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    df = create_dim_supplier()
    src_cnt = spark.table(SILVER_TABLES["supplier"]).count()
    tgt_cnt = df.count()
    log_data_quality("dim_supplier", df, "supplier_key")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_DIMENSIONS["supplier"])
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created dim_supplier", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed dim_supplier. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
