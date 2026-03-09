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

# # Gold: dim_contact


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260207145530
job_id = '4243'
src_catalog = "xero"
src_table = "silver_contacts"
tgt_catalog = "gold"
tgt_table = "dim_contact"
job_group_name = "gold"

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

#==TABLE NAME defined==

src_table = f"`{WORKSPACE_NAME}`.lh_bnj_silver.{src_catalog}.{src_table}"
tgt_table = f"`{WORKSPACE_NAME}`.lh_bnj_gold.{tgt_catalog}.{tgt_table}"
job_id_str = str(job_id).strip()

print(src_table)
print(tgt_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transfrom_dim_contact():
    """
    Create contact dimension from XERO contacts.
    XERO contacts include customers and suppliers.
    
    Silver xero_contacts columns:
    - contact_id, contact_name, contact_type
    - is_customer, is_supplier
    - email, first_name, last_name
    - status, default_currency
    - tenant_id, extracted_at, updated_at
    """
    
    # Read XERO contacts data
    xero_contacts = spark.table(src_table)
    
    # Transform to dimension
    dim_contact = xero_contacts.select(
        monotonically_increasing_id().alias("contact_key"),
        col("contact_id"),
        col("contact_name"),
        col("contact_type"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("is_customer"),
        col("is_supplier"),
        col("default_currency"),
        when(upper(col("status")) == "ACTIVE", True).otherwise(False).alias("is_active")
    ).dropDuplicates(["contact_id"])
    
    return dim_contact

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start dim_contact")
try:
    df = transfrom_dim_contact()
    src_cnt = spark.table(src_table).count()
    tgt_cnt = df.count()
   # log_data_quality("dim_contact", df, "patient_key")
    df.write.format("delta").mode("overwrite").option('overwriteSchema', 'true').saveAsTable(tgt_table)
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created dim_contact", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print(f"✅ Created {tgt_table} with {tgt_cnt} rows")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed dim_contact. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
