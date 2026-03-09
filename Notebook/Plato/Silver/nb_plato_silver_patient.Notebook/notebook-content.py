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
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         },
# META         {
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
# META         },
# META         {
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Plato Bronze → Silver: `patient` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **patient** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260204173700
job_id = '6858'              # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "patient"

BRONZE_TABLES = {"plato_patient": "lh_bnj_bronze.plato.brz_patient"}
SILVER_TABLES = {"patient": "lh_bnj_silver.plato.silver_patient"}
lh_silver_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/e8a5651c-a1b6-49de-b2c5-11b7e197e77a/Tables/plato'


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

def transform_patient():
    """
    Transform bronze_plato_patient to silver_patient.
    Key transformations:
    - Standardize date formats
    - Mask sensitive data (NRIC)
    - Parse and validate phone numbers
    - Standardize gender values
    
    PLATO Schema columns:
    - _id, given_id, name, nric, dob, sex, telephone, email, address, postal
    - corporate.corp, corporate.notes (flattened from MongoDB), doctor, allergies, nationality, etc.
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_patient"])
    
    silver = bronze.select(
        # Primary key
        col("_id").alias("patient_id"),
        col("given_id").alias("patient_code"),
        
        # Name - PLATO stores full name in single 'name' field
        clean_string_column("name").alias("full_name"),
        col("title").alias("title"),
        
        # Mask NRIC (show last 4 characters only)
        when(col("nric").isNotNull() & (length(col("nric")) >= 4),
             concat(lit("****"), substring(col("nric"), -4, 4))
        ).alias("nric_masked"),
        
        # Store original NRIC hash for matching
        sha2(col("nric"), 256).alias("nric_hash"),
        col("nric_type").alias("nric_type"),
        
        # Standardize gender (PLATO uses 'sex' field)
        when(upper(col("sex")).isin("M", "MALE"), "Male")
        .when(upper(col("sex")).isin("F", "FEMALE"), "Female")
        .otherwise("Unknown").alias("gender"),
        
        # Date of birth
        col("dob").alias("date_of_birth"),
        
        # Contact info
        clean_string_column("email").alias("email"),
        regexp_replace(col("telephone"), "[^0-9+]", "").alias("phone_primary"),
        regexp_replace(col("telephone2"), "[^0-9+]", "").alias("phone_secondary"),
        regexp_replace(col("telephone3"), "[^0-9+]", "").alias("phone_tertiary"),
        
        # Address
        clean_string_column("address").alias("address"),
        col("unit_no").alias("unit_number"),
        col("postal").cast("string").alias("postal_code"),
        coalesce(clean_string_column("nationality"), lit("Singapore")).alias("nationality"),
        
        # Status / Flags
        col("dnd").cast(BooleanType()).alias("do_not_disturb"),
        clean_string_column("occupation").alias("occupation"),
        clean_string_column("marital_status").alias("marital_status"),
        
        # Medical info
        clean_string_column("allergies").alias("allergies"),
        clean_string_column("allergies_select").alias("allergies_select"),
        clean_string_column("food_allergies").alias("food_allergies"),
        clean_string_column("food_allergies_select").alias("food_allergies_select"),
        clean_string_column("g6pd").alias("g6pd_status"),
        clean_string_column("alerts").alias("alerts"),
        
        # Doctor assignment
        col("doctor").alias("assigned_doctor"),
        
        # Corporate/Insurance link - use backticks for column names with dots
        col("`corporate.corp`").alias("corporate_id"),
        col("`corporate.notes`").alias("corporate_notes"),
        
        # Additional fields
        clean_string_column("referred_by").alias("referred_by"),
        clean_string_column("notes").alias("notes"),
        clean_string_column("tag").alias("tags"),
        col("`nok.id`").alias("next_of_kin_id"),
        
        # Timestamps
        col("created_on").alias("created_at"),
        col("last_edited").alias("updated_at"),
        col("created_by").alias("created_by"),
        col("last_edited_by").alias("updated_by")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate by patient_id
    silver = deduplicate(silver, ["patient_id"], "updated_at")
    
    return silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start silver patient")
try:
    df = transform_patient()
    src_cnt = spark.table(BRONZE_TABLES["plato_patient"]).count()
    tgt_cnt = df.count()
    log_data_quality("silver_patient", df, "patient_id")
    target_path = f"{lh_silver_path}/silver_patient"
    df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(target_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES['patient']} USING DELTA LOCATION '{target_path}'")
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created silver_patient", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed silver patient. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
