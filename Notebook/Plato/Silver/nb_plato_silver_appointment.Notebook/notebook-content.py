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

# # Plato Bronze → Silver: `appointment` (Single Table)
# 
# This notebook executes the existing per-table SQL logic for **appointment** and writes job-instance logs to `md.etl_log` via `nb_utils`.

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '7966'              # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "appointment"

BRONZE_TABLES = {"plato_appointment": "lh_bnj_bronze.plato.brz_appointment"}
SILVER_TABLES = {"appointment": "lh_bnj_silver.plato.silver_appointment"}
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

def transform_appointment():
    """
    Transform bronze_plato_appointment to silver_appointment.
    
    Source columns from PLATO appointment schema:
    - _id, patient_id, doctor, title, description
    - starttime, endtime, allDay
    - prefix, dropdown, email, handphone
    - color, code_Top, code_Bottom, code_Left, code_Right, code_Background
    - recur
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_appointment"])
    bronze = standardize_column_names(bronze)
    
    silver = bronze.select(
        # Primary key
        col("_id").alias("appointment_id"),
        
        # Relationships
        col("patient_id").alias("patient_id"),
        clean_string_column("doctor").alias("doctor_id"),
        
        # Appointment details
        clean_string_column("title").alias("appointment_type"),
        clean_string_column("description").alias("description"),
        clean_string_column("prefix").alias("prefix"),
        clean_string_column("dropdown").alias("dropdown_value"),
        
        # Times - PLATO uses starttime/endtime
        parse_timestamp("starttime").alias("start_time"),
        parse_timestamp("endtime").alias("end_time"),
        
        # Derive date from starttime
        to_date(col("starttime")).alias("appointment_date"),
        
        # Calculate duration in minutes
        when(col("starttime").isNotNull() & col("endtime").isNotNull(),
             (unix_timestamp(col("endtime")) - unix_timestamp(col("starttime"))) / 60
        ).cast(IntegerType()).alias("duration_minutes"),
        
        # All day flag
        when(col("allday") == 1, True).otherwise(False).alias("is_all_day"),
        
        # Contact for appointment
        clean_string_column("email").alias("contact_email"),
        col("handphone").cast(StringType()).alias("contact_phone"),
        
        # Visual indicators
        clean_string_column("color").alias("color"),
        clean_string_column("code_top").alias("code_top"),
        clean_string_column("code_bottom").alias("code_bottom"),
        clean_string_column("code_left").alias("code_left"),
        clean_string_column("code_right").alias("code_right"),
        clean_string_column("code_background").alias("code_background"),
        
        # Recurrence
        clean_string_column("recur").alias("recurrence_pattern"),
        when(col("recur").isNotNull() & (trim(col("recur")) != ""), True)
        .otherwise(False).alias("is_recurring"),
        
        # Timestamps
        parse_timestamp("created_on").alias("created_on"),
        clean_string_column("created_by").alias("created_by"),
        parse_timestamp("last_edited").alias("updated_at"),
        clean_string_column("last_edited_by").alias("updated_by")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["appointment_id"], "updated_at")
    
    return silver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start silver appointment")
try:
    df = transform_appointment()
    src_cnt = spark.table(BRONZE_TABLES["plato_appointment"]).count()
    tgt_cnt = df.count()
    log_data_quality("silver_appointment", df, "appointment_id")
    target_path = f"{lh_silver_path}/silver_appointment"
    df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(target_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES['appointment']} USING DELTA LOCATION '{target_path}'")
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created silver_appointment", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed silver appointment. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
