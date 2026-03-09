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
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         },
# META         {
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Plato Silver → Gold Fact: `fact_appointment`

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '6858'
src_catalog = "plato"
job_group_name = "gold"
src_table = "silver_appointment"
tgt_catalog = "gold"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


SILVER_TABLES = {"appointment": f"lh_bnj_silver.{src_catalog}.silver_appointment"}
GOLD_FACTS = {"appointment": f"lh_bnj_gold.{tgt_catalog}.fact_appointment"}
GOLD_DIMENSIONS = {"patient": f"lh_bnj_gold.{tgt_catalog}.dim_patient"}

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

def create_fact_appointment():
    """
    Create fact_appointment for Patient Health score.
    Grain: One row per appointment.
    
    silver_appointment actual columns (from error message):
    - appointment_id, patient_id, doctor_id, location_id
    - start_time, end_time (timestamps)
    - color, prefix, code_top, code_bottom, code_left, code_right, code_background
    - is_all_day, recurrence_pattern
    - created_at, updated_at
    
    Note: PLATO doesn't have title, description, status, or revenue tracking for appointments
    """
    
    # Read silver appointment data
    silver_appointment = spark.table(SILVER_TABLES["appointment"])
    
    # Read dimension tables
    dim_patient = spark.table(GOLD_DIMENSIONS["patient"]).select("patient_key", "patient_id")
    
    # Get first visit date per patient for new patient flag
    first_visit = silver_appointment.groupBy("patient_id") \
        .agg(min("start_time").alias("first_visit_date"))
    
    # Transform appointment data using actual silver columns
    fact_appointment = silver_appointment \
        .join(first_visit, "patient_id", "left") \
        .select(
            monotonically_increasing_id().alias("appointment_fact_key"),
            col("appointment_id"),
            get_date_key(col("start_time")).alias("date_key"),
            col("patient_id"),
            col("doctor_id"),
            
            # Times
            col("start_time").cast(TimestampType()),
            col("end_time").cast(TimestampType()),
            
            # Duration in minutes
            ((unix_timestamp(col("end_time")) - unix_timestamp(col("start_time"))) / 60).alias("duration_minutes"),
            
            # All day flag (handle if column exists)
            coalesce(col("is_all_day").cast(BooleanType()), lit(False)).alias("is_all_day"),
            
            # New patient flag (first appointment)
            (to_date(col("start_time")) == to_date(col("first_visit_date"))).alias("is_new_patient"),
            
            # Display/categorization from PLATO
            col("color"),
            col("prefix"),
            
            # Timestamps
            col("created_on").cast(TimestampType())
        )
    
    # Join with dimensions
    fact_appointment = fact_appointment \
        .join(dim_patient, "patient_id", "left")

    # Select final columns
    fact_appointment = fact_appointment.select(
        "appointment_fact_key",
        "appointment_id",
        "date_key",
        coalesce(col("patient_key"), lit(-1)).alias("patient_key"),
        "doctor_id",
        "start_time",
        "end_time",
        "duration_minutes",
        "is_all_day",
        "is_new_patient",
        "color",
        "prefix",
        "created_on"
    ).withColumn("is_current", lit(True)) \
    .withColumn("end_date", lit(None).cast(DateType())) \
    .withColumn("effective_date", current_date()) \
    .withColumn("dw_created_at", current_timestamp()) \
    .withColumn("dw_updated_at", current_timestamp())
    
    return fact_appointment

# Create and save fact_appointment
# try:
#     fact_appointment_df = create_fact_appointment()
#     fact_appointment_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACTS["appointment"])
#     print(f"✅ Created {GOLD_FACTS['appointment']} with {fact_appointment_df.count()} rows")
# except Exception as e:
#     print(f"⚠️ Could not create fact_appointment: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    src_df = create_fact_appointment()
    src_cnt = spark.table(SILVER_TABLES["appointment"]).count()
    tgt_cnt = src_df.count()
    # log_data_quality("fact_appointment", df, "appointment_key")

    merge_dimension(
        src_df,
        GOLD_FACTS["appointment"],
        "appointment_id",
        "appointment_fact_key"
    )   

    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created fact_appointment", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed fact_appointment. {safe_exception_text(e)}")
    raise 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    df = create_fact_appointment()
    src_cnt = spark.table(SILVER_TABLES["appointment"]).count()
    tgt_cnt = df.count()
    # log_data_quality("fact_appointment", df, "appointment_key")
    df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACTS["appointment"])
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created fact_appointment", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed fact_appointment. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
