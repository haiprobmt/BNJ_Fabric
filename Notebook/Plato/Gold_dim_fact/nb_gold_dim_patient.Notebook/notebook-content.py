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

# # Gold: dim_patient


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '6858'
src_catalog = "plato"
job_group_name = "gold"
src_table = "silver_patient"
tgt_catalog = "gold"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


SILVER_TABLES = {"patient": f"lh_bnj_silver.{src_catalog}.silver_patient"}
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

def create_dim_patient():
    """
    Create patient dimension from silver_patient table.
    Includes SCD Type 2 support and derived attributes.
    
    Target schema (44 columns):
    - address, age, age_group, alerts, allergies, allergies_select
    - assigned_doctor, created_by, created_on, date_of_birth, do_not_disturb
    - dw_created_at, dw_updated_at, effective_date, email, end_date
    - food_allergies, food_allergies_select, g6pd_status, given_id, is_current
    - last_edited, last_edited_by, marital_status, nationality, notes, nric
    - nric_type, occupation, patient_id, patient_key, patient_name, postal_code
    - recalls, referred_by, sex, source_system, tags, telephone
    - telephone2, telephone3, title, unit_no
    """
    
    # Read silver patient data
    silver_patient = spark.table(SILVER_TABLES["patient"])
    
    # Transform to dimension with exact schema
    dim_patient = silver_patient.select(
        # Surrogate key
        monotonically_increasing_id().alias("patient_key"),
        
        # Natural key
        col("patient_id"),
        col("patient_code").alias("given_id"),
        
        # Name and title
        col("full_name").alias("patient_name"),
        col("title"),
        
        # NRIC
        col("nric_masked").alias("nric"),
        col("nric_type"),
        
        # Demographics
        col("gender").alias("sex"),
        col("date_of_birth"),
        
        # Calculate age
        floor(datediff(current_date(), col("date_of_birth")) / 365).cast(IntegerType()).alias("age"),
        
        # Age group classification
        when(floor(datediff(current_date(), col("date_of_birth")) / 365) <= 18, "0-18")
        .when(floor(datediff(current_date(), col("date_of_birth")) / 365) <= 35, "19-35")
        .when(floor(datediff(current_date(), col("date_of_birth")) / 365) <= 50, "36-50")
        .when(floor(datediff(current_date(), col("date_of_birth")) / 365) <= 65, "51-65")
        .otherwise("65+").alias("age_group"),
        
        # Location
        col("address"),
        col("unit_number").alias("unit_no"),
        col("postal_code"),
        col("nationality"),
        
        # Contact - multiple phone numbers
        col("phone_primary").alias("telephone"),
        col("phone_secondary").alias("telephone2"),
        col("phone_tertiary").alias("telephone3"),
        col("email"),
        
        # Additional demographics
        col("occupation"),
        col("marital_status"),
        
        # Medical info - allergies
        col("allergies"),
        lit(None).cast(StringType()).alias("allergies_select"),
        lit(None).cast(StringType()).alias("food_allergies"),
        lit(None).cast(StringType()).alias("food_allergies_select"),
        
        # Medical info - other
        col("alerts"),
        col("g6pd_status"),
        lit(None).cast(StringType()).alias("recalls"),
        
        # Relationships
        col("assigned_doctor"),
        col("referred_by"),
        
        # Notes and tags
        col("notes"),
        col("tags"),
        
        # Status
        col("do_not_disturb"),
        
        # Audit fields
        col("created_at").alias("created_on"),
        col("created_by"),
        col("updated_at").alias("last_edited"),
        col("updated_by").alias("last_edited_by"),
        
        # Source system
        lit("PLATO").alias("source_system"),
        
        # SCD Type 2 columns
        current_date().alias("effective_date"),
        lit(None).cast(DateType()).alias("end_date"),
        lit(True).alias("is_current"),
        
        # Data warehouse metadata
        current_timestamp().alias("dw_created_at"),
        current_timestamp().alias("dw_updated_at")
    )
    
    # Reorder columns alphabetically to match target schema
    result = dim_patient.select(
        col("patient_id"),
        col("given_id"),
        col("address"),
        col("age"),
        col("age_group"),
        col("alerts"),
        col("allergies"),
        col("allergies_select"),
        col("assigned_doctor"),
        col("created_by"),
        col("created_on"),
        col("date_of_birth"),
        col("do_not_disturb"),
        col("email"),
        col("food_allergies"),
        col("food_allergies_select"),
        col("g6pd_status"),
        col("last_edited"),
        col("last_edited_by"),
        col("marital_status"),
        col("nationality"),
        col("notes"),
        col("nric"),
        col("nric_type"),
        col("occupation"),
        col("patient_name"),
        col("postal_code"),
        col("recalls"),
        col("referred_by"),
        col("sex"),
        col("source_system"),
        col("tags"),
        col("telephone"),
        col("telephone2"),
        col("telephone3"),
        col("title"),
        col("unit_no"),
        col("is_current"),
        col("end_date"),
        col("effective_date"),
        col("dw_created_at"),
        col("dw_updated_at"),
    )
    
    return result


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start dim_patient")
try:
    src_df = create_dim_patient()
    src_cnt = spark.table(SILVER_TABLES["patient"]).count()
    tgt_cnt = src_df.count()

    log_data_quality("patient", src_df, "patient_id")

    merge_dimension(
        src_df,
        GOLD_DIMENSIONS["patient"],
        ["patient_id"],
        "patient_key"
    )

    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created dim_patient", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed dim_patient. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_job_instance(batch_id, job_id, msg="Start dim_patient")
try:
    df = create_dim_patient()
    src_cnt = spark.table(SILVER_TABLES["patient"]).count()
    tgt_cnt = df.count()
    log_data_quality("dim_patient", df, "patient_key")
    df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["patient"])
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created dim_patient", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed dim_patient. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
