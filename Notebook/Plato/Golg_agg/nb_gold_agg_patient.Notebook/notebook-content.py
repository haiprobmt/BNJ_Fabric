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

# # Gold Aggregation: `agg_patient` (Single Table)


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '6748'
src_catalog = "gold"
job_group_name = "gold_agg"
src_table = ""
tgt_catalog = "gold"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


GOLD_FACTS = {"appointment": f"lh_bnj_gold.{tgt_catalog}.fact_appointment"}
AGG_TABLES = {"patient": f"lh_bnj_gold.{tgt_catalog}.agg_patient"}
GOLD_DIMENSIONS = {"date": f"lh_bnj_gold.{tgt_catalog}.dim_date", "patient": f"lh_bnj_gold.{tgt_catalog}.dim_patient"}

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

def create_agg_patient():
    """
    Create agg_patient aggregate table for Patient Health dashboard.
    Tracks retention, attrition, conversion and satisfaction metrics.
    Overall aggregation across all time periods.
    """
    
    # Read fact_appointment for patient visit tracking
    fact_appointment = spark.table(GOLD_FACTS["appointment"])
    dim_date = spark.table(GOLD_DIMENSIONS["date"])
    dim_patient = spark.table(GOLD_DIMENSIONS["patient"])
    
    # Add date information
    appts_with_date = fact_appointment.join(
        dim_date.select(
            col("date_key"),
            "year", 
            "month", 
            "year_month"
        ),
        "date_key"
    )
    
    # Get overall patient counts (all time)
    overall_patients = appts_with_date.groupBy("patient_key", "date_key", "year", "month", "year_month").agg(
        count("*").alias("total_current_patients"),
        countDistinct(when(col("is_new_patient") == True, col("patient_key"))).alias("new_patients")
    ).withColumn("invoice_date_key", col("date_key"))
    
    # Calculate returning patients (total - new)
    overall_patients = overall_patients.withColumn(
        "returning_patients",
        (col("total_current_patients") - col("new_patients")).cast(IntegerType())
    )
    
    # Get patients with future appointments (proxy for retention)
    current_date_key = int(datetime.now().strftime("%Y%m%d"))
    future_appts = fact_appointment.filter(col("date_key") >= current_date_key) \
        .select("patient_key").distinct()
    patients_with_future = future_appts.count()
    
    # Calculate retention and attrition
    overall_patients = overall_patients.withColumn(
        "patients_with_future_appointments", lit(patients_with_future).cast(IntegerType())
    ).withColumn(
        "retention_rate_pct",
        (col("returning_patients") / greatest(col("total_current_patients"), lit(1)) * 100).cast(FloatType())
    ).withColumn(
        "attrition_rate_pct",
        (lit(100) - col("retention_rate_pct")).cast(FloatType())
    ).withColumn(
        "conversion_rate_pct",
        (col("returning_patients") / greatest(col("new_patients"), lit(1)) * 100).cast(FloatType())
    )
    
    # Follow-up metrics (average appointments per patient - overall)
    followup_stats = appts_with_date.agg(
        (count("*") / countDistinct("patient_key")).cast(FloatType()).alias("avg_followups_per_patient"),
        count("*").alias("total_followup_appointments")
    )
    
    overall_patients = overall_patients.crossJoin(followup_stats)
    
    # Year-over-year comparison (placeholder as we don't have prior period for overall)
    overall_patients = overall_patients.withColumn(
        "total_current_patients_yoy", lit(None).cast(LongType())
    )
    
    # Satisfaction metrics (placeholder - would need survey data)
    overall_patients = overall_patients.withColumn(
        "avg_satisfaction_score", lit(0).cast(IntegerType())
    ).withColumn(
        "total_surveys_completed", lit(0).cast(IntegerType())
    ).withColumn(
        "delighted_patients", lit(0).cast(IntegerType())
    ).withColumn(
        "delighted_patients_pct", lit(0).cast(IntegerType())
    ).withColumn(
        "unhappy_patients", lit(0).cast(IntegerType())
    ).withColumn(
        "unhappy_patients_pct", lit(0).cast(IntegerType())
    ).withColumn(
        "pending_patients", lit(0).cast(IntegerType())
    ).withColumn(
        "lost_patients", lit(0).cast(IntegerType())
    )
    
    # Calculate health scores
    overall_patients = overall_patients.withColumn(
        "retention_score", col("retention_rate_pct").cast(FloatType())
    ).withColumn(
        "attrition_score", (lit(100) - col("attrition_rate_pct")).cast(FloatType())
    ).withColumn(
        "conversion_score", least(col("conversion_rate_pct"), lit(100)).cast(FloatType())
    ).withColumn(
        "growth_score",
        when(col("new_patients") > 0, lit(80)).otherwise(lit(50)).cast(IntegerType())
    ).withColumn(
        "panel_score",
        least(lit(100), (col("total_current_patients") / lit(100) * 10)).cast(FloatType())
    ).withColumn(
        "delight_score", lit(50).cast(IntegerType())
    ).withColumn(
        "unhappy_score", lit(50).cast(IntegerType())
    ).withColumn(
        "patient_health_score",
        (
            col("retention_score") * 0.3 +
            col("conversion_score") * 0.2 +
            col("growth_score") * 0.2 +
            col("panel_score") * 0.3
        ).cast(FloatType())
    )
    
    # Add metadata
    overall_patients = overall_patients \
        .withColumn("invoice_id", lit(None).cast(StringType())) \
        .withColumn("location_key", lit(1).cast(IntegerType())) \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # Select final columns matching target schema
    result = overall_patients.select(
        col("attrition_rate_pct").cast(FloatType()),
        col("attrition_score").cast(FloatType()),
        col("avg_followups_per_patient").cast(FloatType()),
        col("avg_satisfaction_score").cast(IntegerType()),
        col("conversion_rate_pct").cast(FloatType()),
        col("conversion_score").cast(FloatType()),
        col("delight_score").cast(IntegerType()),
        col("delighted_patients").cast(IntegerType()),
        col("delighted_patients_pct").cast(IntegerType()),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("growth_score").cast(IntegerType()),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("location_key").cast(IntegerType()),
        col("lost_patients").cast(IntegerType()),
        col("month").cast(IntegerType()),
        col("new_patients").cast(IntegerType()),
        col("panel_score").cast(FloatType()),
        col("patient_health_score").cast(FloatType()),
        col("patient_key").cast(LongType()),
        col("patients_with_future_appointments").cast(IntegerType()),
        col("pending_patients").cast(IntegerType()),
        col("retention_rate_pct").cast(FloatType()),
        col("retention_score").cast(FloatType()),
        col("returning_patients").cast(IntegerType()),
        col("total_current_patients").cast(LongType()),
        col("total_current_patients_yoy").cast(LongType()),
        col("total_followup_appointments").cast(IntegerType()),
        col("total_surveys_completed").cast(IntegerType()),
        col("unhappy_patients").cast(IntegerType()),
        col("unhappy_patients_pct").cast(IntegerType()),
        col("unhappy_score").cast(IntegerType()),
        col("year").cast(IntegerType()),
        col("year_month").cast(StringType())
    )
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# try:
#     df = create_agg_patient()
#     src_cnt = spark.table(GOLD_FACTS["appointment"]).count()
#     tgt_cnt = df.count()
#     # log_data_quality("agg_patient", df, "patient_key")

#     target_table = AGG_TABLES["patient"]

#     # Ensure table exists before overwrite
#     spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {target_table}
#     USING DELTA
#     """)

#     df.write.format("delta").mode("overwrite").saveAsTable(target_table)

#     end_job_instance(batch_id, job_id, "SUCCESS", msg="Created agg_patient", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
#     print("OK")
# except Exception as e:
#     end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed agg_patient. {safe_exception_text(e)}")
#     raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F

try:
    df = create_agg_patient()
    src_cnt = spark.table(GOLD_FACTS["appointment"]).count()
    target_table = AGG_TABLES["patient"]

    key_cols = ["invoice_date_key", "patient_key", "location_key"]

    dup_cnt = (
        df.groupBy(*key_cols).count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dup_cnt > 0:
        raise Exception(f"Source DF has {dup_cnt} duplicate key groups for {key_cols}")

    null_issues = [c for c in key_cols if df.filter(F.col(c).isNull()).limit(1).count() > 0]
    if null_issues:
        raise Exception(f"NULL merge keys found: {null_issues}")

    if not spark.catalog.tableExists(target_table):
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    else:
        DeltaTable.forName(spark, target_table) \
            .alias("t") \
            .merge(
                df.alias("s"),
                """t.invoice_date_key = s.invoice_date_key
                   AND t.patient_key = s.patient_key
                   AND t.location_key = s.location_key"""
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    final_tgt_cnt = spark.table(target_table).count()
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Merged agg_patient",
                     src_row_num=src_cnt, tgt_row_num=final_tgt_cnt)
    print("OK")

except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED",
                     msg=f"Failed agg_patient. {safe_exception_text(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
