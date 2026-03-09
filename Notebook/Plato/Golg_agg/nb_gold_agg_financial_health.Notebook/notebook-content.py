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

# # Gold Aggregation: `agg_financial_health` (Single Table)


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '8844'
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


GOLD_FACTS = {}
AGG_TABLES = {"financial_health": f"lh_bnj_gold.{tgt_catalog}.agg_financial_health", "ar_ap": f"lh_bnj_gold.{tgt_catalog}.agg_ar_ap", "cashflow": f"lh_bnj_gold.{tgt_catalog}.agg_cashflow"}

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

def create_agg_financial_health():
    """
    Create agg_financial_health aggregate table.
    Combines AR/AP Health, Cashflow Health, and P&L Health into overall financial health.
    """
    
    # Read component aggregate tables
    try:
        agg_ar_ap = spark.table(AGG_TABLES["ar_ap"])
        agg_cashflow = spark.table(AGG_TABLES["cashflow"])
    except Exception as e:
        print(f"⚠️ Required aggregate tables not available: {e}")
        return None
    
    # Get AR/AP health scores
    ar_ap_scores = agg_ar_ap.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("ar_ap_health_score").cast(DecimalType(18,2)).alias("ar_ap_health_score"),
        col("location_key")
    )
    
    # Get Cashflow health scores
    cashflow_scores = agg_cashflow.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("cashflow_health_score")
    )
    
    # Join scores
    financial_health = ar_ap_scores.join(
        cashflow_scores, 
        ["invoice_date_key", "invoice_id"], 
        "outer"
    )
    
    # P&L Health Score placeholder (would need fact_pnl)
    financial_health = financial_health.withColumn(
        "pl_health_score",
        coalesce(col("cashflow_health_score"), lit(50)).cast(DecimalType(18,2))
    )
    
    # Calculate overall financial health score
    financial_health = financial_health.withColumn(
        "financial_health_score",
        (
            coalesce(col("ar_ap_health_score"), lit(50)) * 0.4 +
            coalesce(col("cashflow_health_score"), lit(50)) * 0.4 +
            coalesce(col("pl_health_score"), lit(50)) * 0.2
        ).cast(FloatType())
    )
    
    # Determine health status
    financial_health = financial_health.withColumn(
        "health_status",
        when(col("financial_health_score") >= 80, "Excellent")
        .when(col("financial_health_score") >= 60, "Good")
        .when(col("financial_health_score") >= 40, "Fair")
        .otherwise("Poor").cast(StringType())
    ).withColumn(
        "health_status_color",
        when(col("financial_health_score") >= 80, "green")
        .when(col("financial_health_score") >= 60, "blue")
        .when(col("financial_health_score") >= 40, "yellow")
        .otherwise("red").cast(StringType())
    )
    
    # Add metadata
    financial_health = financial_health \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # Select final columns matching target schema
    result = financial_health.select(
        col("ar_ap_health_score").cast(DecimalType(18,2)),
        col("cashflow_health_score").cast(FloatType()),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("financial_health_score").cast(FloatType()),
        col("health_status").cast(StringType()),
        col("health_status_color").cast(StringType()),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("location_key").cast(LongType()),
        col("pl_health_score").cast(DecimalType(18,2))
    )
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# try:
#     df = create_agg_financial_health()
#     src_cnt = 0
#     tgt_cnt = df.count()
#     # log_data_quality("agg_financial_health", df, "financial_health_key")

#     target_table = AGG_TABLES["financial_health"]

#     # Ensure table exists before overwrite
#     spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {target_table}
#     USING DELTA
#     """)

#     df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

#     end_job_instance(batch_id, job_id, "SUCCESS", msg="Created agg_financial_health", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
#     print("OK")
# except Exception as e:
#     end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed agg_financial_health. {safe_exception_text(e)}")
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
    df = create_agg_financial_health()
    src_cnt = 0

    target_table = AGG_TABLES["financial_health"]
    key_cols = ["invoice_date_key", "invoice_id", "location_key"]

    if df is None:
        raise Exception("create_agg_financial_health() returned None")

    # Validate merge key uniqueness in source df
    dup_cnt = (
        df.groupBy(*key_cols)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dup_cnt > 0:
        raise Exception(f"Merge key is not unique in source dataframe. Found {dup_cnt} duplicate key groups.")

    # Validate no NULLs in merge keys
    # null_issues = []
    # for c in key_cols:
    #     c_null_cnt = df.filter(F.col(c).isNull()).count()
    #     if c_null_cnt > 0:
    #         null_issues.append(f"{c}={c_null_cnt}")

    # if null_issues:
    #     raise Exception(f"Merge key contains NULL values: {', '.join(null_issues)}")

    # First load
    if not spark.catalog.tableExists(target_table):
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)

    # Incremental load
    else:
        delta_target = DeltaTable.forName(spark, target_table)

        merge_condition = """
            t.invoice_date_key = s.invoice_date_key
            AND t.invoice_id = s.invoice_id
            AND t.location_key = s.location_key
        """

        (
            delta_target.alias("t")
            .merge(
                df.alias("s"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    final_tgt_cnt = spark.table(target_table).count()

    end_job_instance(
        batch_id,
        job_id,
        "SUCCESS",
        msg="Merged agg_financial_health",
        src_row_num=src_cnt,
        tgt_row_num=final_tgt_cnt
    )
    print("OK")

except Exception as e:
    end_job_instance(
        batch_id,
        job_id,
        "FAILED",
        msg=f"Failed agg_financial_health. {safe_exception_text(e)}"
    )
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
