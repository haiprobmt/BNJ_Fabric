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

# # Gold Aggregation: `agg_overall_health` (Single Table)


# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '9465'
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

# This cell is generated from runtime parameters. Learn more: https://go.microsoft.com/fwlink/?linkid=2161015
batch_id = "20260308050005"
job_id = "9465"
src_catalog = "gold"
tgt_catalog = "gold_test"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


GOLD_FACTS = {"invoice": f"lh_bnj_gold.{tgt_catalog}.fact_invoice"}
AGG_TABLES = {"overall_health": f"lh_bnj_gold.{tgt_catalog}.agg_overall_health", "financial_health": f"lh_bnj_gold.{tgt_catalog}.agg_financial_health", "ar_ap": f"lh_bnj_gold.{tgt_catalog}.agg_ar_ap", "cashflow": f"lh_bnj_gold.{tgt_catalog}.agg_cashflow",
"patient": f"lh_bnj_gold.{tgt_catalog}.agg_patient", "claims": f"lh_bnj_gold.{tgt_catalog}.agg_claims", "inventory": f"lh_bnj_gold.{tgt_catalog}.agg_inventory"}
GOLD_DIMENSIONS = {"date": f"lh_bnj_gold.{tgt_catalog}.dim_date"}

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

def create_agg_overall_health():
    """
    Create agg_overall_health aggregate table.
    Master health scorecard combining all health dimensions.
    
    Overall Health Score = weighted average of:
    - Financial Health (30%)
    - Patient Health (20%)
    - Claims Health (15%)
    - Inventory Health (15%)
    - Operational Health (20%)
    """
    
    # Read all component aggregate tables
    try:
        agg_financial = spark.table(AGG_TABLES["financial_health"])
        agg_patient = spark.table(AGG_TABLES["patient"])
        agg_claims = spark.table(AGG_TABLES["claims"])
        agg_inventory = spark.table(AGG_TABLES["inventory"])
        agg_ar_ap = spark.table(AGG_TABLES["ar_ap"])
        agg_cashflow = spark.table(AGG_TABLES["cashflow"])
    except Exception as e:
        print(f"⚠️ Required aggregate tables not available: {e}")
        return None
    
    dim_date = spark.table(GOLD_DIMENSIONS["date"])
    
    # Get scores from each aggregate
    financial_scores = agg_financial.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("financial_health_score"),
        col("ar_ap_health_score").cast(FloatType()),
        col("cashflow_health_score"),
        col("pl_health_score").cast(DecimalType(18,2))
    )
    
    patient_scores = agg_patient.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("patient_health_score"),
        col("year"),
        col("month"),
        col("year_month")
    )
    
    claims_scores = agg_claims.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("claims_health_score")
    )
    
    inventory_scores = agg_inventory.select(
        col("invoice_date_key"),
        col("invoice_id"),
        col("inventory_health_score")
    )
    
    # Join all scores
    overall = financial_scores \
        .join(patient_scores, ["invoice_date_key", "invoice_id"], "outer") \
        .join(claims_scores, ["invoice_date_key", "invoice_id"], "outer") \
        .join(inventory_scores, ["invoice_date_key", "invoice_id"], "outer")
    
    # Count invoices
    invoice_counts = spark.table(GOLD_FACTS["invoice"]).groupBy("date_key") \
        .agg(count("*").alias("invoice_count"))
    
    overall = overall.join(
        invoice_counts, 
        overall["invoice_date_key"] == invoice_counts["date_key"], 
        "left"
    ).drop("date_key")
    
    # Operational Health Score using weights from Formular Calculations doc
    # Formula: Op_Health = 0.4×Claims + 0.2×Patient + 0.4×Inventory
    overall = overall.withColumn(
        "operational_health_score",
        (
            coalesce(col("claims_health_score"), lit(50)) * OPERATIONAL_HEALTH_WEIGHTS["claims_health"] +    # 0.4
            coalesce(col("patient_health_score"), lit(50)) * OPERATIONAL_HEALTH_WEIGHTS["patient_health"] +  # 0.2
            coalesce(col("inventory_health_score"), lit(50)) * OPERATIONAL_HEALTH_WEIGHTS["inventory_health"] # 0.4
        ).cast(FloatType())
    )
    
    # Calculate Overall Health Score (General Health - 10% each component per doc Section 11)
    # Formula: General_Health = Average of 10 scores (each weighted 10%)
    overall = overall.withColumn(
        "overall_health_score",
        (
            (coalesce(col("financial_health_score"), lit(50)) +
             coalesce(col("ar_ap_health_score"), lit(50)) +
             coalesce(col("cashflow_health_score"), lit(50)) +
             coalesce(col("pl_health_score"), lit(50)) +
             coalesce(col("patient_health_score"), lit(50)) +
             coalesce(col("claims_health_score"), lit(50)) +
             coalesce(col("inventory_health_score"), lit(50)) +
             coalesce(col("operational_health_score"), lit(50))) / 8 * GENERAL_HEALTH_COMPONENT_WEIGHT * 10  # Each 10%
        ).cast(FloatType())
    )
    
    # YoY calculations
    window_yoy = Window.orderBy("year", "month")
    overall = overall.withColumn(
        "yoy_health_score_prior_year",
        lag("overall_health_score", 12).over(window_yoy)
    ).withColumn(
        "yoy_health_score",
        coalesce(col("yoy_health_score_prior_year"), col("overall_health_score")).cast(FloatType())
    ).withColumn(
        "yoy_health_score_change",
        (col("overall_health_score") - coalesce(col("yoy_health_score_prior_year"), col("overall_health_score"))).cast(FloatType())
    ).withColumn(
        "yoy_health_score_change_pct",
        (col("yoy_health_score_change") / greatest(col("yoy_health_score_prior_year"), lit(1)) * 100).cast(FloatType())
    ).withColumn(
        "overall_health_score_yoy",
        col("yoy_health_score_change").cast(FloatType())
    ).withColumn(
        "yoy_health_change",
        col("yoy_health_score_change").cast(FloatType())
    ).withColumn(
        "yoy_health_change_pct",
        col("yoy_health_score_change_pct").cast(FloatType())
    )
    
    # Load dynamic target from brz_target table
    targets = get_targets_for_period()
    TARGET_YEAR_END_SCORE = targets.get("target_year_end_score", 80.0)
    
    # Targets and forecasting using dynamic target
    overall = overall.withColumn(
        "target_year_end_score", lit(TARGET_YEAR_END_SCORE).cast(FloatType())
    ).withColumn(
        "progress_to_target_pct",
        (col("overall_health_score") / lit(TARGET_YEAR_END_SCORE) * 100).cast(FloatType())
    ).withColumn(
        "forecasted_year_end_score",
        (col("overall_health_score") + coalesce(col("yoy_health_score_change"), lit(0)) * 0.5).cast(FloatType())
    )
    
    # Health status
    overall = overall.withColumn(
        "health_status",
        when(col("overall_health_score") >= 80, "Excellent")
        .when(col("overall_health_score") >= 60, "Good")
        .when(col("overall_health_score") >= 40, "Fair")
        .otherwise("Poor").cast(StringType())
    ).withColumn(
        "health_status_color",
        when(col("overall_health_score") >= 80, "green")
        .when(col("overall_health_score") >= 60, "blue")
        .when(col("overall_health_score") >= 40, "yellow")
        .otherwise("red").cast(StringType())
    )
    
    # Add metadata
    overall = overall \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # Select final columns matching target schema
    result = overall.select(
        col("ar_ap_health_score").cast(FloatType()),
        col("cashflow_health_score").cast(FloatType()),
        col("claims_health_score").cast(FloatType()),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("financial_health_score").cast(FloatType()),
        col("forecasted_year_end_score").cast(FloatType()),
        col("health_status").cast(StringType()),
        col("health_status_color").cast(StringType()),
        col("inventory_health_score").cast(FloatType()),
        col("invoice_count").cast(LongType()),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("month").cast(IntegerType()),
        col("operational_health_score").cast(FloatType()),
        col("overall_health_score").cast(FloatType()),
        col("overall_health_score_yoy").cast(FloatType()),
        col("patient_health_score").cast(FloatType()),
        col("pl_health_score").cast(DecimalType(18,2)),
        col("progress_to_target_pct").cast(FloatType()),
        col("target_year_end_score").cast(FloatType()),
        col("year").cast(IntegerType()),
        col("year_month").cast(StringType()),
        col("yoy_health_change").cast(FloatType()),
        col("yoy_health_change_pct").cast(FloatType()),
        col("yoy_health_score").cast(FloatType()),
        col("yoy_health_score_change").cast(FloatType()),
        col("yoy_health_score_change_pct").cast(FloatType()),
        col("yoy_health_score_prior_year").cast(FloatType())
    )
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, current_timestamp

def create_agg_overall_health():
    """
    Create agg_overall_health (invoice-grain) using the new logic:
    Overall Health = average(Financial Health, Operational Health placeholder)

    Source: agg_financial_health
    Output columns include: location_key, invoice_id, invoice_date_key,
    financial_health_score, operational_health_score, overall_health_score,
    forecasted_year_end_score, target_year_end_score, progress_to_target_pct,
    health_status, dw_created_at, dw_updated_at
    """

    try:
        agg_financial = spark.table(AGG_TABLES["financial_health"])
    except Exception as e:
        print(f"⚠️ Required aggregate table not available: {e}")
        return None

    # Placeholder operational score (later you can replace by weighted avg of patient/claims/inventory)
    operational_score = lit(70.0).cast("double")
    target_year_end_score = lit(80.0).cast("double")

    overall_health = agg_financial.select(
        col("location_key"),
        col("invoice_id"),
        col("invoice_date_key"),

        col("financial_health_score").cast("double").alias("financial_health_score"),
        operational_score.alias("operational_health_score"),

        ((col("financial_health_score").cast("double") + operational_score) / 2.0).alias("overall_health_score"),

        # Forecast and target
        ((col("financial_health_score").cast("double") + operational_score) / 2.0).alias("forecasted_year_end_score"),
        target_year_end_score.alias("target_year_end_score"),
        (((col("financial_health_score").cast("double") + operational_score) / 2.0) / target_year_end_score * 100.0).alias("progress_to_target_pct"),

        # Audit
        current_timestamp().alias("dw_created_at"),
        current_timestamp().alias("dw_updated_at")
    )

    overall_health_final = overall_health.withColumn(
        "health_status",
        when(col("overall_health_score") >= 75, "Green")
        .when(col("overall_health_score") >= 26, "Yellow")
        .otherwise("Red")
    )

    return overall_health_final

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F

try:
    df = create_agg_overall_health()
    src_cnt = spark.table(GOLD_FACTS["invoice"]).count()
    target_table = AGG_TABLES["overall_health"]

    key_cols = ["invoice_date_key", "invoice_id"]

    # Validate uniqueness
    dup_cnt = (
        df.groupBy(*key_cols).count()
          .filter(F.col("count") > 1)
          .count()
    )
    if dup_cnt > 0:
        raise Exception(f"Source DF has {dup_cnt} duplicate key groups for {key_cols}")

    # Validate nulls
    null_issues = [c for c in key_cols if df.filter(F.col(c).isNull()).limit(1).count() > 0]
    if null_issues:
        raise Exception(f"NULL merge keys found: {null_issues}")

    # First load vs merge
    if not spark.catalog.tableExists(target_table):
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    else:
        DeltaTable.forName(spark, target_table) \
            .alias("t") \
            .merge(df.alias("s"),
                   "t.invoice_date_key = s.invoice_date_key AND t.invoice_id = s.invoice_id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    final_tgt_cnt = spark.table(target_table).count()
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Merged agg_overall_health",
                     src_row_num=src_cnt, tgt_row_num=final_tgt_cnt)
    print("OK")

except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED",
                     msg=f"Failed agg_overall_health. {safe_exception_text(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import builtins
from pyspark.sql import functions as F

def any_unique_possible(df, cols=None, exclude_prefixes=("_",)):
    if cols is None:
        cols = [c for c in df.columns if not any(c.startswith(p) for p in exclude_prefixes)]
    total = df.count()
    distinct_all = df.select(*cols).distinct().count()
    dup_rows = total - distinct_all
    print("rows:", total)
    print("distinct(all checked cols):", distinct_all)
    print("duplicate rows across all checked cols:", dup_rows)
    return dup_rows == 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import builtins
from itertools import combinations
from pyspark.sql import functions as F

def find_unique_key_combos(
    df,
    candidate_cols=None,
    max_k=4,
    sample_limit=None,
    exclude_prefixes=("_",),
    only_complete_keys=False,
    show_top=50
):
    work = df
    if sample_limit:
        work = work.limit(sample_limit)

    cols = [c for c in work.columns if not any(c.startswith(p) for p in exclude_prefixes)]

    # Auto-pick candidates (you can override)
    if candidate_cols is None:
        candidate_cols = [c for c in cols if c.endswith("_id") or c.endswith("_key")]
        for c in ["year_month", "year", "month"]:
            if c in cols and c not in candidate_cols:
                candidate_cols.append(c)

    candidate_cols = [c for c in candidate_cols if c in cols]

    total_rows = work.count()
    results = []

    for k in range(1, max_k + 1):
        for combo in combinations(candidate_cols, k):
            tmp = work
            if only_complete_keys:
                cond = None
                for c in combo:
                    cnd = F.col(c).isNotNull()
                    cond = cnd if cond is None else (cond & cnd)
                tmp = tmp.filter(cond)

            tmp_rows = tmp.count()
            if tmp_rows == 0:
                continue

            distinct_keys = tmp.select(*combo).distinct().count()
            dup_rows = tmp_rows - distinct_keys
            uniq_pct = distinct_keys / tmp_rows * 100
            results.append((combo, tmp_rows, distinct_keys, dup_rows, uniq_pct))

    # sort best first (fewest duplicates, then smaller key)
    results.sort(key=lambda x: (x[3], len(x[0])))

    unique = [r for r in results if r[3] == 0]
    non_unique = [r for r in results if r[3] != 0]

    print(f"Total rows: {total_rows:,}")
    print(f"Candidates ({len(candidate_cols)}): {candidate_cols}")
    print(f"Tested combos: {len(results):,}")
    print(f"Unique combos found: {len(unique):,}")

    def show(rows, title):
        print("\n" + title)
        print("-" * len(title))
        topn = builtins.min(show_top, len(rows))
        for combo, tmp_rows, distinct_keys, dup_rows, uniq_pct in rows[:topn]:
            print(f"{list(combo)} | rows_used={tmp_rows:,} distinct={distinct_keys:,} dup_rows={dup_rows:,} uniq%={uniq_pct:.2f}")

    if unique:
        show(unique, f"✅ UNIQUE KEYS (top {builtins.min(show_top, len(unique))})")
    else:
        show(non_unique, f"⚠️ NO UNIQUE KEY FOUND (best {builtins.min(show_top, len(non_unique))})")

    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = create_agg_overall_health()

# IMPORTANT: check separately
df_invoice = df.filter(F.col("invoice_date_key").isNotNull() & F.col("invoice_id").isNotNull())
df_null    = df.filter(F.col("invoice_id").isNull() & F.col("invoice_date_key").isNotNull())

print("invoice rows:", df_invoice.count())
print("null-invoice rows:", df_null.count())

# Use the 'find_unique_key_combos' function from earlier (no base)
# If you don't have it in the notebook, paste it again from previous message.

print("\n== Unique keys for invoice rows ==")
find_unique_key_combos(df_invoice, max_k=3, only_complete_keys=True)

print("\n== Unique keys for null-invoice rows (if you keep them) ==")
find_unique_key_combos(df_null, max_k=3, only_complete_keys=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = create_agg_overall_health()

# invoice rows only
df_invoice = df.filter(F.col("invoice_id").isNotNull() & F.col("invoice_date_key").isNotNull())

# find invoice_ids that are duplicated
dup_ids = (
    df_invoice.groupBy("invoice_id")
              .count()
              .filter(F.col("count") > 1)
              .select("invoice_id")
)

# show duplicated rows (join back)
dup_rows = df_invoice.join(dup_ids, on="invoice_id", how="inner")

print("duplicate invoice_id rows:", dup_rows.count())
dup_rows.orderBy("invoice_id", "invoice_date_key").show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_ids = dup_ids.limit(5)
df_invoice.join(sample_ids, "invoice_id").orderBy("invoice_id").show(200, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
