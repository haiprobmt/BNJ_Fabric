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

# # Plato Bronze → Silver: `invoice` → `silver_item_invoice`

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = None
job_id = None              # required for logging (4-digit int/string is OK)
src_catalog = "plato"
job_group_name = "silver"
src_table = "invoice"
lh_silver_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/e8a5651c-a1b6-49de-b2c5-11b7e197e77a/Tables/plato_test'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# tgt_table = "silver_item_invoice"
# out_path = f"{lh_silver_path}/{tgt_table}"

# # delete the whole delta folder (data + _delta_log)
# if mssparkutils.fs.exists(out_path):
#     mssparkutils.fs.rm(out_path, True)
#     print(f"✅ Deleted: {out_path}")
# else:
#     print(f"⏭️ Not found: {out_path}")


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

import json
import traceback
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode_outer, lit

# -----------------------------
# Validate required params
# -----------------------------
if batch_id is None or str(batch_id).strip() == "":
    raise ValueError("Pipeline must pass batch_id (YYYYmmddhhmmss)")
if job_id is None or str(job_id).strip() == "":
    raise ValueError("Pipeline must pass job_id for this notebook")

job_id_str = str(job_id).strip()
src_table  = "invoice"
tgt_table  = "silver_item_invoice"

lh_silver_path = globals().get("lh_silver_path", None)
if not lh_silver_path or str(lh_silver_path).strip() == "":
    raise ValueError("lh_silver_path is not defined. Pass it as notebook parameter or define it in nb_utils.")

# optional: set src_catalog if your framework uses it
src_catalog = globals().get("src_catalog", "plato")

# -----------------------------
# Logging: start
# -----------------------------
#start_job_instance(batch_id, job_id_str, msg=f"Start SILVER src_table={src_table} -> {tgt_table}")

result_payload = None

try:
    df = spark.table(f"lh_bnj_bronze.{src_catalog}.invoice")
    cols = set(df.columns)

    # ---------------------------------------------------------
    # CASE A: Flattened columns exist like `item._id`, `item.qty`
    # ---------------------------------------------------------
    has_flat_item = any(c.startswith("item.") for c in cols)

    # ---------------------------------------------------------
    # CASE B: There is an actual column named `item` (array/json)
    # ---------------------------------------------------------
    has_item_col = "item" in cols

    if has_flat_item and not has_item_col:
        # Current bronze schema pattern you showed: dot-flattened item.*
        print("[mode] Using flattened item.* columns (no explode)")

        # Build EXACT projection to match your provided explode-based logic
        # (dot-names must be escaped with backticks)
        items_df = (
            df.select(
                col("_id").alias("invoice_id"),

                col("`item._id`").alias("item_id"),
                col("`item.batch_batch`").alias("batch_batch"),
                col("`item.batch_cpu`").alias("batch_cpu"),
                col("`item.batch_expiry`").alias("batch_expiry"),
                col("`item.category`").alias("category"),
                col("`item.cost_price`").alias("cost_price"),
                col("`item.ddose`").alias("ddose"),
                col("`item.dduration`").alias("dduration"),
                col("`item.description`").alias("description"),
                col("`item.dfreq`").alias("dfreq"),
                col("`item.disc_abs`").alias("disc_abs"),
                col("`item.discount`").alias("discount"),
                col("`item.dosage`").alias("dosage"),
                col("`item.dunit`").alias("dunit"),
                col("`item.expiry_after_dispensing`").alias("expiry_after_dispensing"),
                col("`item.facility`").alias("facility"),
                col("`item.facility_due`").alias("facility_due"),
                col("`item.facility_paid`").alias("facility_paid"),
                col("`item.facility_ref`").alias("facility_ref"),
                col("`item.facility_status`").alias("facility_status"),
                col("`item.fixed_price`").alias("fixed_price"),
                col("`item.given_id`").alias("given_id"),
                col("`item.hidden`").alias("hidden"),
                col("`item.id`").alias("item_given_id"),
                col("`item.inventory`").alias("inventory"),
                col("`item.invoice_id`").alias("item_invoice_id"),
                col("`item.min_price`").alias("min_price"),
                col("`item.name`").alias("item_name"),
                col("`item.no_discount`").alias("no_discount"),

                # col("`item.others.batch_batch`").alias("others_batch_batch"),
                # col("`item.others.batch_cpu`").alias("others_batch_cpu"),
                # col("`item.others.batch_expiry`").alias("others_batch_expiry"),

                col("`item.package_original_price`").alias("package_original_price"),
                col("`item.packageitems`").alias("packageitems"),
                col("`item.precautions`").alias("precautions"),
                col("`item.qty`").alias("qty"),
                col("`item.rate`").alias("item_rate"),
                col("`item.recalls`").alias("recalls"),
                col("`item.redeemable`").alias("redeemable"),
                col("`item.redemptions`").alias("redemptions"),
                col("`item.scheme`").alias("item_scheme"),
                col("`item.selling_price`").alias("selling_price"),
                col("`item.sub_total`").alias("item_sub_total"),
                col("`item.track_stock`").alias("track_stock"),
                col("`item.unit`").alias("unit"),
                col("`item.unit_price`").alias("unit_price"),
                col("`item.vaccines`").alias("vaccines"),
            )
            # keep only rows where there is actually an item (avoid creating all-null item rows)
            .filter(
                col("item_id").isNotNull()
                | col("item_given_id").isNotNull()
                | col("item_name").isNotNull()
            )
        )

    elif has_item_col:
        # item exists as array/json -> explode and project exactly the same fields as your logic
        print("[mode] Using item column (array/json) + explode")

        item_type = dict(df.dtypes).get("item")
        item_raw  = col("item")
        item_trim = F.trim(item_raw.cast("string"))

        # normalize to JSON array string
        item_wrapped = F.when(item_trim.startswith("{"), F.concat(lit("["), item_trim, lit("]"))).otherwise(item_trim)

        # handle quoted JSON string
        item_clean = F.when(
            (F.substring(item_wrapped, 1, 1) == lit('"')) & (F.substring(item_wrapped, F.length(item_wrapped), 1) == lit('"')),
            F.regexp_replace(F.substring(item_wrapped, 2, F.length(item_wrapped) - 2), r'\\\"', '"')
        ).otherwise(item_wrapped)

        item_clean = F.when(
            item_raw.isNull()
            | (F.length(F.trim(item_raw.cast("string"))) == 0)
            | (F.lower(F.trim(item_raw.cast("string"))) == lit("null")),
            lit(None).cast("string")
        ).otherwise(item_clean)

        if item_type == "string":
            sample_row = (
                df.where(item_clean.isNotNull() & (F.length(F.trim(item_clean)) > 2))
                  .select(item_clean.alias("item_clean"))
                  .limit(1)
                  .collect()
            )
            if not sample_row:
                # no items -> empty DF with correct columns
                items_df = spark.createDataFrame([], "invoice_id string")
            else:
                sample_json = sample_row[0]["item_clean"]
                schema_ddl = spark.range(1).select(F.schema_of_json(F.lit(sample_json)).alias("s")).first()["s"]

                df2 = df.withColumn(
                    "item_arr",
                    F.when(item_clean.isNull(), F.from_json(F.lit("[]"), schema_ddl))
                     .otherwise(F.from_json(item_clean, schema_ddl))
                )

                items_df = (
                    df2.select(
                        col("_id").alias("invoice_id"),
                        explode_outer("item_arr").alias("item_detail")
                    )
                    .select(
                        "invoice_id",
                        col("item_detail._id").alias("item_id"),
                        col("item_detail.batch_batch").alias("batch_batch"),
                        col("item_detail.batch_cpu").alias("batch_cpu"),
                        col("item_detail.batch_expiry").alias("batch_expiry"),
                        col("item_detail.category").alias("category"),
                        col("item_detail.cost_price").alias("cost_price"),
                        col("item_detail.ddose").alias("ddose"),
                        col("item_detail.dduration").alias("dduration"),
                        col("item_detail.description").alias("description"),
                        col("item_detail.dfreq").alias("dfreq"),
                        col("item_detail.disc_abs").alias("disc_abs"),
                        col("item_detail.discount").alias("discount"),
                        col("item_detail.dosage").alias("dosage"),
                        col("item_detail.dunit").alias("dunit"),
                        col("item_detail.expiry_after_dispensing").alias("expiry_after_dispensing"),
                        col("item_detail.facility").alias("facility"),
                        col("item_detail.facility_due").alias("facility_due"),
                        col("item_detail.facility_paid").alias("facility_paid"),
                        col("item_detail.facility_ref").alias("facility_ref"),
                        col("item_detail.facility_status").alias("facility_status"),
                        col("item_detail.fixed_price").alias("fixed_price"),
                        col("item_detail.given_id").alias("given_id"),
                        col("item_detail.hidden").alias("hidden"),
                        col("item_detail.id").alias("item_given_id"),
                        col("item_detail.inventory").alias("inventory"),
                        col("item_detail.invoice_id").alias("item_invoice_id"),
                        col("item_detail.min_price").alias("min_price"),
                        col("item_detail.name").alias("item_name"),
                        col("item_detail.no_discount").alias("no_discount"),
                        # col("item_detail.others.batch_batch").alias("others_batch_batch"),
                        # col("item_detail.others.batch_cpu").alias("others_batch_cpu"),
                        # col("item_detail.others.batch_expiry").alias("others_batch_expiry"),
                        col("item_detail.package_original_price").alias("package_original_price"),
                        col("item_detail.packageitems").alias("packageitems"),
                        col("item_detail.precautions").alias("precautions"),
                        col("item_detail.qty").alias("qty"),
                        col("item_detail.rate").alias("item_rate"),
                        col("item_detail.recalls").alias("recalls"),
                        col("item_detail.redeemable").alias("redeemable"),
                        col("item_detail.redemptions").alias("redemptions"),
                        col("item_detail.scheme").alias("item_scheme"),
                        col("item_detail.selling_price").alias("selling_price"),
                        col("item_detail.sub_total").alias("item_sub_total"),
                        col("item_detail.track_stock").alias("track_stock"),
                        col("item_detail.unit").alias("unit"),
                        col("item_detail.unit_price").alias("unit_price"),
                        col("item_detail.vaccines").alias("vaccines"),
                    )
                )
        else:
            # already complex (array/struct) -> explode directly
            df2 = df.withColumn("item_arr", col("item"))
            items_df = (
                df2.select(col("_id").alias("invoice_id"), explode_outer("item_arr").alias("item_detail"))
                   .select(
                        "invoice_id",
                        col("item_detail._id").alias("item_id"),
                        col("item_detail.batch_batch").alias("batch_batch"),
                        col("item_detail.batch_cpu").alias("batch_cpu"),
                        col("item_detail.batch_expiry").alias("batch_expiry"),
                        col("item_detail.category").alias("category"),
                        col("item_detail.cost_price").alias("cost_price"),
                        col("item_detail.ddose").alias("ddose"),
                        col("item_detail.dduration").alias("ddduration"),
                        col("item_detail.description").alias("description"),
                        col("item_detail.dfreq").alias("dfreq"),
                        col("item_detail.disc_abs").alias("disc_abs"),
                        col("item_detail.discount").alias("discount"),
                        col("item_detail.dosage").alias("dosage"),
                        col("item_detail.dunit").alias("dunit"),
                        col("item_detail.expiry_after_dispensing").alias("expiry_after_dispensing"),
                        col("item_detail.facility").alias("facility"),
                        col("item_detail.facility_due").alias("facility_due"),
                        col("item_detail.facility_paid").alias("facility_paid"),
                        col("item_detail.facility_ref").alias("facility_ref"),
                        col("item_detail.facility_status").alias("facility_status"),
                        col("item_detail.fixed_price").alias("fixed_price"),
                        col("item_detail.given_id").alias("given_id"),
                        col("item_detail.hidden").alias("hidden"),
                        col("item_detail.id").alias("item_given_id"),
                        col("item_detail.inventory").alias("inventory"),
                        col("item_detail.invoice_id").alias("item_invoice_id"),
                        col("item_detail.min_price").alias("min_price"),
                        col("item_detail.name").alias("item_name"),
                        col("item_detail.no_discount").alias("no_discount"),
                        # col("item_detail.others.batch_batch").alias("others_batch_batch"),
                        # col("item_detail.others.batch_cpu").alias("others_batch_cpu"),
                        # col("item_detail.others.batch_expiry").alias("others_batch_expiry"),
                        col("item_detail.package_original_price").alias("package_original_price"),
                        col("item_detail.packageitems").alias("packageitems"),
                        col("item_detail.precautions").alias("precautions"),
                        col("item_detail.qty").alias("qty"),
                        col("item_detail.rate").alias("item_rate"),
                        col("item_detail.recalls").alias("recalls"),
                        col("item_detail.redeemable").alias("redeemable"),
                        col("item_detail.redemptions").alias("redemptions"),
                        col("item_detail.scheme").alias("item_scheme"),
                        col("item_detail.selling_price").alias("selling_price"),
                        col("item_detail.sub_total").alias("item_sub_total"),
                        col("item_detail.track_stock").alias("track_stock"),
                        col("item_detail.unit").alias("unit"),
                        col("item_detail.unit_price").alias("unit_price"),
                        col("item_detail.vaccines").alias("vaccines"),
                   )
            )
    else:
        end_job_instance(batch_id, job_id_str, "SUCCESS", msg=f"No item columns found in bronze; skipping {tgt_table}.")
        result_payload = {
            "return_code": 0,
            "return_msg": "No item columns found",
            "batch_id": batch_id,
            "job_id": job_id_str,
            "src_table": src_table,
            "tgt_table": tgt_table
        }
        mssparkutils.notebook.exit(json.dumps(result_payload, ensure_ascii=False))

    # -----------------------------
    # Write output (path-based)
    # -----------------------------
    out_path = f"{lh_silver_path}/{tgt_table}"
    items_df.write.format("delta").mode("overwrite").save(out_path)

    row_cnt = None
    try:
        row_cnt = spark.read.format("delta").load(out_path).count()
    except Exception:
        row_cnt = None

    end_job_instance(batch_id, job_id_str, "SUCCESS",
                     msg=f"Completed SILVER src_table={src_table} -> {tgt_table} rows={row_cnt}")

    result_payload = {
        "return_code": 0,
        "return_msg": "OK",
        "batch_id": batch_id,
        "job_id": job_id_str,
        "src_table": src_table,
        "tgt_table": tgt_table,
        "rows_written": row_cnt
    }

except Exception as e:
    if e.__class__.__name__ == "NotebookExit":
        raise

    try:
        detail = safe_exception_text(e)
    except Exception:
        detail = traceback.format_exc()

    end_job_instance(batch_id, job_id_str, "FAILED", msg=f"FAILED {tgt_table}. {detail}"[:8000])

    raise Exception(json.dumps({
        "return_code": -1,
        "return_msg": "FAILED",
        "batch_id": batch_id,
        "job_id": job_id_str,
        "src_table": src_table,
        "tgt_table": tgt_table,
        "error": detail[:8000]
    }, ensure_ascii=False))

mssparkutils.notebook.exit(json.dumps(result_payload, ensure_ascii=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from lh_bnj_silver.plato_test.silver_item_invoice;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
