# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2aae23b9-c116-40b8-9787-a7c48707bf50",
# META       "default_lakehouse_name": "lh_bnj_metadata",
# META       "default_lakehouse_workspace_id": "87076c77-5525-4288-9ae6-8631261bdbd5",
# META       "known_lakehouses": [
# META         {
# META           "id": "2aae23b9-c116-40b8-9787-a7c48707bf50"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import calendar
import time
import random
import traceback
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]  
WORKSPACE_NAME = notebookutils.runtime.context["currentWorkspaceName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

ETL_LOG_TABLE = f"`{WORKSPACE_NAME}`.lh_bnj_metadata.md.etl_log"   # change to your full table name if needed
LOG_SOURCE    = "FABRIC"

# -----------------------
# Helpers
# -----------------------
def build_job_instance_id(batch_id: str, job_id: str) -> str:
    b = str(batch_id).strip()
    j = str(job_id).strip()
    if not b:
        raise ValueError("batch_id is required")
    if not j or len(j) != 4:
        raise ValueError("job_id must be 4 characters (e.g., '1003', '0007', '0000')")
    return f"{b}{j}"

def safe_exception_text(e: Exception, max_len: int = 8000) -> str:
    s = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    return s[:max_len]

# -----------------------
# Core upsert (Delta MERGE)
# -----------------------
def etl_log_upsert(
    batch_id: str,
    job_id: str,
    status: str,
    msg: str = None,
    src_row_num=None,
    tgt_row_num=None,
    set_start: bool = False,
    set_end: bool = False,
    source: str = None,
):
    if not status or str(status).strip() == "":
        raise ValueError("status is required")

    b = str(batch_id).strip()
    j = str(job_id).strip()
    jiid = build_job_instance_id(b, j)

    src = source or LOG_SOURCE

    # IMPORTANT: If md.etl_log.job_id is INT, cast here; if STRING, keep as string.
    # If your column is STRING, change next line to: job_id_value = j
    job_id_value = int(j)

    s = (
        spark.range(1).select(
            F.lit(jiid).alias("job_instance_id"),
            F.lit(job_id_value).alias("job_id"),
            F.lit(b).alias("batch_id"),
            F.lit(status).alias("job_instance_status"),
            F.lit(msg).alias("job_instance_msg"),
            F.lit(src_row_num).cast("bigint").alias("src_row_num"),
            F.lit(tgt_row_num).cast("bigint").alias("tgt_row_num"),
            F.current_timestamp().alias("created_dt"),
            F.current_timestamp().alias("last_updated_dt"),
            F.lit(src).alias("source"),
        )
    )

    dt = DeltaTable.forName(spark, ETL_LOG_TABLE)

    update_set = {
        "job_instance_status": "s.job_instance_status",
        "job_instance_msg": "s.job_instance_msg",
        "src_row_num": "coalesce(s.src_row_num, t.src_row_num)",
        "tgt_row_num": "coalesce(s.tgt_row_num, t.tgt_row_num)",
        "last_updated_dt": "current_timestamp()",
        "source": "s.source",
    }

    if set_start:
        update_set["start_ts"] = "coalesce(t.start_ts, current_timestamp())"

    if set_end:
        update_set["end_ts"] = "current_timestamp()"

    # (
    #     dt.alias("t")
    #     .merge(s.alias("s"), "t.job_instance_id = s.job_instance_id")
    #     .whenMatchedUpdate(set=update_set)
    #     .whenNotMatchedInsert(values={
    #         "job_instance_id": "s.job_instance_id",
    #         "job_id": "s.job_id",
    #         "batch_id": "s.batch_id",
    #         "job_instance_status": "s.job_instance_status",
    #         "job_instance_msg": "s.job_instance_msg",
    #         "start_ts": "case when s.job_instance_status = 'RUNNING' then current_timestamp() else null end",
    #         "end_ts":   "case when s.job_instance_status in ('SUCCESS','FAILED','CANCELLED','SKIPPED') then current_timestamp() else null end",
    #         "src_row_num": "s.src_row_num",
    #         "tgt_row_num": "s.tgt_row_num",
    #         "created_dt": "current_timestamp()",
    #         "last_updated_dt": "current_timestamp()",
    #         "source": "s.source",
    #     })
    #     .execute()
    # )

    max_retries = 3
    for attempt in range(max_retries):
        try:
            (
                dt.alias("t")
                .merge(s.alias("s"), "t.job_instance_id = s.job_instance_id")
                .whenMatchedUpdate(set=update_set)
                .whenNotMatchedInsert(values={
                    "job_instance_id": "s.job_instance_id",
                    "job_id": "s.job_id",
                    "batch_id": "s.batch_id",
                    "job_instance_status": "s.job_instance_status",
                    "job_instance_msg": "s.job_instance_msg",
                    "start_ts": "case when s.job_instance_status = 'RUNNING' then current_timestamp() else null end",
                    "end_ts":   "case when s.job_instance_status in ('SUCCESS','FAILED','CANCELLED','SKIPPED') then current_timestamp() else null end",
                    "src_row_num": "s.src_row_num",
                    "tgt_row_num": "s.tgt_row_num",
                    "created_dt": "current_timestamp()",
                    "last_updated_dt": "current_timestamp()",
                    "source": "s.source",
                })
                .execute()
            )

            break
        except Exception as e: 
            if "ConcurrentAppendException" in str(e) and attempt < max_retries - 1: time.sleep(5) # wait before retry 
            else: raise

# -----------------------
# Public wrappers
# -----------------------
def start_batch(batch_id: str, msg: str = "Pipeline started"):
    etl_log_upsert(batch_id, "0000", "RUNNING", msg, set_start=True, set_end=False)

def end_batch(batch_id: str, status: str, msg: str = None):
    etl_log_upsert(batch_id, "0000", status, msg, set_start=False, set_end=True)

def start_job_instance_legacy(batch_id: str, job_id: str, msg: str = None):
    etl_log_upsert(batch_id, job_id, "RUNNING", msg, set_start=True, set_end=False)

def start_job_instance(batch_id: str, job_id: str, batch_group: str, job_group_name: str):
    #etl_log_upsert(batch_id, job_id, "RUNNING", msg, set_start=True, set_end=False)

    df = spark.table("`WS-ETL-BNJ`.lh_bnj_metadata.md.etl_jobs") \
        .filter(col("active_flg") == "Y")

    if job_group_name:
        df = df.filter(col("job_group_name") == job_group_name)

    if batch_group:
        df = df.filter(col("batch_group") == batch_group)

    if job_id is not None:
        df = df.filter(col("job_id") == job_id)
    
    df = df.select(
        concat(
            lit(batch_id),
            col("job_id")
        ).alias("job_instance_id"),
        col("job_id").cast("int").alias("job_id"),
        lit(batch_id).cast("string").alias("batch_id"),
        lit("RUNNING").alias("job_instance_status"),
        concat(
            lit("Start domain="),
            col("tgt_table")
        ).alias("job_instance_msg"),
        current_timestamp().alias("start_ts"),
        current_timestamp().alias("end_ts"),
        lit(0).alias("src_row_num"),
        lit(0).alias("tgt_row_num"),
        current_timestamp().alias("created_dt"),
        current_timestamp().alias("last_updated_dt"),
        lit(LOG_SOURCE).alias("source")
    )

    log_data = [r.asDict() for r in df.collect()]

    etl_log_schema = StructType([
        StructField("job_instance_id", StringType(), False),
        StructField("job_id", IntegerType(), False),
        StructField("batch_id", StringType(), True),
        StructField("job_instance_status", StringType(), False),
        StructField("job_instance_msg", StringType(), True),
        StructField("start_ts", TimestampType(), True),
        StructField("end_ts", TimestampType(), True),
        StructField("src_row_num", LongType(), True),
        StructField("tgt_row_num", LongType(), True),
        StructField("created_dt", TimestampType(), False),
        StructField("last_updated_dt", TimestampType(), False),
        StructField("source", StringType(), True)
    ])

    df = spark.createDataFrame(log_data, etl_log_schema)
    df.write.format("delta").mode("append").saveAsTable(ETL_LOG_TABLE)

def end_job_instance(batch_id: str, job_id: str, status: str, msg: str = None, src_row_num=None, tgt_row_num=None):
    etl_log_upsert(batch_id, job_id, status, msg, src_row_num=src_row_num, tgt_row_num=tgt_row_num, set_start=False, set_end=True)

# -----------------------
# Optional: pre-create step rows as RUNNING for a whole batch
# -----------------------
def log_job_instance_for_batch(batch_id: str, job_ids: list[str], msg_prefix: str = None):
    """
    Creates/updates RUNNING rows for many job_ids (one MERGE per job_id).
    Useful if you want to mark all expected steps as RUNNING at pipeline start.
    """
    for jid in job_ids:
        m = f"{msg_prefix}{jid}" if msg_prefix else None
        start_job_instance(batch_id, jid, m)

def merge_dimension(
    src_df: DataFrame,
    tgt_table: str,
    business_keys: list[str],
    surrogate_key: str
):
    """
        - If the target table does not exist:
        Creates the table and generates surrogate keys.

    - If the table exists:
        1. Detects new records based on the business key.
        2. Generates surrogate keys for new records.
        3. Performs a Delta MERGE:
            - Updates existing records (SCD Type 1 behavior)
            - Inserts new records.
    """
    window = Window.orderBy(monotonically_increasing_id())

    if not spark.catalog.tableExists(tgt_table):
        df = src_df.withColumn(
            surrogate_key,
            row_number().over(window).cast("bigint")
        )

        
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(tgt_table)
        return

    dim_df = spark.table(tgt_table)
    max_key = dim_df.select(F.max(surrogate_key)).first()[0] or 0

    join_cond = [
        col(f"s.{c}") == F.col(f"t.{c}")
        for c in business_keys
    ]
    new_rows = (
            src_df.alias("s")
            .join(
                dim_df.alias("t"),
                join_cond,
                "left_anti"
            )
            .withColumn(
                surrogate_key,
                (row_number().over(window) + lit(max_key)).cast("bigint")
            )
        )

    delta_dim = DeltaTable.forName(spark, tgt_table)

    merge_condition = " AND ".join(
        [f"t.{c} = s.{c}" for c in business_keys]
    )

    update_set = {c: f"s.{c}" for c in src_df.columns if c != 'dw_created_at'}

    (
        delta_dim.alias("t")
        .merge(
            src_df.alias("s"),
            merge_condition
        )
        .whenMatchedUpdate(set=update_set)
        .execute()
    )

    new_rows.write.mode("append").saveAsTable(tgt_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def lookup_config(
    batch_id: str,
    job_group_name: str = None,
    src_catalog: str = None,
    active_flg: str = "Y",
    job_id: str = None,                 # can be "1665" or "1665,1666"
    job_name_contains: str = None,
    batch_group: str = None,            # keep as string for LIKE-style filtering
    order_by: str = "job_id"
):
    """
    Simple lookup from md.etl_jobs (no joins, no script generation).
    Returns a DataFrame and includes helper columns:
      - job_id_4: zero-padded 4-digit string
      - batch_id: passed in
      - job_instance_id: batch_id + job_id_4
    """

    if not batch_id or str(batch_id).strip() == "":
        raise ValueError("batch_id is required")

    df = spark.table("md.etl_jobs")

    # Base filters
    if active_flg is not None and str(active_flg).strip() != "":
        df = df.filter(F.col("active_flg") == F.lit(active_flg))

    if src_catalog:
        df = df.filter(F.col("src_catalog") == F.lit(src_catalog))

    if job_group_name:
        df = df.filter(F.col("job_group_name") == F.lit(job_group_name))

    # job_id list filter (supports "1665,1666")
    if job_id:
        job_id_list = [x.strip() for x in str(job_id).split(",") if x.strip() != ""]
        if job_id_list:
            # md.etl_jobs.job_id could be int or string -> compare as string
            df = df.filter(F.col("job_id").cast("string").isin(job_id_list))

    if job_name_contains:
        df = df.filter(F.col("job_name").contains(job_name_contains))

    if batch_group:
        # if your md.etl_jobs has a column 'batch_group' and you store patterns like "1,2"
        # This is a contains filter, similar to LIKE '%<batch_group>%'
        df = df.filter(F.col("batch_group").cast("string").contains(str(batch_group)))

    # Add helper columns for pipeline orchestration
    df = (
        df
        .withColumn("job_id_4", F.lpad(F.col("job_id").cast("string"), 4, "0"))
        .withColumn("batch_id", F.lit(str(batch_id)))
        .withColumn("job_instance_id", F.concat(F.lit(str(batch_id)), F.col("job_id_4")))
    )

    # Order
    if order_by and order_by in df.columns:
        df = df.orderBy(order_by)

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------
# Schema/type conversion helpers (Bronze -> Silver)
# -----------------------

from pyspark.sql.functions import col, when, to_date, to_timestamp, lit, explode, from_json, schema_of_json, expr, coalesce
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import re

def generate_conversion_code(df: DataFrame, include_nested: bool = True) -> str:
    """
    Generate withColumn code from DataFrame with nested array support
    Handles nested array columns by renaming them with underscores

    Args:
        df: Spark DataFrame (may have nested columns like 'inventory.cpu')
        include_nested: If True, also generates code for nested arrays

    Returns:
        String containing withColumn transformation code

    Example:
        df has columns: _id, date, inventory.cpu, inventory.qty
        Output:
        df = df.withColumn("date", when(...).otherwise(to_date(...)))
        df = df.withColumn("inventory_cpu", col("`inventory.cpu`").cast("decimal(18,4)"))
        df = df.withColumn("inventory_qty", col("`inventory.qty`").cast("integer"))
    """
    # Get sample data
    sample = df.limit(100).collect()
    data = [row.asDict() for row in sample]

    # Detect schema for all fields
    schema = {}
    nested_arrays = {}
    field_values = {}

    for record in data:
        for key, value in record.items():
            # Check if it's a nested array (not flattened)
            if isinstance(value, list) and value and isinstance(value[0], dict):
                if key not in nested_arrays:
                    nested_arrays[key] = []
                nested_arrays[key].extend(value)
                continue

            # Skip if value is a list (the array column itself)
            if isinstance(value, list):
                continue

            if key not in field_values:
                field_values[key] = []
            field_values[key].append(value)

    # Detect types for all fields
    for field, values in field_values.items():
        schema[field] = detect_type(values, field)

    # Generate code
    lines = []
    lines.append("# ===== Main table conversions =====")

    for field, conv_type in schema.items():
        # Check if field has a dot (nested column)
        if '.' in field:
            # Create new column name with underscore
            new_field_name = field.replace('.', '_')
            # Use backticks to escape the original column name
            original_col = f'"`{field}`"'
        else:
            new_field_name = field
            original_col = f'"{field}"'

        if conv_type == "int":
            lines.append(f'df = df.withColumn("{new_field_name}", col({original_col}).cast("integer"))')
        elif conv_type == "decimal":
            lines.append(f'df = df.withColumn("{new_field_name}", col({original_col}).cast("decimal(18,4)"))')
        elif conv_type == "date":
            lines.append(f'df = df.withColumn("{new_field_name}",\n    when(col({original_col}).isin("0000-00-00", ""), None)\n    .otherwise(to_date(col({original_col}), "yyyy-MM-dd")))')
        elif conv_type == "timestamp":
            lines.append(f'df = df.withColumn("{new_field_name}",\n    when(col({original_col}).isin("0000-00-00 00:00:00", ""), None)\n    .otherwise(to_timestamp(col({original_col}))))')
        elif conv_type == 'timestampspecial':
            def parse_ms_date(raw):
                if raw is None:
                    return None
                m = re.search(r'/Date\((\d+)', raw)
                if not m:
                    return None
                ms = int(m.group())
                return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
            parse_ms_date_udf = udf(parse_ms_date, TimestampType())
            lines.append(f'df = df.withColumn("{new_field_name}", parse_ms_date_udf({original_col}))')
        else:
            lines.append(f'df = df.withColumn("{new_field_name}", col({original_col}))')
    # Add code to drop original nested columns
    nested_cols = [f'"{field}"' for field in schema.keys() if '.' in field]
    if nested_cols:
        lines.append(f'\n# Drop original nested columns')
        lines.append(f'df = df.drop({", ".join(nested_cols)})')

    # Generate code for nested arrays
    if include_nested and nested_arrays:
        lines.append("\n# ===== Nested array conversions =====")
        lines.append("# Use these with explode() to create separate rows or transform in place")

        for array_field, array_values in nested_arrays.items():
            lines.append(f"\n# Nested array: {array_field}")

            # Analyze nested structure
            nested_schema = {}
            nested_field_values = {}

            for item in array_values[:100]:  # Sample first 100 items
                for key, value in item.items():
                    if key not in nested_field_values:
                        nested_field_values[key] = []
                    nested_field_values[key].append(value)

            for field, values in nested_field_values.items():
                nested_schema[field] = detect_type(values, field)

            # Generate transformation code for nested array
            lines.append(f'# To transform {array_field} array:')
            lines.append(f'# Option 1: Explode and create separate table')
            lines.append(f'# {array_field}_df = df.select("_id", explode("{array_field}").alias("{array_field}_item"))')

            # Generate field extractions
            for nested_field, conv_type in nested_schema.items():
                field_path = f'{array_field}_item.{nested_field}'

                if conv_type == "int":
                    lines.append(f'# {array_field}_df = {array_field}_df.withColumn("{nested_field}", col("{field_path}").cast("integer"))')
                elif conv_type == "decimal":
                    lines.append(f'# {array_field}_df = {array_field}_df.withColumn("{nested_field}", col("{field_path}").cast("decimal(18,4)"))')
                elif conv_type == "date":
                    lines.append(f'# {array_field}_df = {array_field}_df.withColumn("{nested_field}", when(col("{field_path}").isin("0000-00-00", ""), None).otherwise(to_date(col("{field_path}"), "yyyy-MM-dd")))')
                elif conv_type == "timestamp":
                    lines.append(f'# {array_field}_df = {array_field}_df.withColumn("{nested_field}", when(col("{field_path}").isin("0000-00-00 00:00:00", ""), None).otherwise(to_timestamp(col("{field_path}"))))')
                elif conv_type == 'timestampspecial':
                    def parse_ms_date(raw):
                        if raw is None:
                            return None
                        m = re.search(r'/Date\((\d+)', raw)
                        if not m:
                            return None
                        ms = int(m.group())
                        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
                    parse_ms_date_udf = udf(parse_ms_date, TimestampType())
                    lines.append(f'df = {array_field}_df.withColumn("{nested_field}", parse_ms_date_udf({field_path}))')
                else:
                    lines.append(f'# {array_field}_df = {array_field}_df.withColumn("{nested_field}", col("{field_path}"))')
    return "\n".join(lines)


def detect_type(values, field_name):
    """Detect field type from values"""
    # Filter nulls and zero dates
    vals = [v for v in values if v not in [None, "", "0000-00-00", "0000-00-00 00:00:00"]]
    if not vals:
        return "str"

    sample = vals[0]

    # ID fields stay as string
    if "_id" in field_name.lower() or field_name.lower().endswith("_id"):
        return "str"

    # Type detection
    if isinstance(sample, int):
        return "int"
    if isinstance(sample, float):
        if any(k in field_name.lower() for k in ["amount", "price", "total", "tax", "cpu", "cost"]):
            return "decimal"
        return "float"
    if isinstance(sample, str):
        # Date detection
        if re.match(r'^\d{4}-\d{2}-\d{2}$', sample):
            return "date"
        # DateTime detection
        if (
            re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', sample) or
            re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$', sample)
            ):
            return "timestamp"
        if re.match(r"/Date\((\d+)", sample):
            return "timestampspecial"
        # Integer strings (but not IDs)
        if all(re.match(r'^-?\d+$', str(v)) for v in vals[:10]):
            if "_id" not in field_name.lower() and not field_name.lower().endswith("code"):
                return "int"

    return "str"


def add_audit_columns(df):
    """Add standard audit columns"""

    return df \
        .withColumn("_silver_processed_at", F.current_timestamp()) \
        .withColumn("_silver_source_file", F.input_file_name()) \
        .withColumn("_silver_row_hash", F.sha2(F.concat_ws("|", *df.columns), 256))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#=== Plato Silver helper functions ===

PROCESSING_TIME = current_timestamp()

def standardize_column_names(df):
    """Convert column names to snake_case"""
    for col_name in df.columns:
        new_name = col_name.lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(col_name, new_name)
    return df

def add_audit_columns(df):
    """Add standard audit columns"""
    return df \
        .withColumn("_silver_processed_at", PROCESSING_TIME) \
        .withColumn("_silver_source_file", input_file_name()) \
        .withColumn("_silver_row_hash", sha2(concat_ws("|", *df.columns), 256))

def clean_string_column(col_name):
    """Clean string column - trim whitespace, handle empty strings"""
    return when(trim(col(col_name)) == "", None).otherwise(trim(col(col_name)))

def safe_col(col_name, default=None):
    """Safely access a column, returning default if null"""
    if default is not None:
        return coalesce(col(col_name), lit(default))
    return col(col_name)

def parse_date(col_name, formats=["yyyy-MM-dd", "dd/MM/yyyy", "MM/dd/yyyy", "yyyy-MM-dd'T'HH:mm:ss"]):
    """Parse date with multiple format support"""
    result = lit(None).cast(DateType())
    for fmt in formats:
        result = coalesce(result, to_date(col(col_name), fmt))
    return result

def parse_timestamp(col_name):
    """Parse timestamp with multiple format support"""
    return coalesce(
        to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col(col_name))
    )

def deduplicate(df, key_columns, order_column="created_at", ascending=False):
    """Remove duplicates keeping the latest record"""
    window = Window.partitionBy(key_columns).orderBy(
        col(order_column).desc() if not ascending else col(order_column).asc()
    )
    return df.withColumn("_rn", row_number().over(window)) \
             .filter(col("_rn") == 1) \
             .drop("_rn")

def log_data_quality(table_name, df, key_column=None):
    """Log data quality metrics"""
    total_rows = df.count()
    null_counts = {}
    
    for col_name in df.columns:
        if not col_name.startswith("_"):
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                null_counts[col_name] = null_count
    
    print(f"\n{'='*60}")
    print(f"Data Quality Report: {table_name}")
    print(f"{'='*60}")
    print(f"Total Rows: {total_rows:,}")
    
    if key_column:
        unique_keys = df.select(key_column).distinct().count()
        print(f"Unique {key_column}: {unique_keys:,}")
        if unique_keys != total_rows:
            print(f"⚠️ Duplicate keys detected: {total_rows - unique_keys:,}")
    
    if null_counts:
        print(f"\nNull Value Summary:")
        for col_name, count in sorted(null_counts.items(), key=lambda x: -x[1])[:10]:
            pct = (count / total_rows) * 100 if total_rows > 0 else 0
            print(f"  {col_name}: {count:,} ({pct:.1f}%)")
    else:
        print("✅ No null values in key columns")
    
    print(f"{'='*60}")
    return total_rows

def optional_col(df, col_name, alias_name=None, cast_type=None):
    """
    Return the column if it exists, otherwise NULL.
    Supports flattened names like 'corporate._id'.
    """
    alias_name = alias_name or col_name

    if col_name in df.columns:
        c = col(f"`{col_name}`")
    else:
        c = lit(None)

    if cast_type is not None:
        c = c.cast(cast_type)

    return c.alias(alias_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim/fact reusable def
# =========================
# Gold shared helpers
# =========================
from pyspark.sql import functions as F

def norm_key(s: str) -> str:
    """Normalize job/table keys for dispatching."""
    return (str(s or "")
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_"))

def generate_surrogate_key(*cols, digits: int = 12):
    """
    Deterministic surrogate key from natural key columns.
    Prefer xxhash64 over hash() for stability/perf.
    """
    # returns BIGINT; pmod keeps it non-negative and bounded
    return F.pmod(F.xxhash64(*cols), F.lit(10 ** digits)).cast("bigint")

def calculate_age(dob_col, reference_date=None):
    """Age in years from DOB to reference_date (defaults to current_date)."""
    ref = reference_date if reference_date is not None else F.current_date()
    return F.floor(F.months_between(ref, dob_col) / F.lit(12)).cast("int")

def get_age_group(age_col):
    """Age bucket."""
    return (F.when(age_col < 13, "Child")
             .when((age_col >= 13) & (age_col < 18), "Teenager")
             .when((age_col >= 18) & (age_col < 65), "Adult")
             .when(age_col >= 65, "Senior")
             .otherwise("Unknown"))

def get_health_status(score_col):
    """Example mapping. Keep only if you actually use it in Gold outputs."""
    return (F.when(score_col >= 75, "Green")
             .when((score_col >= 26) & (score_col < 75), "Yellow")
             .when(score_col < 26, "Red")
             .otherwise("Unknown"))

def get_health_status_color(status_col):
    """Hex color mapping for UI use cases."""
    return (F.when(status_col == "Green", "#00FF00")
             .when(status_col == "Yellow", "#FFFF00")
             .when(status_col == "Red", "#FF0000")
             .otherwise("#CCCCCC"))

def read_dim_current(table_fqn: str):
    """
    Standardize reading 'current' rows if table has is_current.
    If no is_current column, returns the table as-is.
    """
    df = spark.table(table_fqn)
    if "is_current" in df.columns:
        return df.filter(F.col("is_current") == True)
    return df

def add_date_key(df, dim_date_df, date_col, key_alias: str, dim_date_col: str = "full_date"):
    """
    Left join to DIM_DATE to retrieve date_key.
    Assumes dim_date has: full_date (date) and date_key.
    """
    d = dim_date_df.select(
        F.col(dim_date_col).cast("date").alias("_join_date"),
        F.col("date_key").alias(key_alias)
    )
    return (df
            .withColumn("_join_date", F.col(date_col).cast("date"))
            .join(d, on="_join_date", how="left")
            .drop("_join_date"))

def get_date_key(date_col):
    """Convert date column to date_key format (YYYYMMDD)"""
    return date_format(date_col, "yyyyMMdd").cast(IntegerType())

def lookup_dimension_key(df, dim_table, natural_key, surrogate_key, df_key):
    """Join fact with dimension to get surrogate key"""
    dim_df = spark.table(dim_table).select(col(natural_key), col(surrogate_key))
    return df.join(dim_df, df[df_key] == dim_df[natural_key], "left") \
             .drop(natural_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# TARGET DATA LOADING - Load targets from brz_target_revenue table
# ============================================================================

# Target table location
TARGET_TABLE = "`WS-ETL-BNJ`.lh_bnj_bronze.xero.brz_target_revenue"

def get_targets_for_period(fiscal_year: int = None, quarter: int = None):
    """
    Load targets from brz_target_revenue table for a specific fiscal year and quarter.
    
    Args:
        fiscal_year: The fiscal year (e.g., 2026). If None, uses current year.
        quarter: The quarter (1-4). If None, uses current quarter.
    
    Returns:
        Dictionary with target values, or default values if not found.
    """
    from datetime import datetime
    
    # Default to current period if not specified
    if fiscal_year is None:
        fiscal_year = datetime.now().year
    if quarter is None:
        quarter = (datetime.now().month - 1) // 3 + 1
    
    # Default target values (fallback)
    default_targets = {
        "revenue_target": 5000000.0,
        "cost_of_goods_sold": 2800000.0,
        "operating_expenses": 1200000.0,
        "profit": 1000000.0,
        "accounts_receivable": 800000.0,
        "accounts_payable": 600000.0,
        "inventory_value": 900000.0,
        "interest_expense": 120000.0,
        "outstanding_principal": 300000.0,
        "min_inventory_turnover_rate": 4.0,
        "max_inventory_turnover_rate": 10.0,
        "max_expired_stock_percentage": 3.0,
        "min_patient_satisfaction_score": 90.0,
        "target_year_end_score": 80.0
    }
    
    try:
        # Load targets from table
        df_targets = spark.table("`WS-ETL-BNJ`.lh_bnj_bronze.postgresql.brz_target") \
            .filter(
                (col("fiscal_year") == fiscal_year) & 
                (col("quarter") == quarter) &
                (col("deleted_at").isNull())
            ) \
            .orderBy(col("updated_at").desc()) \
            .limit(1)
        
        if df_targets.count() > 0:
            row = df_targets.first()
            targets = {
                "revenue_target": float(row["revenue_target"]) if row["revenue_target"] else default_targets["revenue_target"],
                "cost_of_goods_sold": float(row["cost_of_goods_sold"]) if row["cost_of_goods_sold"] else default_targets["cost_of_goods_sold"],
                "operating_expenses": float(row["operating_expenses"]) if row["operating_expenses"] else default_targets["operating_expenses"],
                "profit": float(row["profit"]) if row["profit"] else default_targets["profit"],
                "accounts_receivable": float(row["accounts_receivable"]) if row["accounts_receivable"] else default_targets["accounts_receivable"],
                "accounts_payable": float(row["accounts_payable"]) if row["accounts_payable"] else default_targets["accounts_payable"],
                "inventory_value": float(row["inventory_value"]) if row["inventory_value"] else default_targets["inventory_value"],
                "interest_expense": float(row["interest_expense"]) if row["interest_expense"] else default_targets["interest_expense"],
                "outstanding_principal": float(row["outstanding_principal"]) if row["outstanding_principal"] else default_targets["outstanding_principal"],
                "min_inventory_turnover_rate": float(row["min_inventory_turnover_rate"]) if row["min_inventory_turnover_rate"] else default_targets["min_inventory_turnover_rate"],
                "max_inventory_turnover_rate": float(row["max_inventory_turnover_rate"]) if row["max_inventory_turnover_rate"] else default_targets["max_inventory_turnover_rate"],
                "max_expired_stock_percentage": float(row["max_expired_stock_percentage"]) if row["max_expired_stock_percentage"] else default_targets["max_expired_stock_percentage"],
                "min_patient_satisfaction_score": float(row["min_patient_satisfaction_score"]) if row["min_patient_satisfaction_score"] else default_targets["min_patient_satisfaction_score"],
                "target_year_end_score": default_targets["target_year_end_score"]
            }
            print(f"✅ Loaded targets for FY{fiscal_year} Q{quarter}")
            return targets
        else:
            print(f"⚠️ No targets found for FY{fiscal_year} Q{quarter}, using defaults")
            return default_targets
            
    except Exception as e:
        print(f"⚠️ Error loading targets: {e}, using defaults")
        return default_targets

def get_targets_broadcast(fiscal_year: int = None, quarter: int = None):
    """
    Get targets as a broadcast variable for use in DataFrame operations.
    Returns a DataFrame with a single row containing all targets.
    """
    targets = get_targets_for_period(fiscal_year, quarter)
    
    # Create a single-row DataFrame with targets
    targets_df = spark.createDataFrame([targets])
    return targets_df

# ============================================================================
# HEALTH SCORE WEIGHTS (from Formular Calculations document)
# ============================================================================

# P&L Health Weights (Document Section 1)
PL_WEIGHTS = {
    "revenue": 0.5,
    "cogs": 0.1,
    "gross_profit": 0.1,
    "opex": 0.2,
    "operating_profit": 0.1
}

# AR/AP Health Weights (Document Section 2)
AR_AGING_WEIGHTS = {'0-30': 1.0, '31-60': 0.7, '61-90': 0.4, '91-180': 0.2, '>181': 0.0}
AP_AGING_WEIGHTS = {'0-30': 0.3, '31-60': 0.6, '61-90': 0.9, '91-180': 1.0, '>181': 1.0}
AR_AP_COMBINED_WEIGHTS = {"ar": 0.7, "ap": 0.3}
AR_AP_AMOUNT_COUNT_WEIGHTS = {"amount": 0.7, "count": 0.3}

# Cashflow Health Weights (Document Section 3)
CASHFLOW_WEIGHTS = {
    "ar_score": 0.45,
    "ap_score": 0.25,
    "inventory_score": 0.15,
    "interest_score": 0.10,
    "principal_score": 0.05
}

# Inventory Health Weights (Document Section 4)
INVENTORY_WEIGHTS = {
    "itr_score": 0.40,
    "expired_score": 0.20,
    "ccc_score": 0.40
}
INVENTORY_WEIGHTS_NO_CCC = {
    "itr_score": 0.80,
    "expired_score": 0.20
}

# Claims Health Weights (Document Section 5)
CLAIMS_WEIGHTS = {
    "approved": 0.4,
    "pending": 0.2,
    "rejected": 0.4
}

# Patient Health Weights (Document Section 6)
PATIENT_WEIGHTS = {
    "panel_score": 0.35,
    "growth_score": 0.25,
    "happy_score": 0.20,
    "unhappy_score": 0.20
}

# Financial Health Weights (Document Section 8)
FINANCIAL_HEALTH_WEIGHTS = {
    "pl_health": 0.5,
    "ar_ap_health": 0.25,
    "cashflow_health": 0.25
}

# Operational Health Weights (Document Section 9)
OPERATIONAL_HEALTH_WEIGHTS = {
    "claims_health": 0.4,
    "patient_health": 0.2,
    "inventory_health": 0.4
}

# General Health - equal 10% weights for each of 10 components (Document Section 11)
GENERAL_HEALTH_COMPONENT_WEIGHT = 0.10

# ============================================================================
# Health score calculation functions
# ============================================================================

def calculate_ar_aging_score(df, ar_weights={'0-30': 1.0, '31-60': 0.7, '61-90': 0.4, '91-180': 0.2, '>181': 0.0}):
    """
    Calculate AR aging score based on aging buckets
    Formula: Weighted average of amounts in each bucket
    Age groups: 0-30, 31-60, 61-90, 91-180, >181
    """
    # Calculate weighted amount
    ar_amt_weighted = (
        col('ar_0_30_days_amount') * ar_weights['0-30'] +
        col('ar_31_60_days_amount') * ar_weights['31-60'] +
        col('ar_61_90_days_amount') * ar_weights['61-90'] +
        col('ar_91_180_days_amount') * ar_weights['91-180'] +
        col('ar_over_180_days_amount') * ar_weights['>181']
    )
    
    # Amount score with division by zero protection
    ar_amt_score = when(col('total_ar_amount') != 0, 
                        (ar_amt_weighted / col('total_ar_amount')) * 100) \
                   .otherwise(0)
    
    # Calculate total count
    ar_total_count = (
        col('ar_0_30_days_count') + 
        col('ar_31_60_days_count') + 
        col('ar_61_90_days_count') + 
        col('ar_91_180_days_count') +
        col('ar_over_180_days_count')
    )
    
    # Calculate weighted count
    ar_cnt_weighted = (
        col('ar_0_30_days_count') * ar_weights['0-30'] +
        col('ar_31_60_days_count') * ar_weights['31-60'] +
        col('ar_61_90_days_count') * ar_weights['61-90'] +
        col('ar_91_180_days_count') * ar_weights['91-180'] +
        col('ar_over_180_days_count') * ar_weights['>181']
    )
    
    # Count score with division by zero protection
    ar_cnt_score = when(ar_total_count != 0,
                        (ar_cnt_weighted / ar_total_count) * 100) \
                   .otherwise(0)
    
    # Combined: 70% amount, 30% count
    return (ar_amt_score * 0.7 + ar_cnt_score * 0.3)

def calculate_ap_aging_score(df, ap_weights={'0-30': 0.3, '31-60': 0.6, '61-90': 0.9, '91-180': 1.0, '>181': 1.0}):
    """
    Calculate AP aging score based on aging buckets
    Note: Longer AP aging is BETTER for cashflow (per Formular Calculations doc)
    Weights: 0-30=0.3, 31-60=0.6, 61-90=0.9, 91-180=1.0, >181=1.0
    Age groups: 0-30, 31-60, 61-90, 91-180, >181
    """
    # Calculate weighted amount
    ap_amt_weighted = (
        col('ap_0_30_days_amount') * ap_weights['0-30'] +
        col('ap_31_60_days_amount') * ap_weights['31-60'] +
        col('ap_61_90_days_amount') * ap_weights['61-90'] +
        col('ap_91_180_days_amount') * ap_weights['91-180'] +
        col('ap_over_180_days_amount') * ap_weights['>181']
    )
    
    # Amount score with division by zero protection
    ap_amt_score = when(col('total_ap_amount') != 0,
                        (ap_amt_weighted / col('total_ap_amount')) * 100) \
                   .otherwise(0)
    
    # Calculate total count
    ap_total_count = (
        col('ap_0_30_days_count') + 
        col('ap_31_60_days_count') + 
        col('ap_61_90_days_count') + 
        col('ap_91_180_days_count') +
        col('ap_over_180_days_count')
    )
    
    # Calculate weighted count
    ap_cnt_weighted = (
        col('ap_0_30_days_count') * ap_weights['0-30'] +
        col('ap_31_60_days_count') * ap_weights['31-60'] +
        col('ap_61_90_days_count') * ap_weights['61-90'] +
        col('ap_91_180_days_count') * ap_weights['91-180'] +
        col('ap_over_180_days_count') * ap_weights['>181']
    )
    
    # Count score with division by zero protection
    ap_cnt_score = when(ap_total_count != 0,
                        (ap_cnt_weighted / ap_total_count) * 100) \
                   .otherwise(0)
    
    # Combined: 70% amount, 30% count
    return (ap_amt_score * 0.7 + ap_cnt_score * 0.3)

def calculate_pl_component_score(actual, target):
    """
    Calculate P&L component score
    If actual <= target: proportional score
    If actual > target: 100 + bonus (capped at +50)
    """
    return when(actual <= target, 
                when(target != 0, (actual / target) * 100).otherwise(0)) \
           .otherwise(
                when(target != 0,
                     least(100 + ((actual - target) / target * 50), 150)) \
                .otherwise(100)
           )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
