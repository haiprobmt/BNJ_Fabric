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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, when, to_date, to_timestamp, lit, explode, from_json, schema_of_json, expr
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F
import traceback

ETL_LOG_TABLE = "md.etl_log"   # change to your full table name if needed
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
    # You said job_id is 4 digits, but not the SQL type. Most people store it as INT.
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

# -----------------------
# Public wrappers
# -----------------------
def log_run_start(batch_id: str, msg: str = "Pipeline started"):
    etl_log_upsert(batch_id, "0000", "RUNNING", msg, set_start=True, set_end=False)

def log_run_end(batch_id: str, status: str, msg: str = None):
    etl_log_upsert(batch_id, "0000", status, msg, set_start=False, set_end=True)

def log_step_start(batch_id: str, job_id: str, msg: str = None):
    etl_log_upsert(batch_id, job_id, "RUNNING", msg, set_start=True, set_end=False)

def log_step_end(batch_id: str, job_id: str, status: str, msg: str = None, src_row_num=None, tgt_row_num=None):
    etl_log_upsert(batch_id, job_id, status, msg, src_row_num=src_row_num, tgt_row_num=tgt_row_num, set_start=False, set_end=True)

# -----------------------
# Optional: pre-create step rows as RUNNING for a whole batch
# -----------------------
def log_steps_running_for_batch(batch_id: str, job_ids: list[str], msg_prefix: str = None):
    """
    Creates/updates RUNNING rows for many job_ids (one MERGE per job_id).
    Useful if you want to mark all expected steps as RUNNING at pipeline start.
    """
    for jid in job_ids:
        m = f"{msg_prefix}{jid}" if msg_prefix else None
        log_step_start(batch_id, jid, m)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
