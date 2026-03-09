# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260204124700
job_id = '0002'
src_catalog = "gold_dim_fact"
job_group_name = "gold_fact"
src_table = ""
tgt_table = "fact_ap_aging"


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

#== DEFINE RELATE TABLES==
SILVER_SCHEMA_PLATO = "`WS-ETL-BNJ`.lh_bnj_silver.plato"
SILVER_SCHEMA_XERO = "`WS-ETL-BNJ`.lh_bnj_silver.xero"
GOLD_SCHEMA = "`WS-ETL-BNJ`.lh_bnj_gold.gold"

SILVER_TABLES = {
    "xero_invoices": f"{SILVER_SCHEMA_XERO}.silver_invoices",
    "xero_payments": f"{SILVER_SCHEMA_XERO}.silver_payments"
}

GOLD_DIMENSIONS = {
    "supplier": f"{GOLD_SCHEMA}.dim_supplier"
}

GOLD_FACTS = {
    "ap_aging": f"{GOLD_SCHEMA}.fact_ap_aging"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_fact_ap_aging():
    """
    Create fact_ap_aging for AP Health score.
    Uses XERO bills/invoices for supplier payments.
    Grain: One row per outstanding bill.
    
    silver_xero_invoices columns (same structure as AR):
    - invoice_id, invoice_number, contact_id, contact_name
    - type (ACCPAY for payables), invoice_type_desc
    - invoice_date, due_date
    - subtotal, total_tax, total
    - amount_due, amount_paid, amount_credited
    - status, reference
    """
    
    # Read XERO invoice data (supplier bills)
    xero_invoices = spark.table(SILVER_TABLES["xero_invoices"])
    xero_payments = spark.table(SILVER_TABLES["xero_payments"])
    
    # Read dimensions
    dim_supplier = spark.table(GOLD_DIMENSIONS["supplier"]).select("supplier_key", "supplier_id")
    
    # Filter to bills (payables)
    bills = xero_invoices.filter(col("type") == "ACCPAY")
    
    # Calculate paid amounts
    payments_by_bill = xero_payments.groupBy("invoice_id") \
        .agg(sum("amount").alias("paid_amount"))
    
    # Calculate outstanding
    ap_aging = bills \
        .join(payments_by_bill, "invoice_id", "left") \
        .withColumn("paid_amount", coalesce(col("paid_amount"), lit(0))) \
        .withColumn("outstanding_amount", col("total") - col("paid_amount")) \
        .filter(col("outstanding_amount") > 0)
    
    # Add aging calculations - use invoice_date (not date)
    ap_aging = ap_aging.select(
        monotonically_increasing_id().alias("ap_aging_key"),
        get_date_key(current_date()).alias("date_key"),
        col("contact_id").alias("supplier_id"),
        col("invoice_id").alias("bill_id"),
        col("invoice_date").alias("bill_date"),
        col("due_date"),
        datediff(current_date(), col("invoice_date")).alias("days_outstanding"),
        when(datediff(current_date(), col("due_date")) <= 0, "Current")
        .when(datediff(current_date(), col("due_date")) <= 30, "1-30")
        .when(datediff(current_date(), col("due_date")) <= 60, "31-60")
        .when(datediff(current_date(), col("due_date")) <= 90, "61-90")
        .otherwise("90+").alias("aging_bucket"),
        col("total").alias("original_amount").cast(DecimalType(18, 2)),
        col("paid_amount").cast(DecimalType(18, 2)),
        col("outstanding_amount").cast(DecimalType(18, 2)),
        (current_date() > col("due_date")).alias("is_overdue")
    )
    
    # Join with dimensions
    ap_aging = ap_aging.join(dim_supplier, "supplier_id", "left")
    
    # Select final columns
    ap_aging = ap_aging.select(
        "ap_aging_key",
        "date_key",
        coalesce(col("supplier_key"), lit(-1)).alias("supplier_key"),
        "bill_id",
        "bill_date",
        "due_date",
        "days_outstanding",
        "aging_bucket",
        "original_amount",
        "paid_amount",
        "outstanding_amount",
        "is_overdue"
    )
    
    return ap_aging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Create and save fact_cashflow
try:

    tgt_table = GOLD_FACTS['ap_aging']
    fact_ap_aging_df = create_fact_ap_aging()
    fact_ap_aging_df.write.format("delta").mode("overwrite").saveAsTable(tgt_table)

    src_cnt = fact_ap_aging_df.count()
    tgt_cnt = spark.table(tgt_table).count()

    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created agg_ap_aging", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print(f"✅ Created {tgt_table} with {src_cnt} rows")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed agg_ap_aging. {safe_exception_text(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
