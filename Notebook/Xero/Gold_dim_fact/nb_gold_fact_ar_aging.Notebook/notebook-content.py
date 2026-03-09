# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_fact_ar_aging():
    """
    Create fact_ar_aging for AR Health score.
    Shows outstanding receivables by aging bucket.
    Grain: One row per outstanding invoice.
    
    SOURCE: XERO (accounting book of record for AR)
    - XERO invoices (type=ACCREC) are the official AR
    - Falls back to PLATO if XERO data unavailable
    """
    
    # Try XERO first (accounting book of record)
    try:
        xero_invoices = spark.table(SILVER_TABLES["xero_invoices"])
        xero_payments = spark.table(SILVER_TABLES["xero_payments"])
        use_xero = True
        print("Using XERO as AR source (book of record)")
    except:
        use_xero = False
        print("XERO unavailable, falling back to PLATO for AR")
    
    if use_xero:
        return create_fact_ar_aging_from_xero()
    
    # Fallback: Read PLATO invoice and payment data
    silver_invoice = spark.table(SILVER_TABLES["invoice"])
    silver_payment = spark.table(SILVER_TABLES["payment"])
    
    # Read dimensions
    dim_patient = spark.table(GOLD_DIMENSIONS["patient"]).select("patient_key", "patient_id")
    dim_payer = spark.table(GOLD_DIMENSIONS["payer"]).select("payer_key", "payer_id")
    
    # Calculate total paid per invoice
    payments_by_invoice = silver_payment.groupBy("invoice_id") \
        .agg(sum("payment_amount").alias("paid_amount"))
    
    # Calculate invoice totals
    invoice_totals = silver_invoice.groupBy(
        "invoice_id", 
        "patient_id", 
        "corporate_id",
        "invoice_date",
        "due_date"
    ).agg(
        sum("net_amount").alias("original_amount")
    )
    
    # Join and calculate outstanding
    ar_aging = invoice_totals \
        .join(payments_by_invoice, "invoice_id", "left") \
        .withColumn("paid_amount", coalesce(col("paid_amount"), lit(0))) \
        .withColumn("outstanding_amount", col("original_amount") - col("paid_amount")) \
        .filter(col("outstanding_amount") > 0)  # Only outstanding invoices
    
    # Add aging calculations
    ar_aging = ar_aging.select(
        monotonically_increasing_id().alias("ar_aging_key"),
        get_date_key(current_date()).alias("date_key"),
        col("patient_id"),
        col("corporate_id").alias("payer_id"),
        col("invoice_id"),
        col("invoice_date"),
        col("due_date"),
        datediff(current_date(), col("invoice_date")).alias("days_outstanding"),
        when(datediff(current_date(), col("due_date")) <= 0, "Current")
        .when(datediff(current_date(), col("due_date")) <= 30, "1-30")
        .when(datediff(current_date(), col("due_date")) <= 60, "31-60")
        .when(datediff(current_date(), col("due_date")) <= 90, "61-90")
        .otherwise("90+").alias("aging_bucket"),
        col("original_amount").cast(DecimalType(18, 2)),
        col("paid_amount").cast(DecimalType(18, 2)),
        col("outstanding_amount").cast(DecimalType(18, 2)),
        (current_date() > col("due_date")).alias("is_overdue")
    )
    
    # Join with dimensions
    ar_aging = ar_aging \
        .join(dim_patient, "patient_id", "left") \
        .join(dim_payer, "payer_id", "left")
    
    # Select final columns
    ar_aging = ar_aging.select(
        "ar_aging_key",
        "date_key",
        coalesce("patient_key", lit(-1)).alias("patient_key"),
        coalesce("payer_key", lit(-1)).alias("payer_key"),
        "invoice_id",
        "invoice_date",
        "due_date",
        "days_outstanding",
        "aging_bucket",
        "original_amount",
        "paid_amount",
        "outstanding_amount",
        "is_overdue"
    )
    
    return ar_aging

# Create and save fact_ar_aging
try:
    fact_ar_aging_df = create_fact_ar_aging()
    fact_ar_aging_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACTS["ar_aging"])
    print(f"✅ Created {GOLD_FACTS['ar_aging']} with {fact_ar_aging_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create fact_ar_aging: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
