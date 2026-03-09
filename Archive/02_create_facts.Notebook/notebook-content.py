# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a",
# META       "default_lakehouse_name": "lh_bnj_silver",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
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

# # BNJ Medical CFO Dashboard - Gold Layer Fact Tables
# 
# This notebook creates fact tables for the CFO Dashboard data warehouse.
# 
# **Facts Created:**
# - fact_invoice
# - fact_payment
# - fact_claims
# - fact_appointment
# - fact_inventory
# - fact_inventory_snapshot
# - fact_ar_aging
# - fact_ap_aging
# - fact_cashflow
# - fact_pnl

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration

# CELL ********************

SILVER_SCHEMA_PLATO = "lh_bnj_silver.plato_test"
SILVER_SCHEMA_XERO = "lh_bnj_silver.xero_test"
GOLD_SCHEMA = "lh_bnj_gold.gold_test"

# Table names
SILVER_TABLES = {
    "patient": f"{SILVER_SCHEMA_PLATO}.silver_patient",
    "invoice": f"{SILVER_SCHEMA_PLATO}.silver_invoice",
    "payment": f"{SILVER_SCHEMA_PLATO}.silver_payment",
    "inventory": f"{SILVER_SCHEMA_PLATO}.silver_inventory",
    "supplier": f"{SILVER_SCHEMA_PLATO}.silver_supplier",
    "corporate": f"{SILVER_SCHEMA_PLATO}.silver_corporate",
    "appointment": f"{SILVER_SCHEMA_PLATO}.silver_appointment",
    "adjustment": f"{SILVER_SCHEMA_PLATO}.silver_adjustment",
    "deliveryorder": f"{SILVER_SCHEMA_PLATO}.silver_deliveryorder",
    "xero_invoices": f"{SILVER_SCHEMA_XERO}.silver_xero_invoices",
    "xero_payments": f"{SILVER_SCHEMA_XERO}.silver_xero_payments",
    "xero_bank_transactions": f"{SILVER_SCHEMA_XERO}.silver_xero_bank_transactions",
    "xero_accounts": f"{SILVER_SCHEMA_XERO}.silver_xero_accounts",
    "xero_contacts": f"{SILVER_SCHEMA_XERO}.silver_xero_contacts"
}

GOLD_DIMENSIONS = {
    "date": f"{GOLD_SCHEMA}.dim_date",
    "patient": f"{GOLD_SCHEMA}.dim_patient",
    "location": f"{GOLD_SCHEMA}.dim_location",
    "product": f"{GOLD_SCHEMA}.dim_product",
    "supplier": f"{GOLD_SCHEMA}.dim_supplier",
    "payer": f"{GOLD_SCHEMA}.dim_payer",
    "account": f"{GOLD_SCHEMA}.dim_account"
}

GOLD_FACTS = {
    "invoice": f"{GOLD_SCHEMA}.fact_invoice",
    "payment": f"{GOLD_SCHEMA}.fact_payment",
    "claims": f"{GOLD_SCHEMA}.fact_claims",
    "appointment": f"{GOLD_SCHEMA}.fact_appointment",
    "inventory": f"{GOLD_SCHEMA}.fact_inventory",
    "inventory_snapshot": f"{GOLD_SCHEMA}.fact_inventory_snapshot",
    "ar_aging": f"{GOLD_SCHEMA}.fact_ar_aging",
    "ap_aging": f"{GOLD_SCHEMA}.fact_ap_aging",
    "cashflow": f"{GOLD_SCHEMA}.fact_cashflow",
    "pnl": f"{GOLD_SCHEMA}.fact_pnl"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Source System Selection Strategy
# 
# **PLATO (Medical Practice Management)** - Use for:
# - `fact_invoice`: Detailed revenue by product, doctor, patient, claims
# - `fact_payment`: Patient/payer collections with clinical context
# - `fact_claims`: Insurance claim submission and approval tracking
# - `fact_appointment`: Patient visits and scheduling
# - `fact_inventory`: Stock movements and expiry tracking
# 
# **XERO (Accounting System)** - Use for:
# - `fact_ar_aging`: Official accounts receivable (book of record)
# - `fact_ap_aging`: Accounts payable to suppliers
# - `fact_cashflow`: Bank transactions and cash position
# - `fact_pnl`: Official P&L reporting
# 
# **Reconciliation**: Monthly check that PLATO revenue ≈ XERO revenue

# MARKDOWN ********************

# ## Helper Functions

# CELL ********************

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

# MARKDOWN ********************

# ## 1. fact_invoice - Invoice Transactions

# CELL ********************

def create_fact_invoice():
    """
    Create fact_invoice from PLATO invoice data.
    Grain: One row per invoice (header level - not line items).
    
    Silver invoice columns available:
    - invoice_id, invoice_number, patient_id, doctor_id, location_id, corporate_id
    - invoice_date, subtotal, tax_amount, total_amount, adjustment_amount
    - gst_applicable, tax_rate, status, status_changed_at
    - is_finalized, finalized_at, finalized_by
    - is_void, void_reason, voided_at, voided_by
    - is_credit_debit_note, cndn_applied_to_invoice
    - scheme, session_number, is_highlighted
    - notes, corporate_notes, invoice_notes
    - manual_time_in, manual_time_out
    - created_at, created_by, updated_at, updated_by
    """
    
    # Read silver invoice data
    silver_invoice = spark.table(SILVER_TABLES["invoice"])
    
    # Read dimension tables for key lookups
    dim_patient = spark.table(GOLD_DIMENSIONS["patient"]).select("patient_key", "patient_id")
    dim_location = spark.table(GOLD_DIMENSIONS["location"]).select("location_key", "location_id")
    dim_payer = spark.table(GOLD_DIMENSIONS["payer"]).select("payer_key", "payer_id")
    
    # Transform invoice data (header level)
    fact_invoice = silver_invoice.select(
        monotonically_increasing_id().alias("invoice_fact_key"),
        col("invoice_id"),
        col("invoice_number"),
        get_date_key(col("invoice_date")).alias("date_key"),
        col("patient_id"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        col("corporate_id").alias("payer_id"),
        col("doctor_id"),
        
        # Invoice type classification
        when(col("is_credit_debit_note") == True, "Credit/Debit Note")
        .when(col("is_void") == True, "Voided")
        .otherwise("Standard").alias("invoice_type"),
        
        # Amounts (header level)
        col("subtotal").cast(DecimalType(18, 2)).alias("subtotal"),
        col("tax_amount").cast(DecimalType(18, 2)).alias("tax_amount"),
        col("total_amount").cast(DecimalType(18, 2)).alias("total_amount"),
        col("adjustment_amount").cast(DecimalType(18, 2)).alias("adjustment_amount"),
        
        # Calculate net amount (total - adjustments if any)
        (col("total_amount") - coalesce(col("adjustment_amount"), lit(0))).cast(DecimalType(18, 2)).alias("net_amount"),
        
        # GST info
        col("gst_applicable"),
        col("tax_rate"),
        
        # Status flags
        col("status"),
        col("is_finalized"),
        col("is_void"),
        col("is_credit_debit_note"),
        
        # Scheme (CHAS, MediSave, etc.)
        col("scheme"),
        
        # Has corporate/insurance payer
        when(col("corporate_id").isNotNull(), True).otherwise(False).alias("has_payer"),
        
        # Timestamps
        col("created_at").cast(TimestampType()),
        col("finalized_at").cast(TimestampType())
    )
    
    # Join with dimension tables to get surrogate keys
    fact_invoice = fact_invoice \
        .join(dim_patient, "patient_id", "left") \
        .join(dim_location, "location_id", "left") \
        .join(dim_payer, "payer_id", "left")
    
    # Select final columns
    fact_invoice = fact_invoice.select(
        "invoice_fact_key",
        "invoice_id",
        "invoice_number",
        "date_key",
        coalesce(col("patient_key"), lit(-1)).alias("patient_key"),
        coalesce(col("location_key"), lit(1)).alias("location_key"),
        coalesce(col("payer_key"), lit(-1)).alias("payer_key"),
        "doctor_id",
        "invoice_type",
        "subtotal",
        "tax_amount",
        "total_amount",
        "adjustment_amount",
        "net_amount",
        "gst_applicable",
        "tax_rate",
        "status",
        "is_finalized",
        "is_void",
        "is_credit_debit_note",
        "scheme",
        "has_payer",
        "created_at",
        "finalized_at"
    )
    
    return fact_invoice

# Create and save fact_invoice
try:
    fact_invoice_df = create_fact_invoice()
    fact_invoice_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACTS["invoice"])
    print(f"✅ Created {GOLD_FACTS['invoice']} with {fact_invoice_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create fact_invoice: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. fact_payment - Payment Transactions

# CELL ********************

def create_fact_payment():
    """
    Create fact_payment from PLATO payment data.
    Grain: One row per payment transaction.
    
    Silver payment columns available (based on actual PLATO schema):
    - payment_id, invoice_id, payment_amount, payment_method
    - payment_mode_raw (original mode value)
    - created_at (used as payment date since PLATO has no separate date)
    - corporate_id (payer), location
    - void, void_reason, voided_at, voided_by
    - session_number, reference
    
    Note: PLATO payments don't have patient_id directly - must join via invoice
    """
    
    # Read silver payment data
    silver_payment = spark.table(SILVER_TABLES["payment"])
    
    # Read dimension tables
    dim_payer = spark.table(GOLD_DIMENSIONS["payer"]).select("payer_key", "payer_id")
    dim_location = spark.table(GOLD_DIMENSIONS["location"]).select("location_key", "location_id")
    
    # Transform payment data
    # Use created_at as the payment date (PLATO doesn't have separate payment_date)
    fact_payment = silver_payment.select(
        monotonically_increasing_id().alias("payment_fact_key"),
        col("payment_id"),
        get_date_key(col("created_at")).alias("date_key"),
        col("corporate_id").alias("payer_id"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        col("invoice_id"),
        col("payment_method"),
        col("payment_amount").cast(DecimalType(18, 2)),
        coalesce(col("is_void").cast(BooleanType()), lit(False)).alias("is_void"),
        col("reference"),
        col("session"),
        col("created_at").cast(TimestampType())
    )
    
    # Join with dimensions
    fact_payment = fact_payment \
        .join(dim_payer, "payer_id", "left") \
        .join(dim_location, "location_id", "left")
    
    # Select final columns
    fact_payment = fact_payment.select(
        "payment_fact_key",
        "payment_id",
        "date_key",
        coalesce(col("payer_key"), lit(-1)).alias("payer_key"),
        coalesce(col("location_key"), lit(1)).alias("location_key"),
        "invoice_id",
        "payment_method",
        "payment_amount",
        "is_void",
        "reference",
        "session",
        "created_at"
    )
    
    return fact_payment

# Create and save fact_payment
try:
    fact_payment_df = create_fact_payment()
    fact_payment_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACTS["payment"])
    print(f"✅ Created {GOLD_FACTS['payment']} with {fact_payment_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create fact_payment: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. fact_claims - Insurance Claims

# CELL ********************

def create_fact_claims():
    """
    Create fact_claims for Claims Health score.
    Claims are invoices with corporate payers (insurance) or schemes (CHAS, MediSave).
    Grain: One row per claim invoice.
    
    PLATO doesn't have separate claim tracking - claims are identified by:
    - corporate_id is not null (insurance/corporate payer)
    - OR scheme is not null (CHAS, MediSave, PG, etc.)
    
    Note: Detailed claim submission/response tracking would require 
    additional data from insurance portals (not in PLATO).
    """
    
    # Read silver invoice data
    silver_invoice = spark.table(SILVER_TABLES["invoice"])
    
    # Filter to claims: invoices with corporate payer OR scheme
    claims_df = silver_invoice.filter(
        (col("corporate_id").isNotNull()) | 
        (col("scheme").isNotNull() & (col("scheme") != ""))
    )
    
    # Read dimension tables
    dim_patient = spark.table(GOLD_DIMENSIONS["patient"]).select("patient_key", "patient_id")
    dim_payer = spark.table(GOLD_DIMENSIONS["payer"]).select("payer_key", "payer_id")
    dim_location = spark.table(GOLD_DIMENSIONS["location"]).select("location_key", "location_id")
    
    # Transform to claims fact
    fact_claims = claims_df.select(
        monotonically_increasing_id().alias("claim_fact_key"),
        col("invoice_id").alias("claim_id"),  # Use invoice_id as claim identifier
        col("invoice_id"),
        get_date_key(col("invoice_date")).alias("date_key"),
        col("patient_id"),
        col("corporate_id").alias("payer_id"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        
        # Claim amount is the total invoice amount
        col("total_amount").cast(DecimalType(18, 2)).alias("claim_amount"),
        
        # Scheme info (CHAS, MediSave, PG, etc.)
        col("scheme"),
        
        # Claim type based on scheme
        when(upper(col("scheme")).contains("CHAS"), "CHAS")
        .when(upper(col("scheme")).contains("MEDI"), "MediSave")
        .when(upper(col("scheme")).contains("PG"), "Pioneer Generation")
        .when(upper(col("scheme")).contains("FLEXI"), "Flexi-MediSave")
        .when(col("corporate_id").isNotNull(), "Corporate/Insurance")
        .otherwise("Other").alias("claim_type"),
        
        # Invoice status (proxy for claim status since PLATO doesn't track claims separately)
        col("status"),
        col("is_finalized"),
        col("is_void"),
        
        # Timestamps
        col("invoice_date").alias("claim_date"),
        col("created_at").cast(TimestampType())
    )
    
    # Join with dimensions
    fact_claims = fact_claims \
        .join(dim_patient, "patient_id", "left") \
        .join(dim_payer, "payer_id", "left") \
        .join(dim_location, "location_id", "left")
    
    # Select final columns
    fact_claims = fact_claims.select(
        "claim_fact_key",
        "claim_id",
        "invoice_id",
        "date_key",
        coalesce(col("patient_key"), lit(-1)).alias("patient_key"),
        coalesce(col("payer_key"), lit(-1)).alias("payer_key"),
        coalesce(col("location_key"), lit(1)).alias("location_key"),
        "claim_amount",
        "scheme",
        "claim_type",
        "status",
        "is_finalized",
        "is_void",
        "claim_date",
        "created_at"
    )
    
    return fact_claims

# Create and save fact_claims
try:
    fact_claims_df = create_fact_claims()
    fact_claims_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACTS["claims"])
    print(f"✅ Created {GOLD_FACTS['claims']} with {fact_claims_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create fact_claims: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. fact_appointment - Appointments

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
    )
    
    return fact_appointment

# Create and save fact_appointment
try:
    fact_appointment_df = create_fact_appointment()
    fact_appointment_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACTS["appointment"])
    print(f"✅ Created {GOLD_FACTS['appointment']} with {fact_appointment_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create fact_appointment: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. fact_inventory - Inventory Movements

# CELL ********************

def create_fact_inventory():
    """
    Create fact_inventory for Inventory Health score.
    Combines adjustments and delivery orders (header level only).
    Grain: One row per inventory transaction.
    
    silver_adjustment columns:
    - adjustment_id, adjustment_date, location_id, invoice_id, serial_number
    - adjustment_reason, remarks, cost_per_unit, quantity
    - is_finalized, finalized_at, created_at, updated_at
    
    silver_deliveryorder columns:
    - delivery_order_id, do_number, serial_number, supplier_id, location_id
    - delivery_date, subtotal, total_amount, discount_amount
    - inventory_batch, inventory_do_id
    - is_paid, is_finalized, created_at, updated_at
    
    Note: PLATO data is header-level only - no line item details like item_id, 
    batch_number per item, or expiry_date. Inventory tracking is at document level.
    """
    
    # Read silver data
    silver_adjustment = spark.table(SILVER_TABLES["adjustment"])
    silver_deliveryorder = spark.table(SILVER_TABLES["deliveryorder"])
    
    # Read dimension tables
    dim_location = spark.table(GOLD_DIMENSIONS["location"]).select("location_key", "location_id")
    dim_supplier = spark.table(GOLD_DIMENSIONS["supplier"]).select("supplier_key", "supplier_id")
    
    # Process adjustments (stock adjustments - in/out)
    adjustments = silver_adjustment.select(
        col("adjustment_id").alias("transaction_id"),
        get_date_key(col("adjustment_date")).alias("date_key"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        lit(None).cast(StringType()).alias("supplier_id"),
        
        # Transaction type from reason
        coalesce(col("adjustment_reason"), lit("Adjustment")).alias("transaction_type"),
        
        # Quantities - positive = in, negative = out
        when(col("quantity") > 0, col("quantity")).otherwise(lit(0)).cast(DecimalType(18, 2)).alias("quantity_in"),
        when(col("quantity") < 0, abs(col("quantity"))).otherwise(lit(0)).cast(DecimalType(18, 2)).alias("quantity_out"),
        
        # Cost
        coalesce(col("cost_per_unit"), lit(0)).cast(DecimalType(18, 2)).alias("unit_cost"),
        (abs(coalesce(col("quantity"), lit(0))) * coalesce(col("cost_per_unit"), lit(0))).cast(DecimalType(18, 2)).alias("total_value"),
        
        # Reference
        col("remarks"),
        col("invoice_id"),
        col("is_finalized"),
        
        # Timestamps
        col("created_at").cast(TimestampType())
    )
    
    # Process delivery orders (purchases - goods received)
    deliveries = silver_deliveryorder.select(
        col("delivery_order_id").alias("transaction_id"),
        get_date_key(col("delivery_date")).alias("date_key"),
        coalesce(col("location_id"), lit("LOC001")).alias("location_id"),
        col("supplier_id"),
        
        lit("Purchase").alias("transaction_type"),
        
        # For delivery orders, we don't have quantity - use total_amount as proxy
        lit(0).cast(DecimalType(18, 2)).alias("quantity_in"),
        lit(0).cast(DecimalType(18, 2)).alias("quantity_out"),
        
        # Cost - use total_amount at header level
        lit(0).cast(DecimalType(18, 2)).alias("unit_cost"),
        coalesce(col("total_amount"), lit(0)).cast(DecimalType(18, 2)).alias("total_value"),
        
        # Reference
        col("remarks"),
        col("invoice_id"),
        col("is_finalized"),
        
        # Timestamps
        col("created_at").cast(TimestampType())
    )
    
    # Union all inventory movements
    fact_inventory = adjustments.unionByName(deliveries)
    
    # Add surrogate key
    fact_inventory = fact_inventory.withColumn(
        "inventory_fact_key", 
        monotonically_increasing_id()
    )
    
    # Join with dimensions
    fact_inventory = fact_inventory \
        .join(dim_location, "location_id", "left") \
        .join(dim_supplier, "supplier_id", "left")
    
    # Select final columns
    fact_inventory = fact_inventory.select(
        "inventory_fact_key",
        "transaction_id",
        "date_key",
        coalesce(col("location_key"), lit(1)).alias("location_key"),
        coalesce(col("supplier_key"), lit(-1)).alias("supplier_key"),
        "transaction_type",
        "quantity_in",
        "quantity_out",
        "unit_cost",
        "total_value",
        "remarks",
        "invoice_id",
        "is_finalized",
        "created_at"
    )
    
    return fact_inventory

# Create and save fact_inventory
try:
    fact_inventory_df = create_fact_inventory()
    fact_inventory_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACTS["inventory"])
    print(f"✅ Created {GOLD_FACTS['inventory']} with {fact_inventory_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create fact_inventory: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. fact_ar_aging - Accounts Receivable Aging
# 
# **Source Priority:**
# 1. XERO (type=ACCREC) - Accounting book of record
# 2. PLATO - Fallback if XERO unavailable

# CELL ********************

def create_fact_ar_aging_from_xero():
    """
    Create AR aging from XERO invoices (book of record).
    XERO type='ACCREC' = Accounts Receivable invoices.
    
    silver_xero_invoices columns:
    - invoice_id, invoice_number, contact_id, contact_name
    - type, invoice_type_desc
    - invoice_date, due_date
    - subtotal, total_tax, total
    - amount_due, amount_paid, amount_credited
    - status, reference
    """
    xero_invoices = spark.table(SILVER_TABLES["xero_invoices"])
    xero_payments = spark.table(SILVER_TABLES["xero_payments"])
    
    # Filter to AR invoices only
    ar_invoices = xero_invoices.filter(col("type") == "ACCREC")
    
    # Calculate paid amounts
    payments_by_invoice = xero_payments.groupBy("invoice_id") \
        .agg(sum("amount").alias("paid_amount"))
    
    # Calculate outstanding
    ar_aging = ar_invoices \
        .join(payments_by_invoice, "invoice_id", "left") \
        .withColumn("paid_amount", coalesce(col("paid_amount"), lit(0))) \
        .withColumn("outstanding_amount", col("amount_due")) \
        .filter(col("outstanding_amount") > 0)
    
    # Add aging calculations - use invoice_date (not date)
    ar_aging = ar_aging.select(
        monotonically_increasing_id().alias("ar_aging_key"),
        get_date_key(current_date()).alias("date_key"),
        lit(-1).alias("patient_key"),  # XERO doesn't have patient context
        lit(-1).alias("payer_key"),
        col("invoice_id"),
        col("invoice_date"),
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
        (current_date() > col("due_date")).alias("is_overdue"),
        lit("XERO").alias("source_system")
    )
    
    return ar_aging

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

# MARKDOWN ********************

# ## 7. fact_ap_aging - Accounts Payable Aging

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

# Create and save fact_ap_aging
try:
    fact_ap_aging_df = create_fact_ap_aging()
    fact_ap_aging_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACTS["ap_aging"])
    print(f"✅ Created {GOLD_FACTS['ap_aging']} with {fact_ap_aging_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create fact_ap_aging: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. fact_cashflow - Cash Flow

# CELL ********************

def create_fact_cashflow():
    """
    Create fact_cashflow for Cashflow Health score.
    Uses XERO bank transactions as single source of truth for cash movements.
    
    silver_xero_bank_transactions columns:
    - bank_transaction_id, bank_account_id, contact_id
    - type (RECEIVE/SPEND/etc), flow_direction (IN/OUT)
    - transaction_date (varchar), total (decimal)
    - status, is_reconciled, reference
    
    silver_xero_accounts columns:
    - account_id, account_name, account_code
    - account_type, account_class (ASSET/LIABILITY/EQUITY/REVENUE/EXPENSE)
    """
    
    # Read data
    xero_bank = spark.table(SILVER_TABLES["xero_bank_transactions"])
    xero_accounts = spark.table(SILVER_TABLES["xero_accounts"])
    xero_contacts = spark.table(SILVER_TABLES["xero_contacts"])
    
    # Read dimensions
    dim_date = spark.table(GOLD_DIMENSIONS["date"])
    
    # Process XERO bank transactions
    fact_cashflow = xero_bank.select(
        # Date key - parse varchar date
        get_date_key(to_date(col("transaction_date"))).alias("date_key"),
        
        col("bank_account_id"),
        col("contact_id"),
        col("type").alias("transaction_type"),
        col("flow_direction"),
        
        # Cash in/out based on flow_direction (not total sign)
        when(col("flow_direction") == "IN", col("total"))
            .otherwise(lit(0)).cast(DecimalType(18, 2)).alias("cash_in"),
        when(col("flow_direction") == "OUT", col("total"))
            .otherwise(lit(0)).cast(DecimalType(18, 2)).alias("cash_out"),
        
        col("total").cast(DecimalType(18, 2)).alias("transaction_amount"),
        col("subtotal").cast(DecimalType(18, 2)),
        col("total_tax").cast(DecimalType(18, 2)),
        
        col("reference"),
        col("status"),
        col("is_reconciled"),
        col("currency_code"),
        col("bank_transaction_id").alias("source_transaction_id")
    )
    
    # Calculate net cashflow (positive = money in, negative = money out)
    fact_cashflow = fact_cashflow.withColumn(
        "net_cashflow",
        (col("cash_in") - col("cash_out")).cast(DecimalType(18, 2))
    )
    
    # Join with accounts to get account details
    xero_accounts_select = xero_accounts.select(
        col("account_id").alias("bank_account_id"),
        col("account_name").alias("bank_account_name"),
        col("account_type").alias("bank_account_type"),
        col("account_class").alias("bank_account_class")
    )
    
    fact_cashflow = fact_cashflow.join(
        xero_accounts_select, 
        "bank_account_id", 
        "left"
    )
    
    # Join with contacts to get contact name
    xero_contacts_select = xero_contacts.select(
        col("contact_id"),
        col("contact_name"),
        col("is_customer"),
        col("is_supplier")
    )
    
    fact_cashflow = fact_cashflow.join(
        xero_contacts_select,
        "contact_id",
        "left"
    )
    
    # Classify cashflow category based on transaction type
    # Operating: RECEIVE, SPEND (day-to-day business)
    # Investing: Asset purchases/sales
    # Financing: Loans, equity
    fact_cashflow = fact_cashflow.withColumn(
        "cashflow_category",
        when(col("transaction_type").isin("RECEIVE", "RECEIVE-OVERPAYMENT", "RECEIVE-PREPAYMENT"), lit("Operating - Receipts"))
        .when(col("transaction_type").isin("SPEND", "SPEND-OVERPAYMENT", "SPEND-PREPAYMENT"), lit("Operating - Payments"))
        .when(col("transaction_type").contains("TRANSFER"), lit("Financing - Transfer"))
        .otherwise(lit("Other"))
    )
    
    # Add surrogate key
    fact_cashflow = fact_cashflow.withColumn(
        "cashflow_key",
        monotonically_increasing_id()
    )
    
    # Select final columns
    fact_cashflow = fact_cashflow.select(
        "cashflow_key",
        "date_key",
        "bank_account_id",
        "bank_account_name",
        "contact_id",
        "contact_name",
        "is_customer",
        "is_supplier",
        "transaction_type",
        "cashflow_category",
        "flow_direction",
        "cash_in",
        "cash_out",
        "net_cashflow",
        "subtotal",
        "total_tax",
        "transaction_amount",
        "reference",
        "status",
        "is_reconciled",
        "currency_code",
        "source_transaction_id"
    )
    
    return fact_cashflow

# Create and save fact_cashflow
try:
    fact_cashflow_df = create_fact_cashflow()
    fact_cashflow_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACTS["cashflow"])
    print(f"✅ Created {GOLD_FACTS['cashflow']} with {fact_cashflow_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create fact_cashflow: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_agg_cashflow():
    """
    Create agg_cashflow aggregate table for Cashflow Health dashboard.
    
    Target Schema:
    - Cash Conversion Cycle (CCC) = DSO + DIO - DPO
    - AR metrics (Days Sales Outstanding, AR amount, AR score)
    - AP metrics (Days Payable Outstanding, AP score)
    - Inventory metrics (Days Inventory Outstanding, Inventory score)
    - Operating Cash Flow metrics
    - Debt/Interest metrics
    - Overall Cashflow Health Score
    
    Data Sources:
    - fact_cashflow: Operating cash flow
    - xero_invoices: AR metrics (DSO)
    - agg_inventory: Inventory metrics (DIO)
    """
    
    # Read data sources
    fact_cashflow = spark.table(GOLD_FACTS["cashflow"])
    xero_invoices = spark.table(SILVER_TABLES["xero_invoices"])
    
    # Try to read inventory data if available
    try:
        agg_inventory = spark.table("lh_bnj_gold.gold.agg_inventory")
        has_inventory = True
    except:
        has_inventory = False
        print("Warning: agg_inventory not available, using defaults for inventory metrics")
    
    # ========== OPERATING CASH FLOW (from fact_cashflow) ==========
    # Aggregate by date
    ocf_daily = fact_cashflow.groupBy("date_key") \
        .agg(
            sum(when(col("cashflow_category") == "Operating - Receipts", col("cash_in")).otherwise(lit(0))).cast(DecimalType(18, 2)).alias("operating_cash_in"),
            sum(when(col("cashflow_category") == "Operating - Payments", col("cash_out")).otherwise(lit(0))).cast(DecimalType(18, 2)).alias("operating_cash_out"),
            first("bank_account_id").alias("location_key_temp")
        )
    
    ocf_daily = ocf_daily.withColumn(
        "operating_cash_flow",
        (col("operating_cash_in") - col("operating_cash_out")).cast(DecimalType(18, 2))
    )
    
    # ========== AR METRICS (from xero_invoices) ==========
    # DSO = (Average AR / Total Credit Sales) * Days in Period
    # For simplicity, calculate AR outstanding and estimate DSO
    ar_metrics = xero_invoices \
        .filter(col("type") == "ACCREC") \
        .groupBy(get_date_key(to_date(col("invoice_date"))).alias("date_key")) \
        .agg(
            sum("amount_due").cast(DecimalType(18, 2)).alias("total_ar_amount"),
            sum("total").cast(DecimalType(18, 2)).alias("total_sales"),
            avg(datediff(
                coalesce(to_date(col("due_date")), current_date()),
                to_date(col("invoice_date"))
            )).cast(DecimalType(18, 2)).alias("avg_payment_terms")
        )
    
    # Calculate DSO (simplified: AR / daily sales * 30)
    ar_metrics = ar_metrics.withColumn(
        "days_sales_outstanding",
        when(col("total_sales") > 0,
             (col("total_ar_amount") / col("total_sales") * 30)
        ).otherwise(lit(0)).cast(DecimalType(18, 2))
    )
    
    # ========== AP METRICS (estimate from payments to suppliers) ==========
    ap_metrics = fact_cashflow \
        .filter(col("is_supplier") == True) \
        .groupBy("date_key") \
        .agg(
            sum("cash_out").cast(DecimalType(18, 2)).alias("total_ap_payments"),
            count("*").alias("ap_payment_count")
        )
    
    # DPO estimation (assume 30-day terms as baseline, adjust based on payment patterns)
    ap_metrics = ap_metrics.withColumn(
        "days_payable_outstanding",
        lit(30).cast(FloatType())  # Default 30 days, would need AP aging for accuracy
    )
    
    # Current liabilities estimate (monthly AP payments as proxy)
    window_30d = Window.orderBy("date_key").rowsBetween(-29, 0)
    ap_metrics = ap_metrics.withColumn(
        "current_liabilities",
        sum("total_ap_payments").over(window_30d).cast(FloatType())
    )
    
    # ========== DEBT/INTEREST METRICS (from financing transactions) ==========
    debt_metrics = fact_cashflow \
        .filter(col("cashflow_category").contains("Financing")) \
        .groupBy("date_key") \
        .agg(
            sum(when(col("reference").contains("interest") | col("reference").contains("Interest"), col("cash_out"))
                .otherwise(lit(0))).cast(FloatType()).alias("interest_expense"),
            sum(when(~(col("reference").contains("interest") | col("reference").contains("Interest")), col("cash_out"))
                .otherwise(lit(0))).cast(FloatType()).alias("principal_repayment")
        )
    
    debt_metrics = debt_metrics.withColumn(
        "total_debt_financing_expense",
        (col("interest_expense") + col("principal_repayment")).cast(FloatType())
    )
    
    # ========== JOIN ALL METRICS ==========
    agg_cashflow = ocf_daily \
        .join(ar_metrics, "date_key", "left") \
        .join(ap_metrics, "date_key", "left") \
        .join(debt_metrics, "date_key", "left")
    
    # ========== INVENTORY METRICS ==========
    if has_inventory:
        inv_metrics = agg_inventory.select(
            col("invoice_date_key").alias("date_key"),
            col("days_inventory_outstanding"),
            col("inventory_key").alias("inventory_id"),
            col("inventory_health_score").alias("inventory_score_raw")
        )
        agg_cashflow = agg_cashflow.join(inv_metrics, "date_key", "left")
    else:
        agg_cashflow = agg_cashflow \
            .withColumn("days_inventory_outstanding", lit(0).cast(DecimalType(18, 2))) \
            .withColumn("inventory_id", lit(None).cast(LongType())) \
            .withColumn("inventory_score_raw", lit(50).cast(FloatType()))
    
    # ========== CASH CONVERSION CYCLE ==========
    # CCC = DSO + DIO - DPO
    agg_cashflow = agg_cashflow.withColumn(
        "cash_conversion_cycle_days",
        (
            coalesce(col("days_sales_outstanding"), lit(0)) +
            coalesce(col("days_inventory_outstanding"), lit(0)) -
            coalesce(col("days_payable_outstanding"), lit(0))
        ).cast(FloatType())
    )
    
    # ========== OPERATING CASH FLOW RATIO ==========
    # OCF Ratio = Operating Cash Flow / Current Liabilities
    agg_cashflow = agg_cashflow.withColumn(
        "operating_cash_flow_ratio",
        when(col("current_liabilities") > 0,
             col("operating_cash_flow") / col("current_liabilities")
        ).otherwise(lit(0)).cast(FloatType())
    )
    
    # ========== HEALTH SCORES (0-100) ==========
    
    # AR Score: Lower DSO = better (100 if DSO <= 30, 0 if DSO >= 90)
    agg_cashflow = agg_cashflow.withColumn(
        "ar_score",
        greatest(lit(0), least(lit(100), 
            lit(100) - (coalesce(col("days_sales_outstanding"), lit(30)) - 30) * (100/60)
        )).cast(DecimalType(18, 2))
    )
    
    # AP Score: Higher DPO = better (but not too high - optimal around 30-45 days)
    agg_cashflow = agg_cashflow.withColumn(
        "ap_score",
        when(col("days_payable_outstanding").between(30, 45), lit(100))
        .when(col("days_payable_outstanding") < 30, col("days_payable_outstanding") / 30 * 100)
        .when(col("days_payable_outstanding") > 45, greatest(lit(0), lit(100) - (col("days_payable_outstanding") - 45) * 2))
        .otherwise(lit(50)).cast(FloatType())
    )
    
    # Inventory Score (from agg_inventory or calculated)
    agg_cashflow = agg_cashflow.withColumn(
        "inventory_score",
        coalesce(col("inventory_score_raw"), lit(50)).cast(DecimalType(18, 2))
    )
    
    # Interest Score: Lower interest expense relative to OCF = better
    agg_cashflow = agg_cashflow.withColumn(
        "interest_score",
        when(col("operating_cash_flow") > 0,
             greatest(lit(0), least(lit(100), 
                 lit(100) - (coalesce(col("interest_expense"), lit(0)) / col("operating_cash_flow") * 100)
             ))
        ).otherwise(lit(50)).cast(FloatType())
    )
    
    # Principal Score: Ability to cover principal payments
    agg_cashflow = agg_cashflow.withColumn(
        "principal_score",
        when(col("operating_cash_flow") > coalesce(col("principal_repayment"), lit(0)), lit(100))
        .when(col("operating_cash_flow") > 0, 
              col("operating_cash_flow") / greatest(col("principal_repayment"), lit(1)) * 100)
        .otherwise(lit(0)).cast(FloatType())
    )
    
    # Overall Cashflow Health Score (weighted average)
    agg_cashflow = agg_cashflow.withColumn(
        "cashflow_health_score",
        (
            coalesce(col("ar_score"), lit(50)) * 0.25 +
            coalesce(col("ap_score"), lit(50)) * 0.20 +
            coalesce(col("inventory_score"), lit(50)) * 0.20 +
            coalesce(col("interest_score"), lit(50)) * 0.15 +
            coalesce(col("principal_score"), lit(50)) * 0.20
        ).cast(FloatType())
    )
    
    # ========== ADD METADATA ==========
    agg_cashflow = agg_cashflow \
        .withColumn("invoice_date_key", col("date_key").cast(IntegerType())) \
        .withColumn("invoice_id", lit(None).cast(StringType())) \
        .withColumn("location_key", coalesce(col("location_key_temp"), lit(1)).cast(LongType())) \
        .withColumn("dw_created_at", current_timestamp()) \
        .withColumn("dw_updated_at", current_timestamp())
    
    # ========== SELECT FINAL COLUMNS (matching target schema) ==========
    result = agg_cashflow.select(
        col("ap_score").cast(FloatType()),
        col("ar_score").cast(DecimalType(18, 2)),
        col("cash_conversion_cycle_days").cast(FloatType()),
        col("cashflow_health_score").cast(FloatType()),
        col("current_liabilities").cast(FloatType()),
        col("days_inventory_outstanding").cast(DecimalType(18, 2)),
        col("days_payable_outstanding").cast(FloatType()),
        col("days_sales_outstanding").cast(DecimalType(18, 2)),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("interest_expense").cast(FloatType()),
        col("interest_score").cast(FloatType()),
        col("inventory_id").cast(LongType()),
        col("inventory_score").cast(DecimalType(18, 2)),
        col("invoice_date_key").cast(IntegerType()),
        col("invoice_id").cast(StringType()),
        col("location_key").cast(LongType()),
        col("operating_cash_flow").cast(DecimalType(18, 2)),
        col("operating_cash_flow_ratio").cast(FloatType()),
        col("principal_repayment").cast(FloatType()),
        col("principal_score").cast(FloatType()),
        col("total_ar_amount").cast(DecimalType(18, 2)),
        col("total_debt_financing_expense").cast(FloatType())
    )
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = create_agg_cashflow()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

AGG_TABLES = {"cashflow": "lh_bnj_gold.gold_test.agg_cashflow"}
target_table = AGG_TABLES["cashflow"]
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary

# CELL ********************

# Display summary of created fact tables
print("\n" + "="*60)
print("FACT TABLES CREATION SUMMARY")
print("="*60)

for fact_name, table_name in GOLD_FACTS.items():
    try:
        count = spark.table(table_name).count()
        print(f"✅ {table_name}: {count:,} rows")
    except:
        print(f"⏳ {table_name}: Pending (waiting for source data)")

print("="*60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
