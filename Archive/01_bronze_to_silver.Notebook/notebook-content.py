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
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # BNJ Medical CFO Dashboard - Bronze to Silver Layer
# 
# This notebook transforms raw Bronze layer data into cleansed Silver layer tables.
# 
# **Source Systems:**
# - PLATO (Medical Practice Management)
# - XERO (Accounting System)
# 
# **Transformations Applied:**
# - Data type standardization
# - Null handling and default values
# - Data quality validation
# - Deduplication
# - Column naming standardization (snake_case)

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration

# CELL ********************

# Bronze table names (raw data from source systems)
BRONZE_TABLES = {
    # PLATO tables
    "plato_patient": "lh_bnj_bronze.plato.patient",
    "plato_invoice": "lh_bnj_bronze.plato.invoice",
    "plato_payment": "lh_bnj_bronze.plato.payment",
    "plato_inventory": "lh_bnj_bronze.plato.inventory",
    "plato_supplier": "lh_bnj_bronze.plato.supplier",
    "plato_corporate": "lh_bnj_bronze.plato.corporate",
    "plato_appointment": "lh_bnj_bronze.plato.brz_appointment",
    "plato_adjustment": "lh_bnj_bronze.plato.adjustment",
    "plato_deliveryorder": "lh_bnj_bronze.plato.brz_deliveryorder",
    "plato_contact": "lh_bnj_bronze.plato.brz_contact",
    "plato_letter": "lh_bnj_bronze.plato.letter",
    # XERO tables
    "xero_invoices": "lh_bnj_bronze.xero.brz_invoices",
    "xero_payments": "lh_bnj_bronze.xero.brz_payments",
    "xero_bank_transactions": "lh_bnj_bronze.xero.brz_bank_transactions",
    "xero_accounts": "lh_bnj_bronze.xero.brz_accounts",
    "xero_contacts": "lh_bnj_bronze.xero.brz_contacts"
}

# Silver table names (cleansed data)
SILVER_TABLES = {
    # PLATO tables
    "patient": "lh_bnj_silver.plato_test.silver_patient",
    "invoice": "lh_bnj_silver.plato_test.silver_invoice",
    "payment": "lh_bnj_silver.plato_test.silver_payment",
    "inventory": "lh_bnj_silver.plato_test.silver_inventory",
    "supplier": "lh_bnj_silver.plato_test.silver_supplier",
    "corporate": "lh_bnj_silver.plato_test.silver_corporate",
    "appointment": "lh_bnj_silver.plato_test.silver_appointment",
    "adjustment": "lh_bnj_silver.plato_test.silver_adjustment",
    "deliveryorder": "lh_bnj_silver.plato_test.silver_deliveryorder",
    "contact": "lh_bnj_silver.plato_test.silver_contact",
    # XERO tables
    "xero_invoices": "lh_bnj_silver.xero_test.silver_xero_invoices",
    "xero_payments": "lh_bnj_silver.xero_test.silver_xero_payments",
    "xero_bank_transactions": "lh_bnj_silver.xero_test.silver_xero_bank_transactions",
    "xero_accounts": "lh_bnj_silver.xero_test.silver_xero_accounts",
    "xero_contacts": "lh_bnj_silver.xero_test.silver_xero_contacts"
}

# Processing timestamp
PROCESSING_TIME = current_timestamp()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Functions

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Quality Checks

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# # PLATO Tables
# 
# Column mappings based on actual PLATO schema:
# - patient: `_id`, `given_id`, `name`, `nric`, `dob`, `sex`, `telephone`, `email`, `address`, `postal`, etc.
# - invoice: `_id`, `patient_id`, `location`, `date`, `doctor`, `status`, `sub_total`, `tax`, `total`, `invoice`, etc.
# - payment: `_id`, `invoice_id`, `mode`, `corp`, `amount`, `location`, `ref`, `void`, etc.
# - inventory: `_id`, `given_id`, `name`, `category`, `selling_price`, `cost_price`, `qty`, `supplier`, etc.
# - supplier: `_id`, `name`, `category`, `email`, `telephone`, `address`, `person`, etc.
# - corporate: `_id`, `given_id`, `name`, `category`, `payment_type`, `scheme`, etc.
# - appointment: `_id`, `patient_id`, `doctor`, `starttime`, `endtime`, `title`, `description`, etc.
# - adjustment: `_id`, `date`, `remarks`, `location`, `reason`, `inventory`, `finalized`, etc.
# - deliveryorder: `_id`, `date`, `do`, `supplier`, `total`, `location`, `paid`, etc.

# MARKDOWN ********************

# ## 1. silver_patient

# CELL ********************

def transform_patient():
    """
    Transform bronze_plato_patient to silver_patient.
    Key transformations:
    - Standardize date formats
    - Mask sensitive data (NRIC)
    - Parse and validate phone numbers
    - Standardize gender values
    
    PLATO Schema columns:
    - _id, given_id, name, nric, dob, sex, telephone, email, address, postal
    - corporate.corp, corporate.notes (flattened from MongoDB), doctor, allergies, nationality, etc.
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_patient"])
    
    silver = bronze.select(
        # Primary key
        col("_id").alias("patient_id"),
        col("given_id").alias("patient_code"),
        
        # Name - PLATO stores full name in single 'name' field
        clean_string_column("name").alias("full_name"),
        col("title").alias("title"),
        
        # Mask NRIC (show last 4 characters only)
        when(col("nric").isNotNull() & (length(col("nric")) >= 4),
             concat(lit("****"), substring(col("nric"), -4, 4))
        ).alias("nric_masked"),
        
        # Store original NRIC hash for matching
        sha2(col("nric"), 256).alias("nric_hash"),
        col("nric_type").alias("nric_type"),
        
        # Standardize gender (PLATO uses 'sex' field)
        when(upper(col("sex")).isin("M", "MALE"), "Male")
        .when(upper(col("sex")).isin("F", "FEMALE"), "Female")
        .otherwise("Unknown").alias("gender"),
        
        # Date of birth
        col("dob").alias("date_of_birth"),
        
        # Contact info
        clean_string_column("email").alias("email"),
        regexp_replace(col("telephone"), "[^0-9+]", "").alias("phone_primary"),
        regexp_replace(col("telephone2"), "[^0-9+]", "").alias("phone_secondary"),
        regexp_replace(col("telephone3"), "[^0-9+]", "").alias("phone_tertiary"),
        
        # Address
        clean_string_column("address").alias("address"),
        col("unit_no").alias("unit_number"),
        col("postal").cast("string").alias("postal_code"),
        coalesce(clean_string_column("nationality"), lit("Singapore")).alias("nationality"),
        
        # Status / Flags
        col("dnd").cast(BooleanType()).alias("do_not_disturb"),
        clean_string_column("occupation").alias("occupation"),
        clean_string_column("marital_status").alias("marital_status"),
        
        # Medical info
        clean_string_column("allergies").alias("allergies"),
        clean_string_column("allergies_select").alias("allergies_select"),
        clean_string_column("food_allergies").alias("food_allergies"),
        clean_string_column("food_allergies_select").alias("food_allergies_select"),
        clean_string_column("g6pd").alias("g6pd_status"),
        clean_string_column("alerts").alias("alerts"),
        
        # Doctor assignment
        col("doctor").alias("assigned_doctor"),
        
        # Corporate/Insurance link - use backticks for column names with dots
        col("`corporate.corp`").alias("corporate_id"),
        col("`corporate.notes`").alias("corporate_notes"),
        
        # Additional fields
        clean_string_column("referred_by").alias("referred_by"),
        clean_string_column("notes").alias("notes"),
        clean_string_column("tag").alias("tags"),
        col("`nok.id`").alias("next_of_kin_id"),
        
        # Timestamps
        col("created_on").alias("created_at"),
        col("last_edited").alias("updated_at"),
        col("created_by").alias("created_by"),
        col("last_edited_by").alias("updated_by")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate by patient_id
    silver = deduplicate(silver, ["patient_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    patient_df = transform_patient()
    log_data_quality("silver_patient", patient_df, "patient_id")
    patient_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["patient"])
    print(f"✅ Created {SILVER_TABLES['patient']}")
except Exception as e:
    print(f"⚠️ Could not create silver_patient: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. silver_invoice

# CELL ********************

def transform_invoice():
    """
    Transform bronze_plato_invoice to silver_invoice.
    
    PLATO Schema columns:
    - _id, patient_id, location, date, doctor, status, status_on
    - sub_total, tax, total, adj_amount, adj
    - invoice (number), invoice_prefix, finalized, void
    - corporate, payment, item, mc, prescription (may be flattened)
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_invoice"])
    silver = bronze.select(
        # Primary key
        col("_id").alias("invoice_id"),
        
        # Invoice number (combine prefix + number)
        concat(
            coalesce(col("invoice_prefix"), lit("")),
            coalesce(col("invoice").cast("string"), lit(""))
        ).alias("invoice_number"),
        
        # References
        col("patient_id").alias("patient_id"),
        col("doctor").alias("doctor_id"),
        col("location").alias("location_id"),
        col("`corporate._id`").alias("corporate_id"),
        
        # Date
        col("date").alias("invoice_date"),
        
        # Amounts
        col("sub_total").cast(DecimalType(18, 4)).alias("subtotal"),
        col("tax").cast(DecimalType(18, 4)).alias("tax_amount"),
        col("total").cast(DecimalType(18, 4)).alias("total_amount"),
        coalesce(col("adj_amount"), lit(0)).cast(DecimalType(18, 4)).alias("adjustment_amount"),
        
        # GST flag
        when(col("no_gst") == 1, False).otherwise(True).alias("gst_applicable"),
        col("rate").alias("tax_rate"),
        
        # Status flags
        col("status").alias("status"),
        col("status_on").alias("status_changed_at"),
        when(col("finalized") == 1, True).otherwise(False).alias("is_finalized"),
        col("finalized_on").alias("finalized_at"),
        col("finalized_by").alias("finalized_by"),
        
        # Void info
        when(col("void") == 1, True).otherwise(False).alias("is_void"),
        col("void_reason").alias("void_reason"),
        col("void_on").alias("voided_at"),
        col("void_by").alias("voided_by"),
        
        # Credit/Debit note
        when(col("cndn") == 1, True).otherwise(False).alias("is_credit_debit_note"),
        col("cndn_apply_to").alias("cndn_applied_to_invoice"),
        
        # Additional info
        col("scheme").alias("scheme"),
        col("session").alias("session_number"),
        col("highlight").cast(BooleanType()).alias("is_highlighted"),
        
        # Notes
        clean_string_column("notes").alias("notes"),
        clean_string_column("corp_notes").alias("corporate_notes"),
        clean_string_column("invoice_notes").alias("invoice_notes"),
        
        # Time tracking
        col("manual_timein").alias("manual_time_in"),
        col("manual_timeout").alias("manual_time_out"),
        
        # Timestamps
        col("created_on").alias("created_at"),
        col("created_by").alias("created_by"),
        col("last_edited").alias("updated_at"),
        col("last_edited_by").alias("updated_by")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["invoice_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    invoice_df = transform_invoice()
    log_data_quality("silver_invoice", invoice_df, "invoice_id")
    invoice_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["invoice"])
    print(f"✅ Created {SILVER_TABLES['invoice']}")
except Exception as e:
    print(f"⚠️ Could not create silver_invoice: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. silver_payment

# CELL ********************

def transform_payment():
    """
    Transform bronze_plato_payment to silver_payment.
    
    Source columns from PLATO payment schema:
    - _id, invoice_id, mode, corp, amount
    - location, ref, session
    - void, void_on, void_by, void_reason
    - created_on, created_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_payment"])
    bronze = standardize_column_names(bronze)
    
    silver = bronze.select(
        # Primary key
        col("_id").alias("payment_id"),
        
        # Relationships
        col("invoice_id").alias("invoice_id"),
        col("corp").alias("corporate_id"),
        clean_string_column("location").alias("location_id"),
        
        # Payment method - PLATO uses 'mode'
        clean_string_column("mode").alias("payment_mode_raw"),
        when(upper(col("mode")).contains("CASH"), "Cash")
        .when(upper(col("mode")).contains("CARD") | 
              upper(col("mode")).contains("CREDIT") |
              upper(col("mode")).contains("VISA") |
              upper(col("mode")).contains("MASTER"), "Credit Card")
        .when(upper(col("mode")).contains("NETS") |
              upper(col("mode")).contains("DEBIT"), "Debit Card")
        .when(upper(col("mode")).contains("PAYNOW") |
              upper(col("mode")).contains("TRANSFER"), "Bank Transfer")
        .when(upper(col("mode")).contains("CHEQUE") |
              upper(col("mode")).contains("CHECK"), "Cheque")
        .when(upper(col("mode")).contains("CORPORATE") |
              upper(col("mode")).contains("CORP"), "Corporate")
        .when(upper(col("mode")).contains("INSURANCE") |
              upper(col("mode")).contains("CLAIM"), "Insurance")
        .otherwise(coalesce(col("mode"), lit("Other"))).alias("payment_method"),
        
        # Amount
        col("amount").cast(DecimalType(18, 4)).alias("payment_amount"),
        
        # Reference
        clean_string_column("ref").alias("reference"),
        col("session").alias("session"),
        
        # Void info
        when(col("void") == 1, True).otherwise(False).alias("is_void"),
        clean_string_column("void_on").alias("voided_at"),
        clean_string_column("void_by").alias("voided_by"),
        clean_string_column("void_reason").alias("void_reason"),
        
        # Timestamps
        parse_timestamp("created_on").alias("created_at"),
        clean_string_column("created_by").alias("created_by")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["payment_id"], "created_at")
    
    return silver

# Execute transformation
try:
    payment_df = transform_payment()
    log_data_quality("silver_payment", payment_df, "payment_id")
    payment_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["payment"])
    print(f"✅ Created {SILVER_TABLES['payment']}")
except Exception as e:
    print(f"⚠️ Could not create silver_payment: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. silver_inventory

# CELL ********************

def transform_inventory():
    """
    Transform bronze_plato_inventory to silver_inventory.
    Key transformations:
    - Standardize product categories
    - Map PLATO column names to silver schema
    
    PLATO Inventory Schema:
    - _id: MongoDB ObjectId (primary key)
    - given_id: Item code
    - name: Item name
    - category, description
    - selling_price, cost_price
    - qty, unit, order_unit
    - dosage, dusage, ddose, dunit, dfreq, dduration (medication fields)
    - supplier, manufacturer, pack_size
    - scheme (e.g., CHAS, MediSave)
    - track_stock, hidden, no_discount, fixed_price
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_inventory"])
    
    silver = bronze.select(
        # Primary identifiers
        col("_id").alias("product_id"),
        col("given_id").alias("product_code"),
        clean_string_column("name").alias("product_name"),
        clean_string_column("description").alias("description"),
        
        # Category standardization
        coalesce(clean_string_column("category"), lit("Uncategorized")).alias("category"),
        
        # Derive product type from category
        when(upper(col("category")).rlike("DRUG|MED|PILL|TAB|CAP|INJECTION|SYRUP"), "Medication")
        .when(upper(col("category")).rlike("CONSUM|DISPOSABLE|GLOVE|MASK|SYRINGE"), "Consumable")
        .when(upper(col("category")).rlike("EQUIP|DEVICE|MACHINE"), "Equipment")
        .when(upper(col("category")).rlike("SERVICE|CONSULT"), "Service")
        .otherwise("Other").alias("product_type"),
        
        # Units
        coalesce(clean_string_column("unit"), lit("EA")).alias("unit_of_measure"),
        clean_string_column("order_unit").alias("order_unit"),
        
        # Pricing
        col("cost_price").cast(DecimalType(18, 4)).alias("unit_cost"),
        col("selling_price").cast(DecimalType(18, 2)).alias("selling_price"),
        
        # Stock management
        col("qty").cast(DecimalType(18, 2)).alias("quantity_on_hand"),
        col("pack_size").cast(IntegerType()).alias("pack_size"),
        
        # Medication-specific fields
        clean_string_column("dosage").alias("dosage"),
        clean_string_column("dusage").alias("dosage_usage"),
        clean_string_column("ddose").alias("default_dose"),
        clean_string_column("dunit").alias("dose_unit"),
        clean_string_column("dfreq").alias("dose_frequency"),
        clean_string_column("dduration").alias("dose_duration"),
        
        # Supplier & Manufacturer
        col("supplier").alias("supplier_id"),
        clean_string_column("manufacturer").alias("manufacturer"),
        
        # Subsidy/scheme info
        clean_string_column("scheme").alias("scheme"),
        
        # Flags - cast BIGINT to Boolean (0=false, non-zero=true)
        coalesce(col("track_stock").cast(BooleanType()), lit(True)).alias("is_track_stock"),
        when(col("hidden").cast(BooleanType()) == True, lit(False)).otherwise(lit(True)).alias("is_active"),
        coalesce(col("no_discount").cast(BooleanType()), lit(False)).alias("no_discount"),
        coalesce(col("fixed_price").cast(BooleanType()), lit(False)).alias("fixed_price"),
        
        # Timestamps
        parse_timestamp("created_on").alias("created_at"),
        parse_timestamp("last_edited").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["product_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    inventory_df = transform_inventory()
    log_data_quality("silver_inventory", inventory_df, "product_id")
    inventory_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["inventory"])
    print(f"✅ Created {SILVER_TABLES['inventory']}")
except Exception as e:
    print(f"⚠️ Could not create silver_inventory: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. silver_supplier

# CELL ********************

def transform_supplier():
    """
    Transform bronze_plato_supplier to silver_supplier.
    
    PLATO Supplier Schema:
    - _id: MongoDB ObjectId (primary key)
    - name: Supplier name
    - category: Supplier category/type
    - email, handphone, telephone, fax
    - address, person (contact person)
    - url, notes, others
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_supplier"])
    
    silver = bronze.select(
        # Primary identifiers
        col("_id").alias("supplier_id"),
        clean_string_column("name").alias("supplier_name"),
        coalesce(clean_string_column("category"), lit("General")).alias("supplier_category"),
        
        # Contact person
        clean_string_column("person").alias("contact_person"),
        
        # Contact details
        clean_string_column("email").alias("email"),
        regexp_replace(col("telephone"), "[^0-9+]", "").alias("phone"),
        regexp_replace(col("handphone"), "[^0-9+]", "").alias("mobile"),
        regexp_replace(col("fax"), "[^0-9+]", "").alias("fax"),
        
        # Address
        clean_string_column("address").alias("address"),
        
        # Additional info
        clean_string_column("url").alias("website"),
        clean_string_column("notes").alias("notes"),
        clean_string_column("others").alias("other_info"),
        
        # Status - default to active (no hidden field in schema)
        lit(True).alias("is_active"),
        
        # Timestamps
        parse_timestamp("created_on").alias("created_at"),
        parse_timestamp("last_edited").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["supplier_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    supplier_df = transform_supplier()
    log_data_quality("silver_supplier", supplier_df, "supplier_id")
    supplier_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["supplier"])
    print(f"✅ Created {SILVER_TABLES['supplier']}")
except Exception as e:
    print(f"⚠️ Could not create silver_supplier: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. silver_corporate

# CELL ********************

def transform_corporate():
    """
    Transform bronze_plato_corporate to silver_corporate.
    Corporate accounts include insurance companies and corporate clients.
    
    PLATO Corporate Schema:
    - _id: MongoDB ObjectId (primary key)
    - given_id: Corporate code
    - name: Corporate/Insurance name
    - category: Type of corporate (Insurance, Corporate, etc.)
    - email, telephone, fax, address, person
    - url, notes
    - insurance: Flag (1 = insurance company)
    - payment_type, amount, invoice_notes
    - scheme, discounts, specials, restricted, others
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_corporate"])
    
    silver = bronze.select(
        # Primary identifiers
        col("_id").alias("corporate_id"),
        col("given_id").alias("corporate_code"),
        clean_string_column("name").alias("corporate_name"),
        
        # Type classification - use category and insurance flag
        when(col("insurance") == 1, "Insurance")
        .when(upper(col("category")).contains("INSUR"), "Insurance")
        .when(upper(col("category")).contains("CORP"), "Corporate")
        .when(upper(col("category")).contains("GOV"), "Government")
        .otherwise(coalesce(col("category"), lit("Other"))).alias("corporate_type"),
        
        # Category as-is
        col("category").alias("category"),
        
        # Contact
        clean_string_column("person").alias("contact_person"),
        clean_string_column("email").alias("email"),
        regexp_replace(col("telephone"), "[^0-9+]", "").alias("phone"),
        regexp_replace(col("fax"), "[^0-9+]", "").alias("fax"),
        
        # Address
        clean_string_column("address").alias("address"),
        
        # Website
        clean_string_column("url").alias("website"),
        
        # Payment and billing
        clean_string_column("payment_type").alias("payment_type"),
        col("amount").cast(DecimalType(18, 2)).alias("credit_amount"),
        
        # Scheme and discounts
        clean_string_column("scheme").alias("scheme"),
        clean_string_column("discounts").alias("discounts"),
        clean_string_column("specials").alias("specials"),
        clean_string_column("restricted").alias("restricted_items"),
        
        # Notes
        clean_string_column("notes").alias("notes"),
        clean_string_column("invoice_notes").alias("invoice_notes"),
        clean_string_column("others").alias("other_info"),
        
        # Insurance flag
        when(col("insurance") == 1, True).otherwise(False).alias("is_insurance"),
        
        # Status - default to active
        lit(True).alias("is_active"),
        
        # Timestamps
        parse_timestamp("created_on").alias("created_at"),
        parse_timestamp("last_edited").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["corporate_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    corporate_df = transform_corporate()
    log_data_quality("silver_corporate", corporate_df, "corporate_id")
    corporate_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["corporate"])
    print(f"✅ Created {SILVER_TABLES['corporate']}")
except Exception as e:
    print(f"⚠️ Could not create silver_corporate: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. silver_appointment

# CELL ********************

def transform_appointment():
    """
    Transform bronze_plato_appointment to silver_appointment.
    
    Source columns from PLATO appointment schema:
    - _id, patient_id, doctor, title, description
    - starttime, endtime, allDay
    - prefix, dropdown, email, handphone
    - color, code_Top, code_Bottom, code_Left, code_Right, code_Background
    - recur
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_appointment"])
    bronze = standardize_column_names(bronze)
    
    silver = bronze.select(
        # Primary key
        col("_id").alias("appointment_id"),
        
        # Relationships
        col("patient_id").alias("patient_id"),
        clean_string_column("doctor").alias("doctor_id"),
        
        # Appointment details
        clean_string_column("title").alias("appointment_type"),
        clean_string_column("description").alias("description"),
        clean_string_column("prefix").alias("prefix"),
        clean_string_column("dropdown").alias("dropdown_value"),
        
        # Times - PLATO uses starttime/endtime
        parse_timestamp("starttime").alias("start_time"),
        parse_timestamp("endtime").alias("end_time"),
        
        # Derive date from starttime
        to_date(col("starttime")).alias("appointment_date"),
        
        # Calculate duration in minutes
        when(col("starttime").isNotNull() & col("endtime").isNotNull(),
             (unix_timestamp(col("endtime")) - unix_timestamp(col("starttime"))) / 60
        ).cast(IntegerType()).alias("duration_minutes"),
        
        # All day flag
        when(col("allday") == 1, True).otherwise(False).alias("is_all_day"),
        
        # Contact for appointment
        clean_string_column("email").alias("contact_email"),
        col("handphone").cast(StringType()).alias("contact_phone"),
        
        # Visual indicators
        clean_string_column("color").alias("color"),
        clean_string_column("code_top").alias("code_top"),
        clean_string_column("code_bottom").alias("code_bottom"),
        clean_string_column("code_left").alias("code_left"),
        clean_string_column("code_right").alias("code_right"),
        clean_string_column("code_background").alias("code_background"),
        
        # Recurrence
        clean_string_column("recur").alias("recurrence_pattern"),
        when(col("recur").isNotNull() & (trim(col("recur")) != ""), True)
        .otherwise(False).alias("is_recurring"),
        
        # Timestamps
        parse_timestamp("created_on").alias("created_at"),
        clean_string_column("created_by").alias("created_by"),
        parse_timestamp("last_edited").alias("updated_at"),
        clean_string_column("last_edited_by").alias("updated_by")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["appointment_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    appointment_df = transform_appointment()
    log_data_quality("silver_appointment", appointment_df, "appointment_id")
    appointment_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["appointment"])
    print(f"✅ Created {SILVER_TABLES['appointment']}")
except Exception as e:
    print(f"⚠️ Could not create silver_appointment: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. silver_adjustment

# CELL ********************

def transform_adjustment():
    """
    Transform bronze_plato_adjustment to silver_adjustment.
    Inventory adjustments (stock in/out, write-offs, etc.)
    
    PLATO Schema columns:
    - _id, date, remarks, location, invoice, reason, serial
    - finalized, finalized_on, finalized_by
    - inventory.cpu, inventory.qty (flattened from MongoDB)
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_adjustment"])
    
    silver = bronze.select(
        # Primary key
        col("_id").alias("adjustment_id"),
        
        # Date
        col("date").alias("adjustment_date"),
        
        # Location and references
        col("location").alias("location_id"),
        col("invoice").alias("invoice_id"),
        col("serial").alias("serial_number"),
        
        # Reason / Type
        col("reason").alias("adjustment_reason"),
        clean_string_column("remarks").alias("remarks"),
        
        # Inventory info - use backticks for flattened MongoDB fields
        col("`inventory.cpu`").cast(DecimalType(18, 4)).alias("cost_per_unit"),
        col("`inventory.qty`").cast(DecimalType(18, 2)).alias("quantity"),
        
        # Finalized status
        when(col("finalized") == 1, True).otherwise(False).alias("is_finalized"),
        col("finalized_on").alias("finalized_at"),
        col("finalized_by").alias("finalized_by"),
        
        # Timestamps
        col("created_on").alias("created_at"),
        col("created_by").alias("created_by"),
        col("last_edited").alias("updated_at"),
        col("last_edited_by").alias("updated_by")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["adjustment_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    adjustment_df = transform_adjustment()
    log_data_quality("silver_adjustment", adjustment_df, "adjustment_id")
    adjustment_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["adjustment"])
    print(f"✅ Created {SILVER_TABLES['adjustment']}")
except Exception as e:
    print(f"⚠️ Could not create silver_adjustment: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. silver_deliveryorder

# CELL ********************

def transform_deliveryorder():
    """
    Transform bronze_plato_deliveryorder to silver_deliveryorder.
    Purchase orders and goods received.
    
    PLATO Schema columns:
    - _id, date, do, serial, invoice, location, supplier
    - sub_total, total, discount, no_gst
    - paid, mode, payment_ref
    - po, remarks, attachments
    - finalized, finalized_on, finalized_by
    - inventory.batch, inventory.do_id (flattened from MongoDB)
    - created_on, created_by, last_edited, last_edited_by
    """
    
    bronze = spark.table(BRONZE_TABLES["plato_deliveryorder"])
    
    silver = bronze.select(
        # Primary key
        col("_id").alias("delivery_order_id"),
        col("do").alias("do_number"),
        col("serial").alias("serial_number"),
        
        # References
        col("supplier").alias("supplier_id"),
        col("location").alias("location_id"),
        col("invoice").alias("invoice_id"),
        col("po").alias("purchase_order_ref"),
        
        # Date
        col("date").alias("delivery_date"),
        
        # Amounts
        col("sub_total").cast(DecimalType(18, 4)).alias("subtotal"),
        col("total").cast(DecimalType(18, 4)).alias("total_amount"),
        coalesce(col("discount"), lit(0)).cast(DecimalType(18, 4)).alias("discount_amount"),
        
        # GST flag
        when(col("no_gst") == 1, False).otherwise(True).alias("gst_applicable"),
        
        # Payment info
        when(col("paid") == 1, True).otherwise(False).alias("is_paid"),
        col("mode").alias("payment_mode"),
        col("payment_ref").alias("payment_reference"),
        
        # Inventory info - use backticks for flattened MongoDB fields
        col("`inventory.batch`").alias("inventory_batch"),
        col("`inventory.do_id`").alias("inventory_do_id"),
        
        # Additional info
        clean_string_column("remarks").alias("remarks"),
        col("attachments").alias("attachments"),
        
        # Finalized status
        when(col("finalized") == 1, True).otherwise(False).alias("is_finalized"),
        col("finalized_on").alias("finalized_at"),
        col("finalized_by").alias("finalized_by"),
        
        # Timestamps
        col("created_on").alias("created_at"),
        col("created_by").alias("created_by"),
        col("last_edited").alias("updated_at"),
        col("last_edited_by").alias("updated_by")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["delivery_order_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    deliveryorder_df = transform_deliveryorder()
    log_data_quality("silver_deliveryorder", deliveryorder_df, "delivery_order_id")
    deliveryorder_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["deliveryorder"])
    print(f"✅ Created {SILVER_TABLES['deliveryorder']}")
except Exception as e:
    print(f"⚠️ Could not create silver_deliveryorder: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# # XERO Tables

# MARKDOWN ********************

# ## 10. silver_xero_invoices

# CELL ********************

def transform_xero_invoices():
    """
    Transform bronze_xero_invoices to silver_xero_invoices.
    ACCREC = Accounts Receivable (sales)
    ACCPAY = Accounts Payable (bills)
    
    XERO Bronze structure has nested 'records.' prefix for all fields.
    e.g., records.invoice_id, records.type, records.contact.contact_id
    """
    
    bronze = spark.table(BRONZE_TABLES["xero_invoices"])
    
    silver = bronze.select(
        # Use backticks for nested column names with dots
        col("`records.invoice_id`").alias("invoice_id"),
        col("`records.invoice_number`").alias("invoice_number"),
        col("`records.contact.contact_id`").alias("contact_id"),
        col("`records.contact.name`").alias("contact_name"),
        
        # Type
        col("`records.type`").alias("type"),  # ACCREC or ACCPAY
        when(col("`records.type`") == "ACCREC", "Sales Invoice")
        .when(col("`records.type`") == "ACCPAY", "Bill")
        .otherwise("Other").alias("invoice_type_desc"),
        
        # Dates
        col("`records.date`").alias("invoice_date"),
        col("`records.due_date`").alias("due_date"),
        
        # Amounts
        col("`records.sub_total`").cast(DecimalType(18, 2)).alias("subtotal"),
        col("`records.total_tax`").cast(DecimalType(18, 2)).alias("total_tax"),
        col("`records.total`").cast(DecimalType(18, 2)).alias("total"),
        col("`records.amount_due`").cast(DecimalType(18, 2)).alias("amount_due"),
        col("`records.amount_paid`").cast(DecimalType(18, 2)).alias("amount_paid"),
        col("`records.amount_credited`").cast(DecimalType(18, 2)).alias("amount_credited"),
        
        # Currency
        coalesce(col("`records.currency_code`"), lit("SGD")).alias("currency_code"),
        coalesce(col("`records.currency_rate`"), lit(1.0)).cast(DecimalType(18, 6)).alias("currency_rate"),
        
        # Status
        col("`records.status`").alias("status"),
        
        # Reference
        col("`records.reference`").alias("reference"),
        
        # Metadata
        col("tenant_id").alias("tenant_id"),
        col("extracted_at").alias("extracted_at"),
        
        # Timestamps
        col("`records.updated_date_utc`").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["invoice_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    xero_invoices_df = transform_xero_invoices()
    log_data_quality("silver_xero_invoices", xero_invoices_df, "invoice_id")
    xero_invoices_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["xero_invoices"])
    print(f"✅ Created {SILVER_TABLES['xero_invoices']}")
except Exception as e:
    print(f"⚠️ Could not create silver_xero_invoices: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. silver_xero_payments

# CELL ********************

def transform_xero_payments():
    """
    Transform bronze_xero_payments to silver_xero_payments.
    
    XERO Bronze structure has nested 'records.' prefix for all fields.
    """
    
    bronze = spark.table(BRONZE_TABLES["xero_payments"])
    
    silver = bronze.select(
        col("`records.payment_id`").alias("payment_id"),
        col("`records.invoice.invoice_id`").alias("invoice_id"),
        col("`records.account.account_id`").alias("account_id"),
        
        # Date
        col("`records.date`").alias("payment_date"),
        
        # Amount
        col("`records.amount`").cast(DecimalType(18, 2)).alias("amount"),
        
        # Currency
        coalesce(col("`records.account.currency_code`"), lit("SGD")).alias("currency_code"),
        coalesce(col("`records.currency_rate`"), lit(1.0)).cast(DecimalType(18, 6)).alias("currency_rate"),
        
        # Payment type
        col("`records.payment_type`").alias("payment_type"),
        
        # Status
        col("`records.status`").alias("status"),
        
        # Reference
        col("`records.reference`").alias("reference"),
        
        # Metadata
        col("tenant_id").alias("tenant_id"),
        col("extracted_at").alias("extracted_at"),
        
        # Timestamps
        col("`records.updated_date_utc`").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["payment_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    xero_payments_df = transform_xero_payments()
    log_data_quality("silver_xero_payments", xero_payments_df, "payment_id")
    xero_payments_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["xero_payments"])
    print(f"✅ Created {SILVER_TABLES['xero_payments']}")
except Exception as e:
    print(f"⚠️ Could not create silver_xero_payments: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. silver_xero_bank_transactions

# CELL ********************

def transform_xero_bank_transactions():
    """
    Transform bronze_xero_bank_transactions to silver_xero_bank_transactions.
    
    XERO Bronze structure has nested 'records.' prefix for all fields.
    """
    
    bronze = spark.table(BRONZE_TABLES["xero_bank_transactions"])
    
    silver = bronze.select(
        col("`records.bank_transaction_id`").alias("bank_transaction_id"),
        col("`records.bank_account.account_id`").alias("bank_account_id"),
        col("`records.contact.contact_id`").alias("contact_id"),
        
        # Type (RECEIVE, SPEND, etc.)
        col("`records.type`").alias("type"),
        when(col("`records.type`").isin("RECEIVE", "RECEIVE-OVERPAYMENT", "RECEIVE-PREPAYMENT"), "Inflow")
        .when(col("`records.type`").isin("SPEND", "SPEND-OVERPAYMENT", "SPEND-PREPAYMENT"), "Outflow")
        .otherwise("Other").alias("flow_direction"),
        
        # Date
        col("`records.date`").alias("transaction_date"),
        
        # Amounts
        col("`records.sub_total`").cast(DecimalType(18, 2)).alias("subtotal"),
        col("`records.total_tax`").cast(DecimalType(18, 2)).alias("total_tax"),
        col("`records.total`").cast(DecimalType(18, 2)).alias("total"),
        
        # Currency
        coalesce(col("`records.currency_code`"), lit("SGD")).alias("currency_code"),
        
        # Status
        col("`records.status`").alias("status"),
        col("`records.is_reconciled`").cast(BooleanType()).alias("is_reconciled"),
        
        # Reference
        col("`records.reference`").alias("reference"),
        
        # Metadata
        col("tenant_id").alias("tenant_id"),
        col("extracted_at").alias("extracted_at"),
        
        # Timestamps
        col("`records.updated_date_utc`").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["bank_transaction_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    xero_bank_df = transform_xero_bank_transactions()
    log_data_quality("silver_xero_bank_transactions", xero_bank_df, "bank_transaction_id")
    xero_bank_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["xero_bank_transactions"])
    print(f"✅ Created {SILVER_TABLES['xero_bank_transactions']}")
except Exception as e:
    print(f"⚠️ Could not create silver_xero_bank_transactions: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. silver_xero_accounts

# CELL ********************

def transform_xero_accounts():
    """
    Transform bronze_xero_accounts to silver_xero_accounts.
    Chart of Accounts.
    
    XERO Bronze structure has nested 'records.' prefix for all fields.
    """
    
    bronze = spark.table(BRONZE_TABLES["xero_accounts"])
    
    silver = bronze.select(
        col("`records.account_id`").alias("account_id"),
        col("`records.code`").alias("account_code"),
        col("`records.name`").alias("account_name"),
        
        # Type and class
        col("`records.type`").alias("account_type"),
        col("`records._class`").alias("account_class"),
        
        # Tax
        col("`records.tax_type`").alias("tax_type"),
        
        # Status
        col("`records.status`").alias("status"),
        coalesce(col("`records.enable_payments_to_account`"), lit(False)).cast(BooleanType()).alias("enable_payments"),
        
        # System account flag
        col("`records.system_account`").alias("system_account_type"),
        
        # Bank account details (if applicable)
        col("`records.bank_account_number`").alias("bank_account_number"),
        col("`records.bank_account_type`").alias("bank_account_type"),
        coalesce(col("`records.currency_code`"), lit("SGD")).alias("currency_code"),
        
        # Metadata
        col("tenant_id").alias("tenant_id"),
        col("extracted_at").alias("extracted_at"),
        
        # Timestamps
        col("`records.updated_date_utc`").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["account_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    xero_accounts_df = transform_xero_accounts()
    log_data_quality("silver_xero_accounts", xero_accounts_df, "account_id")
    xero_accounts_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["xero_accounts"])
    print(f"✅ Created {SILVER_TABLES['xero_accounts']}")
except Exception as e:
    print(f"⚠️ Could not create silver_xero_accounts: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 14. silver_xero_contacts

# CELL ********************

def transform_xero_contacts():
    """
    Transform bronze_xero_contacts to silver_xero_contacts.
    Customers and Suppliers in XERO.
    
    XERO Bronze structure has nested 'records.' prefix for all fields.
    """
    
    bronze = spark.table(BRONZE_TABLES["xero_contacts"])
    
    silver = bronze.select(
        col("`records.contact_id`").alias("contact_id"),
        col("`records.name`").alias("contact_name"),
        
        # Type
        when(col("`records.is_customer`") == True, "Customer")
        .when(col("`records.is_supplier`") == True, "Supplier")
        .otherwise("Other").alias("contact_type"),
        col("`records.is_customer`").cast(BooleanType()).alias("is_customer"),
        col("`records.is_supplier`").cast(BooleanType()).alias("is_supplier"),
        
        # Contact details
        col("`records.email_address`").alias("email"),
        col("`records.first_name`").alias("first_name"),
        col("`records.last_name`").alias("last_name"),
        
        # Status
        col("`records.contact_status`").alias("status"),
        
        # Default currency
        coalesce(col("`records.default_currency`"), lit("SGD")).alias("default_currency"),
        
        # Metadata
        col("tenant_id").alias("tenant_id"),
        col("extracted_at").alias("extracted_at"),
        
        # Timestamps
        col("`records.updated_date_utc`").alias("updated_at")
    )
    
    # Add audit columns
    silver = add_audit_columns(silver)
    
    # Deduplicate
    silver = deduplicate(silver, ["contact_id"], "updated_at")
    
    return silver

# Execute transformation
try:
    xero_contacts_df = transform_xero_contacts()
    log_data_quality("silver_xero_contacts", xero_contacts_df, "contact_id")
    xero_contacts_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLES["xero_contacts"])
    print(f"✅ Created {SILVER_TABLES['xero_contacts']}")
except Exception as e:
    print(f"⚠️ Could not create silver_xero_contacts: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# # Summary

# MARKDOWN ********************

# ## Transformation Summary

# CELL ********************

# Display summary of all silver tables
print("\n" + "="*80)
print("SILVER LAYER TRANSFORMATION SUMMARY")
print("="*80)

tables_created = []
tables_failed = []

for key, table_name in SILVER_TABLES.items():
    try:
        count = spark.table(table_name).count()
        tables_created.append((table_name, count))
        print(f"✅ {table_name}: {count:,} rows")
    except Exception as e:
        tables_failed.append((table_name, str(e)[:50]))
        print(f"❌ {table_name}: FAILED - {str(e)[:50]}")

print("\n" + "-"*80)
print(f"Summary: {len(tables_created)} tables created, {len(tables_failed)} failed")
print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Column Mapping Reference
# 
# ### PLATO Source Column Mappings
# 
# | Table | Source Columns | Silver Columns |
# |-------|----------------|----------------|
# | patient | `_id`, `given_id`, `name`, `nric`, `dob`, `sex` | `patient_id`, `patient_code`, `full_name`, `nric_masked`, `date_of_birth`, `gender` |
# | invoice | `_id`, `patient_id`, `date`, `total`, `status` | `invoice_id`, `patient_id`, `invoice_date`, `total`, `status` |
# | payment | `_id`, `invoice_id`, `mode`, `amount` | `payment_id`, `invoice_id`, `payment_method`, `payment_amount` |
# | inventory | `_id`, `given_id`, `name`, `selling_price`, `cost_price`, `qty` | `product_id`, `product_code`, `product_name`, `selling_price`, `cost_price`, `quantity_on_hand` |
# | supplier | `_id`, `name`, `telephone`, `email` | `supplier_id`, `supplier_name`, `telephone`, `email` |
# | corporate | `_id`, `given_id`, `name`, `insurance` | `corporate_id`, `corporate_code`, `corporate_name`, `is_insurance` |
# | appointment | `_id`, `patient_id`, `doctor`, `starttime`, `endtime` | `appointment_id`, `patient_id`, `doctor_id`, `start_time`, `end_time` |
# | adjustment | `_id`, `date`, `reason`, `inventory` | `adjustment_id`, `adjustment_date`, `adjustment_reason`, `inventory_items_json` |
# | deliveryorder | `_id`, `date`, `supplier`, `total`, `paid` | `delivery_order_id`, `order_date`, `supplier_id`, `total`, `is_paid` |

