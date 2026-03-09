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

# # Gold Layer ETL Transformations
# ## Healthcare CFO Dashboard - Data Pipeline
# 
# **Purpose**: ETL scripts to transform silver layer data into gold layer dimensional model
# 
# **Source**: `lh_bnj_silver.plato` schema
# 
# **Target**: `lh_bnj_gold.gold` schema
# 
# **Execution Order**:
# 1. Dimension tables (can run in parallel)
# 2. Fact tables (depends on dimensions)
# 3. Aggregate tables (depends on facts)
# 4. Health score tables (depends on aggregates)

# MARKDOWN ********************

# ## Table of Contents
# 1. [Setup](#setup)
# 2. [Dimension Table Transformations](#dimensions)
# 3. [Fact Table Transformations](#facts)
# 4. [Aggregate Table Transformations](#aggregates)
# 5. [Health Score Calculations](#health-scores)
# 6. [Orchestration](#orchestration)

# MARKDOWN ********************

# ## 1. Setup <a id='setup'></a>

# CELL ********************

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
import hashlib

# Set catalog and schemas
# spark.sql("USE CATALOG 'lh_bnj_gold'")

# Define source and target schemas
SILVER_SCHEMA = "lh_bnj_silver.plato"
GOLD_SCHEMA = "lh_bnj_gold.gold"

print("✅ Environment configured")
print(f"Source: {SILVER_SCHEMA}")
print(f"Target: {GOLD_SCHEMA}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Utility functions

def generate_surrogate_key(*columns):
    """
    Generate surrogate key using hash of natural key columns
    """
    return abs(hash(concat_ws('|', *columns))) % (10 ** 12)

def get_age_group(age_col):
    """
    Categorize age into groups
    """
    return when(age_col < 13, 'Child') \
           .when((age_col >= 13) & (age_col < 18), 'Teenager') \
           .when((age_col >= 18) & (age_col < 65), 'Adult') \
           .when(age_col >= 65, 'Senior') \
           .otherwise('Unknown')

def calculate_age(dob_col, reference_date=None):
    """
    Calculate age from date of birth
    """
    if reference_date is None:
        reference_date = current_date()
    return floor(datediff(reference_date, dob_col) / 365.25)

def get_health_status(score_col):
    """
    Convert health score to traffic light status
    """
    return when(score_col >= 75, 'Green') \
           .when((score_col >= 26) & (score_col < 75), 'Yellow') \
           .when(score_col < 26, 'Red') \
           .otherwise('Unknown')

def get_health_status_color(status_col):
    """
    Get color code for health status
    """
    return when(status_col == 'Green', '#00FF00') \
           .when(status_col == 'Yellow', '#FFFF00') \
           .when(status_col == 'Red', '#FF0000') \
           .otherwise('#CCCCCC')

print("✅ Utility functions loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Dimension Table Transformations <a id='dimensions'></a>

# CELL ********************

# ETL 1: DIM_DATE - Generate date dimension

def generate_date_dimension(start_date='2020-01-01', end_date='2030-12-31'):
    """
    Generate complete date dimension table
    """
    # Create date range
    from datetime import datetime, timedelta
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    dates = []
    current = start
    
    while current <= end:
        dates.append((current,))
        current += timedelta(days=1)
    
    # Create DataFrame
    date_df = spark.createDataFrame(dates, ['full_date'])
    
    # Generate all date attributes
    dim_date = date_df.select(
        # Date key: YYYYMMDD format
        date_format('full_date', 'yyyyMMdd').cast('int').alias('date_key'),
        col('full_date'),
        
        # Date components
        year('full_date').alias('year'),
        quarter('full_date').alias('quarter'),
        concat(lit('Q'), quarter('full_date')).alias('quarter_name'),
        month('full_date').alias('month'),
        date_format('full_date', 'MMMM').alias('month_name'),
        date_format('full_date', 'MMM').alias('month_abbr'),
        weekofyear('full_date').alias('week_of_year'),
        dayofmonth('full_date').alias('day_of_month'),
        dayofweek('full_date').alias('day_of_week'),
        date_format('full_date', 'EEEE').alias('day_of_week_name'),
        dayofyear('full_date').alias('day_of_year'),
        
        # Business attributes
        when(dayofweek('full_date').isin([1, 7]), True).otherwise(False).alias('is_weekend'),
        lit(False).alias('is_holiday'),  # To be updated separately
        lit(None).cast('string').alias('holiday_name'),
        
        # Fiscal year (assuming April-March fiscal year for Singapore)
        when(month('full_date') >= 4, year('full_date')).otherwise(year('full_date') - 1).alias('fiscal_year'),
        when(month('full_date') >= 4, 
             (month('full_date') - 3 + 11) / 3
        ).otherwise(
             (month('full_date') + 9) / 3
        ).cast('int').alias('fiscal_quarter'),
        when(month('full_date') >= 4, month('full_date') - 3).otherwise(month('full_date') + 9).alias('fiscal_month'),
        
        # Formatted strings
        date_format('full_date', 'yyyy-MM').alias('year_month'),
        concat(year('full_date'), lit('-Q'), quarter('full_date')).alias('year_quarter'),
        date_format('full_date', 'dd MMM yyyy').alias('date_display')
    )
    
    # Write to Delta table
    dim_date = dim_date.withColumn("full_date", col("full_date").cast("date"))
    dim_date.write \
        .mode('overwrite') \
        .format('delta') \
        .save(f"abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/102823e0-12f1-4ca5-b61b-a2df5d75beb2/Tables/gold/dim_date")
    
    row_count = dim_date.count()
    print(f"✅ DIM_DATE created with {row_count:,} rows")
    return dim_date

# Execute
dim_date = generate_date_dimension()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ETL 2: DIM_PATIENT - Load patient dimension with SCD Type 2

def load_dim_patient():
    """
    Load patient dimension from silver layer
    Implements SCD Type 2 for tracking changes
    """
    # Read from silver
    silver_patient = spark.table(f"{SILVER_SCHEMA}.silver_patient")
    
    # Calculate age
    df = silver_patient.withColumn(
        'age', 
        calculate_age(col('date_of_birth'))
    ).withColumn(
        'age_group',
        get_age_group(col('age'))
    )
    
    # Generate surrogate key
    dim_patient = df.select(
        abs(hash(concat(col('patient_id'), coalesce(col('last_edited'), col('created_on'))))).alias('patient_key'),
        
        # Natural key
        col('patient_id'),
        
        # Patient demographics
        col('given_id'),
        col('patient_name'),
        col('title'),
        col('nric'),
        col('nric_type'),
        col('date_of_birth'),
        col('age'),
        col('age_group'),
        col('sex'),
        col('nationality'),
        col('marital_status'),
        
        # Contact information
        col('email'),
        col('telephone'),
        col('telephone2'),
        col('telephone3'),
        col('address'),
        col('postal_code'),
        col('unit_no'),
        
        # Medical information
        col('allergies_select'),
        col('allergies'),
        col('food_allergies_select'),
        col('food_allergies'),
        col('g6pd_status'),
        col('alerts'),
        
        # Other attributes
        col('occupation'),
        col('do_not_disturb'),
        col('referred_by'),
        col('assigned_doctor'),
        col('tags'),
        col('recalls'),
        col('notes'),
        
        # SCD Type 2 attributes
        coalesce(col('created_on'), current_timestamp()).alias('effective_date'),
        lit(None).cast('timestamp').alias('end_date'),
        lit(True).alias('is_current'),
        
        # Audit fields
        lit('PLATO').alias('source_system'),
        col('created_on'),
        col('created_by'),
        col('last_edited'),
        col('last_edited_by'),
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    
    # Write to Delta table
    dim_patient = dim_patient.withColumn("patient_key", col("patient_key").cast("long"))
    dim_patient = dim_patient.withColumn("age", col("age").cast("int"))

    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
    
    dim_patient.write \
        .mode('overwrite') \
        .format('delta') \
        .partitionBy('is_current') \
        .saveAsTable(f"{GOLD_SCHEMA}.dim_patient")
    
    row_count = dim_patient.count()
    print(f"✅ DIM_PATIENT created with {row_count:,} rows")
    return dim_patient

# Execute
dim_patient = load_dim_patient()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ETL 3: DIM_INVENTORY - Load inventory dimension with SCD Type 2

def load_dim_inventory():
    """
    Load inventory dimension from silver layer
    """
    silver_inventory = spark.table(f"{SILVER_SCHEMA}.silver_inventory")
    
    dim_inventory = silver_inventory.select(
        abs(hash(concat(col('inventory_id'), coalesce(col('last_edited'), col('created_on'))))).alias('inventory_key'),
        
        # Natural key
        col('inventory_id'),
        
        # Item identification
        col('given_id'),
        col('item_name'),
        col('description'),
        col('category'),
        col('inventory_type'),
        
        # Pricing
        col('selling_price'),
        col('cost_price'),
        col('min_price'),
        col('package_original_price'),
        
        # Stock management
        col('track_stock'),
        col('unit'),
        col('order_unit'),
        col('pack_size'),
        
        # Medication-specific
        col('dosage'),
        col('dosage_usage'),
        col('dosage_dose'),
        col('dosage_unit'),
        col('dosage_frequency'),
        col('dosage_duration'),
        col('precautions'),
        
        # Supplier and manufacturer
        col('supplier_id'),
        col('manufacturer'),
        
        # Business rules
        col('is_redeemable'),
        col('redemption_count'),
        col('is_fixed_price'),
        col('no_discount'),
        col('has_expiry_after_dispensing'),
        col('is_hidden'),
        
        # SCD Type 2 attributes
        coalesce(col('created_on'), current_timestamp()).alias('effective_date'),
        lit(None).cast('timestamp').alias('end_date'),
        lit(True).alias('is_current'),
        
        # Audit fields
        lit('PLATO').alias('source_system'),
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    
    # Write to Delta table
    dim_inventory = dim_inventory.withColumn("inventory_key", col("inventory_key").cast("long"))
    dim_inventory.write \
        .mode('overwrite') \
        .format('delta') \
        .partitionBy('category', 'is_current') \
        .saveAsTable(f"{GOLD_SCHEMA}.dim_inventory")
    
    row_count = dim_inventory.count()
    print(f"✅ DIM_INVENTORY created with {row_count:,} rows")
    return dim_inventory

# Execute
dim_inventory = load_dim_inventory()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ETL 4: DIM_CORPORATE - Load corporate dimension with SCD Type 2

def load_dim_corporate():
    """
    Load corporate/insurance dimension from silver layer
    """
    silver_corporate = spark.table(f"{SILVER_SCHEMA}.silver_corporate")
    
    dim_corporate = silver_corporate.select(
        abs(hash(concat(col('corporate_id'), coalesce(col('last_edited'), col('created_on'))))).alias('corporate_key'),
        
        # Natural key
        col('corporate_id'),
        
        # Corporate details
        col('given_id'),
        col('corporate_name'),
        col('category'),
        col('is_insurance'),
        
        # Contact information
        col('contact_person'),
        col('email'),
        col('telephone'),
        col('fax'),
        col('address'),
        col('url'),
        
        # Payment terms
        col('default_amount'),
        col('payment_type'),
        col('scheme'),
        
        # Policy details (JSON)
        col('discounts_json'),
        col('specials_json'),
        col('restricted_json'),
        
        # SCD Type 2 attributes
        coalesce(col('created_on'), current_timestamp()).alias('effective_date'),
        lit(None).cast('timestamp').alias('end_date'),
        lit(True).alias('is_current'),
        
        # Audit fields
        lit('PLATO').alias('source_system'),
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    dim_corporate = dim_corporate.withColumn("corporate_key", col("corporate_key").cast("long"))
    # Write to Delta table
    dim_corporate.write \
        .mode('overwrite') \
        .format('delta') \
        .partitionBy('is_insurance', 'is_current') \
        .saveAsTable(f"{GOLD_SCHEMA}.dim_corporate")
    
    row_count = dim_corporate.count()
    print(f"✅ DIM_CORPORATE created with {row_count:,} rows")
    return dim_corporate

# Execute
dim_corporate = load_dim_corporate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ETL 5: DIM_LOCATION - Load location dimension

def load_dim_location():
    """
    Load location dimension
    Extract unique locations from base_invoice
    """
    silver_base_invoice = spark.table(f"{SILVER_SCHEMA}.silver_base_invoice")
    
    # Get distinct locations
    locations = silver_base_invoice.select('location').distinct().filter(col('location').isNotNull())
    
    dim_location = locations.select(
        abs(hash(col('location'))).alias('location_key'),
        col('location').alias('location_id'),
        col('location').alias('location_name'),
        col('location').alias('location_code'),
        lit(None).cast('string').alias('address'),
        lit(None).cast('string').alias('city'),
        lit(None).cast('string').alias('postal_code'),
        lit('Singapore').alias('country'),
        lit(True).alias('is_active'),
        lit('Main').alias('location_type'),
        
        # SCD Type 2 attributes
        current_timestamp().alias('effective_date'),
        lit(None).cast('timestamp').alias('end_date'),
        lit(True).alias('is_current'),
        
        # Audit fields
        lit('PLATO').alias('source_system'),
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    
    # Write to Delta table
    dim_location.write \
        .mode('overwrite') \
        .format('delta') \
        .saveAsTable(f"{GOLD_SCHEMA}.dim_location")
    
    row_count = dim_location.count()
    print(f"✅ DIM_LOCATION created with {row_count:,} rows")
    return dim_location

# Execute
dim_location = load_dim_location()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ETL 6: DIM_PAYMENT_MODE - Load payment mode dimension

def load_dim_payment_mode():
    """
    Load payment mode dimension
    Extract unique payment modes from payment table
    """
    silver_payment = spark.table(f"{SILVER_SCHEMA}.silver_payment")
    
    # Get distinct payment modes
    payment_modes = silver_payment.select('payment_mode').distinct().filter(col('payment_mode').isNotNull())
    
    # Add row number for surrogate key
    window = Window.orderBy('payment_mode')
    
    dim_payment_mode = payment_modes.withColumn('payment_mode_key', row_number().over(window)).select(
        col('payment_mode_key'),
        col('payment_mode').alias('payment_mode_code'),
        col('payment_mode').alias('payment_mode_name'),
        
        # Categorize payment modes
        when(col('payment_mode').isin(['CASH', 'Cash']), 'Cash')
        .when(col('payment_mode').rlike('(?i)card|credit|debit|visa|master'), 'Card')
        .when(col('payment_mode').rlike('(?i)paynow|grabpay|nets|bank|transfer'), 'Digital')
        .when(col('payment_mode').rlike('(?i)insurance|medisave|medishield'), 'Insurance')
        .when(col('payment_mode').rlike('(?i)corporate|company'), 'Corporate')
        .otherwise('Other').alias('payment_mode_category'),
        
        # Attributes
        when(col('payment_mode').rlike('(?i)card|bank|transfer|cheque'), True).otherwise(False).alias('requires_reference'),
        when(col('payment_mode').rlike('(?i)card|paynow|grabpay|nets|bank|transfer'), True).otherwise(False).alias('is_electronic'),
        lit(True).alias('is_active'),
        
        # Audit fields
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    
    # Write to Delta table
    dim_payment_mode.write \
        .mode('overwrite') \
        .format('delta') \
        .saveAsTable(f"{GOLD_SCHEMA}.dim_payment_mode")
    
    row_count = dim_payment_mode.count()
    print(f"✅ DIM_PAYMENT_MODE created with {row_count:,} rows")
    return dim_payment_mode

# Execute
dim_payment_mode = load_dim_payment_mode()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Fact Table Transformations <a id='facts'></a>

# CELL ********************

# ETL 7: FACT_INVOICE_HEADER - Load invoice header facts

def load_fact_invoice_header():
    """
    Load invoice header fact table from silver layer
    """
    # Read silver tables
    silver_base_invoice = spark.table(f"{SILVER_SCHEMA}.silver_base_invoice")
    silver_corporate_invoice = spark.table(f"{SILVER_SCHEMA}.silver_corporate_invoice")
    
    # Read dimension lookups
    dim_patient = spark.table(f"{GOLD_SCHEMA}.dim_patient").filter(col('is_current') == True)
    dim_location = spark.table(f"{GOLD_SCHEMA}.dim_location").filter(col('is_current') == True)
    dim_corporate = spark.table(f"{GOLD_SCHEMA}.dim_corporate").filter(col('is_current') == True)
    dim_date = spark.table(f"{GOLD_SCHEMA}.dim_date")
    
    # Left join corporate invoices
    invoices = silver_base_invoice.join(
        silver_corporate_invoice,
        'invoice_id',
        'left'
    )
    
    # Join with dimensions
    fact = invoices \
        .join(dim_patient, invoices.patient_id == dim_patient.patient_id, 'left') \
        .join(dim_location, invoices.location == dim_location.location_id, 'left') \
        .join(dim_corporate, invoices.corp_id == dim_corporate.corporate_id, 'left') \
        .join(dim_date.alias('d1'), to_date(invoices.date) == col('d1.full_date'), 'left') \
        .join(dim_date.alias('d2'), to_date(invoices.created_on) == col('d2.full_date'), 'left') \
        .join(dim_date.alias('d3'), to_date(invoices.finalized_on) == col('d3.full_date'), 'left')
    
    fact_invoice_header = fact.select(
        abs(hash(invoices.invoice_id)).alias('invoice_fact_key'),
        
        # Natural key
        invoices.invoice_id,
        
        # Dimension foreign keys
        col('d1.date_key').alias('invoice_date_key'),
        col('d2.date_key').alias('created_date_key'),
        col('d3.date_key').alias('finalized_date_key'),
        
        col('patient_key'),
        col('location_key'),
        col('corporate_key'),
        
        # Invoice identifiers
        invoices.invoice.alias('invoice_number'),
        invoices.invoice_prefix,
        concat(coalesce(invoices.invoice_prefix, lit('')), invoices.invoice.cast('string')).alias('full_invoice_number'),
        invoices.session,
        
        # Invoice classification
        when(invoices.corp_id.isNotNull(), 'Corporate').otherwise('Regular').alias('invoice_type'),
        invoices.scheme,
        invoices.rate,
        
        # Invoice amounts
        invoices.sub_total.cast('decimal(18,2)').alias('sub_total_amount'),
        invoices.tax.cast('decimal(18,2)').alias('tax_amount'),
        invoices.adj_amount.cast('decimal(18,2)').alias('adjustment_amount'),
        invoices.total.cast('decimal(18,2)').alias('total_amount'),
        
        # Tax flags
        when(invoices.no_gst == 1, True).otherwise(False).alias('no_gst_flag'),
        
        # Invoice status
        invoices.status,
        when(invoices.finalized == 1, True).otherwise(False).alias('is_finalized'),
        when(coalesce(invoices['void'], lit(0)) == 1, True).otherwise(False).alias('is_void'),
        when(invoices.adj == 1, True).otherwise(False).alias('is_adjustment'),
        when((invoices.cndn == 1) & (invoices.adj_amount < 0), True).otherwise(False).alias('is_credit_note'),
        when((invoices.cndn == 1) & (invoices.adj_amount > 0), True).otherwise(False).alias('is_debit_note'),
        
        # Corporate invoice specifics
        silver_corporate_invoice.corp_amount.cast('decimal(18,2)'),
        silver_corporate_invoice.corp_ref.alias('corp_reference'),
        
        # Timestamps
        invoices.created_on,
        invoices.finalized_on,
        invoices.void_on,
        
        # User tracking
        invoices.created_by,
        invoices.void_reason,
        
        # Audit fields
        lit('PLATO').alias('source_system'),
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    fact_invoice_header = fact_invoice_header.withColumn("void_on", col("void_on").cast("timestamp"))
    fact_invoice_header = fact_invoice_header.withColumnRenamed("session", "session_id")
    fact_invoice_header = (
        fact_invoice_header
        .withColumn("invoice_fact_key", col("invoice_fact_key").cast("long"))
        .withColumn("invoice_id", col("invoice_id").cast("string"))
        .withColumn("invoice_date_key", col("invoice_date_key").cast("int"))
        .withColumn("created_date_key", col("created_date_key").cast("int"))
        .withColumn("patient_key", col("patient_key").cast("long"))
        .withColumn("location_key", col("location_key").cast("long"))
    )
    # Write to Delta table
    fact_invoice_header.write \
        .mode('overwrite') \
        .format('delta') \
        .partitionBy('invoice_date_key') \
        .saveAsTable(f"{GOLD_SCHEMA}.fact_invoice_header")
    
    row_count = fact_invoice_header.count()
    print(f"✅ FACT_INVOICE_HEADER created with {row_count:,} rows")
    return fact_invoice_header

# Execute
fact_invoice_header = load_fact_invoice_header()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ETL 8: FACT_INVOICE_LINE - Load invoice line item facts

def load_fact_invoice_line():
    """
    Load invoice line item fact table
    """
    # Read silver tables
    silver_item_invoice = spark.table(f"{SILVER_SCHEMA}.silver_item_invoice")
    fact_invoice_header = spark.table(f"{GOLD_SCHEMA}.fact_invoice_header")
    
    # Read dimension lookups
    dim_inventory = spark.table(f"{GOLD_SCHEMA}.dim_inventory").filter(col('is_current') == True)
    dim_date = spark.table(f"{GOLD_SCHEMA}.dim_date")
    
    # Add line number within each invoice
    window = Window.partitionBy('invoice_id').orderBy('item_id')
    
    items_with_line_num = silver_item_invoice.withColumn(
        'line_number',
        row_number().over(window)
    )
    
    # Join with fact header and dimensions
    fact = (
        items_with_line_num
        .join(fact_invoice_header, 'invoice_id', 'inner')
        .join(
            dim_inventory,
            (items_with_line_num.given_id == dim_inventory.given_id) |
            (items_with_line_num.item_name == dim_inventory.item_name),
            'left'
        )
    )
    
    # -------------------------------------------
    # SAFE MARGIN CALCULATIONS
    # -------------------------------------------
    
    numerator = (
        (items_with_line_num.qty * items_with_line_num.unit_price * (1 - items_with_line_num.discount / 100))
        -
        (items_with_line_num.qty * items_with_line_num.cost_price)
    )

    denominator = (
        items_with_line_num.qty * items_with_line_num.unit_price * (1 - items_with_line_num.discount / 100)
    )

    line_margin_percentage_expr = (
        when(denominator == 0, None)
        .otherwise((numerator / denominator) * 100)
        .cast("decimal(5,2)")
        .alias("line_margin_percentage")
    )

    # Final projection
    fact_invoice_line = fact.select(
        # Surrogate key
        abs(hash(concat(items_with_line_num.invoice_id, items_with_line_num.item_id)))
            .alias('invoice_line_fact_key'),

        # Foreign keys
        col('invoice_fact_key'),
        items_with_line_num.invoice_id,
        coalesce(col('inventory_key'), lit(-1)).alias('inventory_key'),
        col('invoice_date_key'),
        col('patient_key'),
        col('location_key'),

        # Line item identifiers
        items_with_line_num.item_id,
        col('line_number'),

        # Item details
        items_with_line_num.item_name,
        items_with_line_num.category,
        items_with_line_num.description,

        # Batch info
        items_with_line_num.batch_batch.alias('batch_number'),
        to_date(from_unixtime(items_with_line_num.batch_expiry)).alias('batch_expiry_date'),
        items_with_line_num.batch_cpu.cast('decimal(18,2)'),

        # Quantity & pricing
        items_with_line_num.qty.cast('int').alias('quantity'),
        items_with_line_num.unit,
        items_with_line_num.unit_price.cast('decimal(18,2)'),
        items_with_line_num.selling_price.cast('decimal(18,2)'),
        items_with_line_num.cost_price.cast('decimal(18,2)'),

        # Discounts
        items_with_line_num.discount.cast('decimal(5,2)').alias('discount_percentage'),
        (
            items_with_line_num.qty *
            items_with_line_num.unit_price *
            items_with_line_num.discount / 100
        )
        .cast('decimal(18,2)')
        .alias('discount_amount'),

        # Line totals
        items_with_line_num.item_sub_total.cast('decimal(18,2)').alias('line_sub_total'),
        (
            items_with_line_num.qty *
            items_with_line_num.unit_price *
            (1 - items_with_line_num.discount / 100)
        )
        .cast('decimal(18,2)')
        .alias('line_total_amount'),

        # Margin totals
        (items_with_line_num.qty * items_with_line_num.cost_price)
            .cast('decimal(18,2)')
            .alias('line_cost_total'),

        (
            numerator
        )
        .cast('decimal(18,2)')
        .alias('line_margin_amount'),

        # SAFE PERCENTAGE
        line_margin_percentage_expr,

        # Flags
        when(items_with_line_num.packageitems.isNotNull(), True)
            .otherwise(False)
            .alias("is_package_item"),

        when(items_with_line_num.track_stock == 1, True)
            .otherwise(False)
            .alias("track_stock_flag"),

        when(items_with_line_num.no_discount == 1, True)
            .otherwise(False)
            .alias("no_discount_flag"),

        # Audit
        lit('PLATO').alias('source_system'),
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    fact_invoice_line = (
        fact_invoice_line
        .withColumn("invoice_line_fact_key", col("invoice_line_fact_key").cast("long"))
    )
    # Write to Delta table
    fact_invoice_line.write \
        .mode('overwrite') \
        .format('delta') \
        .partitionBy('invoice_date_key') \
        .saveAsTable(f"{GOLD_SCHEMA}.fact_invoice_line")
    
    row_count = fact_invoice_line.count()
    print(f"✅ FACT_INVOICE_LINE created with {row_count:,} rows")
    return fact_invoice_line


# Execute
fact_invoice_line = load_fact_invoice_line()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ETL 9: FACT_PAYMENT - Load payment fact table

def load_fact_payment():
    """
    Load payment fact table
    """
    # Read silver tables
    silver_payment = spark.table(f"{SILVER_SCHEMA}.silver_payment")
    fact_invoice_header = spark.table(f"{GOLD_SCHEMA}.fact_invoice_header")
    
    # Read dimension lookups
    dim_patient = spark.table(f"{GOLD_SCHEMA}.dim_patient").filter(col('is_current') == True)
    dim_location = spark.table(f"{GOLD_SCHEMA}.dim_location").filter(col('is_current') == True)
    dim_payment_mode = spark.table(f"{GOLD_SCHEMA}.dim_payment_mode")
    silver_corporate = spark.table(f"{SILVER_SCHEMA}.silver_corporate")
    dim_date = spark.table(f"{GOLD_SCHEMA}.dim_date")
    
    # Join with dimensions
    fact = silver_payment \
        .join(fact_invoice_header, 'invoice_id', 'inner') \
        .join(silver_corporate, 'corporate_name', 'left') \
        .join(dim_payment_mode, silver_payment.payment_mode == dim_payment_mode.payment_mode_code, 'left') \
        .join(dim_date, to_date(silver_payment.created_on) == dim_date.full_date, 'left')

    fact_payment = fact.select(
        abs(hash(silver_payment.payment_id)).alias('payment_fact_key'),
        
        # Natural key
        silver_payment.payment_id,
        
        # Dimension foreign keys
        col('date_key').alias('payment_date_key'),
        col('invoice_fact_key'),
        silver_payment.invoice_id,
        col('patient_key'),
        col('location_key'),
        coalesce(col('payment_mode_key'), lit(-1)).alias('payment_mode_key'),
        silver_corporate.corporate_id,
        
        # Payment details
        silver_payment.amount.cast('decimal(18,2)').alias('payment_amount'),
        silver_payment.payment_mode,
        silver_payment.reference.alias('reference_number'),
        silver_payment.session_id,
        
        # Payment status
        when(silver_payment.is_voided == 1, True).otherwise(False).alias('is_voided'),
        silver_payment.void_on,
        silver_payment.void_by,
        silver_payment.void_reason,
        
        # Timestamps
        silver_payment.created_on,
        silver_payment.created_by,
        
        # Audit fields
        lit('PLATO').alias('source_system'),
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    fact_payment = (
        fact_payment
        .withColumn("payment_fact_key", col("payment_fact_key").cast("long"))
    )
    # Write to Delta table
    fact_payment.write \
        .mode('overwrite') \
        .format('delta') \
        .partitionBy('payment_date_key') \
        .saveAsTable(f"{GOLD_SCHEMA}.fact_payment")
    
    row_count = fact_payment.count()
    print(f"✅ FACT_PAYMENT created with {row_count:,} rows")
    return fact_payment

# Execute
fact_payment = load_fact_payment()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ETL 10: FACT_APPOINTMENT - Load appointment fact table

def load_fact_appointment():
    """
    Load appointment fact table
    """
    # Read silver tables
    silver_appointment = spark.table(f"{SILVER_SCHEMA}.silver_appointment")
    
    # Read dimension lookups
    dim_patient = spark.table(f"{GOLD_SCHEMA}.dim_patient").filter(col('is_current') == True)
    dim_location = spark.table(f"{GOLD_SCHEMA}.dim_location").filter(col('is_current') == True)
    dim_date = spark.table(f"{GOLD_SCHEMA}.dim_date")
    
    # Join with dimensions
    fact = silver_appointment \
        .join(dim_patient, silver_appointment.patient_id == dim_patient.patient_id, 'left') \
        .join(dim_date, to_date(silver_appointment.start_time) == dim_date.full_date, 'left')
    
    fact_appointment = fact.select(
        abs(hash(silver_appointment.appointment_id)).alias('appointment_fact_key'),
        
        # Natural key
        silver_appointment.appointment_id,
        
        # Dimension foreign keys
        col('date_key').alias('appointment_date_key'),
        col('patient_key'),
        lit(None).cast('bigint').alias('location_key'),  # To be filled if location info available
        
        # Appointment details
        silver_appointment.title,
        silver_appointment.description,
        silver_appointment.dropdown.alias('appointment_type'),
        
        # Timing
        silver_appointment.start_time,
        silver_appointment.end_time,
        ((unix_timestamp(silver_appointment.end_time) - unix_timestamp(silver_appointment.start_time)) / 60).cast('int').alias('duration_minutes'),
        when(silver_appointment.is_all_day == 1, True).otherwise(False).alias('is_all_day'),
        
        # Status - to be derived from other data or set manually
        lit('Scheduled').alias('appointment_status'),
        lit(False).alias('is_first_visit'),
        lit(False).alias('is_follow_up'),
        
        # Contact
        silver_appointment.email,
        silver_appointment.handphone.cast('string'),
        
        # Recurrence
        silver_appointment.recurrence,
        
        # Timestamps
        silver_appointment.created_on,
        silver_appointment.created_by,
        
        # Audit fields
        lit('PLATO').alias('source_system'),
        current_timestamp().alias('dw_created_at'),
        current_timestamp().alias('dw_updated_at')
    )
    fact_appointment = (
        fact_appointment
        .withColumn("appointment_fact_key", col("appointment_fact_key").cast("long"))
    )
    # Write to Delta table
    fact_appointment.write \
        .mode('overwrite') \
        .format('delta') \
        .partitionBy('appointment_date_key') \
        .saveAsTable(f"{GOLD_SCHEMA}.fact_appointment")
    
    row_count = fact_appointment.count()
    print(f"✅ FACT_APPOINTMENT created with {row_count:,} rows")
    return fact_appointment

# Execute
fact_appointment = load_fact_appointment()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary
# 
# ### Completed ETL Jobs:
# 
# **Dimension Tables (6)**:
# 1. ✅ `dim_date` - Date dimension with 11 years of data
# 2. ✅ `dim_patient` - Patient dimension with demographics
# 3. ✅ `dim_inventory` - Inventory/items dimension
# 4. ✅ `dim_corporate` - Corporate/insurance dimension
# 5. ✅ `dim_location` - Location dimension
# 6. ✅ `dim_payment_mode` - Payment methods
# 
# **Fact Tables (4)**:
# 1. ✅ `fact_invoice_header` - Invoice header facts
# 2. ✅ `fact_invoice_line` - Invoice line item facts with margins
# 3. ✅ `fact_payment` - Payment transaction facts
# 4. ✅ `fact_appointment` - Appointment facts
# 
# ### Next Steps:
# 1. Create aggregate tables with health score calculations (Part 2)
# 2. Set up incremental load processes
# 3. Create data quality checks
# 4. Schedule daily/monthly refreshes
