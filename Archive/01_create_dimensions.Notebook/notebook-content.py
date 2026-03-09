# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c9d7507e-938a-4c6d-a042-d8743e386ab5",
# META       "default_lakehouse_name": "lh_bnj_bronze",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
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

# # BNJ Medical CFO Dashboard - Gold Layer Dimensions
# 
# This notebook creates dimension tables for the CFO Dashboard data warehouse.
# 
# **Dimensions Created:**
# - dim_date
# - dim_patient
# - dim_location
# - dim_product
# - dim_supplier
# - dim_payer
# - dim_account

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import calendar

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

# Lakehouse paths
SILVER_SCHEMA_PLATO = "lh_bnj_silver.plato_test"
SILVER_SCHEMA_XERO = "lh_bnj_silver.xero_test"
GOLD_SCHEMA = "lh_bnj_gold.gold_test"

# Table names
SILVER_TABLES = {
    "patient": f"{SILVER_SCHEMA_PLATO}.silver_patient",
    "invoice": f"{SILVER_SCHEMA_PLATO}.silver_invoice",
    "inventory": f"{SILVER_SCHEMA_PLATO}.silver_inventory",
    "supplier": f"{SILVER_SCHEMA_PLATO}.silver_supplier",
    "corporate": f"{SILVER_SCHEMA_PLATO}.silver_corporate",
    "appointment": f"{SILVER_SCHEMA_PLATO}.silver_appointment",
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
    "account": f"{GOLD_SCHEMA}.dim_account",
    "contact": f"{GOLD_SCHEMA}.dim_contact",
    "inventory": f"{GOLD_SCHEMA}.dim_inventory"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. dim_date - Calendar Dimension

# CELL ********************

def create_dim_date(start_date: str = "2020-01-01", end_date: str = "2030-12-31"):
    """
    Create a comprehensive date dimension table.
    Includes fiscal year calculations and Singapore public holidays.
    
    Target schema (20 columns):
    - date_display, date_key, day_of_month, day_of_week, day_of_week_name
    - day_of_year, fiscal_month, fiscal_quarter, fiscal_year, full_date
    - holiday_name, is_holiday, is_weekend, month, month_abbr
    - month_name, quarter, quarter_name, week_of_year, year
    - year_month, year_quarter
    """
    
    # Generate date range
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    dates = []
    current = start
    
    # Singapore public holidays (sample - should be expanded)
    sg_holidays = {
        (1, 1): "New Year's Day",
        (5, 1): "Labour Day",
        (8, 9): "National Day",
        (12, 25): "Christmas Day"
    }
    
    while current <= end:
        date_key = int(current.strftime("%Y%m%d"))
        
        # Calculate fiscal year (assuming April start)
        fiscal_year = current.year if current.month >= 4 else current.year - 1
        fiscal_quarter = ((current.month - 4) % 12) // 3 + 1
        if fiscal_quarter <= 0:
            fiscal_quarter += 4
        fiscal_month = ((current.month - 4) % 12) + 1
        
        # Check if weekend
        is_weekend = current.weekday() >= 5
        
        # Check if holiday
        holiday_key = (current.month, current.day)
        is_holiday = holiday_key in sg_holidays
        holiday_name = sg_holidays.get(holiday_key, None)
        
        dates.append({
            "date_display": current.strftime("%d %b %Y"),
            "date_key": date_key,
            "day_of_month": current.day,
            "day_of_week": current.weekday() + 1,  # 1=Monday, 7=Sunday
            "day_of_week_name": current.strftime("%A"),
            "day_of_year": current.timetuple().tm_yday,
            "fiscal_month": fiscal_month,
            "fiscal_quarter": fiscal_quarter,
            "fiscal_year": fiscal_year,
            "full_date": current.date(),
            "holiday_name": holiday_name,
            "is_holiday": is_holiday,
            "is_weekend": is_weekend,
            "month": current.month,
            "month_abbr": current.strftime("%b"),
            "month_name": current.strftime("%B"),
            "quarter": (current.month - 1) // 3 + 1,
            "quarter_name": f"Q{(current.month - 1) // 3 + 1}",
            "week_of_year": current.isocalendar()[1],
            "year": current.year,
            "year_month": current.strftime("%Y-%m"),
            "year_quarter": f"{current.year}-Q{(current.month - 1) // 3 + 1}"
        })
        
        current += timedelta(days=1)
    
    # Create DataFrame with exact schema
    schema = StructType([
        StructField("date_display", StringType(), True),
        StructField("date_key", IntegerType(), False),
        StructField("day_of_month", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("day_of_week_name", StringType(), True),
        StructField("day_of_year", IntegerType(), True),
        StructField("fiscal_month", IntegerType(), True),
        StructField("fiscal_quarter", IntegerType(), True),
        StructField("fiscal_year", IntegerType(), True),
        StructField("full_date", DateType(), False),
        StructField("holiday_name", StringType(), True),
        StructField("is_holiday", BooleanType(), True),
        StructField("is_weekend", BooleanType(), True),
        StructField("month", IntegerType(), True),
        StructField("month_abbr", StringType(), True),
        StructField("month_name", StringType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("quarter_name", StringType(), True),
        StructField("week_of_year", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("year_month", StringType(), True),
        StructField("year_quarter", StringType(), True)
    ])
    
    df = spark.createDataFrame(dates, schema)
    
    return df

# Create and save dim_date
dim_date_df = create_dim_date()
dim_date_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["date"])
print(f"✅ Created {GOLD_DIMENSIONS['date']} with {dim_date_df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. dim_patient - Patient Dimension

# CELL ********************

def create_dim_patient():
    """
    Create patient dimension from silver_patient table.
    Includes SCD Type 2 support and derived attributes.
    
    Target schema (44 columns):
    - address, age, age_group, alerts, allergies, allergies_select
    - assigned_doctor, created_by, created_on, date_of_birth, do_not_disturb
    - dw_created_at, dw_updated_at, effective_date, email, end_date
    - food_allergies, food_allergies_select, g6pd_status, given_id, is_current
    - last_edited, last_edited_by, marital_status, nationality, notes, nric
    - nric_type, occupation, patient_id, patient_key, patient_name, postal_code
    - recalls, referred_by, sex, source_system, tags, telephone
    - telephone2, telephone3, title, unit_no
    """
    
    # Read silver patient data
    silver_patient = spark.table(SILVER_TABLES["patient"])
    
    # Transform to dimension with exact schema
    dim_patient = silver_patient.select(
        # Surrogate key
        monotonically_increasing_id().alias("patient_key"),
        
        # Natural key
        col("patient_id"),
        # col("patient_code").alias("given_id"),
        
        # Name and title
        col("full_name").alias("patient_name"),
        col("title"),
        
        # NRIC
        col("nric_masked").alias("nric"),
        col("nric_type"),
        
        # Demographics
        col("gender").alias("sex"),
        col("date_of_birth"),
        
        # Calculate age
        floor(datediff(current_date(), col("date_of_birth")) / 365).cast(IntegerType()).alias("age"),
        
        # Age group classification
        when(floor(datediff(current_date(), col("date_of_birth")) / 365) <= 18, "0-18")
        .when(floor(datediff(current_date(), col("date_of_birth")) / 365) <= 35, "19-35")
        .when(floor(datediff(current_date(), col("date_of_birth")) / 365) <= 50, "36-50")
        .when(floor(datediff(current_date(), col("date_of_birth")) / 365) <= 65, "51-65")
        .otherwise("65+").alias("age_group"),
        
        # Location
        col("address"),
        col("unit_number").alias("unit_no"),
        col("postal_code"),
        col("nationality"),
        
        # Contact - multiple phone numbers
        col("phone_primary").alias("telephone"),
        col("phone_secondary").alias("telephone2"),
        col("phone_tertiary").alias("telephone3"),
        col("email"),
        
        # Additional demographics
        col("occupation"),
        col("marital_status"),
        
        # Medical info - allergies
        col("allergies"),
        lit(None).cast(StringType()).alias("allergies_select"),
        lit(None).cast(StringType()).alias("food_allergies"),
        lit(None).cast(StringType()).alias("food_allergies_select"),
        
        # Medical info - other
        col("alerts"),
        col("g6pd_status"),
        lit(None).cast(StringType()).alias("recalls"),
        
        # Relationships
        col("assigned_doctor"),
        col("referred_by"),
        
        # Notes and tags
        col("notes"),
        col("tags"),
        
        # Status
        col("do_not_disturb"),
        
        # Audit fields
        col("created_at").alias("created_on"),
        col("created_by"),
        col("updated_at").alias("last_edited"),
        col("updated_by").alias("last_edited_by"),
        
        # Source system
        lit("PLATO").alias("source_system"),
        
        # SCD Type 2 columns
        current_date().alias("effective_date"),
        lit(None).cast(DateType()).alias("end_date"),
        lit(True).alias("is_current"),
        
        # Data warehouse metadata
        current_timestamp().alias("dw_created_at"),
        current_timestamp().alias("dw_updated_at")
    )
    
    # Reorder columns alphabetically to match target schema
    result = dim_patient.select(
        col("address"),
        col("age"),
        col("age_group"),
        col("alerts"),
        col("allergies"),
        col("allergies_select"),
        col("assigned_doctor"),
        col("created_by"),
        col("created_on"),
        col("date_of_birth"),
        col("do_not_disturb"),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("effective_date"),
        col("email"),
        col("end_date"),
        col("food_allergies"),
        col("food_allergies_select"),
        col("g6pd_status"),
        # col("given_id"),
        col("is_current"),
        col("last_edited"),
        col("last_edited_by"),
        col("marital_status"),
        col("nationality"),
        col("notes"),
        col("nric"),
        col("nric_type"),
        col("occupation"),
        col("patient_id"),
        col("patient_key"),
        col("patient_name"),
        col("postal_code"),
        col("recalls"),
        col("referred_by"),
        col("sex"),
        col("source_system"),
        col("tags"),
        col("telephone"),
        col("telephone2"),
        col("telephone3"),
        col("title"),
        col("unit_no")
    )
    
    return result

# Create and save dim_patient
try:
    dim_patient_df = create_dim_patient()
    dim_patient_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["patient"])
    print(f"✅ Created {GOLD_DIMENSIONS['patient']} with {dim_patient_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create dim_patient: {e}")
    print("   Will create when silver_patient is available")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. dim_location - Location Dimension

# CELL ********************

def create_dim_location():
    """
    Create location dimension.
    Extract from system setup or create default for single location.
    """
    
    # For now, create a default location (expand based on actual data)
    locations = [
        {
            "location_key": 1,
            "location_id": "LOC001",
            "location_name": "BNJ Main Clinic",
            "location_type": "Main",
            "address": "Singapore",
            "postal_code": "000000",
            "region": "Central",
            "is_active": True
        }
    ]
    
    schema = StructType([
        StructField("location_key", IntegerType(), False),
        StructField("location_id", StringType(), False),
        StructField("location_name", StringType(), True),
        StructField("location_type", StringType(), True),
        StructField("address", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("region", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])
    
    df = spark.createDataFrame(locations, schema)
    return df

# Create and save dim_location
dim_location_df = create_dim_location()
dim_location_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["location"])
print(f"✅ Created {GOLD_DIMENSIONS['location']} with {dim_location_df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. dim_product - Product/Inventory Item Dimension

# CELL ********************

def create_dim_inventory():
    """
    Create inventory dimension from silver_inventory table.
    
    Target schema (37 columns):
    - category, cost_price, description, dosage, dosage_dose, dosage_duration
    - dosage_frequency, dosage_unit, dosage_usage, dw_created_at, dw_updated_at
    - effective_date, end_date, given_id, has_expiry_after_dispensing
    - inventory_id, inventory_key, inventory_type, is_current, is_fixed_price
    - is_hidden, is_redeemable, item_name, manufacturer, min_price, no_discount
    - order_unit, pack_size, package_original_price, precautions, redemption_count
    - selling_price, source_system, supplier_id, track_stock, unit
    """
    
    # Read silver inventory data
    silver_inventory = spark.table(SILVER_TABLES["inventory"])
    
    # Transform to dimension with exact schema
    dim_inventory = silver_inventory.select(
        # Surrogate key
        monotonically_increasing_id().alias("inventory_key"),
        
        # Natural key
        col("product_id").alias("inventory_id"),
        col("product_code").alias("given_id"),
        
        # Descriptive columns
        col("product_name").alias("item_name"),
        col("description"),
        col("category"),
        col("product_type").alias("inventory_type"),
        
        # Pricing
        col("unit_cost").cast(DecimalType(18, 4)).alias("cost_price"),
        col("selling_price").cast(DecimalType(18, 2)),
        lit(None).cast(DecimalType(18, 2)).alias("min_price"),
        lit(None).cast(DecimalType(18, 2)).alias("package_original_price"),
        
        # Units and packaging
        col("unit_of_measure").alias("unit"),
        col("order_unit"),
        col("pack_size"),
        
        # Dosage information
        col("dosage"),
        col("default_dose").alias("dosage_dose"),
        col("dose_unit").alias("dosage_unit"),
        col("dose_frequency").alias("dosage_frequency"),
        col("dose_duration").alias("dosage_duration"),
        col("dosage_usage"),
        lit(None).cast(StringType()).alias("precautions"),
        
        # Supplier and manufacturer
        col("supplier_id"),
        col("manufacturer"),
        
        # Flags
        col("is_track_stock").alias("track_stock"),
        coalesce(col("fixed_price"), lit(False)).alias("is_fixed_price"),
        coalesce(col("no_discount"), lit(False)).alias("no_discount"),
        lit(False).alias("is_hidden"),
        lit(False).alias("is_redeemable"),
        lit(0).alias("redemption_count"),
        lit(False).alias("has_expiry_after_dispensing"),
        
        # Source system
        lit("PLATO").alias("source_system"),
        
        # SCD Type 2 columns
        current_date().alias("effective_date"),
        lit(None).cast(DateType()).alias("end_date"),
        lit(True).alias("is_current"),
        
        # Data warehouse metadata
        current_timestamp().alias("dw_created_at"),
        current_timestamp().alias("dw_updated_at")
    ).dropDuplicates(["inventory_id"])
    
    # Reorder columns alphabetically to match target schema
    result = dim_inventory.select(
        col("category"),
        col("cost_price"),
        col("description"),
        col("dosage"),
        col("dosage_dose"),
        col("dosage_duration"),
        col("dosage_frequency"),
        col("dosage_unit"),
        col("dosage_usage"),
        col("dw_created_at"),
        col("dw_updated_at"),
        col("effective_date"),
        col("end_date"),
        col("given_id"),
        col("has_expiry_after_dispensing"),
        col("inventory_id"),
        col("inventory_key"),
        col("inventory_type"),
        col("is_current"),
        col("is_fixed_price"),
        col("is_hidden"),
        col("is_redeemable"),
        col("item_name"),
        col("manufacturer"),
        col("min_price"),
        col("no_discount"),
        col("order_unit"),
        col("pack_size"),
        col("package_original_price"),
        col("precautions"),
        col("redemption_count"),
        col("selling_price"),
        col("source_system"),
        col("supplier_id"),
        col("track_stock"),
        col("unit")
    )
    
    return result

# Create and save dim_inventory
try:
    dim_inventory_df = create_dim_inventory()
    dim_inventory_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["inventory"])
    print(f"✅ Created {GOLD_DIMENSIONS['inventory']} with {dim_inventory_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create dim_inventory: {e}")
    print("   Will create when silver_inventory is available")
    
# Also create dim_product as alias for backward compatibility
try:
    dim_inventory_df = spark.table(GOLD_DIMENSIONS["inventory"])
    dim_inventory_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["product"])
    print(f"✅ Created {GOLD_DIMENSIONS['product']} (alias for dim_inventory)")
except Exception as e:
    print(f"⚠️ Could not create dim_product alias: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. dim_supplier - Supplier Dimension

# CELL ********************

def create_dim_supplier():
    """
    Create supplier dimension from silver_supplier table.
    
    Silver supplier columns:
    - supplier_id, supplier_name, supplier_category
    - contact_person, email, phone, mobile, fax
    - address, website, notes, other_info
    - is_active, created_at, updated_at
    """
    
    # Read silver supplier data
    silver_supplier = spark.table(SILVER_TABLES["supplier"])
    
    # Transform to dimension
    dim_supplier = silver_supplier.select(
        monotonically_increasing_id().alias("supplier_key"),
        col("supplier_id"),
        col("supplier_name"),
        col("supplier_category"),
        col("contact_person"),
        col("phone"),
        col("mobile"),
        col("email"),
        col("address"),
        col("website"),
        coalesce(col("is_active"), lit(True)).alias("is_active")
    ).dropDuplicates(["supplier_id"])
    
    return dim_supplier

# Create and save dim_supplier
try:
    dim_supplier_df = create_dim_supplier()
    dim_supplier_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["supplier"])
    print(f"✅ Created {GOLD_DIMENSIONS['supplier']} with {dim_supplier_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create dim_supplier: {e}")
    print("   Will create when silver_supplier is available")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. dim_payer - Payer (Insurance/Corporate) Dimension

# CELL ********************

def create_dim_payer():
    """
    Create payer dimension from silver_corporate table.
    Includes insurance companies and corporate accounts.
    
    Silver corporate columns:
    - corporate_id, corporate_code, corporate_name, corporate_type
    - category, contact_person, email, phone, fax
    - address, website
    - payment_type, credit_amount
    - scheme, discounts, specials, restricted_items
    - notes, invoice_notes, other_info
    - is_insurance, is_active
    - created_at, updated_at
    """
    
    # Read silver corporate data
    silver_corporate = spark.table(SILVER_TABLES["corporate"])
    
    # Transform to dimension
    dim_payer = silver_corporate.select(
        monotonically_increasing_id().alias("payer_key"),
        col("corporate_id").alias("payer_id"),
        col("corporate_code").alias("payer_code"),
        col("corporate_name").alias("payer_name"),
        col("corporate_type").alias("payer_type"),
        col("contact_person"),
        col("phone"),
        col("email"),
        col("address"),
        col("payment_type"),
        col("credit_amount"),
        col("scheme"),
        col("is_insurance"),
        coalesce(col("is_active"), lit(True)).alias("is_active")
    ).dropDuplicates(["payer_id"])
    
    return dim_payer

# Create and save dim_payer
try:
    dim_payer_df = create_dim_payer()
    dim_payer_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["payer"])
    print(f"✅ Created {GOLD_DIMENSIONS['payer']} with {dim_payer_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create dim_payer: {e}")
    print("   Will create when silver_corporate is available")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. dim_account - Chart of Accounts Dimension (from XERO)

# CELL ********************

def create_dim_account():
    """
    Create account dimension from XERO accounts.
    """
    
    # Read XERO accounts data
    xero_accounts = spark.table(SILVER_TABLES["xero_accounts"])
    
    # Transform to dimension
    dim_account = xero_accounts.select(
        monotonically_increasing_id().alias("account_key"),
        col("account_id"),
        col("account_code"),
        col("account_name"),
        col("account_type"),
        col("account_class"),
        col("tax_type"),
        col("system_account_type").alias("is_system_account"),
        col("status").alias("is_active")
    ).dropDuplicates(["account_id"])
    
    return dim_account

# Create and save dim_account
try:
    dim_account_df = create_dim_account()
    dim_account_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["account"])
    print(f"✅ Created {GOLD_DIMENSIONS['account']} with {dim_account_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create dim_account: {e}")
    print("   Will create when silver_xero_accounts is available")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. dim_contact - XERO Contact Dimension

# CELL ********************

def create_dim_contact():
    """
    Create contact dimension from XERO contacts.
    XERO contacts include customers and suppliers.
    
    Silver xero_contacts columns:
    - contact_id, contact_name, contact_type
    - is_customer, is_supplier
    - email, first_name, last_name
    - status, default_currency
    - tenant_id, extracted_at, updated_at
    """
    
    # Read XERO contacts data
    xero_contacts = spark.table(SILVER_TABLES["contacts"])
    
    # Transform to dimension
    dim_contact = xero_contacts.select(
        monotonically_increasing_id().alias("contact_key"),
        col("contact_id"),
        col("contact_name"),
        col("contact_type"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("is_customer"),
        col("is_supplier"),
        col("default_currency"),
        when(upper(col("status")) == "ACTIVE", True).otherwise(False).alias("is_active")
    ).dropDuplicates(["contact_id"])
    
    return dim_contact

# Create and save dim_contact
try:
    dim_contact_df = create_dim_contact()
    dim_contact_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["contact"])
    print(f"✅ Created {GOLD_DIMENSIONS['contact']} with {dim_contact_df.count()} rows")
except Exception as e:
    print(f"⚠️ Could not create dim_contact: {e}")
    print("   Will create when silver_xero_contacts is available")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary

# CELL ********************

# Display summary of created dimensions
print("\n" + "="*60)
print("DIMENSION TABLES CREATION SUMMARY")
print("="*60)

for dim_name, table_name in GOLD_DIMENSIONS.items():
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
