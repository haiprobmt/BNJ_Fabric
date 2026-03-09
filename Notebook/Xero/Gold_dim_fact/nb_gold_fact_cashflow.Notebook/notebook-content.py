# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 20260207145530
job_id = '4243'
src_catalog = "gold_dim_fact"
job_group_name = "gold_fact"
src_table = ""
tgt_table = "fact_cashflow"
tgt_catalog = "gold"

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
SILVER_SCHEMA_XERO = f"`{WORKSPACE_NAME}`.lh_bnj_silver.{src_catalog}"
GOLD_SCHEMA = f"`{WORKSPACE_NAME}`.lh_bnj_gold.{tgt_catalog}"

# Table names
SILVER_TABLES = {
    "xero_bank_transactions": f"{SILVER_SCHEMA_XERO}.silver_bank_transactions",
    "xero_accounts": f"{SILVER_SCHEMA_XERO}.silver_accounts",
    "xero_contacts": f"{SILVER_SCHEMA_XERO}.silver_contacts"
}

GOLD_DIMENSIONS = {
    "date": f"{GOLD_SCHEMA}.dim_date"
}

GOLD_FACTS = {
    "cashflow": f"{GOLD_SCHEMA}.fact_cashflow"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Create and save fact_cashflow
try:

    tgt_table = GOLD_FACTS['cashflow']
    fact_cashflow_df = create_fact_cashflow()
    fact_cashflow_df.write.format("delta").mode("overwrite").saveAsTable(tgt_table)

    src_cnt = fact_cashflow_df.count()
    tgt_cnt = spark.table(tgt_table).count()

    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created agg_cashflow", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print(f"✅ Created {tgt_table} with {src_cnt} rows")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed agg_cashflow. {safe_exception_text(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
