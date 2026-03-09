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
# META         },
# META         {
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         },
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

# CELL ********************

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Pipeline parameters (set via Fabric pipeline notebook parameters)
batch_id = 1
job_id = '6858'
src_catalog = "system"
job_group_name = "gold"
src_table = ""
tgt_catalog = "gold"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


SILVER_TABLES = {}
GOLD_DIMENSIONS = {"date": f"lh_bnj_gold.{tgt_catalog}.dim_date"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
# dim_date_df = create_dim_date()
# dim_date_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["date"])
# print(f"✅ Created {GOLD_DIMENSIONS['date']} with {dim_date_df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


try:
    df = create_dim_date()
    src_cnt = 0
    tgt_cnt = df.count()
    log_data_quality("dim_date", df, "date_key")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_DIMENSIONS["date"])
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created dim_date", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed dim_date. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
