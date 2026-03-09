# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "102823e0-12f1-4ca5-b61b-a2df5d75beb2",
# META       "default_lakehouse_name": "lh_bnj_gold",
# META       "default_lakehouse_workspace_id": "c81c26a1-3386-4efb-b6ea-59363a6145b7",
# META       "known_lakehouses": [
# META         {
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         },
# META         {
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
# META         },
# META         {
# META           "id": "e8a5651c-a1b6-49de-b2c5-11b7e197e77a"
# META         },
# META         {
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold: dim_location


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

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SILVER_TABLES = {}
GOLD_DIMENSIONS = {"location": f"lh_bnj_gold.{tgt_catalog}.dim_location"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
# dim_location_df = create_dim_location()
# dim_location_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["location"])
# print(f"✅ Created {GOLD_DIMENSIONS['location']} with {dim_location_df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    df = create_dim_location()
    src_cnt = 0
    tgt_cnt = df.count()
    log_data_quality("dim_location", df, "location_key")

    df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIMENSIONS["location"])
    
    end_job_instance(batch_id, job_id, "SUCCESS", msg="Created dim_location", src_row_num=src_cnt, tgt_row_num=tgt_cnt)
    print("OK")
except Exception as e:
    end_job_instance(batch_id, job_id, "FAILED", msg=f"Failed dim_location. {safe_exception_text(e)}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
