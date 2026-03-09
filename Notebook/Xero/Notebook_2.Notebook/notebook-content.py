# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import sempy.fabric as fabric
import sempy_labs as labs
import pandas as pd
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

WORKSPACE_ID = fabric.get_workspace_id()
WORKSPACE_ID


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_id = fabric.get_artifact_id()
notebook_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the notebook item 
notebook_id = "f5a068be-4da8-46b4-84d3-1b2b85557c34" # List all job instances for this notebook job_instances = notebook.list_item_job_instances()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pipeline_run_info = labs.list_item_job_instances(item=notebook_id, workspace=WORKSPACE_ID)
pipeline_run_info

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Combine all pipelines information into 1 dataframe
pipeline_run_info_table = pd.DataFrame()
for pipeline_id in pipeline_list_id:
    pipeline_run_info = labs.list_item_job_instances(item=pipeline_id, workspace=WORKSPACE_ID)
    pipeline_run_info_table = pd.concat([pipeline_run_info_table, pipeline_run_info], axis=0, join='outer')

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

# MAGIC %%sql
# MAGIC select * from `WS-ETL-BNJ`.lh_bnj_metadata.md.etl_jobs
# MAGIC where tgt_table = 'dim_account'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
