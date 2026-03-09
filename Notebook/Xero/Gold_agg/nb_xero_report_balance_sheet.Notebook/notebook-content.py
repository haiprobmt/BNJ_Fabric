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
# META           "id": "102823e0-12f1-4ca5-b61b-a2df5d75beb2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run ./02_Xero_Authentication

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

batch_id = 20260209125800
job_id = '7540'
src_table = ''
src_catalog = 'xero'
tgt_table = 'report_balance_sheet'
tgt_catalog = 'gold'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configuration
WORKSPACE_NAME = 'WS-ETL-BNJ'
# SILVER_SCHEMA = f"`{WORKSPACE_NAME}`.lh_bnj_silver.{src_catalog}"
GOLD_SCHEMA = f"`{WORKSPACE_NAME}`.lh_bnj_gold.{tgt_catalog}"  # Target schema
TABLE_NAME = tgt_table

# Full table path
TARGET_TABLE = f"{GOLD_SCHEMA}.{TABLE_NAME}"

# print(f"Source: {SILVER_SCHEMA}")
print(f"Target: {TARGET_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date
from dateutil.relativedelta import relativedelta
from dateutil import parser

timeframe = "MONTH"
standard_layout = True
payments_only = False

start_date = date(2020, 1, 31)
end_date = date(2025, 12, 31)

bs_results = []

current_date = start_date
while current_date <= end_date:
    bs = accounting_api.get_report_balance_sheet(
        xero_tenant_id=tenant_id,
        date=current_date,
        timeframe=timeframe,
        standard_layout=standard_layout,
        payments_only=payments_only
    )

    bs_results.append({
        "snapshot_date": current_date,
        "report": bs
    })
    # Move to next month-end
    current_date = (current_date + relativedelta(months=1)).replace(day=1) \
                   + relativedelta(months=1, days=-1)
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from datetime import datetime

def balance_sheet_list_to_pandas(bs_results):
    records = []

    for item in bs_results:
        bs_result = item["report"]
        snapshot_date = item["snapshot_date"]

        report = bs_result.reports[0]

        # Header row → period labels (Current / Previous month)
        header_cells = report.rows[0].cells
        period_labels = [c.value for c in header_cells[1:]]

        current_section = None
        current_subsection = None

        for row in report.rows:
            if row.row_type.value == "Section":

                # Main section
                if not row.rows:
                    current_section = row.title
                    current_subsection = None
                    continue

                # Sub-section
                current_subsection = row.title

                for r in row.rows:
                    if r.row_type.value != "Row":
                        continue

                    cells = r.cells
                    account_name = cells[0].value

                    # Extract account ID
                    account_id = None
                    if cells[0].attributes:
                        for attr in cells[0].attributes:
                            if attr.id == "account":
                                account_id = attr.value

                    for i, cell in enumerate(cells[1:]):
                        records.append({
                            "snapshot_date": snapshot_date,
                            "section": current_section,
                            "sub_section": current_subsection,
                            "account_id": account_id,
                            "account_name": account_name,
                            "period_label": period_labels[i],
                            "amount": float(cell.value)
                        })

    return pd.DataFrame(records)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.createDataFrame(balance_sheet_list_to_pandas(bs_results))
df.write.mode('overwrite').saveAsTable(TARGET_TABLE)
#df.write.format('delta').mode('overwrite').save('abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/102823e0-12f1-4ca5-b61b-a2df5d75beb2/Tables/gold_test/report_balance_sheet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
