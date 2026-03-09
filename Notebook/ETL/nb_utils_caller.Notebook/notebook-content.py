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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import json


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

METHOD_NAME = "start_batch"
PARAMETERS = '''
{
  "batch_id": "20260121153022",
  "msg": "Pipeline started"
}
'''


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

# import json

# ALLOWED_METHODS = {
#     "etl_log_upsert",
#     "start_batch",
#     "end_batch",
#     "start_job_instance",
#     "end_job_instance",
#     "log_job_instance_for_batch"
# }

# # Validate method
# if METHOD_NAME not in ALLOWED_METHODS:
#     mssparkutils.notebook.exit(json.dumps({
#         "return_code": -1,
#         "return_msg": f"Unknown METHOD_NAME: {METHOD_NAME}. Allowed: {sorted(ALLOWED_METHODS)}"
#     }))

# # Parse parameters
# try:
#     parameters = json.loads(PARAMETERS) if PARAMETERS else {}
# except Exception as e:
#     mssparkutils.notebook.exit(json.dumps({
#         "return_code": -2,
#         "return_msg": f"Invalid PARAMETERS JSON: {e}",
#         "raw": PARAMETERS
#     }))

# # Normalize job_id to 4 digits if present (pipeline might pass int)
# if "job_id" in parameters and parameters["job_id"] is not None:
#     jid = parameters["job_id"]
#     # if numeric like 3694 or "3694"
#     try:
#         jid_int = int(str(jid).strip())
#         parameters["job_id"] = f"{jid_int:04d}"
#     except Exception:
#         # if already string like "0000"
#         parameters["job_id"] = str(jid).strip()

# # Normalize job_ids list if present
# if "job_ids" in parameters and isinstance(parameters["job_ids"], list):
#     norm = []
#     for x in parameters["job_ids"]:
#         try:
#             xi = int(str(x).strip())
#             norm.append(f"{xi:04d}")
#         except Exception:
#             norm.append(str(x).strip())
#     parameters["job_ids"] = norm

# # Execute
# return_code = 1

# try:
#     func = globals()[METHOD_NAME]
#     func(**parameters)
#     # mssparkutils.notebook.exit(json.dumps({"return_code": 0, "return_msg": "OK"}))
# except Exception as e:
#     # safe_exception_text exists in nb_utils
#     try:
#         detail = safe_exception_text(e)
#     except Exception:
#         detail = str(e)
#     return_code = -1

# if return_code == -1:  
#     mssparkutils.notebook.exit(json.dumps({"return_code": return_code, "return_msg": "FAILED", "error": detail}))
# else:
#     mssparkutils.notebook.exit(json.dumps({"return_code": return_code, "return_msg": "SUCCESSED"}))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from notebookutils.mssparkutils.handlers.notebookHandler import NotebookExit

ALLOWED_METHODS = {
    # logging
    "etl_log_upsert",
    "start_batch",
    "end_batch",
    "start_job_instance",
    "end_job_instance",
    "log_job_instance_for_batch",
    # lookup
    "lookup_config"   # (or rename to lookup_etl_jobs if that is your actual def name)
}

# -------------------------
# Validate method
# -------------------------
if not METHOD_NAME or str(METHOD_NAME).strip() == "":
    mssparkutils.notebook.exit(json.dumps({
        "return_code": -1,
        "return_msg": "METHOD_NAME is required"
    }, default=str))

METHOD_NAME = str(METHOD_NAME).strip()

if METHOD_NAME not in ALLOWED_METHODS:
    mssparkutils.notebook.exit(json.dumps({
        "return_code": -1,
        "return_msg": f"Unknown METHOD_NAME: {METHOD_NAME}. Allowed: {sorted(ALLOWED_METHODS)}"
    }, default=str))

# -------------------------
# Parse parameters
# -------------------------
try:
    parameters = json.loads(PARAMETERS) if PARAMETERS and str(PARAMETERS).strip() else {}
except Exception as e:
    mssparkutils.notebook.exit(json.dumps({
        "return_code": -2,
        "return_msg": f"Invalid PARAMETERS JSON: {e}",
        "raw": PARAMETERS
    }, default=str))

# -------------------------
# Normalize job_id/job_ids
# -------------------------
def _norm_job_id(x):
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    try:
        return f"{int(s):04d}"
    except Exception:
        # already "0000" or non-numeric
        return s

if "job_id" in parameters:
    parameters["job_id"] = _norm_job_id(parameters.get("job_id"))

if "job_ids" in parameters and isinstance(parameters["job_ids"], list):
    parameters["job_ids"] = [_norm_job_id(x) for x in parameters["job_ids"] if _norm_job_id(x) is not None]

# -------------------------
# Execute
# -------------------------
try:
    if METHOD_NAME not in globals():
        raise ValueError(f"Function {METHOD_NAME} not found in globals(). Did you %run nb_utils?")

    func = globals()[METHOD_NAME]

    # Special: lookup_config returns a Spark DataFrame -> return JSON array (for ForEach items)
    if METHOD_NAME == "lookup_config":
        df = func(**parameters)
        rows = [r.asDict(recursive=True) for r in df.collect()]
        mssparkutils.notebook.exit(json.dumps(rows, default=str))

    # Default: call func and return OK
    result = func(**parameters)

    # NOTE: your old code returned return_code=1 on success; that is confusing.
    # Use 0 for success.
    mssparkutils.notebook.exit(json.dumps({
        "return_code": 0,
        "return_msg": "OK",
        "result": result
    }, default=str))

except NotebookExit:
    # Fabric uses this exception to return the exitValue to the pipeline.
    # DO NOT catch it as a failure.
    raise

except Exception as e:
    # safe_exception_text exists in nb_utils (best effort)
    try:
        detail = safe_exception_text(e)
    except Exception:
        detail = str(e)

    mssparkutils.notebook.exit(json.dumps({
        "return_code": -1,
        "return_msg": "FAILED",
        "error": detail
    }, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
