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
# META           "id": "c9d7507e-938a-4c6d-a042-d8743e386ab5"
# META         },
# META         {
# META           "id": "48bd1f5e-ef56-4df0-8515-17758bcbd734"
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

batch_id = '20260130112233'
job_id = 9652
batch_group = "plato"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================
# Plato extract - streaming per page, fast & safe
# + Write run/step logs to md.etl_log
# + Raise Exception if any job fails (pipeline activity = Failed)
# ==========================================================

from datetime import date
import time, json, random, requests
from typing import Optional, Dict, Any, Union, List
from pyspark.sql import functions as F
import hashlib, json

# -----------------------------
# Config
# -----------------------------
BASE_URL = "https://clinic.platomedical.com/api/drics/"
OUTPUT_DIR = "abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/plato/Data"
HEADERS = {"Authorization": "Bearer 4573dfbca452d15dbe808c1eb9982798"}

MAX_429_RETRIES  = 3
SLEEP_429_SEC    = 60
TIMEOUT_SEC      = 90

session = requests.Session()

# -----------------------------
# Helpers
# -----------------------------
def sanitize_name(s: str) -> str:
    return (
        s.replace("/", "_")
         .replace("?", "_")
         .replace("&", "_")
         .replace("=", "_")
         .strip()
    )

def get_plato_page(
    page: int,
    domain: str,
    headers: Dict[str, str],
    timeout: int = TIMEOUT_SEC,
) -> Dict[str, Any]:

    params = {"current_page": page}
    req_headers = {"Accept": "application/json"}
    req_headers.update(headers)

    today = date.today()
    start_date = today.replace(day=1).isoformat()   # first day of month
    end_date   = today.isoformat()                  # today

    sep = "&" if "?" in domain else "?"
    full_url = (
        f"{BASE_URL}{domain.lstrip('/')}"
        f"{sep}start_date={start_date}&end_date={end_date}"
    )
    print("full_url:", full_url)

    for attempt in range(1, MAX_429_RETRIES + 1):
        resp = session.get(full_url, params=params, headers=req_headers, timeout=timeout)
        print("status_code:", resp.status_code)
        if resp.status_code in (401, 403):
            resp.raise_for_status()

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = float(retry_after) if (retry_after and retry_after.isdigit()) else SLEEP_429_SEC
            print(f"[429] {domain} page={page}. Sleeping {wait}s...")
            time.sleep(wait + random.uniform(0, 1.0))
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError(f"429 persists after {MAX_429_RETRIES} retries. url={full_url} page={page}")

def extract_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for k in ("data", "results", "items", "value"):
            if k in payload and isinstance(payload[k], list):
                return payload[k]
        # single object fallback
        return [payload]

    return []

# -----------------------------
# Read active staging jobs
# -----------------------------
job_id_param = job_id  # pipeline can pass null
prev_page_hash = None

jobs_df = (
    spark.table("md.etl_jobs")
    .filter((F.col("active_flg") == "Y") & (F.col("src_catalog") == "plato"))
    .filter(F.col("job_group_name") == "staging")
    .select("job_id", "job_name")
)

if job_id_param is not None and str(job_id_param).strip() != "":
    job_id_list = [x.strip() for x in str(job_id_param).split(",") if x.strip() != ""]
    jobs_df = jobs_df.filter(F.col("job_id").cast("string").isin(job_id_list))

jobs = jobs_df.orderBy("job_id").collect()

# -----------------------------
# Run
# -----------------------------
failed_any = False
failed_details = []

for j in jobs:
    job_id = str(j["job_id"]).strip()
    domain = str(j["job_name"]).strip()

    if not domain:
        continue

    domain_safe = sanitize_name(domain)
    domain_out_dir = f"{OUTPUT_DIR}/{batch_id}/{domain_safe}"

    # LOG: step start
    #start_job_instance(batch_id, job_id, msg=f"Start domain={domain}")

    try:
        page = 1
        total_rows = 0
        total_pages = 0

        all_rows = []

        page = 1
        total_rows = 0
        total_pages = 0

        while True:
            payload = get_plato_page(page=page, domain=domain, headers=HEADERS)
            rows = extract_rows(payload)

            if not rows:
                print(f"[{job_id}] {domain} page={page} -> no rows, stopping.")
                break

            if not rows:
                print(f"[{job_id}] {domain} page={page} -> no rows, stopping.")
                break

            # ✅ STOP if this page repeats previous page (API ignores current_page)
            page_hash = hashlib.md5(
                json.dumps(rows, sort_keys=True, ensure_ascii=False).encode("utf-8")
            ).hexdigest()

            if prev_page_hash is not None and page_hash == prev_page_hash:
                print(f"[{job_id}] {domain} page={page} repeats previous page -> stopping to avoid infinite loop.")
                break

            prev_page_hash = page_hash

            all_rows.extend(rows)

            print(f"[{job_id}] {domain} page={page} rows={len(rows)}")

            total_rows += len(rows)
            total_pages += 1

            # stop if API tells us last page
            if isinstance(payload, dict) and "pages" in payload:
                if page >= int(payload["pages"]):
                    break

            page += 1

        # Write SINGLE file
        out_path = f"{OUTPUT_DIR}/{batch_id}/{domain_safe}.json"
        mssparkutils.fs.put(
            out_path,
            json.dumps(all_rows, ensure_ascii=False),
            overwrite=True
        )

        print(f"[{job_id}] Wrote {total_rows} rows to {out_path}")


        # LOG: step success
        end_job_instance(
            batch_id, job_id, "SUCCESS",
            msg=f"Completed domain={domain}. pages={total_pages}.",
            src_row_num=total_rows,
            tgt_row_num=total_rows
        )

        print(f"[{job_id}] Done {domain}. Pages={total_pages}, Records={total_rows}")

    except Exception as e:
        failed_any = True

        end_job_instance(
            batch_id, job_id, "FAILED",
            msg=f"Failed domain={domain}. {safe_exception_text(e)}"
        )

        failed_details.append({
            "job_id": job_id,
            "domain": domain,
            "error": str(e)[:1500]
        })

        print(f"[{job_id}] FAILED {domain}: {e}")

        # Continue running next job instead of stopping the pipeline
        # continue
        break

# -----------------------------
# Finalize notebook result
# -----------------------------
if failed_any:
    payload = {
        "return_code": -1,
        "return_msg": "FAILED",
        "batch_id": batch_id,
        "failed_details": failed_details
    }
    raise Exception(json.dumps(payload, ensure_ascii=False))

else:
    payload = {"return_code": 0, "return_msg": "OK", "batch_id": batch_id}
    mssparkutils.notebook.exit(json.dumps(payload, ensure_ascii=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # ==========================================================
# # Plato extract - loop all domains in md.etl_jobs (staging)
# # + Write run/step logs to md.etl_log
# # + Raise Exception if any job fails (pipeline activity = Failed)
# # ==========================================================
# import time, json, random, requests
# from typing import Optional, Dict, Any, Union, List
# from pyspark.sql import functions as F

# # -----------------------------
# # Config
# # -----------------------------
# BASE_URL = "https://clinic.platomedical.com/api/drics/"
# OUTPUT_DIR = "abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/plato_dev"
# HEADERS = {"Authorization": "Bearer 4573dfbca452d15dbe808c1eb9982798"}

# REQUEST_PACE_SEC = 0.1
# MAX_429_RETRIES  = 10
# SLEEP_429_SEC    = 30
# TIMEOUT_SEC      = 60

# session = requests.Session()

# # -----------------------------
# # Batch id (from pipeline preferred)
# # -----------------------------
# # batch_id = globals().get("batch_id") or time.strftime("%Y%m%d%H%M%S")

# # -----------------------------
# # Helpers
# # -----------------------------
# def sanitize_name(s: str) -> str:
#     return (
#         s.replace("/", "_")
#          .replace("?", "_")
#          .replace("&", "_")
#          .replace("=", "_")
#          .strip()
#     )

# def get_plato_data(
#     page: Optional[int] = None,
#     params_extra: Optional[Dict[str, str]] = None,
#     headers: Optional[Dict[str, str]] = None,
#     domain: str = "appointment",
#     timeout: int = TIMEOUT_SEC,
# ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
#     params = {}
#     if page is not None:
#         params["current_page"] = page
#     if params_extra:
#         params.update(params_extra)

#     req_headers = {"Accept": "application/json"}
#     if headers:
#         req_headers.update(headers)

#     full_url = f"{BASE_URL}{domain.lstrip('/')}"

#     for attempt in range(1, MAX_429_RETRIES + 1):
#         resp = session.get(full_url, params=params, headers=req_headers, timeout=timeout)

#         if resp.status_code in (401, 403):
#             resp.raise_for_status()

#         if resp.status_code == 429:
#             retry_after = resp.headers.get("Retry-After")
#             wait = float(retry_after) if (retry_after and retry_after.isdigit()) else SLEEP_429_SEC
#             time.sleep(wait + random.uniform(0, 1.0))
#             continue

#         resp.raise_for_status()
#         data = resp.json()
#         time.sleep(REQUEST_PACE_SEC)

#         if isinstance(data, dict) and "pages" in data and "per_page" in data:
#             return data
#         if isinstance(data, list):
#             return data
#         if isinstance(data, dict):
#             for k in ("data", "results", "items", "value"):
#                 if k in data and isinstance(data[k], list):
#                     return data[k]
#             return [data]
#         return []

#     raise RuntimeError(f"429 persists after {MAX_429_RETRIES} retries. url={full_url} params={params}")

# def get_pages(domain: str) -> int:
#     info = get_plato_data(headers=HEADERS, domain=f"{domain}/count")
#     return int(info["pages"])

# # -----------------------------
# # Read active staging jobs
# # -----------------------------
# job_id_param = job_id  # pipeline can pass null

# jobs_df = (
#     spark.table("md.etl_jobs")
#     .filter((F.col("active_flg") == "Y") & (F.col("src_catalog") == "plato"))
#     .filter(F.col("job_group_name") == "staging")
#     .select("job_id", "job_name")
# )

# # Apply filter only if job_id_param provided
# if job_id_param is not None and str(job_id_param).strip() != "":
#     job_id_list = [x.strip() for x in str(job_id_param).split(",") if x.strip() != ""]
#     # Compare as string to be safe even if job_id column is INT
#     jobs_df = jobs_df.filter(F.col("job_id").cast("string").isin(job_id_list))

# jobs = jobs_df.orderBy("job_id").collect()

# # jobs = (
# #     spark.table("md.etl_jobs")
# #     .filter((F.col("active_flg") == "Y") & (F.col("src_catalog") == "plato"))
# #     .filter(F.col("job_group_name") == "staging")
# #     # optional: test 1 job
# #     .filter(F.col("job_id") == 1665)
# #     .select("job_id", "job_name")
# #     .orderBy("job_id")
# #     .collect()
# # )

# # -----------------------------
# # Run
# # -----------------------------
# failed_any = False
# failed_details = []  # keep a compact summary for exception payload

# for j in jobs:
#     job_id = str(j["job_id"]).strip()     # your logger expects 4 chars; ensure you store 1665 not 1665.0
#     domain = str(j["job_name"]).strip()

#     if not domain:
#         continue

#     out_path = f"{OUTPUT_DIR}/{sanitize_name(domain)}.json"

#     # LOG: step start
#     start_job_instance(batch_id, job_id, msg=f"Start domain={domain}")

#     try:
#         total_pages = get_pages(domain)

#         data = []
#         page = 1
#         while page <= total_pages:
#             rows = get_plato_data(page=page, headers=HEADERS, domain=domain)
#             data.extend(rows or [])
#             page += 1

#         json_text = json.dumps(data, ensure_ascii=False)
#         mssparkutils.fs.put(out_path, json_text, overwrite=True)

#         # LOG: step success
#         end_job_instance(
#             batch_id, job_id, "SUCCESS",
#             msg=f"Completed domain={domain}. pages={total_pages}. output={out_path}",
#             src_row_num=len(data),
#             tgt_row_num=len(data)
#         )

#         print(f"[{job_id}] Done {domain}. Pages={total_pages}, Records={len(data)}")

#     except Exception as e:
#         failed_any = True

#         # LOG: step failed
#         end_job_instance(
#             batch_id, job_id, "FAILED",
#             msg=f"Failed domain={domain}. {safe_exception_text(e)}"
#         )

#         failed_details.append({
#             "job_id": job_id,
#             "domain": domain,
#             "error": str(e)[:1500]
#         })

#         print(f"[{job_id}] FAILED {domain}: {e}")

#         # IMPORTANT: stop immediately so pipeline marks activity Failed
#         break

# # -----------------------------
# # Finalize notebook result
# # -----------------------------
# if failed_any:
#     payload = {
#         "return_code": -1,
#         "return_msg": "FAILED",
#         "batch_id": batch_id,
#         "failed_details": failed_details
#     }
#     # Raising makes pipeline activity = Failed
#     raise Exception(json.dumps(payload, ensure_ascii=False))

# else:
#     payload = {"return_code": 0, "return_msg": "OK", "batch_id": batch_id}
#     mssparkutils.notebook.exit(json.dumps(payload, ensure_ascii=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
