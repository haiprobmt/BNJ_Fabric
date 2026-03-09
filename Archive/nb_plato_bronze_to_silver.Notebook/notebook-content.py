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

# PARAMETERS CELL ********************

src_schema = 'plato'
src_table = 'patient'

tgt_schema = 'plato'
tgt_table = 'slv_patient'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS lh_silver.plato.{tgt_table}
    (
        patient_id string,
        given_id string,
        patient_name string,
        nric string,
        date_of_birth date,
        marital_status string,
        sex string,
        nationality string,
        nric_type string,
        title string,
        allergies_select string,
        allergies string,
        food_allergies_select string,
        food_allergies string,
        g6pd_status string,
        alerts string,
        telephone string,
        telephone2 string,
        telephone3 string,
        email string,
        address string,
        postal_code string,
        unit_no string,
        do_not_disturb boolean,
        occupation string,
        notes string,
        referred_by string,
        assigned_doctor string,
        tags string,
        recalls string,
        created_on timestamp,
        created_by string,
        last_edited timestamp,
        last_edited_by string,
        silver_loaded_at timestamp
    )
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "/nb_utilities"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

code = generate_conversion_code(df)
exec(code)
df.createOrReplaceTempView(f'vw_{tgt_table}')
df = spark.read.format('delta').load(f'abfss://a65b3ca0-2c82-4670-8dd5-c56f093348a9@onelake.dfs.fabric.microsoft.com/00f74f32-4a00-4dc5-9d91-32fb6d0e5ba9/Tables/plato/{src_table}')
# df.createOrReplaceTempView()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_silver_path = 'abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/e8a5651c-a1b6-49de-b2c5-11b7e197e77a/Tables/plato'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# slv_patient
df_final = spark.sql(f"""
        SELECT
            DISTINCT
            -- Primary Key
            TRIM(_id) AS patient_id,
            TRIM(given_id) AS given_id,
            
            -- Basic Information
            TRIM(name) AS patient_name,
            UPPER(TRIM(nric)) AS nric,
            TRY_CAST(dob AS DATE) AS date_of_birth,
            TRIM(marital_status) AS marital_status,
            TRIM(sex) AS sex,
            TRIM(nationality) AS nationality,
            TRIM(nric_type) AS nric_type,
            TRIM(title) AS title,
            
            -- Medical Information
            TRIM(allergies_select) AS allergies_select,
            NULLIF(TRIM(allergies), '') AS allergies,
            TRIM(food_allergies_select) AS food_allergies_select,
            NULLIF(TRIM(food_allergies), '') AS food_allergies,
            TRIM(g6pd) AS g6pd_status,
            NULLIF(TRIM(alerts), '') AS alerts,
            
            -- Contact Information
            TRIM(telephone) AS telephone,
            TRIM(telephone2) AS telephone2,
            TRIM(telephone3) AS telephone3,
            TRIM(email) AS email,
            NULLIF(TRIM(address), '') AS address,
            TRIM(postal) AS postal_code,
            TRIM(unit_no) AS unit_no,
            
            -- Additional Information
            CAST(COALESCE(dnd, 0) AS BOOLEAN) AS do_not_disturb,
            TRIM(occupation) AS occupation,
            NULLIF(TRIM(notes), '') AS notes,
            TRIM(referred_by) AS referred_by,
            TRIM(doctor) AS assigned_doctor,
            NULLIF(TRIM(tag), '') AS tags,
            NULLIF(TRIM(recalls), '') AS recalls,
            
            -- Audit Fields
            TRY_CAST(created_on AS TIMESTAMP) AS created_on,
            TRIM(created_by) AS created_by,
            TRY_CAST(last_edited AS TIMESTAMP) AS last_edited,
            TRIM(last_edited_by) AS last_edited_by,
            
            -- Metadata
            CURRENT_TIMESTAMP() AS silver_loaded_at
            
        FROM lh_bnj_bronze.plato.patient
        WHERE _id IS NOT NULL;
        """)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
df_final.write.format('delta').mode('overwrite').save(f'{lh_silver_path}/silver_patient')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inventory
df_final = spark.sql(f"""
        SELECT
            -- Primary Keys
            TRIM(_id) AS inventory_id,
            TRIM(given_id) AS given_id,
            
            -- Basic Information
            TRIM(name) AS item_name,
            TRIM(category) AS category,
            NULLIF(TRIM(description), '') AS description,
            TRIM(inventory) AS inventory_type,
            
            -- Pricing
            TRY_CAST(selling_price AS DECIMAL(18,2)) AS selling_price,
            TRY_CAST(cost_price AS DECIMAL(18,2)) AS cost_price,
            TRY_CAST(min_price AS DECIMAL(18,2)) AS min_price,
            TRY_CAST(package_original_price AS DECIMAL(18,2)) AS package_original_price,
            
            -- Inventory Management
            CAST(COALESCE(track_stock, 0) AS BOOLEAN) AS track_stock,
            TRY_CAST(stickers AS INTEGER) AS stickers,
            TRY_CAST(pack_size AS INTEGER) AS pack_size,
            TRY_CAST(qty AS DECIMAL(18,2)) AS quantity_on_hand,
            
            -- Dosage Information
            NULLIF(TRIM(dosage), '') AS dosage,
            NULLIF(TRIM(dusage), '') AS dosage_usage,
            NULLIF(TRIM(ddose), '') AS dosage_dose,
            NULLIF(TRIM(dunit), '') AS dosage_unit,
            NULLIF(TRIM(dfreq), '') AS dosage_frequency,
            NULLIF(TRIM(dduration), '') AS dosage_duration,
            
            -- Units
            TRIM(unit) AS unit,
            TRIM(order_unit) AS order_unit,
            NULLIF(TRIM(units), '') AS units_json,
            
            -- Supplier & Manufacturer
            TRIM(scheme) AS scheme,
            TRIM(supplier) AS supplier_id,
            TRIM(manufacturer) AS manufacturer,
            NULLIF(TRIM(bonus), '') AS bonus,
            TRIM(sales_ref) AS sales_reference,
            
            -- Product Attributes
            NULLIF(TRIM(remarks), '') AS remarks,
            TRIM(facility) AS facility,
            NULLIF(TRIM(recalls), '') AS recalls,
            NULLIF(TRIM(dental), '') AS dental_info,
            NULLIF(TRIM(packageitems), '') AS package_items_json,
            NULLIF(TRIM(precautions), '') AS precautions,
            NULLIF(TRIM(vaccines), '') AS vaccines_info,
            
            -- Redemption & Package Settings
            CAST(COALESCE(redeemable, 0) AS BOOLEAN) AS is_redeemable,
            TRY_CAST(redemptions AS INTEGER) AS redemption_count,
            CAST(COALESCE(multiredeemable, 0) AS BOOLEAN) AS is_multi_redeemable,
            NULLIF(TRIM(multiredemptions), '') AS multi_redemptions_json,
            
            -- Pricing Flags
            CAST(COALESCE(fixed_price, 0) AS BOOLEAN) AS is_fixed_price,
            CAST(COALESCE(no_discount, 0) AS BOOLEAN) AS no_discount,
            CAST(COALESCE(expiry_after_dispensing, 0) AS BOOLEAN) AS has_expiry_after_dispensing,
            CAST(COALESCE(hidden, 0) AS BOOLEAN) AS is_hidden,
            
            -- Other
            NULLIF(TRIM(others), '') AS others_json,
            
            -- Audit Fields
            TRY_CAST(created_on AS TIMESTAMP) AS created_on,
            TRIM(created_by) AS created_by,
            TRY_CAST(last_edited AS TIMESTAMP) AS last_edited,
            TRIM(last_edited_by) AS last_edited_by,
            
            -- Metadata
            CURRENT_TIMESTAMP() AS silver_loaded_at
            
        FROM lh_bnj_bronze.plato.inventory
        WHERE _id IS NOT NULL;
        """)
# df_final.printSchema()
df_final.write.format('delta').mode('overwrite').save(f'{lh_silver_path}/silver_inventory')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Coporate
df_final = spark.sql(f"""
        SELECT
            -- Primary Key
            TRIM(_id) AS corporate_id,
            TRIM(given_id) AS given_id,
            
            -- Basic Information
            TRIM(name) AS corporate_name,
            TRIM(category) AS category,
            CAST(COALESCE(insurance, 0) AS BOOLEAN) AS is_insurance,
            
            -- Contact Information
            NULLIF(TRIM(address), '') AS address,
            TRIM(email) AS email,
            TRIM(telephone) AS telephone,
            TRIM(fax) AS fax,
            TRIM(url) AS url,
            TRIM(person) AS contact_person,
            
            -- Financial Settings
            TRY_CAST(amount AS DECIMAL(18,2)) AS default_amount,
            TRIM(payment_type) AS payment_type,
            TRIM(scheme) AS scheme,
            NULLIF(TRIM(discounts), '') AS discounts_json,
            NULLIF(TRIM(specials), '') AS specials_json,
            NULLIF(TRIM(restricted), '') AS restricted_json,
            
            -- Notes
            NULLIF(TRIM(notes), '') AS notes,
            NULLIF(TRIM(invoice_notes), '') AS invoice_notes,
            NULLIF(TRIM(others), '') AS others_json,
            
            -- Audit Fields
            TRY_CAST(created_on AS TIMESTAMP) AS created_on,
            TRIM(created_by) AS created_by,
            TRY_CAST(last_edited AS TIMESTAMP) AS last_edited,
            TRIM(last_edited_by) AS last_edited_by,
            
            -- Metadata
            CURRENT_TIMESTAMP() AS silver_loaded_at
            
        FROM lh_bnj_bronze.plato.corporate
        WHERE _id IS NOT NULL;
        """)
# df_final.printSchema()
df_final.write.format('delta').mode('overwrite').save(f'{lh_silver_path}/silver_corporate')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# supplier
df_final = spark.sql(f"""
            SELECT
                -- Primary Key
                TRIM(_id) AS supplier_id,
                
                -- Basic Information
                TRIM(name) AS supplier_name,
                TRIM(category) AS category,
                TRIM(person) AS contact_person,
                
                -- Contact Information
                TRIM(email) AS email,
                TRIM(handphone) AS handphone,
                TRIM(telephone) AS telephone,
                TRIM(fax) AS fax,
                NULLIF(TRIM(address), '') AS address,
                TRIM(url) AS url,
                
                -- Notes
                NULLIF(TRIM(notes), '') AS notes,
                NULLIF(TRIM(others), '') AS others_json,
                
                -- Audit Fields
                TRY_CAST(created_on AS TIMESTAMP) AS created_on,
                TRIM(created_by) AS created_by,
                TRY_CAST(last_edited AS TIMESTAMP) AS last_edited,
                TRIM(last_edited_by) AS last_edited_by,
                
                -- Metadata
                CURRENT_TIMESTAMP() AS silver_loaded_at
                
            FROM lh_bnj_bronze.plato.supplier
            WHERE _id IS NOT NULL;
        """)
# df_final.printSchema()
df_final.write.format('delta').mode('overwrite').save(f'{lh_silver_path}/silver_supplier')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# contact
df_final = spark.sql(f"""
        SELECT
            -- Primary Key
            TRIM(_id) AS contact_id,
            
            -- Basic Information
            TRIM(name) AS contact_name,
            TRIM(person) AS person_name,
            TRIM(category) AS category,
            
            -- Contact Information
            TRIM(email) AS email,
            TRIM(handphone) AS handphone,
            TRIM(telephone) AS telephone,
            TRIM(fax) AS fax,
            NULLIF(TRIM(address), '') AS address,
            TRIM(url) AS url,
            
            -- Notes
            NULLIF(TRIM(notes), '') AS notes,
            
            -- Audit Fields
            TRY_CAST(created_on AS TIMESTAMP) AS created_on,
            TRIM(created_by) AS created_by,
            TRY_CAST(last_edited AS TIMESTAMP) AS last_edited,
            TRIM(last_edited_by) AS last_edited_by,
            
            -- Metadata
            CURRENT_TIMESTAMP() AS silver_loaded_at
        FROM lh_bnj_bronze.plato.contact
        WHERE _id IS NOT NULL;
        """)
# df_final.printSchema()
df_final.write.format('delta').mode('overwrite').save(f'{lh_silver_path}/silver_contact')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# invoice
from pyspark.sql.functions import explode, col, explode_outer

df = spark.sql('select * from lh_bnj_bronze.plato.invoice')

base_invoice_df = df.select(
    col("_id").alias("invoice_id"),
    col("adj"),
    col("adj_amount"),
    col("cndn"),
    col("cndn_apply_to"),
    col("corp_notes"),
    col("created_by"),
    col("created_on"),
    col("date"),
    col("doctor"),
    col("finalized"),
    col("finalized_by"),
    col("finalized_on"),
    col("highlight"),
    col("invoice"),
    col("invoice_notes"),
    col("invoice_prefix"),
    col("last_edited"),
    col("last_edited_by"),
    col("location"),
    col("manual_timein"),
    col("manual_timeout"),
    col("no_gst"),
    col("notes"),
    col("patient_id"),
    col("rate"),
    col("scheme"),
    col("session"),
    col("status"),
    col("status_on"),
    col("sub_total"),
    col("tax"),
    col("total"),
    col("void"),
    col("void_by"),
    col("void_on"),
    col("void_reason")
)
base_invoice_df.write.format('delta').mode('overwrite').save(f'{lh_silver_path}/silver_base_invoice')
# =============================================================================
# STEP 2: EXPLODE AND FLATTEN CORPORATE ARRAY
# =============================================================================
corporate_df = df.select(
    col("_id").alias("invoice_id"),
    explode_outer("corporate").alias("corporate_item")
).select(
    "invoice_id",
    col("corporate_item._id").alias("corp_id"),
    col("corporate_item.amount").alias("corp_amount"),
    col("corporate_item.corp").alias("corp_name"),
    col("corporate_item.created_by").alias("corp_created_by"),
    col("corporate_item.created_on").alias("corp_created_on"),
    col("corporate_item.invoice_id").alias("corp_invoice_id"),
    col("corporate_item.location").alias("corp_location"),
    col("corporate_item.ref").alias("corp_ref"),
    col("corporate_item.void").alias("corp_void"),
    col("corporate_item.void_by").alias("corp_void_by"),
    col("corporate_item.void_on").alias("corp_void_on"),
    col("corporate_item.void_reason").alias("corp_void_reason")
)
corporate_df.write.format('delta').mode('overwrite').save(f'{lh_silver_path}/silver_corporate_invoice')
# =============================================================================
# STEP 3: EXPLODE AND FLATTEN ITEMS ARRAY
# =============================================================================
items_df = df.select(
    col("_id").alias("invoice_id"),
    explode_outer("item").alias("item_detail")
).select(
    "invoice_id",
    col("item_detail._id").alias("item_id"),
    col("item_detail.batch_batch").alias("batch_batch"),
    col("item_detail.batch_cpu").alias("batch_cpu"),
    col("item_detail.batch_expiry").alias("batch_expiry"),
    col("item_detail.category").alias("category"),
    col("item_detail.cost_price").alias("cost_price"),
    col("item_detail.ddose").alias("ddose"),
    col("item_detail.dduration").alias("dduration"),
    col("item_detail.description").alias("description"),
    col("item_detail.dfreq").alias("dfreq"),
    col("item_detail.disc_abs").alias("disc_abs"),
    col("item_detail.discount").alias("discount"),
    col("item_detail.dosage").alias("dosage"),
    col("item_detail.dunit").alias("dunit"),
    col("item_detail.expiry_after_dispensing").alias("expiry_after_dispensing"),
    col("item_detail.facility").alias("facility"),
    col("item_detail.facility_due").alias("facility_due"),
    col("item_detail.facility_paid").alias("facility_paid"),
    col("item_detail.facility_ref").alias("facility_ref"),
    col("item_detail.facility_status").alias("facility_status"),
    col("item_detail.fixed_price").alias("fixed_price"),
    col("item_detail.given_id").alias("given_id"),
    col("item_detail.hidden").alias("hidden"),
    col("item_detail.id").alias("item_given_id"),
    col("item_detail.inventory").alias("inventory"),
    col("item_detail.invoice_id").alias("item_invoice_id"),
    col("item_detail.min_price").alias("min_price"),
    col("item_detail.name").alias("item_name"),
    col("item_detail.no_discount").alias("no_discount"),
    col("item_detail.others.batch_batch").alias("others_batch_batch"),
    col("item_detail.others.batch_cpu").alias("others_batch_cpu"),
    col("item_detail.others.batch_expiry").alias("others_batch_expiry"),
    col("item_detail.package_original_price").alias("package_original_price"),
    col("item_detail.packageitems").alias("packageitems"),
    col("item_detail.precautions").alias("precautions"),
    col("item_detail.qty").alias("qty"),
    col("item_detail.rate").alias("item_rate"),
    col("item_detail.recalls").alias("recalls"),
    col("item_detail.redeemable").alias("redeemable"),
    col("item_detail.redemptions").alias("redemptions"),
    col("item_detail.scheme").alias("item_scheme"),
    col("item_detail.selling_price").alias("selling_price"),
    col("item_detail.sub_total").alias("item_sub_total"),
    col("item_detail.track_stock").alias("track_stock"),
    col("item_detail.unit").alias("unit"),
    col("item_detail.unit_price").alias("unit_price"),
    col("item_detail.vaccines").alias("vaccines")
)
items_df.write.format('delta').mode('overwrite').save(f'{lh_silver_path}/silver_item_invoice')
# =============================================================================
# STEP 4: EXPLODE AND FLATTEN PAYMENT ARRAY
# =============================================================================
# payment_df = df.select(
#     col("_id").alias("invoice_id"),
#     explode_outer("payment").alias("payment_detail")
# ).select(
#     "invoice_id",
#     col("payment_detail._id").alias("payment_id"),
#     col("payment_detail.amount").alias("payment_amount"),
#     col("payment_detail.corp").alias("payment_corp"),
#     col("payment_detail.created_by").alias("payment_created_by"),
#     col("payment_detail.created_on").alias("payment_created_on"),
#     col("payment_detail.invoice_id").alias("payment_invoice_id"),
#     col("payment_detail.location").alias("payment_location"),
#     col("payment_detail.mode").alias("payment_mode"),
#     col("payment_detail.ref").alias("payment_ref"),
#     col("payment_detail.session").alias("payment_session"),
#     col("payment_detail.void").alias("payment_void"),
#     col("payment_detail.void_by").alias("payment_void_by"),
#     col("payment_detail.void_on").alias("payment_void_on"),
#     col("payment_detail.void_reason").alias("payment_void_reason")
# )

# =============================================================================
# STEP 5: EXPLODE AND FLATTEN MC (MEDICAL CERTIFICATE) ARRAY
# =============================================================================
mc_df = df.select(
    col("_id").alias("invoice_id"),
    explode_outer("mc").alias("mc_detail")
).select(
    "invoice_id",
    col("mc_detail.cancel").alias("mc_cancel"),
    col("mc_detail.created_by").alias("mc_created_by"),
    col("mc_detail.created_on").alias("mc_created_on"),
    col("mc_detail.id").alias("mc_id"),
    col("mc_detail.serial").alias("mc_serial"),
    col("mc_detail.text").alias("mc_text"),
    col("mc_detail.title").alias("mc_title"),
    # MC Others fields
    col("mc_detail.others.days").alias("mc_days"),
    col("mc_detail.others.email").alias("mc_email"),
    col("mc_detail.others.fromdate").alias("mc_fromdate"),
    col("mc_detail.others.fromemail").alias("mc_fromemail"),
    col("mc_detail.others.sendemail").alias("mc_sendemail"),
    col("mc_detail.others.sendsms").alias("mc_sendsms"),
    col("mc_detail.others.telephone").alias("mc_telephone"),
    col("mc_detail.others.todate").alias("mc_todate"),
    col("mc_detail.others.type").alias("mc_type"),
    # DigiMC nested fields
    col("mc_detail.others.digimc.CreatedByName").alias("digimc_created_by_name"),
    col("mc_detail.others.digimc.CreatedWhen").alias("digimc_created_when"),
    col("mc_detail.others.digimc.DOB").alias("digimc_dob"),
    col("mc_detail.others.digimc.DutyComment").alias("digimc_duty_comment"),
    col("mc_detail.others.digimc.FlagCourt").alias("digimc_flag_court"),
    col("mc_detail.others.digimc.FlagDuration").alias("digimc_flag_duration"),
    col("mc_detail.others.digimc.FlagFit").alias("digimc_flag_fit"),
    col("mc_detail.others.digimc.FlagUnfit").alias("digimc_flag_unfit"),
    col("mc_detail.others.digimc.Institution").alias("digimc_institution"),
    col("mc_detail.others.digimc.MCType").alias("digimc_mc_type"),
    col("mc_detail.others.digimc.MedicalCertificateID").alias("digimc_certificate_id"),
    col("mc_detail.others.digimc.PatientID").alias("digimc_patient_id"),
    col("mc_detail.others.digimc.PatientName").alias("digimc_patient_name"),
    col("mc_detail.others.digimc.ProviderMCR").alias("digimc_provider_mcr"),
    col("mc_detail.others.digimc.Status").alias("digimc_status"),
    col("mc_detail.others.digimc.ToSend").alias("digimc_to_send"),
    col("mc_detail.others.digimc.UnfitFrom").alias("digimc_unfit_from"),
    col("mc_detail.others.digimc.UnfitTo").alias("digimc_unfit_to"),
    col("mc_detail.others.digimc.VisitID").alias("digimc_visit_id"),
    col("mc_detail.others.digimc.Ward").alias("digimc_ward"),
    # Response nested fields
    col("mc_detail.others.response.id").alias("mc_response_id"),
    col("mc_detail.others.response.message").alias("mc_response_message"),
    col("mc_detail.others.response.url").alias("mc_response_url")
)
mc_df.write.format('delta').mode('overwrite').save(f'{lh_silver_path}/silver_mc_invoice')
# =============================================================================
# STEP 6: EXPLODE AND FLATTEN PRESCRIPTION ARRAY (with nested meds)
# =============================================================================
# First explode prescription
prescription_exploded = df.select(
    col("_id").alias("invoice_id"),
    explode_outer("prescription").alias("prescription_detail")
).select(
    "invoice_id",
    col("prescription_detail.cancel").alias("prescription_cancel"),
    col("prescription_detail.created_by").alias("prescription_created_by"),
    col("prescription_detail.created_on").alias("prescription_created_on"),
    col("prescription_detail.id").alias("prescription_id"),
    col("prescription_detail.others").alias("prescription_others"),
    col("prescription_detail.serial").alias("prescription_serial"),
    col("prescription_detail.meds").alias("meds")
)

# Then explode the meds array within prescription
prescription_df = prescription_exploded.select(
    "invoice_id",
    "prescription_cancel",
    "prescription_created_by",
    "prescription_created_on",
    "prescription_id",
    "prescription_others",
    "prescription_serial",
    explode_outer("meds").alias("med_detail")
).select(
    "invoice_id",
    "prescription_cancel",
    "prescription_created_by",
    "prescription_created_on",
    "prescription_id",
    "prescription_others",
    "prescription_serial",
    col("med_detail.dosage").alias("med_dosage"),
    col("med_detail.name").alias("med_name")
)
prescription_df.write.format('delta').mode('overwrite').save(f'{lh_silver_path}/silver_prescription_invoice')
# df_final = spark.sql(f"""
#         MERGE INTO lh_bnj_silver.plato.silver_invoice AS target
#         USING (
#             SELECT
#                 -- Primary Key
#                 TRIM(_id) AS invoice_id,
#                 TRIM(patient_id) AS patient_id,
                
#                 -- Invoice Details
#                 TRIM(location) AS location_id,
#                 TRY_CAST(date AS DATE) AS invoice_date,
#                 CAST(COALESCE(no_gst, 0) AS BOOLEAN) AS no_gst,
#                 TRIM(doctor) AS doctor_id,
                
#                 -- Status
#                 TRIM(status) AS status,
#                 TRY_CAST(status_on AS TIMESTAMP) AS status_on,
#                 CAST(COALESCE(adj, 0) AS BOOLEAN) AS is_adjusted,
#                 CAST(COALESCE(highlight, 0) AS BOOLEAN) AS is_highlighted,
                
#                 -- Financial Details
#                 TRIM(rate) AS rate,
#                 TRY_CAST(sub_total AS DECIMAL(18,2)) AS sub_total,
#                 TRY_CAST(tax AS DECIMAL(18,2)) AS tax,
#                 TRY_CAST(total AS DECIMAL(18,2)) AS total,
#                 TRY_CAST(adj_amount AS DECIMAL(18,2)) AS adjustment_amount,
                
#                 -- Invoice Number
#                 TRIM(invoice_prefix) AS invoice_prefix,
#                 TRY_CAST(invoice AS INTEGER) AS invoice_number,
                
#                 -- Finalization
#                 CAST(COALESCE(finalized, 0) AS BOOLEAN) AS is_finalized,
#                 TRY_CAST(finalized_on AS TIMESTAMP) AS finalized_on,
#                 TRIM(finalized_by) AS finalized_by,
                
#                 -- Notes
#                 NULLIF(TRIM(notes), '') AS notes,
#                 NULLIF(TRIM(corp_notes), '') AS corporate_notes,
#                 NULLIF(TRIM(invoice_notes), '') AS invoice_notes,
                
#                 -- Void Information
#                 CAST(COALESCE(void, 0) AS BOOLEAN) AS is_voided,
#                 NULLIF(TRIM(void_reason), '') AS void_reason,
#                 TRY_CAST(void_on AS TIMESTAMP) AS void_on,
#                 TRIM(void_by) AS void_by,
                
#                 -- Session & Timing
#                 TRY_CAST(session AS INTEGER) AS session_id,
#                 TRY_CAST(manual_timein AS TIMESTAMP) AS manual_timein,
#                 TRY_CAST(manual_timeout AS TIMESTAMP) AS manual_timeout,
                
#                 -- Credit/Debit Note
#                 CAST(COALESCE(cndn, 0) AS BOOLEAN) AS is_cndn,
#                 TRIM(cndn_apply_to) AS cndn_apply_to,
                
#                 -- Corporate & Scheme
#                 TRIM(scheme) AS scheme,
#                 NULLIF(TRIM(others), '') AS others_json,
                
#                 -- Prescriptions & MC
#                 NULLIF(TRIM(mc), '') AS mc_json,
#                 NULLIF(TRIM(prescription), '') AS prescription_json,
#                 NULLIF(TRIM(corporate), '') AS corporate_json,
                
#                 -- Audit Fields
#                 TRY_CAST(created_on AS TIMESTAMP) AS created_on,
#                 TRIM(created_by) AS created_by,
#                 TRY_CAST(last_edited AS TIMESTAMP) AS last_edited,
#                 TRIM(last_edited_by) AS last_edited_by,
                
#                 -- Metadata
#                 CURRENT_TIMESTAMP() AS silver_loaded_at
                
#             FROM lh_bnj_bronze.plato.invoice
#             WHERE _id IS NOT NULL
#             --AND (
#             --    last_edited >= DATEADD(day, -7, CURRENT_DATE())  -- Last 7 days
#             --    OR created_on >= DATEADD(day, -7, CURRENT_DATE())
#             --)
#         ) AS source
#         ON target.invoice_id = source.invoice_id

#         WHEN MATCHED AND (
#             -- Update if any field has changed
#             target.status != source.status
#             OR target.is_finalized != source.is_finalized
#             OR target.is_voided != source.is_voided
#             OR target.last_edited != source.last_edited
#         ) THEN UPDATE SET
#             target.patient_id = source.patient_id,
#             target.location_id = source.location_id,
#             target.invoice_date = source.invoice_date,
#             target.no_gst = source.no_gst,
#             target.doctor_id = source.doctor_id,
#             target.status = source.status,
#             target.status_on = source.status_on,
#             target.is_adjusted = source.is_adjusted,
#             target.is_highlighted = source.is_highlighted,
#             target.rate = source.rate,
#             target.sub_total = source.sub_total,
#             target.tax = source.tax,
#             target.total = source.total,
#             target.adjustment_amount = source.adjustment_amount,
#             target.invoice_prefix = source.invoice_prefix,
#             target.invoice_number = source.invoice_number,
#             target.is_finalized = source.is_finalized,
#             target.finalized_on = source.finalized_on,
#             target.finalized_by = source.finalized_by,
#             target.notes = source.notes,
#             target.corporate_notes = source.corporate_notes,
#             target.invoice_notes = source.invoice_notes,
#             target.is_voided = source.is_voided,
#             target.void_reason = source.void_reason,
#             target.void_on = source.void_on,
#             target.void_by = source.void_by,
#             target.session_id = source.session_id,
#             target.manual_timein = source.manual_timein,
#             target.manual_timeout = source.manual_timeout,
#             target.is_cndn = source.is_cndn,
#             target.cndn_apply_to = source.cndn_apply_to,
#             target.scheme = source.scheme,
#             target.others_json = source.others_json,
#             target.mc_json = source.mc_json,
#             target.prescription_json = source.prescription_json,
#             target.corporate_json = source.corporate_json,
#             target.last_edited = source.last_edited,
#             target.last_edited_by = source.last_edited_by,
#             target.silver_loaded_at = source.silver_loaded_at

#         WHEN NOT MATCHED THEN INSERT (
#             invoice_id, patient_id, location_id, invoice_date, no_gst, doctor_id,
#             status, status_on, is_adjusted, is_highlighted, rate, sub_total, tax, total,
#             adjustment_amount, invoice_prefix, invoice_number, is_finalized, finalized_on,
#             finalized_by, notes, corporate_notes, invoice_notes, is_voided, void_reason,
#             void_on, void_by, session_id, manual_timein, manual_timeout, is_cndn,
#             cndn_apply_to, scheme, others_json, mc_json, prescription_json, corporate_json,
#             created_on, created_by, last_edited, last_edited_by, silver_loaded_at
#         ) VALUES (
#             source.invoice_id, source.patient_id, source.location_id, source.invoice_date,
#             source.no_gst, source.doctor_id, source.status, source.status_on, source.is_adjusted,
#             source.is_highlighted, source.rate, source.sub_total, source.tax, source.total,
#             source.adjustment_amount, source.invoice_prefix, source.invoice_number,
#             source.is_finalized, source.finalized_on, source.finalized_by, source.notes,
#             source.corporate_notes, source.invoice_notes, source.is_voided, source.void_reason,
#             source.void_on, source.void_by, source.session_id, source.manual_timein,
#             source.manual_timeout, source.is_cndn, source.cndn_apply_to, source.scheme,
#             source.others_json, source.mc_json, source.prescription_json, source.corporate_json,
#             source.created_on, source.created_by, source.last_edited, source.last_edited_by,
#             source.silver_loaded_at
#         );
#         """)
# display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# payment
df_final = spark.sql(f"""
MERGE INTO lh_bnj_silver.plato.silver_payment AS target
USING (
    SELECT
        TRY_CAST(_id AS BIGINT) AS payment_id,
        TRIM(invoice_id) AS invoice_id,
        TRIM(mode) AS payment_mode,
        TRIM(corp) AS corporate_name,
        TRY_CAST(amount AS DECIMAL(18,2)) AS amount,
        TRIM(location) AS location_id,
        NULLIF(TRIM(ref), '') AS reference,
        TRY_CAST(session AS INTEGER) AS session_id,
        CAST(COALESCE(void, 0) AS BOOLEAN) AS is_voided,
        TRY_CAST(void_on AS TIMESTAMP) AS void_on,
        TRIM(void_by) AS void_by,
        NULLIF(TRIM(void_reason), '') AS void_reason,
        TRY_CAST(created_on AS TIMESTAMP) AS created_on,
        TRIM(created_by) AS created_by,
        CURRENT_TIMESTAMP() AS silver_loaded_at
    FROM lh_bnj_bronze.plato.payment
    WHERE _id IS NOT NULL
    --  AND created_on >= DATEADD(day, -7, CURRENT_DATE())
) AS source
ON target.payment_id = source.payment_id

WHEN MATCHED AND (
    target.is_voided != source.is_voided
    OR target.void_reason != source.void_reason
) THEN UPDATE SET
    target.invoice_id = source.invoice_id,
    target.payment_mode = source.payment_mode,
    target.corporate_name = source.corporate_name,
    target.amount = source.amount,
    target.location_id = source.location_id,
    target.reference = source.reference,
    target.session_id = source.session_id,
    target.is_voided = source.is_voided,
    target.void_on = source.void_on,
    target.void_by = source.void_by,
    target.void_reason = source.void_reason,
    target.silver_loaded_at = source.silver_loaded_at

WHEN NOT MATCHED THEN INSERT (
    payment_id, invoice_id, payment_mode, corporate_name, amount, location_id,
    reference, session_id, is_voided, void_on, void_by, void_reason,
    created_on, created_by, silver_loaded_at
) VALUES (
    source.payment_id, source.invoice_id, source.payment_mode, source.corporate_name,
    source.amount, source.location_id, source.reference, source.session_id,
    source.is_voided, source.void_on, source.void_by, source.void_reason,
    source.created_on, source.created_by, source.silver_loaded_at
);
""")
display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_utilities

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/plato/invoice.json")
# df now is a Spark DataFrame containing JSON data from "abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Files/plato/invoice.json".
code = generate_conversion_code(df)
exec(code)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format('delta').mode('overwrite').save('abfss://c81c26a1-3386-4efb-b6ea-59363a6145b7@onelake.dfs.fabric.microsoft.com/c9d7507e-938a-4c6d-a042-d8743e386ab5/Tables/plato/invoice')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# appointment
df_final = spark.sql(f"""
MERGE INTO lh_bnj_silver.plato.silver_appointment AS target
USING (
    SELECT
        TRIM(_id) AS appointment_id,
        TRIM(patient_id) AS patient_id,
        TRIM(title) AS title,
        TRIM(prefix) AS prefix,
        NULLIF(TRIM(description), '') AS description,
        NULLIF(TRIM(dropdown), '') AS dropdown,
        TRIM(email) AS email,
        TRY_CAST(TRIM(handphone) AS INTEGER) AS handphone,
        TRY_CAST(starttime AS TIMESTAMP) AS start_time,
        TRY_CAST(endtime AS TIMESTAMP) AS end_time,
        CAST(COALESCE(allDay, 0) AS BOOLEAN) AS is_all_day,
        TRIM(color) AS color,
        NULLIF(TRIM(code_Top), '') AS code_top,
        NULLIF(TRIM(code_Bottom), '') AS code_bottom,
        NULLIF(TRIM(code_Left), '') AS code_left,
        NULLIF(TRIM(code_Right), '') AS code_right,
        NULLIF(TRIM(code_Background), '') AS code_background,
        TRIM(doctor) AS doctor_id,
        NULLIF(TRIM(recur), '') AS recurrence,
        TRY_CAST(created_on AS TIMESTAMP) AS created_on,
        TRIM(created_by) AS created_by,
        TRY_CAST(last_edited AS TIMESTAMP) AS last_edited,
        TRIM(last_edited_by) AS last_edited_by,
        CURRENT_TIMESTAMP() AS silver_loaded_at
    FROM lh_bnj_bronze.plato.appointment
    WHERE _id IS NOT NULL
    --  AND (
    --      last_edited >= DATEADD(day, -30, CURRENT_DATE())
    --      OR created_on >= DATEADD(day, -30, CURRENT_DATE())
    --  )
) AS source
ON target.appointment_id = source.appointment_id

WHEN MATCHED AND target.last_edited != source.last_edited THEN UPDATE SET
    target.patient_id = source.patient_id,
    target.title = source.title,
    target.prefix = source.prefix,
    target.description = source.description,
    target.dropdown = source.dropdown,
    target.email = source.email,
    target.handphone = source.handphone,
    target.start_time = source.start_time,
    target.end_time = source.end_time,
    target.is_all_day = source.is_all_day,
    target.color = source.color,
    target.code_top = source.code_top,
    target.code_bottom = source.code_bottom,
    target.code_left = source.code_left,
    target.code_right = source.code_right,
    target.code_background = source.code_background,
    target.doctor_id = source.doctor_id,
    target.recurrence = source.recurrence,
    target.last_edited = source.last_edited,
    target.last_edited_by = source.last_edited_by,
    target.silver_loaded_at = source.silver_loaded_at

WHEN NOT MATCHED THEN INSERT (
    appointment_id, patient_id, title, prefix, description, dropdown, email,
    handphone, start_time, end_time, is_all_day, color, code_top, code_bottom,
    code_left, code_right, code_background, doctor_id, recurrence, created_on,
    created_by, last_edited, last_edited_by, silver_loaded_at
) VALUES (
    source.appointment_id, source.patient_id, source.title, source.prefix,
    source.description, source.dropdown, source.email, source.handphone,
    source.start_time, source.end_time, source.is_all_day, source.color,
    source.code_top, source.code_bottom, source.code_left, source.code_right,
    source.code_background, source.doctor_id, source.recurrence, source.created_on,
    source.created_by, source.last_edited, source.last_edited_by, source.silver_loaded_at
);
""")
display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_bnj_silver.plato.silver_inventory LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
