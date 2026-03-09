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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- =========================
# MAGIC -- silver_patient
# MAGIC -- =========================
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_patient;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_patient (
# MAGIC   patient_id            string,
# MAGIC   given_id              string,
# MAGIC   patient_name          string,
# MAGIC   nric                  string,
# MAGIC   date_of_birth         date,
# MAGIC   marital_status        string,
# MAGIC   sex                   string,
# MAGIC   nationality           string,
# MAGIC   nric_type             string,
# MAGIC   title                 string,
# MAGIC   allergies_select      string,
# MAGIC   allergies             string,
# MAGIC   food_allergies_select string,
# MAGIC   food_allergies        string,
# MAGIC   g6pd_status           string,
# MAGIC   alerts                string,
# MAGIC   email                 string,
# MAGIC   telephone             string,
# MAGIC   telephone2            string,
# MAGIC   telephone3            string,
# MAGIC   address               string,
# MAGIC   postal_code           string,
# MAGIC   unit_no               string,
# MAGIC   occupation            string,
# MAGIC   do_not_disturb        bigint,
# MAGIC   referred_by           string,
# MAGIC   assigned_doctor       string,
# MAGIC   tags                  string,
# MAGIC   recalls               string,
# MAGIC   notes                 string,
# MAGIC   created_on            string,
# MAGIC   created_by            string,
# MAGIC   last_edited           string,
# MAGIC   last_edited_by        string,
# MAGIC   silver_loaded_at      timestamp
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC -- =========================
# MAGIC -- silver_payment
# MAGIC -- =========================
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_payment;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_payment (
# MAGIC   payment_id bigint,
# MAGIC   invoice_id string,
# MAGIC   payment_mode string,
# MAGIC   corporate_name string,
# MAGIC   amount decimal(18,2),
# MAGIC   location_id string,
# MAGIC   reference string,
# MAGIC   session_id int,
# MAGIC   is_voided boolean,
# MAGIC   void_on timestamp,
# MAGIC   void_by string,
# MAGIC   void_reason string,
# MAGIC   created_on timestamp,
# MAGIC   created_by string,
# MAGIC   silver_loaded_at timestamp
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- =========================
# MAGIC -- silver_supplier
# MAGIC -- =========================
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_supplier;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_supplier (
# MAGIC   supplier_id string,
# MAGIC   supplier_name string,
# MAGIC   category string,
# MAGIC   contact_person string,
# MAGIC   email string,
# MAGIC   handphone string,
# MAGIC   telephone string,
# MAGIC   fax string,
# MAGIC   address string,
# MAGIC   url string,
# MAGIC   notes string,
# MAGIC   others_json string,
# MAGIC   created_on timestamp,
# MAGIC   created_by string,
# MAGIC   last_edited timestamp,
# MAGIC   last_edited_by string,
# MAGIC   silver_loaded_at timestamp
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- =========================
# MAGIC -- silver_mc_invoice
# MAGIC -- =========================
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_mc_invoice;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_mc_invoice (
# MAGIC   invoice_id string,
# MAGIC   mc_cancel string,
# MAGIC   mc_created_by string,
# MAGIC   mc_created_on string,
# MAGIC   mc_id string,
# MAGIC   mc_serial string,
# MAGIC   mc_text string,
# MAGIC   mc_title string,
# MAGIC   mc_days string,
# MAGIC   mc_email string,
# MAGIC   mc_fromdate string,
# MAGIC   mc_fromemail string,
# MAGIC   mc_sendemail string,
# MAGIC   mc_sendsms string,
# MAGIC   mc_telephone string,
# MAGIC   mc_todate string,
# MAGIC   mc_type string,
# MAGIC 
# MAGIC   digimc_created_by_name string,
# MAGIC   digimc_created_when string,
# MAGIC   digimc_dob string,
# MAGIC   digimc_duty_comment string,
# MAGIC   digimc_flag_court string,
# MAGIC   digimc_flag_duration string,
# MAGIC   digimc_flag_fit string,
# MAGIC   digimc_flag_unfit string,
# MAGIC   digimc_institution string,
# MAGIC   digimc_mc_type string,
# MAGIC   digimc_certificate_id string,
# MAGIC   digimc_patient_id string,
# MAGIC   digimc_patient_name string,
# MAGIC   digimc_provider_mcr string,
# MAGIC   digimc_status string,
# MAGIC   digimc_to_send string,
# MAGIC   digimc_unfit_from string,
# MAGIC   digimc_unfit_to string,
# MAGIC   digimc_visit_id string,
# MAGIC   digimc_ward string,
# MAGIC 
# MAGIC   mc_response_id string,
# MAGIC   mc_response_message string,
# MAGIC   mc_response_url string
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC -- =========================
# MAGIC -- silver_item_invoice
# MAGIC -- =========================
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_item_invoice;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_item_invoice (
# MAGIC   invoice_id                 STRING,
# MAGIC   item_id                    BIGINT,
# MAGIC   batch_batch                STRING,
# MAGIC   batch_cpu                  DOUBLE,
# MAGIC   batch_expiry               BIGINT,
# MAGIC   category                   STRING,
# MAGIC   cost_price                 DOUBLE,
# MAGIC   ddose                      STRING,
# MAGIC   dduration                  STRING,
# MAGIC   description                STRING,
# MAGIC   dfreq                      STRING,
# MAGIC   disc_abs                   BIGINT,
# MAGIC   discount                   BIGINT,
# MAGIC   dosage                     STRING,
# MAGIC   dunit                      STRING,
# MAGIC   expiry_after_dispensing    BIGINT,
# MAGIC   facility                   STRING,
# MAGIC   facility_due               STRING,
# MAGIC   facility_paid              BIGINT,
# MAGIC   facility_ref               STRING,
# MAGIC   facility_status            STRING,
# MAGIC   fixed_price                BIGINT,
# MAGIC   given_id                   STRING,
# MAGIC   hidden                     BIGINT,
# MAGIC   item_given_id              STRING,
# MAGIC   inventory                  STRING,
# MAGIC   item_invoice_id            STRING,
# MAGIC   min_price                  BIGINT,
# MAGIC   item_name                  STRING,
# MAGIC   no_discount                BIGINT,
# MAGIC   -- others_batch_batch         STRING,
# MAGIC   -- others_batch_cpu           DOUBLE,
# MAGIC   -- others_batch_expiry        BIGINT,
# MAGIC   package_original_price     BIGINT,
# MAGIC   packageitems               STRING,
# MAGIC   precautions                STRING,
# MAGIC   qty                        BIGINT,
# MAGIC   item_rate                  STRING,
# MAGIC   recalls                    STRING,
# MAGIC   redeemable                 BIGINT,
# MAGIC   redemptions                BIGINT,
# MAGIC   item_scheme                STRING,
# MAGIC   selling_price              DOUBLE,
# MAGIC   item_sub_total             DOUBLE,
# MAGIC   track_stock                BIGINT,
# MAGIC   unit                       STRING,
# MAGIC   unit_price                 DOUBLE,
# MAGIC   vaccines                   STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC -- =========================
# MAGIC -- silver_prescription_invoice
# MAGIC -- =========================
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_prescription_invoice;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_prescription_invoice (
# MAGIC   invoice_id string,
# MAGIC   prescription_cancel string,
# MAGIC   prescription_created_by string,
# MAGIC   prescription_created_on string,
# MAGIC   prescription_id string,
# MAGIC   prescription_others string,
# MAGIC   prescription_serial string,
# MAGIC   med_dosage string,
# MAGIC   med_name string
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC %%sql
# MAGIC 
# MAGIC -- =========================
# MAGIC -- silver_inventory
# MAGIC -- =========================
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_inventory;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_inventory (
# MAGIC   inventory_id                 string,
# MAGIC   given_id                     string,
# MAGIC   item_name                    string,
# MAGIC   category                     string,
# MAGIC   description                  string,
# MAGIC   inventory_type               string,
# MAGIC   selling_price                decimal(18,2),
# MAGIC   cost_price                   decimal(18,2),
# MAGIC   min_price                    decimal(18,2),
# MAGIC   package_original_price       decimal(18,2),
# MAGIC   track_stock                  boolean,
# MAGIC   stickers                     int,
# MAGIC   pack_size                    int,
# MAGIC   quantity_on_hand             decimal(18,2),
# MAGIC   dosage                       string,
# MAGIC   dosage_usage                 string,
# MAGIC   dosage_dose                  string,
# MAGIC   dosage_unit                  string,
# MAGIC   dosage_frequency             string,
# MAGIC   dosage_duration              string,
# MAGIC   unit                         string,
# MAGIC   order_unit                   string,
# MAGIC   units_json                   string,
# MAGIC   scheme                       string,
# MAGIC   supplier_id                  string,
# MAGIC   manufacturer                 string,
# MAGIC   bonus                        string,
# MAGIC   sales_reference              string,
# MAGIC   remarks                      string,
# MAGIC   facility                     string,
# MAGIC   recalls                      string,
# MAGIC   dental_info                  string,
# MAGIC   package_items_json           string,
# MAGIC   precautions                  string,
# MAGIC   vaccines_info                string,
# MAGIC   is_redeemable                boolean,
# MAGIC   redemption_count             int,
# MAGIC   is_multi_redeemable          boolean,
# MAGIC   multi_redemptions_json       string,
# MAGIC   is_fixed_price               boolean,
# MAGIC   no_discount                  boolean,
# MAGIC   has_expiry_after_dispensing  boolean,
# MAGIC   is_hidden                    boolean,
# MAGIC   others_json                  string,
# MAGIC   created_on                   timestamp,
# MAGIC   created_by                   string,
# MAGIC   last_edited                  timestamp,
# MAGIC   last_edited_by               string,
# MAGIC   silver_loaded_at             timestamp
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC -- =========================
# MAGIC -- silver_contact
# MAGIC -- =========================
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_contact;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_contact (
# MAGIC   contact_id string,
# MAGIC   contact_name string,
# MAGIC   person_name string,
# MAGIC   category string,
# MAGIC   email string,
# MAGIC   handphone string,
# MAGIC   telephone string,
# MAGIC   fax string,
# MAGIC   address string,
# MAGIC   url string,
# MAGIC   notes string,
# MAGIC   created_on timestamp,
# MAGIC   created_by string,
# MAGIC   last_edited timestamp,
# MAGIC   last_edited_by string,
# MAGIC   silver_loaded_at timestamp
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- =========================
# MAGIC -- silver_corporate
# MAGIC -- =========================
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_corporate;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_corporate (
# MAGIC   corporate_id string,
# MAGIC   given_id string,
# MAGIC   corporate_name string,
# MAGIC   category string,
# MAGIC   is_insurance boolean,
# MAGIC   address string,
# MAGIC   email string,
# MAGIC   telephone string,
# MAGIC   fax string,
# MAGIC   url string,
# MAGIC   contact_person string,
# MAGIC   default_amount decimal(18,2),
# MAGIC   payment_type string,
# MAGIC   scheme string,
# MAGIC   discounts_json string,
# MAGIC   specials_json string,
# MAGIC   restricted_json string,
# MAGIC   notes string,
# MAGIC   invoice_notes string,
# MAGIC   others_json string,
# MAGIC   created_on timestamp,
# MAGIC   created_by string,
# MAGIC   last_edited timestamp,
# MAGIC   last_edited_by string,
# MAGIC   silver_loaded_at timestamp
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- =========================
# MAGIC -- silver_base_invoice
# MAGIC -- =========================
# MAGIC 
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_base_invoice;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_base_invoice (
# MAGIC   invoice_id string,
# MAGIC   adj bigint,
# MAGIC   adj_amount string,
# MAGIC   cndn bigint,
# MAGIC   cndn_apply_to string,
# MAGIC   corp_notes string,
# MAGIC   created_by string,
# MAGIC   created_on string,
# MAGIC   date string,
# MAGIC   doctor string,
# MAGIC   finalized bigint,
# MAGIC   finalized_by string,
# MAGIC   finalized_on string,
# MAGIC   highlight bigint,
# MAGIC   invoice bigint,
# MAGIC   invoice_notes string,
# MAGIC   invoice_prefix string,
# MAGIC   last_edited string,
# MAGIC   last_edited_by string,
# MAGIC   location string,
# MAGIC   manual_timein string,
# MAGIC   manual_timeout string,
# MAGIC   no_gst bigint,
# MAGIC   notes string,
# MAGIC   patient_id string,
# MAGIC   rate string,
# MAGIC   scheme string,
# MAGIC   session bigint,
# MAGIC   status string,
# MAGIC   status_on string,
# MAGIC   sub_total string,
# MAGIC   tax string,
# MAGIC   total string,
# MAGIC   void bigint,
# MAGIC   void_by string,
# MAGIC   void_on string,
# MAGIC   void_reason string
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC -- =========================
# MAGIC -- silver_corporate_invoice
# MAGIC -- =========================
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_corporate_invoice;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_corporate_invoice (
# MAGIC   invoice_id string,
# MAGIC   corp_id string,
# MAGIC   corp_amount decimal(18,2),
# MAGIC   corp_name string,
# MAGIC   corp_created_by string,
# MAGIC   corp_created_on string,
# MAGIC   corp_invoice_id string,
# MAGIC   corp_location string,
# MAGIC   corp_ref string,
# MAGIC   corp_void string,
# MAGIC   corp_void_by string,
# MAGIC   corp_void_on string,
# MAGIC   corp_void_reason string
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- =========================
# MAGIC -- silver_appointment
# MAGIC -- =========================
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato_test.silver_appointment;
# MAGIC 
# MAGIC CREATE TABLE lh_bnj_silver.plato_test.silver_appointment (
# MAGIC   appointment_id     string,
# MAGIC   patient_id         string,
# MAGIC   title              string,
# MAGIC   prefix             string,
# MAGIC   description        string,
# MAGIC   dropdown           string,
# MAGIC   email              string,
# MAGIC   handphone          int,
# MAGIC   start_time         timestamp,
# MAGIC   end_time           timestamp,
# MAGIC   is_all_day         boolean,
# MAGIC   color              string,
# MAGIC   code_top           string,
# MAGIC   code_bottom        string,
# MAGIC   code_left          string,
# MAGIC   code_right         string,
# MAGIC   code_background    string,
# MAGIC   doctor_id          string,
# MAGIC   recurrence         string,
# MAGIC   created_on         timestamp,
# MAGIC   created_by         string,
# MAGIC   last_edited        timestamp,
# MAGIC   last_edited_by     string,
# MAGIC   silver_loaded_at   timestamp
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_silver.plato.silver_invoice (
# MAGIC  invoice_id string,
# MAGIC  patient_id string,
# MAGIC  location_id string,
# MAGIC  invoice_date date,
# MAGIC  no_gst boolean,
# MAGIC  doctor_id string,
# MAGIC  status string,
# MAGIC  status_on timestamp,
# MAGIC  is_adjusted boolean,
# MAGIC  is_highlighted boolean,
# MAGIC  rate string,
# MAGIC  sub_total decimal(18,2),
# MAGIC  tax decimal(18,2),
# MAGIC  total decimal(18,2),
# MAGIC  adjustment_amount decimal(18,2),
# MAGIC  invoice_prefix string,
# MAGIC  invoice_number integer,
# MAGIC  is_finalized boolean,
# MAGIC  finalized_on timestamp,
# MAGIC  finalized_by string,
# MAGIC  notes string,
# MAGIC  corporate_notes string,
# MAGIC  invoice_notes string,
# MAGIC  is_voided boolean,
# MAGIC  void_reason string,
# MAGIC  void_on timestamp,
# MAGIC  void_by string,
# MAGIC  session_id integer,
# MAGIC  manual_timein timestamp,
# MAGIC  manual_timeout timestamp,
# MAGIC  is_cndn boolean,
# MAGIC  cndn_apply_to string,
# MAGIC  scheme string,
# MAGIC  others_json string,
# MAGIC  mc_json string,
# MAGIC  prescription_json string,
# MAGIC  corporate_json string,
# MAGIC  created_on timestamp,
# MAGIC  created_by string,
# MAGIC  last_edited timestamp,
# MAGIC  last_edited_by string,
# MAGIC  silver_loaded_at timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato.silver_payment;
# MAGIC CREATE TABLE lh_bnj_silver.plato.silver_payment (
# MAGIC  payment_id long,
# MAGIC  invoice_id string,
# MAGIC  payment_mode string,
# MAGIC  silver_payment string,
# MAGIC  amount decimal(18,2),
# MAGIC  location_id string,
# MAGIC  reference string,
# MAGIC  session_id integer,
# MAGIC  is_voided integer,
# MAGIC  void_on timestamp,
# MAGIC  void_by string,
# MAGIC  void_reason string,
# MAGIC  created_on timestamp,
# MAGIC  created_by string,
# MAGIC  silver_loaded_at timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_silver.plato.silver_appointment (
# MAGIC  appointment_id string,
# MAGIC  patient_id string,
# MAGIC  title string,
# MAGIC  prefix string,
# MAGIC  description string,
# MAGIC  dropdown string,
# MAGIC  email string,
# MAGIC  handphone integer,
# MAGIC  start_time timestamp,
# MAGIC  end_time timestamp,
# MAGIC  is_all_day integer,
# MAGIC  color string,
# MAGIC  code_top string,
# MAGIC  code_bottom string,
# MAGIC  code_left string,
# MAGIC  code_right string,
# MAGIC  code_background string,
# MAGIC  doctor_id string,
# MAGIC  recurrence string,
# MAGIC  created_on timestamp,
# MAGIC  created_by string,
# MAGIC  last_edited timestamp,
# MAGIC  last_edited_by string,
# MAGIC  silver_loaded_at timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS lh_bnj_silver.plato.silver_patient;
# MAGIC CREATE TABLE lh_bnj_silver.plato.silver_patient (
# MAGIC  patient_id string,
# MAGIC  given_id string,
# MAGIC  patient_name string,
# MAGIC  nric string,
# MAGIC  date_of_birth date,
# MAGIC  marital_status string,
# MAGIC  sex string,
# MAGIC  nationality string,
# MAGIC  nric_type string,
# MAGIC  title string,
# MAGIC  allergies_select string,
# MAGIC  allergies string,
# MAGIC  food_allergies_select string,
# MAGIC  food_allergies string,
# MAGIC  g6pd_status string,
# MAGIC  alerts string,
# MAGIC  telephone string,
# MAGIC  telephone2 string,
# MAGIC  telephone3 string,
# MAGIC  email string,
# MAGIC  address string,
# MAGIC  postal_code string,
# MAGIC  unit_no string,
# MAGIC  do_not_disturb boolean,
# MAGIC  occupation string,
# MAGIC  notes string,
# MAGIC  referred_by string,
# MAGIC  assigned_doctor string,
# MAGIC  tags string,
# MAGIC  recalls string,
# MAGIC  created_on timestamp,
# MAGIC  created_by string,
# MAGIC  last_edited timestamp,
# MAGIC  last_edited_by string,
# MAGIC  silver_loaded_at timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_silver.plato.silver_inventory (
# MAGIC  inventory_id string,
# MAGIC  given_id string,
# MAGIC  item_name string,
# MAGIC  category string,
# MAGIC  description string,
# MAGIC  inventory_type string,
# MAGIC  selling_price decimal(18,2),
# MAGIC  cost_price decimal(18,2),
# MAGIC  min_price decimal(18,2),
# MAGIC  package_original_price decimal(18,2),
# MAGIC  track_stock boolean,
# MAGIC  stickers integer,
# MAGIC  pack_size integer,
# MAGIC  quantity_on_hand decimal(18,2),
# MAGIC  dosage string,
# MAGIC  dosage_usage string,
# MAGIC  dosage_dose string,
# MAGIC  dosage_unit string,
# MAGIC  dosage_frequency string,
# MAGIC  dosage_duration string,
# MAGIC  unit string,
# MAGIC  order_unit string,
# MAGIC  units_json string,
# MAGIC  scheme string,
# MAGIC  supplier_id string,
# MAGIC  manufacturer string,
# MAGIC  bonus string,
# MAGIC  sales_reference string,
# MAGIC  remarks string,
# MAGIC  facility string,
# MAGIC  recalls string,
# MAGIC  dental_info string,
# MAGIC  package_items_json string,
# MAGIC  precautions string,
# MAGIC  vaccines_info string,
# MAGIC  is_redeemable boolean,
# MAGIC  redemption_count integer,
# MAGIC  is_multi_redeemable boolean,
# MAGIC  multi_redemptions_json string,
# MAGIC  is_fixed_price boolean,
# MAGIC  no_discount boolean,
# MAGIC  has_expiry_after_dispensing boolean,
# MAGIC  is_hidden boolean,
# MAGIC  others_json string,
# MAGIC  created_on timestamp,
# MAGIC  created_by string,
# MAGIC  last_edited timestamp,
# MAGIC  last_edited_by string,
# MAGIC  silver_loaded_at timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_silver.plato.silver_corporate (
# MAGIC  corporate_id string,
# MAGIC  given_id string,
# MAGIC  corporate_name string,
# MAGIC  category string,
# MAGIC  is_insurance boolean,
# MAGIC  address string,
# MAGIC  email string,
# MAGIC  telephone string,
# MAGIC  fax string,
# MAGIC  url string,
# MAGIC  contact_person string,
# MAGIC  default_amount decimal(18,2),
# MAGIC  payment_type string,
# MAGIC  scheme string,
# MAGIC  discounts_json string,
# MAGIC  specials_json string,
# MAGIC  restricted_json string,
# MAGIC  notes string,
# MAGIC  invoice_notes string,
# MAGIC  others_json string,
# MAGIC  created_on timestamp,
# MAGIC  created_by string,
# MAGIC  last_edited timestamp,
# MAGIC  last_edited_by string,
# MAGIC  silver_loaded_at timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE lh_bnj_bronze.plato.silver_supplier (
# MAGIC  supplier_id string,
# MAGIC  supplier_name string,
# MAGIC  category string,
# MAGIC  contact_person string,
# MAGIC  email string,
# MAGIC  handphone string,
# MAGIC  telephone string,
# MAGIC  fax string,
# MAGIC  address string,
# MAGIC  url string,
# MAGIC  notes string,
# MAGIC  others_json string,
# MAGIC  created_on timestamp,
# MAGIC  created_by string,
# MAGIC  last_edited timestamp,
# MAGIC  last_edited_by string,
# MAGIC  silver_loaded_at timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
