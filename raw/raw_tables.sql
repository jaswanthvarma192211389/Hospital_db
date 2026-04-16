create database if not exists  hospital ;

use database hospital;

create schema if not exists raw;
create schema if not exists validated;
create schema if not exists curated;
create schema if not exists external_stages;


CREATE FILE FORMAT IF NOT EXISTS RAW.CSV_FORMAT
  TYPE = 'CSV'
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  ESCAPE_UNENCLOSED_FIELD = NONE
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO'
  NULL_IF = ('', 'NULL', 'null');


create stage if not exists external_stages.hospital_stage;

CREATE OR REPLACE TABLE raw.raw_patients (
    patient_id VARCHAR,
    name VARCHAR,
    dob DATE,
    gender VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    insurance_id VARCHAR,
    registration_date DATE,
    -- Audit tags
    _file_name VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw.raw_doctors (
    doctor_id VARCHAR,
    name VARCHAR,
    specialization VARCHAR,
    join_date DATE,
    phone VARCHAR,
    email VARCHAR,
    qualification VARCHAR,
    department VARCHAR,
    status VARCHAR,
    -- Audit tags
    _file_name VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw.raw_admissions (
    admission_id VARCHAR,
    patient_id VARCHAR,
    admission_time TIMESTAMP_NTZ,
    discharge_time TIMESTAMP_NTZ,
    department VARCHAR,
    ward VARCHAR,
    bed_no VARCHAR, -- Kept string to allow raw data logging before converting to number
    attending_doctor_id VARCHAR,
    admission_type VARCHAR,
    status VARCHAR,
    -- Audit tags
    _file_name VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw.raw_procedures (
    procedure_id VARCHAR,
    admission_id VARCHAR,
    patient_id VARCHAR,
    procedure_name VARCHAR,
    surgeon_id VARCHAR,
    scheduled_time TIMESTAMP_NTZ,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    operating_room VARCHAR,
    anaesthesia_type VARCHAR,
    status VARCHAR,
    -- Audit tags
    _file_name VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw.raw_billing (
    bill_id VARCHAR,
    admission_id VARCHAR,
    patient_id VARCHAR,
    total_amount FLOAT,
    insurance_amount FLOAT,
    out_of_pocket_amount FLOAT,
    billing_time TIMESTAMP_NTZ,
    payment_mode VARCHAR,
    is_flagged VARCHAR, -- raw boolean as varchar
    -- Audit tags
    _file_name VARCHAR,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


create or replace stream patient_stream on table raw.raw_patients;
create or replace stream billing_stream on table raw.raw_billing;
create or replace stream doctors_stream on table raw.raw_doctors;
create or replace stream admissions_stream on table raw.raw_admissions;
create or replace stream procedures_stream on table raw.raw_procedures;