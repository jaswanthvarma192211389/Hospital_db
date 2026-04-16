CREATE OR REPLACE TABLE validated.valid_patients (
    patient_id VARCHAR PRIMARY KEY,
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
    _load_timestamp TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE validated.valid_doctors (
    doctor_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    specialization VARCHAR,
    join_date DATE,
    phone VARCHAR,
    email VARCHAR,
    qualification VARCHAR,
    department VARCHAR,
    status VARCHAR,
    _load_timestamp TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE validated.valid_admissions (
    admission_id VARCHAR PRIMARY KEY,
    patient_id VARCHAR,
    admission_time TIMESTAMP_NTZ,
    discharge_time TIMESTAMP_NTZ,
    department VARCHAR,
    ward VARCHAR,
    bed_no NUMBER, -- Transformed to Number based on DQ Rules
    attending_doctor_id VARCHAR,
    admission_type VARCHAR,
    status VARCHAR,
    _load_timestamp TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE validated.valid_procedures (
    procedure_id VARCHAR PRIMARY KEY,
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
    _load_timestamp TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE validated.valid_billing (
    bill_id VARCHAR PRIMARY KEY,
    admission_id VARCHAR,
    patient_id VARCHAR,
    total_amount FLOAT,
    insurance_amount FLOAT,
    out_of_pocket_amount FLOAT,
    billing_time TIMESTAMP_NTZ,
    payment_mode VARCHAR,
    is_flagged BOOLEAN, -- Standardized to BOOLEAN
    _load_timestamp TIMESTAMP_NTZ
);

CREATE SCHEMA IF NOT EXISTS governance;
CREATE OR REPLACE TABLE governance.dq_exception_log (
    error_id VARCHAR DEFAULT UUID_STRING(),
    source_table VARCHAR,
    business_key VARCHAR,
    error_type VARCHAR,
    error_message VARCHAR,
    _recorded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
