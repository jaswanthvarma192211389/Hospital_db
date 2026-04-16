CREATE TABLE curated.dim_patient (
    patient_sk STRING,
    patient_id STRING,
    name STRING,
    state STRING,
    insurance_id STRING,
    start_date DATE,
    end_date DATE,
    current_flag STRING
);

CREATE TABLE curated.dim_doctor (
    doctor_sk STRING,
    doctor_id STRING,
    name STRING,
    department STRING,
    start_date DATE,
    end_date DATE,
    current_flag STRING
);

CREATE TABLE curated.fact_hospital_events (
    event_sk STRING,
    event_type STRING,

    admission_id STRING,
    procedure_id STRING,
    bill_id STRING,

    patient_sk STRING,
    doctor_sk STRING,

    admission_time TIMESTAMP,
    discharge_time TIMESTAMP,

    scheduled_time TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,

    billing_time TIMESTAMP,
    total_amount FLOAT,

    bed_no NUMBER,
    department STRING
);
