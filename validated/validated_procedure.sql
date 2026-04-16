CREATE OR REPLACE PROCEDURE validated.process_data_quality()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- -------------------------------------------------------------
    -- A. PATIENT STREAM PROCESSING
    -- -------------------------------------------------------------
    -- Since we're consuming the stream, we wrap it in a temporary table to avoid advancing the stream prematurely.
    CREATE TEMPORARY TABLE cur_patients_stream AS SELECT * FROM HOSPITAL.EXTERNAL_STAGES.PATIENT_STREAM;

    -- Upsert Good Records (Deduplication applied)
    MERGE INTO validated.valid_patients tgt
    USING (
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY patient_id ORDER BY _load_timestamp DESC) as rn
            FROM cur_patients_stream
        ) WHERE rn = 1
    ) src ON tgt.patient_id = src.patient_id
    WHEN MATCHED THEN UPDATE SET tgt.phone = src.phone, tgt.address = src.address, tgt.insurance_id = src.insurance_id
    WHEN NOT MATCHED THEN INSERT (patient_id, name, dob, gender, email, phone, address, city, state, insurance_id, registration_date, _load_timestamp)
    VALUES (src.patient_id, src.name, src.dob, src.gender, src.email, src.phone, src.address, UPPER(src.city), UPPER(src.state), src.insurance_id, src.registration_date, src._load_timestamp);

    -- -------------------------------------------------------------
    -- B. DOCTORS STREAM PROCESSING
    -- -------------------------------------------------------------
    CREATE TEMPORARY TABLE cur_doctors_stream AS SELECT * FROM HOSPITAL.EXTERNAL_STAGES.DOCTORS_STREAM;

    MERGE INTO validated.valid_doctors tgt
    USING (
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY doctor_id ORDER BY _load_timestamp DESC) as rn
            FROM cur_doctors_stream
        ) WHERE rn = 1
    ) src ON tgt.doctor_id = src.doctor_id
    WHEN MATCHED THEN UPDATE SET tgt.status = src.status, tgt.qualification = src.qualification
    WHEN NOT MATCHED THEN INSERT (doctor_id, name, specialization, join_date, phone, email, qualification, department, status, _load_timestamp)
    VALUES (src.doctor_id, src.name, src.specialization, src.join_date, src.phone, src.email, src.qualification, UPPER(src.department), src.status, src._load_timestamp);

    -- -------------------------------------------------------------
    -- C. ADMISSIONS STREAM PROCESSING (DQ Applied)
    -- -------------------------------------------------------------
    CREATE TEMPORARY TABLE cur_admissions_stream AS SELECT * FROM HOSPITAL.EXTERNAL_STAGES.ADMISSIONS_STREAM;

    -- 1. Log Exceptions (Temporal & range checks)
    INSERT INTO governance.dq_exception_log (source_table, business_key, error_type, error_message)
    SELECT 'raw_admissions', admission_id, 'TEMPORAL/VALUE', 'Invalid discharge time or bad bed_no'
    FROM cur_admissions_stream
    WHERE discharge_time < admission_time OR TRY_CAST(bed_no AS NUMBER) <= 0;

    -- 2. Upsert Good Data
    MERGE INTO validated.valid_admissions tgt
    USING (
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY admission_id ORDER BY _load_timestamp DESC) as rn
            FROM cur_admissions_stream
            -- Ensure temporal logic & positive numeric bed
            WHERE (discharge_time >= admission_time OR discharge_time IS NULL)
              AND TRY_CAST(bed_no AS NUMBER) > 0
              -- Ensure Referential Integrity to valid_patients and valid_doctors
              AND patient_id IN (SELECT patient_id FROM validated.valid_patients)
              AND attending_doctor_id IN (SELECT doctor_id FROM validated.valid_doctors)
        ) WHERE rn = 1
    ) src ON tgt.admission_id = src.admission_id
    WHEN MATCHED THEN UPDATE SET tgt.discharge_time = src.discharge_time, tgt.status = src.status
    WHEN NOT MATCHED THEN INSERT (admission_id, patient_id, admission_time, discharge_time, department, ward, bed_no, attending_doctor_id, admission_type, status, _load_timestamp)
    VALUES (src.admission_id, src.patient_id, src.admission_time, src.discharge_time, UPPER(src.department), UPPER(src.ward), TRY_CAST(src.bed_no AS NUMBER), src.attending_doctor_id, src.admission_type, src.status, src._load_timestamp);

    -- -------------------------------------------------------------
    -- D. PROCEDURES STREAM PROCESSING (DQ Applied)
    -- -------------------------------------------------------------
    CREATE TEMPORARY TABLE cur_procedures_stream AS SELECT * FROM HOSPITAL.EXTERNAL_STAGES.PROCEDURES_STREAM;

    -- 1. Log Exceptions
    INSERT INTO governance.dq_exception_log (source_table, business_key, error_type, error_message)
    SELECT 'raw_procedures', procedure_id, 'TEMPORAL', 'Invalid procedure times'
    FROM cur_procedures_stream
    WHERE start_time < scheduled_time OR end_time < start_time;

    -- 2. Upsert Good Data
    MERGE INTO validated.valid_procedures tgt
    USING (
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY procedure_id ORDER BY _load_timestamp DESC) as rn 
            FROM cur_procedures_stream
            WHERE start_time >= scheduled_time 
              AND end_time >= start_time
              -- Referential Integrity (must have valid admission)
              AND admission_id IN (SELECT admission_id FROM validated.valid_admissions)
        ) WHERE rn = 1
    ) src ON tgt.procedure_id = src.procedure_id
    WHEN MATCHED THEN UPDATE SET tgt.end_time = src.end_time, tgt.status = src.status
    WHEN NOT MATCHED THEN INSERT (procedure_id, admission_id, patient_id, procedure_name, surgeon_id, scheduled_time, start_time, end_time, operating_room, anaesthesia_type, status, _load_timestamp)
    VALUES (src.procedure_id, src.admission_id, src.patient_id, src.procedure_name, src.surgeon_id, src.scheduled_time, src.start_time, src.end_time, src.operating_room, src.anaesthesia_type, src.status, src._load_timestamp);

    -- -------------------------------------------------------------
    -- E. BILLING STREAM PROCESSING (DQ Applied)
    -- -------------------------------------------------------------
    CREATE TEMPORARY TABLE cur_billing_stream AS SELECT * FROM HOSPITAL.EXTERNAL_STAGES.BILLING_STREAM;

    -- 1. Log Exceptions
    INSERT INTO governance.dq_exception_log (source_table, business_key, error_type, error_message)
    SELECT 'raw_billing', bill_id, 'VALUE/TEMPORAL', 'Amount <= 0 or Billing < Admission'
    FROM cur_billing_stream b
    LEFT JOIN validated.valid_admissions a ON b.admission_id = a.admission_id
    WHERE b.total_amount <= 0 OR b.billing_time < a.admission_time;

    -- 2. Upsert Good Data
    MERGE INTO validated.valid_billing tgt
    USING (
        SELECT * FROM (
            SELECT b.*, ROW_NUMBER() OVER(PARTITION BY b.bill_id ORDER BY b._load_timestamp DESC) as rn
            FROM cur_billing_stream b
            LEFT JOIN validated.valid_admissions a ON b.admission_id = a.admission_id
            WHERE b.total_amount > 0 
              AND (b.billing_time >= a.admission_time OR a.admission_time IS NULL)
              -- Referential Integrity
              AND b.admission_id IN (SELECT admission_id FROM validated.valid_admissions)
        ) WHERE rn = 1
    ) src ON tgt.bill_id = src.bill_id
    WHEN MATCHED THEN UPDATE SET tgt.is_flagged = TRY_CAST(src.is_flagged AS BOOLEAN)
    WHEN NOT MATCHED THEN INSERT (bill_id, admission_id, patient_id, total_amount, insurance_amount, out_of_pocket_amount, billing_time, payment_mode, is_flagged, _load_timestamp)
    VALUES (src.bill_id, src.admission_id, src.patient_id, src.total_amount, src.insurance_amount, src.out_of_pocket_amount, src.billing_time, src.payment_mode, TRY_CAST(src.is_flagged AS BOOLEAN), src._load_timestamp);

    RETURN 'Validation and Insert into VALIDATED layer completed successfully.';
END;
$$;