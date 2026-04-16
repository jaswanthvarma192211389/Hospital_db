CREATE OR REPLACE PROCEDURE curated.populate_curated_models()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- -------------------------------------------------------------
    -- SCD-2 for PATIENT DIMENSION
    -- -------------------------------------------------------------
    UPDATE curated.dim_patient tgt
    SET tgt.end_date = CURRENT_DATE(), tgt.current_flag = 'N'
    FROM validated.valid_patients src
    WHERE tgt.patient_id = src.patient_id 
      AND tgt.current_flag = 'Y' 
      AND (tgt.name != src.name OR tgt.state != src.state OR tgt.insurance_id != src.insurance_id);

    MERGE INTO curated.dim_patient tgt
    USING validated.valid_patients src
    ON tgt.patient_id = src.patient_id AND tgt.current_flag = 'Y'
    WHEN NOT MATCHED THEN 
      INSERT (patient_sk, patient_id, name, state, insurance_id, start_date, end_date, current_flag)
      VALUES (UUID_STRING(), src.patient_id, src.name, src.state, src.insurance_id, CURRENT_DATE(), NULL, 'Y');

    -- -------------------------------------------------------------
    -- SCD-2 for DOCTOR DIMENSION
    -- -------------------------------------------------------------
    UPDATE curated.dim_doctor tgt
    SET tgt.end_date = CURRENT_DATE(), tgt.current_flag = 'N'
    FROM validated.valid_doctors src
    WHERE tgt.doctor_id = src.doctor_id 
      AND tgt.current_flag = 'Y' 
      AND (tgt.department != src.department);

    MERGE INTO curated.dim_doctor tgt
    USING validated.valid_doctors src
    ON tgt.doctor_id = src.doctor_id AND tgt.current_flag = 'Y'
    WHEN NOT MATCHED THEN 
      INSERT (doctor_sk, doctor_id, name, department, start_date, end_date, current_flag)
      VALUES (UUID_STRING(), src.doctor_id, src.name, src.department, CURRENT_DATE(), NULL, 'Y');

    -- -------------------------------------------------------------
    -- POPULATE FACT TABLE (fact_hospital_events)
    -- -------------------------------------------------------------
    
    -- A. ADMISSION Events
    MERGE INTO curated.fact_hospital_events tgt
    USING (
         SELECT 
            'ADMISSION' as e_type,
            a.admission_id,
            p.patient_sk,
            d.doctor_sk,
            a.admission_time,
            a.discharge_time,
            a.department,
            a.bed_no
         FROM validated.valid_admissions a
         JOIN curated.dim_patient p ON a.patient_id = p.patient_id AND p.current_flag = 'Y'
         JOIN curated.dim_doctor d ON a.attending_doctor_id = d.doctor_id AND d.current_flag = 'Y'
    ) src ON tgt.admission_id = src.admission_id AND tgt.event_type = src.e_type
    WHEN MATCHED THEN UPDATE SET 
        tgt.discharge_time = src.discharge_time, 
        tgt.department = src.department,
        tgt.bed_no = src.bed_no
    WHEN NOT MATCHED THEN INSERT 
        (event_sk, event_type, admission_id, patient_sk, doctor_sk, admission_time, discharge_time, department, bed_no)
    VALUES 
        (UUID_STRING(), src.e_type, src.admission_id, src.patient_sk, src.doctor_sk, src.admission_time, src.discharge_time, src.department, src.bed_no);

    -- B. PROCEDURE Events
    MERGE INTO curated.fact_hospital_events tgt
    USING (
         SELECT 
            'PROCEDURE' as e_type,
            pr.procedure_id,
            pr.admission_id,
            p.patient_sk,
            vd.doctor_sk as surgeon_sk,
            pr.scheduled_time,
            pr.start_time,
            pr.end_time,
            va.department
         FROM validated.valid_procedures pr
         JOIN validated.valid_admissions va ON pr.admission_id = va.admission_id
         JOIN curated.dim_patient p ON va.patient_id = p.patient_id AND p.current_flag = 'Y'
         JOIN curated.dim_doctor vd ON pr.surgeon_id = vd.doctor_id AND vd.current_flag = 'Y'
    ) src ON tgt.procedure_id = src.procedure_id AND tgt.event_type = src.e_type
    WHEN MATCHED THEN UPDATE SET tgt.end_time = src.end_time
    WHEN NOT MATCHED THEN INSERT 
        (event_sk, event_type, admission_id, procedure_id, patient_sk, doctor_sk, scheduled_time, start_time, end_time, department)
    VALUES 
        (UUID_STRING(), src.e_type, src.admission_id, src.procedure_id, src.patient_sk, src.surgeon_sk, src.scheduled_time, src.start_time, src.end_time, src.department);

    -- C. BILLING Events
    MERGE INTO curated.fact_hospital_events tgt
    USING (
         SELECT 
            'BILLING' as e_type,
            b.bill_id,
            b.admission_id,
            p.patient_sk,
            b.total_amount,
            b.billing_time,
            va.department
         FROM validated.valid_billing b
         JOIN validated.valid_admissions va ON b.admission_id = va.admission_id
         JOIN curated.dim_patient p ON va.patient_id = p.patient_id AND p.current_flag = 'Y'
    ) src ON tgt.bill_id = src.bill_id AND tgt.event_type = src.e_type
    WHEN MATCHED THEN UPDATE SET tgt.total_amount = src.total_amount
    WHEN NOT MATCHED THEN INSERT 
        (event_sk, event_type, admission_id, bill_id, patient_sk, total_amount, billing_time, department)
    VALUES 
        (UUID_STRING(), src.e_type, src.admission_id, src.bill_id, src.patient_sk, src.total_amount, src.billing_time, src.department);

    RETURN 'Curated models populated successfully.';
END;
$$;