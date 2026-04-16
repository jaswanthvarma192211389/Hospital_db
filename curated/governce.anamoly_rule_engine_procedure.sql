CREATE OR REPLACE TABLE governance.anomaly_rule_hits (
    anomaly_id VARCHAR DEFAULT UUID_STRING(),
    rule_name VARCHAR,
    business_key VARCHAR,
    anomaly_desc VARCHAR,
    detected_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE governance.run_anomaly_engine()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- 1. Bed Conflict Rule
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    SELECT 'BED_CONFLICT', a1.admission_id, CONCAT('Overlaps with ', a2.admission_id, ' in ward ', a1.ward, ' bed ', a1.bed_no)
    FROM validated.valid_admissions a1
    JOIN validated.valid_admissions a2 
      ON a1.bed_no = a2.bed_no AND a1.ward = a2.ward AND a1.admission_id != a2.admission_id
    WHERE a1.admission_time <= COALESCE(a2.discharge_time, CURRENT_TIMESTAMP())
      AND COALESCE(a1.discharge_time, CURRENT_TIMESTAMP()) >= a2.admission_time;

    -- 2. Procedure Schedule Clash
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    SELECT 'SURGEON_CLASH', p1.procedure_id, CONCAT('Surgeon double booked: conflicts with ', p2.procedure_id)
    FROM validated.valid_procedures p1
    JOIN validated.valid_procedures p2
      ON p1.surgeon_id = p2.surgeon_id AND p1.procedure_id != p2.procedure_id
    WHERE p1.start_time <= p2.end_time AND p1.end_time >= p2.start_time;

    -- 3. Operation Theatre Overrun
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    SELECT 'OT_OVERRUN', procedure_id, CONCAT('Duration exceeded 4 hrs: ', DATEDIFF(minute, start_time, end_time), ' mins')
    FROM validated.valid_procedures
    WHERE DATEDIFF(minute, start_time, end_time) > 240;

    -- 4. Multiple active admissions
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    SELECT 'MULTIPLE_ACTIVE_ADMISSIONS', patient_id, 'Patient has multiple active distinct admissions simultaneously.'
    FROM validated.valid_admissions
    WHERE discharge_time IS NULL
    GROUP BY patient_id HAVING COUNT(*) > 1;

    -- 5. Billing amount unusually high vs department avg (> 3 std dev)
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    WITH DeptAvg AS (
        SELECT a.department, AVG(b.total_amount) as avg_amt, STDDEV(b.total_amount) as std_dev
        FROM validated.valid_billing b JOIN validated.valid_admissions a ON b.admission_id = a.admission_id GROUP BY a.department
    )
    SELECT 'BILL_OUTLIER', b.bill_id, CONCAT('Amount ', b.total_amount, ' exceeds dept avg ', d.avg_amt)
    FROM validated.valid_billing b
    JOIN validated.valid_admissions a ON b.admission_id = a.admission_id
    JOIN DeptAvg d ON a.department = d.department
    WHERE b.total_amount > (d.avg_amt + (3 * COALESCE(d.std_dev, 1)));

    -- 6. Billing same time for same patient
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    SELECT 'DUPLICATE_BILL_TIME', patient_id, CONCAT('Billed multiple times at ', billing_time)
    FROM validated.valid_billing
    GROUP BY patient_id, billing_time HAVING COUNT(bill_id) > 1;

    RETURN 'Anomaly Engine run executed.';
END;
$$;