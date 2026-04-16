CREATE SCHEMA IF NOT EXISTS analytical;

-- KPI 1: Bed Occupancy Score
CREATE OR REPLACE VIEW analytical.kpi_1_bed_occupancy AS
SELECT 
    SUM(DATEDIFF(hour, admission_time, COALESCE(discharge_time, CURRENT_TIMESTAMP()))) as occupied_hours,
    (500 * 24 * 30) as total_available_hours, -- Target static capacity proxy
    (SUM(DATEDIFF(hour, admission_time, COALESCE(discharge_time, CURRENT_TIMESTAMP()))) / (500.0 * 24 * 30)) * 100 AS bed_occupancy_percentage
FROM curated.fact_hospital_events
WHERE event_type = 'ADMISSION' 
  AND admission_time >= DATEADD(day, -30, CURRENT_DATE());

-- KPI 2: Admission Turnaround Time (ATT)
CREATE OR REPLACE VIEW analytical.kpi_2_admission_turnaround AS
SELECT 
    department,
    AVG(DATEDIFF(hour, admission_time, discharge_time)) AS avg_turnaround_time_hours
FROM curated.fact_hospital_events
WHERE event_type = 'ADMISSION' AND discharge_time IS NOT NULL
GROUP BY department;

-- KPI 3: Surgery Efficiency Index
CREATE OR REPLACE VIEW analytical.kpi_3_surgery_efficiency AS
SELECT 
    department,
    COUNT(*) as total_surgeries,
    SUM(CASE WHEN ABS(DATEDIFF(minute, scheduled_time, start_time)) <= 10 THEN 1 ELSE 0 END) as on_time_surgeries,
    (SUM(CASE WHEN ABS(DATEDIFF(minute, scheduled_time, start_time)) <= 10 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) * 100 AS efficiency_index_percentage
FROM curated.fact_hospital_events
WHERE event_type = 'PROCEDURE'
GROUP BY department;

-- KPI 4: Doctor Workload Index
CREATE OR REPLACE VIEW analytical.kpi_4_doctor_workload AS
WITH events_handled AS (
    SELECT doctor_sk, COUNT(*) as volume 
    FROM curated.fact_hospital_events 
    WHERE event_type IN ('ADMISSION', 'PROCEDURE') 
      AND COALESCE(admission_time, start_time) >= DATEADD(day, -30, CURRENT_DATE()) 
    GROUP BY doctor_sk
)
SELECT 
    d.name,
    d.department,
    COALESCE(e.volume, 0) / 30.0 AS avg_daily_workload_index
FROM curated.dim_doctor d
LEFT JOIN events_handled e ON d.doctor_sk = e.doctor_sk
WHERE d.current_flag = 'Y';

-- KPI 5: Billing Accuracy Index
CREATE OR REPLACE VIEW analytical.kpi_5_billing_accuracy AS
SELECT 
    (SELECT COUNT(*) FROM curated.fact_hospital_events WHERE event_type = 'BILLING') as total_bills,
    (SELECT COUNT(*) FROM governance.anomaly_rule_hits WHERE rule_name LIKE 'BILL%') as billing_anomalies,
    (1.0 - (
      (SELECT CAST(COUNT(*) AS FLOAT) FROM governance.anomaly_rule_hits WHERE rule_name LIKE 'BILL%') / 
      NULLIF((SELECT COUNT(*) FROM curated.fact_hospital_events WHERE event_type = 'BILLING'), 0)
    )) * 100 AS billing_accuracy_percentage;


-- KPI 1: Bed Occupancy Score (Removed 30-day filter)
-- Now calculates occupancy across the entire dataset's lifespan
CREATE OR REPLACE VIEW analytical.kpi_1_bed_occupancy AS
SELECT 
    SUM(DATEDIFF(hour, admission_time, COALESCE(discharge_time, CURRENT_TIMESTAMP()))) as occupied_hours,
    (500 * 24 * 30) as total_available_hours, -- Target static capacity proxy
    (SUM(DATEDIFF(hour, admission_time, COALESCE(discharge_time, CURRENT_TIMESTAMP()))) / (500.0 * 24 * 30)) * 100 AS bed_occupancy_percentage
FROM curated.fact_hospital_events
WHERE event_type = 'ADMISSION';

-- KPI 4: Doctor Workload Index (Removed 30-day filter)
CREATE OR REPLACE VIEW analytical.kpi_4_doctor_workload AS
WITH events_handled AS (
    SELECT doctor_sk, COUNT(*) as volume 
    FROM curated.fact_hospital_events 
    WHERE event_type IN ('ADMISSION', 'PROCEDURE') 
    -- Notice the DATEADD filter is removed here!
    GROUP BY doctor_sk
)
SELECT 
    d.name,
    d.department,
    -- We assume 30 days is the standard reporting period for the divisor metric
    COALESCE(e.volume, 0) / 30.0 AS avg_daily_workload_index
FROM curated.dim_doctor d
LEFT JOIN events_handled e ON d.doctor_sk = e.doctor_sk
WHERE d.current_flag = 'Y';