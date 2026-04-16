CREATE OR REPLACE TASK validated.task_process_dq
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('EXTERNAL_STAGES.patient_stream') OR SYSTEM$STREAM_HAS_DATA('EXTERNAL_STAGES.admissions_stream')
AS CALL validated.process_data_quality();

CREATE OR REPLACE TASK validated.task_populate_curated
  WAREHOUSE = COMPUTE_WH
  AFTER validated.task_process_dq
AS CALL curated.populate_curated_models();

CREATE OR REPLACE TASK validated.task_anomaly_engine
  WAREHOUSE = COMPUTE_WH
  AFTER validated.task_populate_curated
AS CALL governance.run_anomaly_engine();