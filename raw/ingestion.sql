copy into raw.raw_patients
from @external_stages.hospital_stage
file_format=HOSPITAL.RAW.CSV_FORMAT
PATTERN ='.*patients.*\.csv'
ON_ERROR = CONTINUE;

copy into raw.raw_procedures
from @external_stages.hospital_stage
file_format=HOSPITAL.RAW.CSV_FORMAT
PATTERN ='.*procedures.*\.csv'
ON_ERROR = CONTINUE;

copy into raw.raw_billing
from @external_stages.hospital_stage
file_format=HOSPITAL.RAW.CSV_FORMAT
PATTERN ='.*billing.*\.csv'
ON_ERROR = CONTINUE;

copy into raw.raw_doctors
from @external_stages.hospital_stage
file_format=HOSPITAL.RAW.CSV_FORMAT
PATTERN ='.*doctor.*\.csv'
ON_ERROR = CONTINUE;

copy into raw.raw_admissions
from @external_stages.hospital_stage
file_format=HOSPITAL.RAW.CSV_FORMAT
PATTERN ='.*admissions.*\.csv'
ON_ERROR = CONTINUE;