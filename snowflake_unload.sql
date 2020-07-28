CREATE OR REPLACE file format mycsvunloadformat
    type = 'CSV'
    field_delimiter = ','
    compression = NONE
    null_if = ()
    field_optionally_enclosed_by = NONE;
    
  
CREATE OR REPLACE stage unload_onto_s3 
    file_format = mycsvunloadformat
    url = 's3://snowflake-drop2dynamodb';


COPY INTO @unload_onto_s3 
FROM (
  SELECT
      SHA2(CONCAT(DEVICEID, ARRIVALTIME)) AS "id"
      , *
  FROM parkingsensor
  WHERE 
      ARRIVALTIME::DATE = '2020-05-01'
      AND DURATIONMINUTES > 15
      AND NOT VEHICLEPRESENT
)
max_file_size = 10000000
header = true;
