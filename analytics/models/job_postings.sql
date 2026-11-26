{{ config(materialized='view') }}
SELECT
	JOB_ID,
	JOB_TITLE,
	COMPANY_NAME,
	JOB_LOCATION,
	JOB_VIA,
	JOB_SCHEDULE_TYPE,
	JOB_WORK_FROM_HOME,
	SEARCH_DATE,
	SEARCH_TERM
FROM  {{ source('raw', 'raw_job_postings') }} 
WHERE ERROR is NULL    