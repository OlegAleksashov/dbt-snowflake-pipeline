{% set extension_keywords = [
	"No degree mentioned",
	"Health insurance",
	"Dental insurance",
	"Paid time off"
] %}

WITH source AS (
	SELECT *
	FROM {{ source('raw', 'raw_job_postings') }}
	WHERE ERROR IS DISTINCT FROM TRUE
	  AND JOB_ID IS NOT NULL
),

parsed AS (
	SELECT
	  *,
	  TRY_PARSE_JSON(JOB_EXTENSIONS_RAW)::ARRAY AS JOB_EXTENSIONS
	FROM source
)

SELECT
  JOB_ID,
  JOB_TITLE,
  TRIM(COMPANY_NAME) AS COMPANY_NAME,
  JOB_LOCATION,
  TRIM(REGEXP_REPLACE(JOB_VIA, '^via\\s*', '', 1, 1, 'i')) AS JOB_VIA,
  JOB_POSTED_AT,
  JOB_SALARY,
  JOB_SCHEDULE_TYPE,
  JOB_WORK_FROM_HOME,
  JOB_EXTENSIONS_RAW,
  {% for keyword in extension_keywords -%}
  ARRAY_CONTAINS('{{ keyword }}'::VARIANT, JOB_EXTENSIONS) AS HAS_{{ slugify(keyword) }},
  {% endfor -%}
  SEARCH_TIME AS SEARCH_AT,
  SEARCH_DATE,
  SEARCH_TERM,
  SEARCH_LOCATION
FROM parsed
