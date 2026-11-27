WITH source AS (
    SELECT *
    FROM {{ source('raw', 'raw_job_skills') }}
),
cleaned AS (
    SELECT
      JOB_ID,
      SKILL_ID,
      TRIM(SKILL_KEYWORD) AS SKILL_KEYWORD
    FROM source  
)

SELECT * FROM cleaned