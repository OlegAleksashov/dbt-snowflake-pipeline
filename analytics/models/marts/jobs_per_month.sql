SELECT 
  date_trunc('month', search_date) AS month,
  search_term AS role,
  count(*) AS postings
FROM {{ ref('stg_job_postings') }}
GROUP BY month, role
ORDER BY month, role 