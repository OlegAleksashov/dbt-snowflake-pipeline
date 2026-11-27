WITH totals AS (
    SELECT month, SUM(postings) AS total_postings
    FROM {{ ref('stg_jobs_per_month') }}
    GROUP BY month
),
remote AS (
    SELECT DATE_TRUNC('month', search_date) AS month, COUNT(*) AS remote_postings
    FROM {{ ref('stg_job_postings') }}
    WHERE job_work_from_home
    GROUP BY month
)
SELECT month, total_postings, remote_postings,
       ROUND(100.0 * remote_postings / total_postings, 1) AS pct_remote
FROM totals
JOIN remote USING (month)
ORDER BY month
