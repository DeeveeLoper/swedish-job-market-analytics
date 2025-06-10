{{ config(materialized='view') }}

SELECT 
    occupation_label,
    occupation_field_label,
    COUNT(*) as total_jobs,
    COUNT(DISTINCT employer_name) as unique_employers,
    AVG(scope_of_work_min) as avg_scope_min,
    AVG(scope_of_work_max) as avg_scope_max,
    SUM(CASE WHEN experience_required THEN 1 ELSE 0 END) as jobs_requiring_experience,
    ROUND(SUM(CASE WHEN experience_required THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_requiring_experience
FROM {{ ref('fct_job_advertisements') }}
GROUP BY occupation_label, occupation_field_label
HAVING COUNT(*) >= 3
ORDER BY total_jobs DESC
