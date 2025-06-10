{{ config(materialized='view') }}

SELECT 
    occupation_field_label,
    COUNT(*) as total_jobs,
    COUNT(DISTINCT employer_name) as unique_employers,
    COUNT(DISTINCT workplace_city) as cities,
    AVG(scope_of_work_min) as avg_min_scope,
    AVG(scope_of_work_max) as avg_max_scope,
    SUM(CASE WHEN experience_required THEN 1 ELSE 0 END) as jobs_requiring_experience,
    MIN(publication_date) as earliest_job,
    MAX(publication_date) as latest_job
FROM {{ ref('fct_job_advertisements') }}
GROUP BY occupation_field_label
ORDER BY total_jobs DESC
