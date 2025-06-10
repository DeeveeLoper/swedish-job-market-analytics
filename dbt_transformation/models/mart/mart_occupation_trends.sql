{{ config(materialized='view') }}

SELECT 
    occupation_field_label,
    DATE_TRUNC('week', publication_date) as week_start,
    COUNT(*) as jobs_posted,
    COUNT(DISTINCT employer_name) as unique_employers,
    AVG(vacancies) as avg_vacancies,
    SUM(CASE WHEN experience_required THEN 1 ELSE 0 END) as jobs_requiring_experience
FROM {{ ref('fct_job_advertisements') }}
GROUP BY occupation_field_label, DATE_TRUNC('week', publication_date)
ORDER BY week_start DESC, jobs_posted DESC
