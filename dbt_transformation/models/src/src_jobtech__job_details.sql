{{ config(materialized='ephemeral') }}

SELECT
    id,
    headline,
    publication_date,
    description_text AS description,
    description_html_formatted AS description_html_formatted,
    CAST(employment_type AS VARCHAR) AS employment_type,
    CAST(duration AS VARCHAR) AS duration,
    CAST(salary_type AS VARCHAR) AS salary_type,
    scope_of_work_min,
    scope_of_work_max,
    COALESCE(
        json_extract_string(application_details, '$.url'),
        application_url
    ) AS application_url
FROM {{ source('jobtech_data', 'job_ads') }}
WHERE id IS NOT NULL