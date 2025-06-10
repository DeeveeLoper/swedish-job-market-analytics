{{ config(materialized='view') }}

SELECT DISTINCT
    id,
    headline,
    description_text,
    description_text_formatted,
    description_conditions,
    webpage_url,
    logo_url,
    label,
    employment_type,
    duration,
    salary_type,
    salary_description,
    working_hours_type,
    scope_of_work_min,
    scope_of_work_max,
    vacancies,
    relevance,
    removed,
    access,
    external_id,
    source_type
FROM {{ ref('src_jobtech__job_ads') }}