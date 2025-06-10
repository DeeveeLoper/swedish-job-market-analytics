{% macro generate_occupation_mart(occupation_field_name, file_safe_name) %}
{{ config(materialized='view') }}

WITH base_jobs AS (
    SELECT
        jd.publication_date,
        f.job_details_id AS job_id,
        jd.headline,
        f.vacancies,
        f.relevance,
        o.occupation,
        o.occupation_group,
        o.occupation_field,
        f.application_deadline,
        jd.description,
        jd.employment_type,
        jd.duration,
        jd.salary_type,
        e.employer_name,
        e.employer_workplace,
        e.workplace_region,
        e.workplace_city,
        a.experience_required,
        a.driver_license,
        a.access_to_own_car
    FROM {{ ref('fct_job_ads') }} f
    LEFT JOIN {{ ref('dim_job_details') }} jd ON f.job_details_id = jd.job_details_id
    LEFT JOIN {{ ref('dim_occupation') }} o ON f.occupation_id = o.occupation_id
    LEFT JOIN {{ ref('dim_employer') }} e ON f.employer_id = e.employer_id
    LEFT JOIN {{ ref('dim_auxiliary_attributes') }} a ON f.auxiliary_attributes_id = a.auxiliary_attributes_id
    WHERE o.occupation_field = '{{ occupation_field_name }}'
)
SELECT * FROM base_jobs
{% endmacro %}