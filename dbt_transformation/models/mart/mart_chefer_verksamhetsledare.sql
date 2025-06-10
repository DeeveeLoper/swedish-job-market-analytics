{{ config(materialized='view') }}

SELECT 
    job_ad_id,
    headline,
    description_text,
    employer_name,
    workplace_city,
    workplace_region,
    workplace_country,
    occupation_label,
    occupation_field_label,
    employment_type,
    duration,
    salary_type,
    scope_of_work_min,
    scope_of_work_max,
    publication_date,
    application_deadline,
    vacancies,
    experience_required,
    driving_license_required,
    access_to_own_car

FROM {{ ref('fct_job_advertisements') }}

WHERE occupation_field_label = 'Chefer och verksamhetsledare'
   OR occupation_label ILIKE '%chef%'
   OR occupation_label ILIKE '%ledare%'
   OR occupation_label ILIKE '%manager%'
   OR headline ILIKE '%chef%'
   OR headline ILIKE '%ledare%'

ORDER BY publication_date DESC
