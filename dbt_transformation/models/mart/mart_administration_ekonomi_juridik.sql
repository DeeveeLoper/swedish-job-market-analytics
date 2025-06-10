{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Administration, ekonomi, juridik'
   OR occupation_label ILIKE '%ekonom%'
   OR occupation_label ILIKE '%jurist%'
   OR occupation_label ILIKE '%administratör%'
   OR headline ILIKE '%ekonom%'
   OR headline ILIKE '%juridik%'
ORDER BY publication_date DESC
