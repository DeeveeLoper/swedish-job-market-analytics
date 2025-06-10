{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label ILIKE '%data%'
   OR occupation_field_label ILIKE '%it%'
   OR occupation_label ILIKE '%programmerare%'
   OR occupation_label ILIKE '%utvecklare%'
   OR occupation_label ILIKE '%data%'
   OR headline ILIKE '%data%'
   OR headline ILIKE '%it%'
   OR headline ILIKE '%utvecklare%'
   OR headline ILIKE '%programmerare%'
ORDER BY publication_date DESC
