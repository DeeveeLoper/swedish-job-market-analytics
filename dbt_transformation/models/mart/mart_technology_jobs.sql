{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label ILIKE '%teknik%'
   OR occupation_field_label ILIKE '%ingenjör%'
   OR occupation_label ILIKE '%ingenjör%'
   OR occupation_label ILIKE '%tekniker%'
   OR occupation_label ILIKE '%civilingenjör%'
   OR headline ILIKE '%ingenjör%'
   OR headline ILIKE '%tekniker%'
ORDER BY publication_date DESC
