{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Sanering och renhållning'
   OR occupation_label ILIKE '%städning%'
   OR occupation_label ILIKE '%renhållning%'
   OR occupation_label ILIKE '%sanering%'
   OR headline ILIKE '%städ%'
ORDER BY publication_date DESC
