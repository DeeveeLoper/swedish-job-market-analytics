{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Försäljning, inköp, marknadsföring'
   OR occupation_label ILIKE '%försäljning%'
   OR occupation_label ILIKE '%marknadsföring%'
   OR occupation_label ILIKE '%inköp%'
   OR occupation_label ILIKE '%säljare%'
   OR headline ILIKE '%försäljning%'
   OR headline ILIKE '%marknadsföring%'
   OR headline ILIKE '%säljare%'
ORDER BY publication_date DESC
