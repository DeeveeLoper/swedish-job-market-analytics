{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Hotell, restaurang, storhushåll'
   OR occupation_label ILIKE '%kock%'
   OR occupation_label ILIKE '%servitör%'
   OR occupation_label ILIKE '%hotell%'
   OR occupation_label ILIKE '%restaurang%'
   OR headline ILIKE '%restaurang%'
   OR headline ILIKE '%hotell%'
ORDER BY publication_date DESC
