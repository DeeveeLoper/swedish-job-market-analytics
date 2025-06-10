{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Naturbruk'
   OR occupation_label ILIKE '%lantbruk%'
   OR occupation_label ILIKE '%jordbruk%'
   OR occupation_label ILIKE '%skogsbruk%'
   OR headline ILIKE '%naturbruk%'
ORDER BY publication_date DESC
