{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Installation, drift, underhåll'
   OR occupation_label ILIKE '%installationer%'
   OR occupation_label ILIKE '%drift%'
   OR occupation_label ILIKE '%underhåll%'
   OR occupation_label ILIKE '%service%'
   OR headline ILIKE '%installation%'
   OR headline ILIKE '%underhåll%'
ORDER BY publication_date DESC
