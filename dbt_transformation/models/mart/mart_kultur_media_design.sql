{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Kultur, media, design'
   OR occupation_label ILIKE '%designer%'
   OR occupation_label ILIKE '%grafisk%'
   OR occupation_label ILIKE '%journalist%'
   OR occupation_label ILIKE '%fotograf%'
   OR headline ILIKE '%design%'
   OR headline ILIKE '%media%'
ORDER BY publication_date DESC
