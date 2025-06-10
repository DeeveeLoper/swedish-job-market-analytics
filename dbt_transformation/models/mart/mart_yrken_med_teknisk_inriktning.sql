{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Yrken med teknisk inriktning'
   OR occupation_label ILIKE '%tekniker%'
   OR occupation_label ILIKE '%teknisk%'
   OR occupation_label ILIKE '%reparatör%'
   OR headline ILIKE '%teknisk%'
   OR headline ILIKE '%tekniker%'
ORDER BY publication_date DESC
