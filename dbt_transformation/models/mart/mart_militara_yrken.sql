{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Militära yrken'
   OR occupation_label ILIKE '%militär%'
   OR occupation_label ILIKE '%soldat%'
   OR occupation_label ILIKE '%försvar%'
   OR headline ILIKE '%militär%'
ORDER BY publication_date DESC
