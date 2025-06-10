{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Naturvetenskapligt arbete'
   OR occupation_label ILIKE '%kemist%'
   OR occupation_label ILIKE '%biolog%'
   OR occupation_label ILIKE '%forskare%'
   OR occupation_label ILIKE '%laborator%'
   OR headline ILIKE '%forskning%'
   OR headline ILIKE '%laborator%'
ORDER BY publication_date DESC
