{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label ILIKE '%bygg%'
   OR occupation_field_label ILIKE '%anläggning%'
   OR occupation_label ILIKE '%byggnads%'
   OR occupation_label ILIKE '%snickare%'
   OR occupation_label ILIKE '%elektriker%'
   OR headline ILIKE '%bygg%'
   OR headline ILIKE '%konstruktör%'
ORDER BY publication_date DESC
