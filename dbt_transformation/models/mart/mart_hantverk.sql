{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Hantverk'
   OR occupation_label ILIKE '%hantverkare%'
   OR occupation_label ILIKE '%snickare%'
   OR occupation_label ILIKE '%målare%'
   OR headline ILIKE '%hantverk%'
ORDER BY publication_date DESC
