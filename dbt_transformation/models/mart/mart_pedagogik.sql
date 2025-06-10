{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Pedagogik'
   OR occupation_label ILIKE '%lärare%'
   OR occupation_label ILIKE '%pedagog%'
   OR occupation_label ILIKE '%förskola%'
   OR occupation_label ILIKE '%utbildning%'
   OR headline ILIKE '%lärare%'
   OR headline ILIKE '%pedagog%'
ORDER BY publication_date DESC
