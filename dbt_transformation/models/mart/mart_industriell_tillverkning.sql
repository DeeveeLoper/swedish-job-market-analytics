{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Industriell tillverkning'
   OR occupation_label ILIKE '%operatör%'
   OR occupation_label ILIKE '%tillverkning%'
   OR occupation_label ILIKE '%industri%'
   OR headline ILIKE '%tillverkning%'
ORDER BY publication_date DESC
