{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Transport, distribution, lager'
   OR occupation_label ILIKE '%lastbilschaufför%'
   OR occupation_label ILIKE '%lagerarbetare%'
   OR occupation_label ILIKE '%förare%'
   OR occupation_label ILIKE '%logistik%'
   OR headline ILIKE '%transport%'
   OR headline ILIKE '%lager%'
ORDER BY publication_date DESC
