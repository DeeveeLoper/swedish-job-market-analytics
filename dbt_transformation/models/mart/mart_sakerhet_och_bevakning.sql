{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Säkerhet och bevakning'
   OR occupation_label ILIKE '%väktare%'
   OR occupation_label ILIKE '%ordningsvakt%'
   OR occupation_label ILIKE '%brandman%'
   OR occupation_label ILIKE '%säkerhet%'
   OR headline ILIKE '%säkerhet%'
   OR headline ILIKE '%bevakning%'
ORDER BY publication_date DESC
