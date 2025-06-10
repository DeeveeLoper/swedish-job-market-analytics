{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label ILIKE '%hälso%'
   OR occupation_field_label ILIKE '%sjukvård%'
   OR occupation_field_label ILIKE '%health%'
   OR occupation_label ILIKE '%nurse%'
   OR occupation_label ILIKE '%doctor%'
   OR occupation_label ILIKE '%medical%'
ORDER BY publication_date DESC
