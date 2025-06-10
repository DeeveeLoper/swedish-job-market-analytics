{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label ILIKE '%hälso%'
   OR occupation_field_label ILIKE '%sjukvård%'
   OR occupation_label ILIKE '%sjuksköterska%'
   OR occupation_label ILIKE '%läkare%'
   OR occupation_label ILIKE '%vårdare%'
   OR headline ILIKE '%sjukvård%'
   OR headline ILIKE '%vård%'
ORDER BY publication_date DESC
