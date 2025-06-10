{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Kropps- och skönhetsvård'
   OR occupation_label ILIKE '%frisör%'
   OR occupation_label ILIKE '%massör%'
   OR occupation_label ILIKE '%hudterapeut%'
   OR occupation_label ILIKE '%nageltekniker%'
   OR headline ILIKE '%frisör%'
   OR headline ILIKE '%skönhet%'
ORDER BY publication_date DESC
