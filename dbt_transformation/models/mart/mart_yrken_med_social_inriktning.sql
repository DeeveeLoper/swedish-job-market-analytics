{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_job_advertisements') }}
WHERE occupation_field_label = 'Yrken med social inriktning'
   OR occupation_label ILIKE '%kurator%'
   OR occupation_label ILIKE '%socialsekreterare%'
   OR occupation_label ILIKE '%behandlingsassistent%'
   OR headline ILIKE '%social%'
   OR headline ILIKE '%kurator%'
ORDER BY publication_date DESC
