{{ config(materialized='table') }}

SELECT DISTINCT
    occupation_field_concept_id,
    occupation_field_label,
    occupation_concept_id,
    occupation_label,
    occupation_group_concept_id,
    occupation_group_label,
    occupation_field_code,
    occupation_field_name
FROM {{ ref('src_jobtech__job_ads') }}
WHERE occupation_field_concept_id IS NOT NULL