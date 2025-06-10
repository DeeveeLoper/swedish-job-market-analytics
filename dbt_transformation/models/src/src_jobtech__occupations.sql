{{ config(materialized='ephemeral') }}

SELECT DISTINCT
    json_extract_string(occupation_field, '$.concept_id') as occupation_field_concept_id,
    json_extract_string(occupation_field, '$.label') as occupation_field_label,
    json_extract_string(occupation, '$.concept_id') as occupation_concept_id,
    json_extract_string(occupation, '$.label') as occupation_label,
    json_extract_string(occupation_group, '$.concept_id') as occupation_group_concept_id,
    json_extract_string(occupation_group, '$.label') as occupation_group_label
FROM {{ source('jobtech_data', 'job_ads') }}
WHERE json_extract_string(occupation_field, '$.concept_id') IS NOT NULL