{{ config(materialized='ephemeral') }}

SELECT DISTINCT
    id,
    json_extract_string(employer, '$.name') as employer_name,
    json_extract_string(employer, '$.workplace') as employer_workplace, 
    json_extract_string(employer, '$.organization_number') as employer_organization_number,
    json_extract_string(workplace_address, '$.street_address') as workplace_street_address,
    json_extract_string(workplace_address, '$.region') as workplace_region,
    json_extract_string(workplace_address, '$.postcode') as workplace_postcode,
    json_extract_string(workplace_address, '$.city') as workplace_city,
    json_extract_string(workplace_address, '$.country') as workplace_country,
    json_extract_string(workplace_address, '$.municipality') as workplace_municipality
FROM {{ source('jobtech_data', 'job_ads') }}
WHERE json_extract_string(employer, '$.name') IS NOT NULL