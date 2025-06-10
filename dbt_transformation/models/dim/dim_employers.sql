{{ config(materialized='table') }}

SELECT DISTINCT
    employer_name,
    employer_workplace,
    employer_organization_number,
    employer_url,
    employer_email,
    employer_phone_number,
    workplace_street_address,
    workplace_city,
    workplace_region,
    workplace_postcode,
    workplace_country,
    workplace_municipality,
    workplace_municipality_code,
    workplace_region_code,
    workplace_country_code
FROM {{ ref('src_jobtech__job_ads') }}
WHERE employer_name IS NOT NULL