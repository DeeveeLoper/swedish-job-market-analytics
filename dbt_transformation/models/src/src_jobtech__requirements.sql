{{ config(materialized='ephemeral') }}

SELECT DISTINCT
    COALESCE(experience_required, false) as experience_required,
    COALESCE(driving_license_required, false) as driving_license_required,
    COALESCE(access_to_own_car, false) as access_to_own_car
FROM {{ source('jobtech_data', 'job_ads') }}