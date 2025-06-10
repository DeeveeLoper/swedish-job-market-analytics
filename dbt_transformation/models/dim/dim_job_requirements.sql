{{ config(materialized='view') }}

SELECT DISTINCT
    experience_required,
    driving_license_required,
    access_to_own_car as own_car_required,
    
    -- Create composite requirement profile
    CASE 
        WHEN experience_required AND driving_license_required AND access_to_own_car THEN 'High Requirements'
        WHEN experience_required AND (driving_license_required OR access_to_own_car) THEN 'Medium-High Requirements'
        WHEN experience_required THEN 'Experience Required Only'
        WHEN driving_license_required OR access_to_own_car THEN 'License/Car Required Only'
        ELSE 'Basic Requirements'
    END as requirement_profile,
    
    -- Count of requirements
    CAST(experience_required AS INTEGER) + 
    CAST(driving_license_required AS INTEGER) + 
    CAST(access_to_own_car AS INTEGER) as total_requirements

FROM {{ ref('src_jobtech__job_ads') }}
WHERE experience_required IS NOT NULL 
   OR driving_license_required IS NOT NULL 
   OR access_to_own_car IS NOT NULL