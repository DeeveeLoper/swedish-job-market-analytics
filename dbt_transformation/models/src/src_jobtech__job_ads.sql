{{ config(materialized='ephemeral') }}

SELECT
    id,
    headline,
    label,
    webpage_url,
    logo_url,
    
    -- Description info
    description__text as description_text,
    description__text_formatted as description_text_formatted,
    description__conditions as description_conditions,
    
    -- Employer info
    employer__name as employer_name,
    employer__workplace as employer_workplace,
    employer__organization_number as employer_organization_number,
    employer__url as employer_url,
    employer__email as employer_email,
    employer__phone_number as employer_phone_number,
    
    -- Workplace info  
    workplace_address__street_address as workplace_street_address,
    workplace_address__region as workplace_region,
    workplace_address__city as workplace_city,
    workplace_address__postcode as workplace_postcode,
    workplace_address__country as workplace_country,
    workplace_address__municipality as workplace_municipality,
    workplace_address__municipality_code as workplace_municipality_code,
    workplace_address__region_code as workplace_region_code,
    workplace_address__country_code as workplace_country_code,
    
    -- Occupation info
    occupation_field__concept_id as occupation_field_concept_id,
    occupation_field__label as occupation_field_label,
    occupation__concept_id as occupation_concept_id,
    occupation__label as occupation_label,
    occupation_group__concept_id as occupation_group_concept_id,
    occupation_group__label as occupation_group_label,
    
    -- Employment details
    employment_type__concept_id as employment_type_concept_id,
    employment_type__label as employment_type,
    duration__concept_id as duration_concept_id,
    duration__label as duration,
    salary_type__concept_id as salary_type_concept_id,
    salary_type__label as salary_type,
    salary_description,
    working_hours_type__concept_id as working_hours_type_concept_id,
    working_hours_type__label as working_hours_type,
    
    -- Scope of work
    scope_of_work__min as scope_of_work_min,
    scope_of_work__max as scope_of_work_max,
    
    -- Dates
    publication_date,
    last_publication_date,
    application_deadline,
    
    -- Other details
    number_of_vacancies as vacancies,
    relevance,
    experience_required,
    driving_license_required,
    access_to_own_car,
    removed,
    access,
    external_id,
    
    -- Application details
    application_details__information as application_information,
    application_details__reference as application_reference,
    application_details__email as application_email,
    application_details__via_af as application_via_af,
    application_details__url as application_url,
    application_details__other as application_other,
    
    -- Metadata
    _extracted_at as extracted_at,
    _occupation_field_code as occupation_field_code,
    _occupation_field_name as occupation_field_name,
    
    -- DLT metadata
    source_type,
    timestamp,
    _dlt_load_id,
    _dlt_id

FROM {{ source('jobtech_data', 'job_ads') }}