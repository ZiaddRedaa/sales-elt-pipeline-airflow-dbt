{{ config(materialized='view') }}

WITH raw_accounts AS (
    SELECT * FROM {{ source('raw', 'accounts') }}
)

SELECT 
    account AS account_name,
    CASE 
        WHEN LOWER(sector) = 'technolgy' THEN 'technology'
        ELSE LOWER(sector) 
    END AS sector,
    year_established,
    revenue,
    employees,
    office_location,
    COALESCE(subsidiary_of, 'Independent') AS subsidiary_of
FROM raw_accounts