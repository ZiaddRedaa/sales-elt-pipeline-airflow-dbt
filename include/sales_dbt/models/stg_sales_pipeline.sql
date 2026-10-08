{{ config(materialized='view') }}

WITH raw_pipeline AS (
    SELECT * FROM {{ source('raw', 'sales_pipeline') }}
)

SELECT 
    opportunity_id,
    sales_agent,
    
    CASE 
        WHEN product = 'GTXPro' THEN 'GTX Pro'
        WHEN product = 'GTX Plus Basic' THEN 'GTX Basic'
        WHEN product = 'GTX Plus Pro' THEN 'GTX Pro'
        ELSE product 
    END AS product_name,

    COALESCE(account, 'Unknown Account') AS account_name,
    
    deal_stage,
    
    CAST(engage_date AS DATE) AS engage_date,
    CAST(close_date AS DATE) AS close_date,
    
    COALESCE(close_value, 0.0) AS close_value,

    CASE 
        WHEN close_date IS NOT NULL AND engage_date IS NOT NULL 
        THEN date_diff('day', CAST(engage_date AS DATE), CAST(close_date AS DATE))
        ELSE NULL 
    END AS sales_cycle_days

FROM raw_pipeline