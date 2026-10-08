{{ config(materialized='table') }}

WITH pipeline AS (
    SELECT * FROM {{ ref('stg_sales_pipeline') }}
),
teams AS (
    SELECT * FROM {{ source('raw', 'sales_teams') }}
),
products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
accounts AS (
    SELECT * FROM {{ ref('stg_accounts') }}
)

SELECT 
    p.opportunity_id,
    p.deal_stage,
    p.engage_date,
    p.close_date,
    p.sales_cycle_days,
    p.close_value,
    
    t.sales_agent,
    t.manager,
    t.regional_office,
    
    pr.product_name,
    pr.series,
    pr.sales_price,
    
    a.account_name,
    a.sector,
    a.subsidiary_of,

    (p.close_value - pr.sales_price) AS price_difference

FROM pipeline p
LEFT JOIN teams t ON p.sales_agent = t.sales_agent
LEFT JOIN products pr ON p.product_name = pr.product_name
LEFT JOIN accounts a ON p.account_name = a.account_name
