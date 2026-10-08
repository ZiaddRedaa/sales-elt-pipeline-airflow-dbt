{{ config(materialized='view') }}

WITH raw_products AS (
    SELECT * FROM {{ source('raw', 'products') }}
)
SELECT 
    TRIM(product) AS product_name,
    TRIM(series) AS series,
    sales_price
FROM raw_products