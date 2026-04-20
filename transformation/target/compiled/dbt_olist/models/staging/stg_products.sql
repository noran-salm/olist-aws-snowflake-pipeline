-- models/staging/stg_products.sql

WITH source AS (
    SELECT * FROM OLIST_DW.RAW.dim_products
)

SELECT
    product_id,
    COALESCE(NULLIF(TRIM(category), ''), 'unknown') AS category,
    name_length,
    description_length,
    photos_qty,
    product_weight_kg,
    length_cm,
    height_cm,
    width_cm,
    -- Derived: volumetric weight (kg) using standard courier formula
    ROUND((length_cm * height_cm * width_cm) / 5000, 3) AS volumetric_weight_kg
FROM source
WHERE product_id IS NOT NULL