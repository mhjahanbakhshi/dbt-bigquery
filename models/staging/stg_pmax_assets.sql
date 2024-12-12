WITH cleaned AS (
    SELECT
        -- Asset details
        TRIM(Asset) AS asset,
        TRIM(Level) AS level,
        TRIM(Status) AS status,
        TRIM(`Asset type`) AS asset_type,
        TRIM(Performance) AS performance,

        -- Performance metrics
        SAFE_CAST(Conversions AS FLOAT64) AS conversions,

        -- Additional fields
        TRIM(string_field_6) AS string_field_6,
        TRIM(string_field_7) AS string_field_7
    FROM {{ source('shop', 'PMax_Assets') }}
)

SELECT
    -- Pass-through of cleaned data
    asset,
    level,
    status,
    asset_type,
    performance,
    conversions,
    string_field_6,
    string_field_7
FROM cleaned