WITH cleaned AS (
    SELECT
        -- Keyword details
        TRIM(Keyword) AS keyword,
        TRIM(MatchType) AS match_type,
        TRIM(AdGroup) AS ad_group,
        TRIM(Status) AS status,
        TRIM(CurrencyCode) AS currency_code,
        
        -- Performance metrics
        SAFE_CAST(CTR AS FLOAT64) AS ctr,
        SAFE_CAST(Cost AS FLOAT64) AS cost,
        SAFE_CAST(CVRClickToLeads AS FLOAT64) AS cvr_click_to_leads,
        SAFE_CAST(CostPerApply AS FLOAT64) AS cost_per_apply,
        SAFE_CAST(CostPerLeadsAll AS FLOAT64) AS cost_per_leads_all,
        SAFE_CAST(ConversionRate AS FLOAT64) AS conversion_rate,
        SAFE_CAST(Conversions AS FLOAT64) AS conversions
    FROM {{ source('shop', 'GoogleSearch_Keywords') }}
)

SELECT
    -- Pass-through of cleaned data
    keyword,
    match_type,
    ad_group,
    status,
    currency_code,
    ctr,
    cost,
    cvr_click_to_leads,
    cost_per_apply,
    cost_per_leads_all,
    conversion_rate,
    conversions
FROM cleaned