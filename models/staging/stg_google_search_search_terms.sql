WITH cleaned AS (
    SELECT
        -- Search term details
        TRIM(SearchTerm) AS search_term,
        TRIM(MatchType) AS match_type,
        TRIM(AddedExcluded) AS added_excluded,
        TRIM(AdGroup) AS ad_group,
        TRIM(CurrencyCode) AS currency_code,
        
        -- Performance metrics
        CAST(Clicks AS INT64) AS clicks,
        CAST(Impr AS INT64) AS impressions,
        SAFE_CAST(Ctr AS FLOAT64) AS ctr,
        SAFE_CAST(AvgCpc AS FLOAT64) AS avg_cpc,
        SAFE_CAST(Cost AS FLOAT64) AS cost,
        SAFE_CAST(CostApply AS FLOAT64) AS cost_apply,
        SAFE_CAST(CostLeadsAll AS FLOAT64) AS cost_leads_all,
        SAFE_CAST(ConvRate AS FLOAT64) AS conversion_rate,
        SAFE_CAST(Conversions AS FLOAT64) AS conversions
    FROM {{ source('shop', 'GoogleSearch_Search_Terms') }}
)

SELECT
    -- Pass-through of cleaned data
    search_term,
    match_type,
    added_excluded,
    ad_group,
    currency_code,
    clicks,
    impressions,
    ctr,
    avg_cpc,
    cost,
    cost_apply,
    cost_leads_all,
    conversion_rate,
    conversions
FROM cleaned