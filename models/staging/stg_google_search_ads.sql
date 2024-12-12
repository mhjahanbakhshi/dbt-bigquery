WITH cleaned AS (
    SELECT
        -- Ad details
        TRIM(FinalURL) AS final_url,
        TRIM(AdGroup) AS ad_group,
        TRIM(Status) AS status,
        TRIM(AdStrength) AS ad_strength,
        
        -- Headline and description (trim and normalize to snake_case)
        TRIM(Headline1) AS headline_1,
        TRIM(Headline2) AS headline_2,
        TRIM(Headline3) AS headline_3,
        TRIM(Headline4) AS headline_4,
        TRIM(Headline5) AS headline_5,
        TRIM(Headline6) AS headline_6,
        TRIM(Headline7) AS headline_7,
        TRIM(Headline8) AS headline_8,
        TRIM(Headline9) AS headline_9,
        TRIM(Headline10) AS headline_10,
        TRIM(Headline11) AS headline_11,
        TRIM(Headline12) AS headline_12,
        TRIM(Headline13) AS headline_13,
        TRIM(Headline14) AS headline_14,
        TRIM(Headline15) AS headline_15,
        TRIM(Description1) AS description_1,
        TRIM(Description2) AS description_2,
        TRIM(Description3) AS description_3,
        TRIM(Description4) AS description_4,
        
        -- Metrics
        CAST(Clicks AS INT64) AS clicks,
        CAST(Impressions AS INT64) AS impressions,
        SAFE_CAST(REPLACE(CAST(CTR AS STRING), '%', '') AS FLOAT64) / 100 AS ctr,
        SAFE_CAST(REPLACE(CAST(AvgCPC AS STRING), ',', '') AS FLOAT64) AS avg_cpc,
        SAFE_CAST(REPLACE(CAST(Cost AS STRING), ',', '') AS FLOAT64) AS cost,
        SAFE_CAST(REPLACE(CAST(CVRClickToLeads AS STRING), '%', '') AS FLOAT64) / 100 AS cvr_click_to_leads,
        SAFE_CAST(REPLACE(CAST(CostPerApply AS STRING), ',', '') AS FLOAT64) AS cost_per_apply,
        SAFE_CAST(REPLACE(CAST(CostPerLeadsAll AS STRING), ',', '') AS FLOAT64) AS cost_per_leads_all,
        SAFE_CAST(REPLACE(CAST(ConversionRate AS STRING), '%', '') AS FLOAT64) / 100 AS conversion_rate,
        CAST(Conversions AS FLOAT64) AS conversions
    FROM {{ source('shop', 'GoogleSearch_Ads') }}
)

SELECT
    -- Pass-through of cleaned data
    final_url,
    ad_group,
    status,
    ad_strength,
    
    -- Headline and description
    headline_1, headline_2, headline_3, headline_4, headline_5,
    headline_6, headline_7, headline_8, headline_9, headline_10,
    headline_11, headline_12, headline_13, headline_14, headline_15,
    description_1, description_2, description_3, description_4,
    
    -- Metrics
    clicks,
    impressions,
    ctr,
    avg_cpc,
    cost,
    cvr_click_to_leads,
    cost_per_apply,
    cost_per_leads_all,
    conversion_rate,
    conversions
FROM cleaned