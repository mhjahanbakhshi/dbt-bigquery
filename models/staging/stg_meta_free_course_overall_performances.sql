WITH cleaned AS (
    SELECT
        -- Core identifiers
        TRIM(CampaignName) AS campaign_name,
        TRIM(AdSetName) AS ad_set_name,
        TRIM(AdSetDelivery) AS ad_set_delivery,
        TRIM(AttributionSetting) AS attribution_setting,

        -- Performance metrics
        CAST(Results AS INT64) AS results,
        CAST(Reach AS INT64) AS reach,
        CAST(Impressions AS INT64) AS impressions,
        SAFE_CAST(REPLACE(CAST(CostPerResults AS STRING), ',', '') AS FLOAT64) AS cost_per_results,
        SAFE_CAST(REPLACE(CAST(AmountSpentEur AS STRING), ',', '') AS FLOAT64) AS amount_spent_eur,

        -- Unique metrics
        SAFE_CAST(REPLACE(CAST(UniqueCtrAll AS STRING), '%', '') AS FLOAT64) / 100 AS unique_ctr_all,
        CAST(UniqueClicksAll AS INT64) AS unique_clicks_all
    FROM {{ source('shop', 'MetaFreeCourse_Overall_Performances') }}
)

SELECT
    -- Pass-through of cleaned data
    campaign_name,
    ad_set_name,
    ad_set_delivery,
    attribution_setting,
    results,
    reach,
    impressions,
    cost_per_results,
    amount_spent_eur,
    unique_ctr_all,
    unique_clicks_all
FROM cleaned