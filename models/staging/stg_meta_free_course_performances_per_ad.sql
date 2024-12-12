WITH cleaned AS (
    SELECT
        -- Ad details
        TRIM(AdName) AS ad_name,
        TRIM(AdDelivery) AS ad_delivery,
        TRIM(AdSetName) AS ad_set_name,
        TRIM(AttributionSetting) AS attribution_setting,
        TRIM(FinalUrl) AS final_url,

        -- Performance metrics
        CAST(Results AS INT64) AS results,
        CAST(Reach AS INT64) AS reach,
        CAST(Impressions AS INT64) AS impressions,
        SAFE_CAST(CostPerResults AS FLOAT64) AS cost_per_results,
        SAFE_CAST(AmountSpentEur AS FLOAT64) AS amount_spent_eur,
        SAFE_CAST(UniqueCtrAll AS FLOAT64) AS unique_ctr_all,
        CAST(UniqueClicksAll AS INT64) AS unique_clicks_all
    FROM {{ source('shop', 'MetaFreeCourse_Performances_Per_Ad') }}
)

SELECT
    -- Pass-through of cleaned data
    ad_name,
    ad_delivery,
    ad_set_name,
    attribution_setting,
    results,
    reach,
    impressions,
    cost_per_results,
    amount_spent_eur,
    unique_ctr_all,
    unique_clicks_all,
    final_url
FROM cleaned