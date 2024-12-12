WITH cleaned AS (
    SELECT
        -- Ad details
        TRIM(AdName) AS ad_name,
        TRIM(AdDelivery) AS ad_delivery,
        TRIM(AdSetName) AS ad_set_name,

        -- Performance metrics
        CAST(Results AS INT64) AS results,
        CAST(Reach AS INT64) AS reach,
        CAST(Impressions AS INT64) AS impressions,
        SAFE_CAST(CostPerResults AS FLOAT64) AS cost_per_results,
        SAFE_CAST(AmountSpentEur AS FLOAT64) AS amount_spent_eur,
        SAFE_CAST(UniqueCtrAll100 AS FLOAT64) AS unique_ctr_all_100,
        SAFE_CAST(UniqueCtrAll AS FLOAT64) AS unique_ctr_all,
        CAST(UniqueClicksAll AS INT64) AS unique_clicks_all
    FROM {{ source('shop', 'MetaRetargeting_Performances_Per_Ad') }}
)

SELECT
    -- Pass-through of cleaned data
    ad_name,
    ad_delivery,
    ad_set_name,
    results,
    reach,
    impressions,
    cost_per_results,
    amount_spent_eur,
    unique_ctr_all_100,
    unique_ctr_all,
    unique_clicks_all
FROM cleaned