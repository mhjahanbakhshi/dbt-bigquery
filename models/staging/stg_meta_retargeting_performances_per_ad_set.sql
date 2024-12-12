WITH cleaned AS (
    SELECT
        -- Ad set details
        TRIM(AdSetName) AS ad_set_name,
        TRIM(AdSetDelivery) AS ad_set_delivery,
        TRIM(AdSetBudget) AS ad_set_budget,
        SAFE_CAST(LastSignificantEdit AS TIMESTAMP) AS last_significant_edit,
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
    FROM {{ source('shop', 'MetaRetargeting_Performances_Per_Ad_Set') }}
)

SELECT
    -- Pass-through of cleaned data
    ad_set_name,
    ad_set_delivery,
    ad_set_budget,
    last_significant_edit,
    attribution_setting,
    final_url,
    results,
    reach,
    impressions,
    cost_per_results,
    amount_spent_eur,
    unique_ctr_all,
    unique_clicks_all
FROM cleaned