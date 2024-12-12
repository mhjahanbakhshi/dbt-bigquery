WITH cleaned AS (
    SELECT
        -- Campaign details
        TRIM(CampaignName) AS campaign_name,
        TRIM(CampaignDelivery) AS campaign_delivery,
        CAST(AdSetBudget AS INT64) AS ad_set_budget,
        TRIM(AdSetBudgetType) AS ad_set_budget_type,
        TRIM(AttributionSetting) AS attribution_setting,

        -- Performance metrics
        CAST(Results AS INT64) AS results,
        CAST(Reach AS INT64) AS reach,
        CAST(Impressions AS INT64) AS impressions,
        SAFE_CAST(CostPerResults AS FLOAT64) AS cost_per_results,
        SAFE_CAST(AmountSpentEur AS FLOAT64) AS amount_spent_eur,
        SAFE_CAST(UniqueCtrAll AS FLOAT64) AS unique_ctr_all,
        CAST(UniqueClicksAll AS INT64) AS unique_clicks_all
    FROM {{ source('shop', 'MetaRetargeting_Overall_Performances') }}
)

SELECT
    -- Pass-through of cleaned data
    campaign_name,
    campaign_delivery,
    ad_set_budget,
    ad_set_budget_type,
    attribution_setting,
    results,
    reach,
    impressions,
    cost_per_results,
    amount_spent_eur,
    unique_ctr_all,
    unique_clicks_all
FROM cleaned