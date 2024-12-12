WITH cleaned AS (
    SELECT
        -- Campaign details
        TRIM(Campaign) AS campaign,
        TRIM(CurrencyCode) AS currency_code,
        CAST(Budget AS INT64) AS budget,
        TRIM(BudgetType) AS budget_type,
        TRIM(CampaignType) AS campaign_type,
        TRIM(BidStrategyType) AS bid_strategy_type,

        -- Performance metrics
        SAFE_CAST(CTR AS FLOAT64) AS ctr,
        SAFE_CAST(Cost AS FLOAT64) AS cost,
        SAFE_CAST(CVRVisitToLeads AS FLOAT64) AS cvr_visit_to_leads,
        SAFE_CAST(CVRClickToLeads AS FLOAT64) AS cvr_click_to_leads,
        SAFE_CAST(CostPerApply AS FLOAT64) AS cost_per_apply,
        SAFE_CAST(CostPerLeadsAll AS FLOAT64) AS cost_per_leads_all,
        CAST(Impressions AS INT64) AS impressions,
        CAST(Clicks AS INT64) AS clicks,
        SAFE_CAST(ConversionRate AS FLOAT64) AS conversion_rate,
        SAFE_CAST(Conversions AS FLOAT64) AS conversions,
        SAFE_CAST(AvgCPC AS FLOAT64) AS avg_cpc
    FROM {{ source('shop', 'GoogleSearch_Overall_Performances') }}
)

SELECT
    -- Pass-through of cleaned data
    campaign,
    currency_code,
    budget,
    budget_type,
    campaign_type,
    bid_strategy_type,
    ctr,
    cost,
    cvr_visit_to_leads,
    cvr_click_to_leads,
    cost_per_apply,
    cost_per_leads_all,
    impressions,
    clicks,
    conversion_rate,
    conversions,
    avg_cpc
FROM cleaned