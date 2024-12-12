WITH cleaned AS (
    SELECT
        -- Campaign details
        TRIM(Campaign) AS campaign,
        TRIM(CurrencyCode) AS currency_code,
        CAST(Budget AS INT64) AS budget,
        TRIM(BudgetType) AS budget_type,
        TRIM(Status) AS status,
        TRIM(CampaignType) AS campaign_type,
        TRIM(BidStrategyType) AS bid_strategy_type,

        -- Performance metrics
        CAST(Clicks AS INT64) AS clicks,
        CAST(Impr AS INT64) AS impressions,
        SAFE_CAST(Ctr AS FLOAT64) AS ctr,
        SAFE_CAST(AvgCpc AS FLOAT64) AS avg_cpc,
        SAFE_CAST(Cost AS FLOAT64) AS cost,
        SAFE_CAST(CvrClickLeads AS FLOAT64) AS cvr_click_leads,
        SAFE_CAST(CvrVisitLeads AS FLOAT64) AS cvr_visit_leads,
        SAFE_CAST(CostApply AS FLOAT64) AS cost_apply,
        SAFE_CAST(Apply AS FLOAT64) AS apply,
        SAFE_CAST(CostLeadsAll AS FLOAT64) AS cost_leads_all,
        SAFE_CAST(ConvRate AS FLOAT64) AS conversion_rate,
        SAFE_CAST(Conversions AS FLOAT64) AS conversions
    FROM {{ source('shop', 'PMax_Overall_Performance') }}
)

SELECT
    -- Pass-through of cleaned data
    campaign,
    currency_code,
    budget,
    budget_type,
    status,
    campaign_type,
    bid_strategy_type,
    clicks,
    impressions,
    ctr,
    avg_cpc,
    cost,
    cvr_click_leads,
    cvr_visit_leads,
    cost_apply,
    apply,
    cost_leads_all,
    conversion_rate,
    conversions
FROM cleaned