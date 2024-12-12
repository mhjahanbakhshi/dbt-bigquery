WITH cleaned AS (
    SELECT
        -- Ad Group details
        TRIM(AdGroup) AS ad_group,
        TRIM(Status) AS status,
        Currency AS currency,
        
        -- Performance metrics
        SAFE_CAST(CTR AS FLOAT64) AS ctr,
        SAFE_CAST(Cost AS FLOAT64) AS cost,
        SAFE_CAST(CVRClickToLeads AS FLOAT64) AS cvr_click_to_leads,
        SAFE_CAST(LeadsBootcamp AS FLOAT64) AS leads_bootcamp,
        SAFE_CAST(CostperApply AS FLOAT64) AS cost_per_apply,
        SAFE_CAST(CostperLeadAll AS FLOAT64) AS cost_per_lead_all,
        SAFE_CAST(ConversionRate AS FLOAT64) AS conversion_rate,
        SAFE_CAST(Conversions AS FLOAT64) AS conversions
    FROM {{ source('shop', 'GoogleSearch_Ad_Groups') }}
)

SELECT
    -- Pass-through of cleaned data
    ad_group,
    status,
    currency,
    ctr,
    cost,
    cvr_click_to_leads,
    leads_bootcamp,
    cost_per_apply,
    cost_per_lead_all,
    conversion_rate,
    conversions
FROM cleaned
