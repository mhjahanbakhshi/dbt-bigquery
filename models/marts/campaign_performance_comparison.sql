WITH google_search AS (
    SELECT
        'Google Search Campaign' AS campaign_type,
        campaign AS campaign_name,
        cost AS total_cost,
        cost_per_apply,
        ctr,
        SAFE_DIVIDE(SAFE_DIVIDE(cost,cost_per_apply),clicks) AS conversion_rate,
        SAFE_DIVIDE(cost,cost_per_apply) AS total_conversions,
        impressions,
        clicks,
        CASE
            WHEN cost_per_apply < 352 THEN 'Target Met'
            ELSE 'Target Not Met'
        END AS kpi_status
    FROM {{ ref('stg_google_search_overall_performances') }}
),
pmax_campaign AS (
    SELECT
        'PMax Campaign' AS campaign_type,
        campaign AS campaign_name,
        cost AS total_cost,
        cost_apply AS cost_per_apply,
        ctr,
        SAFE_DIVIDE(apply, clicks) AS conversion_rate,
        apply AS total_conversions,
        impressions,
        clicks,
        CASE
            WHEN cost_apply < 352 THEN 'Target Met'
            ELSE 'Target Not Met'
        END AS kpi_status
    FROM {{ ref('stg_pmax_overall_performance') }}
),
meta_retargeting AS (
    SELECT
        'Meta Retargeting Campaign' AS campaign_type,
        campaign_name,
        amount_spent_eur AS total_cost,
        cost_per_results AS cost_per_apply,
        unique_ctr_all AS ctr,
        SAFE_DIVIDE(results, unique_clicks_all) AS conversion_rate,
        results AS total_conversions,
        impressions,
        unique_clicks_all AS clicks,
        CASE
            WHEN cost_per_results < 250 THEN 'Target Met'
            ELSE 'Target Not Met'
        END AS kpi_status
    FROM {{ ref('stg_meta_retargeting_overall_performances') }}
),
meta_free_course AS (
    SELECT
        'Meta Free Course Campaign' AS campaign_type,
        campaign_name,
        amount_spent_eur AS total_cost,
        cost_per_results AS cost_per_apply,
        unique_ctr_all AS ctr,
        SAFE_DIVIDE(results, unique_clicks_all) AS conversion_rate,
        results AS total_conversions,
        impressions,
        unique_clicks_all AS clicks,
        CASE
            WHEN cost_per_results < 250 THEN 'Target Met'
            ELSE 'Target Not Met'
        END AS kpi_status
    FROM {{ ref('stg_meta_free_course_overall_performances') }}
)

-- Combine all campaigns into a single table
SELECT * FROM google_search
UNION ALL
SELECT * FROM pmax_campaign
UNION ALL
SELECT * FROM meta_retargeting
UNION ALL
SELECT * FROM meta_free_course