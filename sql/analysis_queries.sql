-- ============================================================
-- E-Commerce Returns Analytics -- Advanced Analysis Queries
-- ============================================================

-- ANALYSIS 1: Root-Cause Defect Pareto (80/20)
WITH reason_counts AS (
    SELECT return_reason, COUNT(*) AS return_count, SUM(return_cost) AS total_cost
    FROM returns GROUP BY return_reason
),
ranked AS (
    SELECT *,
        SUM(return_count) OVER (ORDER BY return_count DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(return_count) OVER () AS grand_total
    FROM reason_counts
)
SELECT return_reason, return_count,
    ROUND(total_cost,2) AS total_cost,
    ROUND(return_count*100.0/grand_total,2) AS pct_of_total,
    ROUND(running_count*100.0/grand_total,2) AS cumulative_pct
FROM ranked ORDER BY return_count DESC;

-- ANALYSIS 2: Category x Reason Cost Heatmap
SELECT category, return_reason, COUNT(*) AS return_count,
    ROUND(SUM(return_cost),2) AS total_cost,
    ROUND(AVG(return_cost),2) AS avg_cost,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat
FROM returns GROUP BY category, return_reason ORDER BY total_cost DESC LIMIT 20;

-- ANALYSIS 3: Seasonal Pattern (quarter x category)
SELECT year, quarter, category, COUNT(*) AS total_returns,
    ROUND(SUM(return_cost),2) AS total_cost,
    ROUND(AVG(resolution_days),1) AS avg_resolution_days
FROM returns GROUP BY year, quarter, category ORDER BY year, quarter, total_returns DESC;

-- ANALYSIS 4: Resolution Bottleneck Detection
SELECT region, category, COUNT(*) AS return_count,
    ROUND(AVG(resolution_days),1) AS avg_resolution_days,
    MAX(resolution_days) AS max_resolution_days,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat,
    SUM(CASE WHEN resolution_days>7 THEN 1 ELSE 0 END) AS delayed_count,
    ROUND(SUM(CASE WHEN resolution_days>7 THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS delay_rate_pct
FROM returns GROUP BY region, category ORDER BY avg_resolution_days DESC LIMIT 20;

-- ANALYSIS 5: CSAT vs Resolution Speed Correlation
SELECT resolution_days, COUNT(*) AS return_count,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat,
    ROUND(AVG(return_cost),2) AS avg_cost
FROM returns WHERE resolution_days<=15 GROUP BY resolution_days ORDER BY resolution_days;

-- ANALYSIS 6: Repeat Returners (>3 returns)
SELECT customer_id, customer_tier, COUNT(*) AS total_returns,
    ROUND(SUM(return_cost),2) AS total_return_cost,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat,
    MAX(return_date) AS latest_return
FROM returns GROUP BY customer_id, customer_tier HAVING COUNT(*)>3 ORDER BY total_returns DESC LIMIT 20;

-- REC-1: Electronics Defect QC
SELECT 'REC-1: Electronics QC Improvement' AS recommendation,
    category, return_reason, COUNT(*) AS returns,
    ROUND(SUM(return_cost),2) AS total_cost, ROUND(AVG(customer_satisfaction),2) AS avg_csat
FROM returns WHERE category='Electronics' AND return_reason='Defective / Damaged'
GROUP BY category, return_reason;

-- REC-2: Apparel Listing Accuracy
SELECT 'REC-2: Apparel Listing Enhancement' AS recommendation,
    category, return_reason, COUNT(*) AS returns, ROUND(SUM(return_cost),2) AS total_cost
FROM returns WHERE category='Apparel' AND return_reason IN ('Sizing / Fit Issue','Item Not as Described')
GROUP BY category, return_reason;

-- REC-3: South Region SLA
SELECT 'REC-3: South Region Resolution SLA' AS recommendation,
    region, COUNT(*) AS returns,
    ROUND(AVG(resolution_days),1) AS avg_resolution_days,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat
FROM returns WHERE region='South' GROUP BY region;
