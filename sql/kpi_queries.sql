-- ============================================================
-- E-Commerce Returns Analytics -- Core KPI Queries
-- ============================================================

-- KPI 1: Overall Return Rate
SELECT
    COUNT(DISTINCT return_id)                               AS total_returns,
    COUNT(DISTINCT order_id)                                AS total_orders,
    ROUND(COUNT(DISTINCT return_id)*100.0/NULLIF(COUNT(DISTINCT order_id),0),2) AS return_rate_pct
FROM returns;

-- KPI 2: Return Rate by Category
SELECT
    category, COUNT(*) AS total_returns,
    ROUND(AVG(return_cost),2) AS avg_return_cost,
    ROUND(AVG(resolution_days),1) AS avg_resolution_days,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat,
    COUNT(*)*100.0/SUM(COUNT(*)) OVER () AS pct_of_total
FROM returns GROUP BY category ORDER BY total_returns DESC;

-- KPI 3: Cost-Per-Return by Region
SELECT
    region, COUNT(*) AS total_returns,
    ROUND(SUM(return_cost),2) AS total_return_cost,
    ROUND(AVG(return_cost),2) AS avg_cost_per_return,
    ROUND(AVG(resolution_days),1) AS avg_resolution_days,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat
FROM returns GROUP BY region ORDER BY total_return_cost DESC;

-- KPI 4: Resolution Time SLA Buckets
SELECT
    CASE
        WHEN resolution_days <= 2  THEN '0-2 days (Fast)'
        WHEN resolution_days <= 5  THEN '3-5 days (On-Time)'
        WHEN resolution_days <= 10 THEN '6-10 days (Delayed)'
        ELSE '11+ days (Critical)'
    END AS resolution_bucket,
    COUNT(*) AS return_count,
    ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER (),2) AS pct_of_total,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat
FROM returns GROUP BY resolution_bucket ORDER BY MIN(resolution_days);

-- KPI 5: Customer Satisfaction by Return Reason
SELECT
    return_reason, COUNT(*) AS return_count,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat,
    ROUND(AVG(return_cost),2) AS avg_return_cost,
    ROUND(AVG(resolution_days),1) AS avg_resolution_days
FROM returns GROUP BY return_reason ORDER BY return_count DESC;

-- KPI 6: Monthly Return Volume & Cost Trend
SELECT year, month, quarter,
    COUNT(*) AS total_returns,
    ROUND(SUM(return_cost),2) AS total_cost,
    ROUND(AVG(return_cost),2) AS avg_cost,
    ROUND(AVG(resolution_days),1) AS avg_resolution_days,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat
FROM returns GROUP BY year, month, quarter ORDER BY month;

-- KPI 7: Resolution Type Effectiveness
SELECT
    resolution_type, COUNT(*) AS return_count,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat,
    ROUND(AVG(resolution_days),1) AS avg_resolution_days,
    ROUND(AVG(return_cost),2) AS avg_return_cost
FROM returns GROUP BY resolution_type ORDER BY avg_csat DESC;

-- KPI 8: High-Value Returns (product_price > $200)
SELECT
    category, return_reason, COUNT(*) AS return_count,
    ROUND(AVG(product_price),2) AS avg_product_price,
    ROUND(SUM(return_cost),2) AS total_return_cost,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat
FROM returns WHERE product_price > 200
GROUP BY category, return_reason ORDER BY total_return_cost DESC LIMIT 15;

-- KPI 9: Customer Tier Analysis
SELECT
    customer_tier, COUNT(*) AS total_returns,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(AVG(return_cost),2) AS avg_return_cost,
    ROUND(AVG(customer_satisfaction),2) AS avg_csat,
    ROUND(AVG(resolution_days),1) AS avg_resolution_days,
    ROUND(COUNT(*)*1.0/COUNT(DISTINCT customer_id),2) AS returns_per_customer
FROM returns GROUP BY customer_tier
ORDER BY FIELD(customer_tier,'Platinum','Gold','Silver','Bronze');

-- KPI 10: Top 10 Products by Return Volume
SELECT
    r.product_id, p.product_name, p.category, p.unit_price,
    COUNT(r.return_id) AS total_returns,
    ROUND(SUM(r.return_cost),2) AS total_return_cost,
    ROUND(AVG(r.customer_satisfaction),2) AS avg_csat
FROM returns r JOIN products p ON r.product_id=p.product_id
GROUP BY r.product_id, p.product_name, p.category, p.unit_price
ORDER BY total_returns DESC LIMIT 10;
