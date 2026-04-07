"""
E-Commerce Returns Analytics Pipeline
======================================
ETL pipeline: CSV -> SQLite -> KPI calculations -> report exports

Usage:
    python pipeline.py

Outputs (written to ../reports/):
    kpi_summary.csv, category_analysis.csv, region_analysis.csv,
    reason_analysis.csv, monthly_trend.csv, sla_buckets.csv,
    resolution_types.csv, category_reason_heatmap.csv,
    recommendations.csv, dashboard_data.json
"""

import sqlite3
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(BASE_DIR, "../data")
REPORTS_DIR = os.path.join(BASE_DIR, "../reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

print("=" * 60)
print("  E-Commerce Returns Analytics Pipeline")
print("=" * 60)
print(f"\n[1/6] Loading data...")

returns_df   = pd.read_csv(os.path.join(DATA_DIR, "returns.csv"),   parse_dates=["return_date", "resolution_date"])
products_df  = pd.read_csv(os.path.join(DATA_DIR, "products.csv"))
customers_df = pd.read_csv(os.path.join(DATA_DIR, "customers.csv"))
print(f"      Returns: {len(returns_df):,} | Products: {len(products_df):,} | Customers: {len(customers_df):,}")

print("\n[2/6] Building SQLite database...")
conn = sqlite3.connect(":memory:")
returns_df.to_sql("returns",   conn, if_exists="replace", index=False)
products_df.to_sql("products", conn, if_exists="replace", index=False)
customers_df.to_sql("customers", conn, if_exists="replace", index=False)

def sql(query):
    return pd.read_sql_query(query, conn)

print("\n[3/6] Calculating KPIs...")

summary = sql("""
    SELECT COUNT(DISTINCT return_id) AS total_returns,
        COUNT(DISTINCT order_id) AS unique_orders,
        ROUND(COUNT(DISTINCT return_id)*100.0/COUNT(DISTINCT order_id),2) AS return_rate_pct,
        ROUND(SUM(return_cost),2) AS total_return_cost,
        ROUND(AVG(return_cost),2) AS avg_cost_per_return,
        ROUND(AVG(resolution_days),1) AS avg_resolution_days,
        ROUND(AVG(customer_satisfaction),2) AS avg_csat,
        MIN(return_date) AS data_start, MAX(return_date) AS data_end
    FROM returns""")

category_df = sql("""
    SELECT category, COUNT(*) AS total_returns,
        ROUND(SUM(return_cost),2) AS total_cost, ROUND(AVG(return_cost),2) AS avg_cost,
        ROUND(AVG(resolution_days),1) AS avg_resolution_days,
        ROUND(AVG(customer_satisfaction),2) AS avg_csat,
        ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM returns),2) AS pct_of_total
    FROM returns GROUP BY category ORDER BY total_returns DESC""")

region_df = sql("""
    SELECT region, COUNT(*) AS total_returns,
        ROUND(SUM(return_cost),2) AS total_cost, ROUND(AVG(return_cost),2) AS avg_cost_per_return,
        ROUND(AVG(resolution_days),1) AS avg_resolution_days,
        ROUND(AVG(customer_satisfaction),2) AS avg_csat
    FROM returns GROUP BY region ORDER BY total_returns DESC""")

reason_df = sql("""
    SELECT return_reason, COUNT(*) AS return_count,
        ROUND(SUM(return_cost),2) AS total_cost, ROUND(AVG(return_cost),2) AS avg_cost,
        ROUND(AVG(customer_satisfaction),2) AS avg_csat,
        ROUND(AVG(resolution_days),1) AS avg_resolution_days,
        ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM returns),2) AS pct_of_total
    FROM returns GROUP BY return_reason ORDER BY return_count DESC""")

monthly_df = sql("""
    SELECT month, year, quarter, COUNT(*) AS total_returns,
        ROUND(SUM(return_cost),2) AS total_cost, ROUND(AVG(return_cost),2) AS avg_cost,
        ROUND(AVG(resolution_days),1) AS avg_resolution_days,
        ROUND(AVG(customer_satisfaction),2) AS avg_csat
    FROM returns GROUP BY month, year, quarter ORDER BY month""")

sla_df = sql("""
    SELECT CASE
        WHEN resolution_days<=2  THEN '0-2 days (Fast)'
        WHEN resolution_days<=5  THEN '3-5 days (On-Time)'
        WHEN resolution_days<=10 THEN '6-10 days (Delayed)'
        ELSE '11+ days (Critical)' END AS sla_bucket,
        COUNT(*) AS return_count,
        ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM returns),2) AS pct,
        ROUND(AVG(customer_satisfaction),2) AS avg_csat
    FROM returns GROUP BY sla_bucket ORDER BY MIN(resolution_days)""")

resolution_df = sql("""
    SELECT resolution_type, COUNT(*) AS return_count,
        ROUND(AVG(customer_satisfaction),2) AS avg_csat,
        ROUND(AVG(resolution_days),1) AS avg_resolution_days,
        ROUND(SUM(return_cost),2) AS total_cost
    FROM returns GROUP BY resolution_type ORDER BY avg_csat DESC""")

heatmap_df = sql("""
    SELECT category, return_reason, COUNT(*) AS cnt,
        ROUND(SUM(return_cost),2) AS total_cost,
        ROUND(AVG(customer_satisfaction),2) AS avg_csat
    FROM returns GROUP BY category, return_reason ORDER BY total_cost DESC""")

print("\n[4/6] Generating recommendations...")
top1 = heatmap_df.iloc[0]
top2 = heatmap_df.iloc[1]
worst_region = region_df.sort_values("avg_csat").iloc[0]

recommendations = pd.DataFrame([
    {
        "rec_id": "REC-01", "title": "Reduce Defect-Driven Returns in Electronics",
        "finding": f"'{top1['return_reason']}' in {top1['category']} = ${top1['total_cost']:,.0f} cost, CSAT {top1['avg_csat']}/5.",
        "action": "Enforce pre-shipment QC with Electronics suppliers. Target 20% defect reduction.",
        "estimated_impact": f"~${top1['total_cost']*0.20:,.0f} cost savings per cycle",
    },
    {
        "rec_id": "REC-02", "title": f"Fix '{top2['return_reason']}' in {top2['category']}",
        "finding": f"${top2['total_cost']:,.0f} total cost, CSAT {top2['avg_csat']}/5.",
        "action": "Audit product descriptions and imagery. Add 360 images, size guides, verified reviews.",
        "estimated_impact": f"~${top2['total_cost']*0.25:,.0f} cost savings + CSAT lift",
    },
    {
        "rec_id": "REC-03", "title": f"Expedite Resolution in {worst_region['region']} Region",
        "finding": f"{worst_region['region']} has lowest CSAT ({worst_region['avg_csat']}/5).",
        "action": f"Add dedicated returns agent in {worst_region['region']}. Automate SLA tracking. Target <= 5 days.",
        "estimated_impact": "35% reduction in manual reporting; +0.4 CSAT improvement",
    },
])

print("\n[5/6] Exporting reports...")
summary.to_csv(        os.path.join(REPORTS_DIR, "kpi_summary.csv"),             index=False)
category_df.to_csv(    os.path.join(REPORTS_DIR, "category_analysis.csv"),       index=False)
region_df.to_csv(      os.path.join(REPORTS_DIR, "region_analysis.csv"),         index=False)
reason_df.to_csv(      os.path.join(REPORTS_DIR, "reason_analysis.csv"),         index=False)
monthly_df.to_csv(     os.path.join(REPORTS_DIR, "monthly_trend.csv"),           index=False)
sla_df.to_csv(         os.path.join(REPORTS_DIR, "sla_buckets.csv"),             index=False)
resolution_df.to_csv(  os.path.join(REPORTS_DIR, "resolution_types.csv"),        index=False)
heatmap_df.to_csv(     os.path.join(REPORTS_DIR, "category_reason_heatmap.csv"), index=False)
recommendations.to_csv(os.path.join(REPORTS_DIR, "recommendations.csv"),         index=False)

dashboard_data = {
    "generated_at": datetime.now().isoformat(),
    "summary": summary.to_dict(orient="records")[0],
    "by_category": category_df.to_dict(orient="records"),
    "by_region": region_df.to_dict(orient="records"),
    "by_reason": reason_df.to_dict(orient="records"),
    "monthly": monthly_df.to_dict(orient="records"),
    "sla": sla_df.to_dict(orient="records"),
    "resolution": resolution_df.to_dict(orient="records"),
    "heatmap": heatmap_df.head(20).to_dict(orient="records"),
    "recommendations": recommendations.to_dict(orient="records"),
}
with open(os.path.join(REPORTS_DIR, "dashboard_data.json"), "w") as f:
    json.dump(dashboard_data, f, indent=2, default=str)

print("\n[6/6] Done!")
s = summary.iloc[0]
print(f"  Total Returns: {int(s['total_returns']):,} | Return Rate: {s['return_rate_pct']}%")
print(f"  Total Cost: ${s['total_return_cost']:,.2f} | Avg Cost: ${s['avg_cost_per_return']} | Avg CSAT: {s['avg_csat']}/5")
conn.close()
