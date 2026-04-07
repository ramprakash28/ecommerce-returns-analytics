# E-Commerce Returns Analytics & Process Improvement Dashboard

**Tech Stack:** Python · SQL · SQLite · HTML/JS (Chart.js) · Excel-ready CSV exports
**Timeline:** Jan 2026 – Feb 2026
**Dataset:** 52,000 return transactions · 18,000 customers · 400 products · 2-year window (2024–2025)

---

## What This Project Is About

E-commerce businesses lose millions of dollars every year to product returns — in shipping costs, restocking fees, and lost customer trust. The problem isn't just the volume of returns; it's that most companies don't know *why* returns happen or *where* the most damage is concentrated.

This project builds a full end-to-end analytics pipeline that:

- Processes 52,000+ return transactions across product categories, regions, and return reasons
- Automatically calculates the four most important return KPIs: **return rate**, **cost-per-return**, **resolution time**, and **customer satisfaction (CSAT)**
- Surfaces root-cause patterns through SQL analysis and a visual dashboard
- Delivers three concrete, data-backed process improvement recommendations

The result is a self-service dashboard that allows non-technical stakeholders to slice and explore return trends on their own — reducing dependence on manual reporting.

---

## Project Structure

```
ecommerce-returns-analytics/
│
├── data/
│   ├── generate_data.py      # Synthetic data generator (52K transactions)
│   ├── returns.csv           # Main fact table — one row per return (git-ignored, run script)
│   ├── products.csv          # 400 products across 7 categories
│   └── customers.csv         # 18,000 customers across 5 regions
│
├── sql/
│   ├── schema.sql            # Database schema + index definitions
│   ├── kpi_queries.sql       # 10 core KPI queries (return rate, cost, CSAT, SLA)
│   └── analysis_queries.sql  # Advanced: Pareto, heatmaps, bottlenecks, recommendations
│
├── python/
│   └── pipeline.py           # ETL pipeline: CSV → SQLite → KPI calc → reports
│
├── dashboard/
│   └── dashboard.html        # Interactive self-service dashboard (open in browser)
│
├── reports/                  # Auto-generated output files (created by pipeline.py)
│   ├── kpi_summary.csv
│   ├── category_analysis.csv
│   ├── region_analysis.csv
│   ├── reason_analysis.csv
│   ├── monthly_trend.csv
│   ├── sla_buckets.csv
│   ├── resolution_types.csv
│   ├── category_reason_heatmap.csv
│   ├── recommendations.csv
│   └── dashboard_data.json
│
└── README.md
```

---

## How to Run

### Prerequisites

- Python 3.8 or higher
- pip

### Step 1 — Install dependencies

```bash
pip install pandas numpy
```

SQLite is built into Python — no extra installation needed.

### Step 2 — Generate the dataset

```bash
cd data
python generate_data.py
```

This creates `returns.csv` (52,000 rows), `products.csv`, and `customers.csv` inside the `/data` folder.

### Step 3 — Run the analytics pipeline

```bash
cd ../python
python pipeline.py
```

The pipeline runs in 6 steps and prints a live console summary:
1. Load CSVs into memory
2. Build an in-memory SQLite database
3. Run all KPI calculations using SQL
4. Generate process improvement recommendations
5. Export 9 CSV reports + a JSON file to `/reports/`
6. Print KPI summary to the terminal

### Step 4 — Open the dashboard

Open `dashboard/dashboard.html` directly in any browser. No web server needed — all data is self-contained in the file.

---

## KPIs Tracked

| KPI | Description |
|-----|-------------|
| **Return Rate** | Returns as a percentage of unique orders |
| **Cost-per-Return** | Average processing + logistics cost per return transaction |
| **Resolution Time** | Average days from return initiation to resolution |
| **Customer Satisfaction (CSAT)** | 1–5 score collected at resolution, segmented by reason and region |

---

## Dashboard Features

The `dashboard.html` file is a fully interactive, Power BI-style analytics dashboard built with Chart.js. It includes:

- **5 KPI summary cards** — Total returns, total cost, avg cost/return, avg resolution days, avg CSAT
- **Monthly trend chart** — Return volume (bars) + avg cost (line) across 24 months
- **SLA resolution donut** — % of returns resolved in Fast / On-Time / Delayed / Critical buckets
- **Category breakdown** — Volume and total cost comparison across 7 product categories
- **Region breakdown** — Volume per region with CSAT overlay
- **Return reasons Pareto** — Volume + CSAT score by root cause (8 reasons)
- **Resolution type mix** — Donut of Refund / Exchange / Store Credit / Rejected
- **Category × Reason heatmap table** — Top 20 costliest combinations with visual cost bars
- **3 process improvement recommendations** — Data-backed, priority-ranked action items
- **Live filters** — Filter everything by Category, Region, Year, and Return Reason
- **CSV export** — Export filtered data with one click

---

## Data Model

### `returns` (fact table — 52,000 rows)

| Column | Type | Description |
|--------|------|-------------|
| return_id | string | Unique return identifier |
| order_id | string | Source order |
| customer_id | string | FK to customers |
| product_id | string | FK to products |
| category | string | Product category |
| region | string | Customer region |
| return_reason | string | Root cause of return |
| resolution_type | string | Refund / Exchange / Store Credit / Rejected |
| return_date | date | When return was initiated |
| resolution_date | date | When return was resolved |
| resolution_days | int | Days to resolution |
| product_price | decimal | Original product price |
| return_cost | decimal | Cost to process the return |
| customer_satisfaction | int | CSAT score (1–5) |
| customer_tier | string | Bronze / Silver / Gold / Platinum |
| quarter | string | Q1–Q4 |
| month | string | YYYY-MM |
| year | int | 2024 or 2025 |

---

## Process Improvement Recommendations

**REC-01 — Reduce Defect-Driven Returns in Electronics (HIGH PRIORITY)**
"Defective / Damaged" returns in Electronics account for $163,320 in return costs with avg CSAT of 2.08/5. Enforce pre-shipment QC inspections with suppliers. Estimated savings: ~$32,664 per cycle.

**REC-02 — Fix Product Listing Accuracy (MEDIUM PRIORITY)**
"Item Not as Described" drives $169,736 in total return costs. Audit product descriptions for top-20 return SKUs, add 360° images and size charts. Estimated savings: $26,674–$66,750.

**REC-03 — Automate Resolution & SLA Tracking (EFFICIENCY WIN)**
26% of returns take 6+ days to resolve. Deploy automated SLA tracking with 3-day escalation triggers. Projected: 35% reduction in manual reporting workload.

---

## Code Flow

```
generate_data.py  →  returns.csv + products.csv + customers.csv
                                   ↓
                             pipeline.py  (CSV → SQLite → KPI SQL → exports)
                                   ↓
                    /reports/  (9 CSVs + dashboard_data.json)
                                   ↓
                          dashboard.html  (charts + filters)
```

---

## Skills Demonstrated

- **Data Engineering** — Relational schema design + automated ETL pipeline
- **SQL Analytics** — 20+ queries: aggregations, window functions, CTEs, Pareto, heatmaps
- **Python (Pandas + SQLite)** — End-to-end pipeline with structured report exports
- **Data Visualization** — Self-service dashboard with live filters and multi-axis charts
- **Business Insight** — Three prioritized, cost-quantified process improvement recommendations

---

## Author

**Ram** — Data Analyst  
bollamramprakash@gmail.com  
https://github.com/ramprakash28
