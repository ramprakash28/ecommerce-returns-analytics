"""
E-Commerce Returns Data Generator
Generates 50,000+ realistic return transaction records for analytics pipeline.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

random.seed(42)
np.random.seed(42)

NUM_RETURNS     = 52_000
NUM_CUSTOMERS   = 18_000
NUM_PRODUCTS    = 400
START_DATE      = datetime(2024, 1, 1)
END_DATE        = datetime(2025, 12, 31)

CATEGORIES = {
    "Electronics":       {"weight": 0.22, "avg_price": 280, "return_cost_mult": 1.6},
    "Apparel":           {"weight": 0.28, "avg_price":  65, "return_cost_mult": 0.9},
    "Home & Kitchen":    {"weight": 0.18, "avg_price":  95, "return_cost_mult": 1.1},
    "Sports & Outdoors": {"weight": 0.12, "avg_price": 110, "return_cost_mult": 1.2},
    "Beauty":            {"weight": 0.08, "avg_price":  40, "return_cost_mult": 0.7},
    "Toys & Games":      {"weight": 0.07, "avg_price":  55, "return_cost_mult": 0.8},
    "Books & Media":     {"weight": 0.05, "avg_price":  20, "return_cost_mult": 0.5},
}

REGIONS = {
    "North":    0.22,
    "South":    0.27,
    "East":     0.25,
    "West":     0.18,
    "Central":  0.08,
}

RETURN_REASONS = {
    "Defective / Damaged":       0.28,
    "Wrong Item Sent":           0.14,
    "Item Not as Described":     0.18,
    "Changed Mind":              0.16,
    "Sizing / Fit Issue":        0.10,
    "Arrived Late":              0.07,
    "Duplicate Order":           0.04,
    "Gift / Unwanted":           0.03,
}

RESOLUTION_TYPES = {
    "Refund":       0.52,
    "Exchange":     0.28,
    "Store Credit": 0.15,
    "Rejected":     0.05,
}

RESOLUTION_DAYS_BY_REASON = {
    "Defective / Damaged":       (5, 3),
    "Wrong Item Sent":           (4, 2),
    "Item Not as Described":     (6, 3),
    "Changed Mind":              (3, 2),
    "Sizing / Fit Issue":        (3, 2),
    "Arrived Late":              (7, 4),
    "Duplicate Order":           (2, 1),
    "Gift / Unwanted":           (5, 3),
}

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def weighted_choice(choices_dict):
    keys = list(choices_dict.keys())
    weights = list(choices_dict.values())
    return random.choices(keys, weights=weights, k=1)[0]

print("Generating products...")
products = []
cat_keys = list(CATEGORIES.keys())
cat_weights = [CATEGORIES[c]["weight"] for c in cat_keys]
for pid in range(1, NUM_PRODUCTS + 1):
    cat = random.choices(cat_keys, weights=cat_weights, k=1)[0]
    base_price = CATEGORIES[cat]["avg_price"]
    price = round(max(5, np.random.normal(base_price, base_price * 0.35)), 2)
    products.append({"product_id": f"PROD-{pid:04d}", "product_name": f"{cat} Item {pid}", "category": cat, "unit_price": price})
products_df = pd.DataFrame(products)

print("Generating customers...")
customers = []
region_keys = list(REGIONS.keys())
region_weights = list(REGIONS.values())
for cid in range(1, NUM_CUSTOMERS + 1):
    region = random.choices(region_keys, weights=region_weights, k=1)[0]
    customers.append({
        "customer_id": f"CUST-{cid:05d}",
        "customer_name": f"Customer {cid}",
        "region": region,
        "customer_tier": random.choices(["Bronze", "Silver", "Gold", "Platinum"], weights=[0.45, 0.30, 0.17, 0.08], k=1)[0],
    })
customers_df = pd.DataFrame(customers)

print("Generating return transactions...")
returns = []
for i in range(1, NUM_RETURNS + 1):
    customer = customers_df.sample(1).iloc[0]
    product  = products_df.sample(1).iloc[0]
    cat_info = CATEGORIES[product["category"]]
    return_date = random_date(START_DATE, END_DATE)
    reason = weighted_choice(RETURN_REASONS)
    resolution = weighted_choice(RESOLUTION_TYPES)
    mu, sigma = RESOLUTION_DAYS_BY_REASON[reason]
    res_days = max(1, int(np.random.normal(mu, sigma)))
    resolution_date = return_date + timedelta(days=res_days)
    base_cost = product["unit_price"] * 0.12 * cat_info["return_cost_mult"]
    return_cost = round(max(2.5, base_cost * np.random.normal(1.0, 0.15)), 2)
    if reason in ("Defective / Damaged", "Wrong Item Sent"):
        csat = max(1, min(5, int(np.random.normal(2.5, 0.9))))
    elif reason in ("Changed Mind", "Gift / Unwanted"):
        csat = max(1, min(5, int(np.random.normal(3.8, 0.8))))
    else:
        csat = max(1, min(5, int(np.random.normal(3.2, 1.0))))
    returns.append({
        "return_id": f"RET-{i:06d}", "order_id": f"ORD-{random.randint(1, 200000):07d}",
        "customer_id": customer["customer_id"], "product_id": product["product_id"],
        "category": product["category"], "region": customer["region"],
        "return_reason": reason, "resolution_type": resolution,
        "return_date": return_date.strftime("%Y-%m-%d"), "resolution_date": resolution_date.strftime("%Y-%m-%d"),
        "resolution_days": res_days, "product_price": product["unit_price"],
        "return_cost": return_cost, "customer_satisfaction": csat,
        "customer_tier": customer["customer_tier"],
        "quarter": f"Q{(return_date.month - 1) // 3 + 1}",
        "month": return_date.strftime("%Y-%m"), "year": return_date.year,
    })
returns_df = pd.DataFrame(returns)

out_dir = os.path.dirname(os.path.abspath(__file__))
returns_df.to_csv(os.path.join(out_dir, "returns.csv"), index=False)
products_df.to_csv(os.path.join(out_dir, "products.csv"), index=False)
customers_df.to_csv(os.path.join(out_dir, "customers.csv"), index=False)

print(f"\n✅ Data generation complete!")
print(f"   Returns:   {len(returns_df):,} rows  ->  returns.csv")
print(f"   Products:  {len(products_df):,} rows  ->  products.csv")
print(f"   Customers: {len(customers_df):,} rows  ->  customers.csv")
