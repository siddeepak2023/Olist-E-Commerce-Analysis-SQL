[olist_README.md](https://github.com/user-attachments/files/26104991/olist_README.md)
# Olist E-Commerce Revenue & Sales Analysis

A end-to-end business intelligence project analyzing **96,000+ real e-commerce transactions** from Olist, Brazil's largest online marketplace. Built with Python, SQL, and Tableau.

---

## Business Problem

What drives revenue on Brazil's largest e-commerce platform? This project answers that question by analyzing real transaction data across 9 relational tables — identifying top product categories, regional performance, payment behavior, and the impact of delivery speed on customer satisfaction.

---

## Key Findings

- **Health & Beauty** is the #1 revenue category at **$1.4M (9.2% of total revenue)**
- **São Paulo** accounts for **37% of all revenue** — far ahead of any other state
- **Credit card** dominates at **79% of transactions**, with an average of 3.6 installments
- Orders with **21+ day delivery have a 2.97 avg review score** vs 4.33 for fast delivery — a 31% drop
- Revenue grew from **$143 in Sept 2016 to $1.15M in Nov 2017** — 8,000x growth in 14 months

---

## Dashboard

[View Live Tableau Dashboard →](YOUR_TABLEAU_PUBLIC_URL_HERE)

![Dashboard Preview](dashboard_screenshot.png)

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python (Pandas, NumPy) | Data cleaning & feature engineering |
| SQLite + SQL | 7 analytical queries across relational tables |
| Tableau Public | Interactive 4-chart dashboard |
| Excel | Initial data exploration & export |

---

## Dataset

**Source:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — Kaggle  
**Type:** Real transaction data donated by Olist  
**Size:** 96,478 delivered orders across 9 CSV files  
**Period:** September 2016 – August 2018

---

## Project Structure

```
olist_ecommerce_analysis/
│
├── 01_cleaning.py          # Load, clean & join all 9 CSV files into master table
├── 02_sql_analysis.py      # 7 SQL queries: revenue, growth, regions, payments
├── 03_export_tableau.py    # Export results to Excel for Tableau
│
├── olist_master_clean.csv  # Cleaned master dataset (110,197 rows)
├── olist.db                # SQLite database with all tables
├── olist_tableau_data.xlsx # Formatted results for Tableau
│
└── README.md
```

---

## SQL Highlights

This project uses advanced SQL including **window functions**, **CTEs**, and **multi-table JOINs**:

```sql
-- Month-over-month revenue growth using window functions
WITH monthly AS (
    SELECT year, month, month_name,
        ROUND(SUM(revenue), 2) AS monthly_revenue
    FROM master
    GROUP BY year, month, month_name
)
SELECT year, month_name, monthly_revenue,
    ROUND(
        (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year, month))
        / LAG(monthly_revenue) OVER (ORDER BY year, month) * 100, 1
    ) AS mom_growth_pct
FROM monthly
ORDER BY year, month
```

```sql
-- Category revenue tiers using NTILE CTE
WITH category_stats AS (
    SELECT category,
        ROUND(SUM(revenue), 2) AS total_revenue,
        COUNT(DISTINCT order_id) AS total_orders
    FROM master
    GROUP BY category
),
percentiles AS (
    SELECT *, NTILE(3) OVER (ORDER BY total_revenue) AS revenue_tier
    FROM category_stats
)
SELECT category, total_revenue,
    CASE revenue_tier
        WHEN 3 THEN 'High'
        WHEN 2 THEN 'Mid'
        WHEN 1 THEN 'Low'
    END AS tier
FROM percentiles
ORDER BY total_revenue DESC
```

---

## How to Run

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/olist_ecommerce_analysis.git
cd olist_ecommerce_analysis

# 2. Install dependencies
pip install pandas numpy matplotlib seaborn sqlalchemy openpyxl

# 3. Download dataset from Kaggle
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# Place all CSV files in the project folder

# 4. Run scripts in order
python 01_cleaning.py
python 02_sql_analysis.py
python 03_export_tableau.py
```

---

## Business Insights & Recommendations

1. **Double down on Health & Beauty** — highest revenue category with strong avg order value ($149). Priority for promotional campaigns.

2. **Fix delivery in the Northeast** — Bahia (BA) averages 19 delivery days and has a below-average review score. Partnering with local carriers could recover significant customer satisfaction.

3. **Promote installment plans** — Credit card customers average 3.6 installments per order. Highlighting flexible payment options could increase avg order value across other payment methods.

4. **Target São Paulo for growth** — SP drives 37% of revenue but only 40% of orders, suggesting higher-value customers. Loyalty programs here would have outsized impact.

---

*Built by Siddharth Deepak | Business Analytics & AI, UT Dallas*
