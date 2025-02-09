# Time-Series and Cohort Analytics Guide

## Overview

This guide covers advanced temporal analytics techniques implemented in this data warehouse project, focusing on time-series analysis and cohort retention tracking. These analytical patterns are essential for understanding business trends, customer behavior over time, and the long-term health of a customer base.

## Table of Contents

1. [Time-Series Analysis](#time-series-analysis)
2. [Cohort Analysis](#cohort-analysis)
3. [Customer Churn Analysis](#customer-churn-analysis)
4. [Seasonality and Peak Period Analysis](#seasonality-and-peak-period-analysis)
5. [Business Applications](#business-applications)
6. [SQL Techniques Used](#sql-techniques-used)

---

## Time-Series Analysis

### What is Time-Series Analysis?

Time-series analysis examines data points collected at successive time intervals to identify trends, patterns, and anomalies. In business intelligence, it helps answer questions like:

- Is revenue growing or declining?
- Are we accelerating or decelerating?
- What are our growth rates period-over-period?

### Key Metrics Implemented

#### 1. Month-over-Month (MoM) Growth

Month-over-month growth compares the current month's performance to the immediately previous month. This metric is sensitive to short-term changes and is useful for detecting recent trends or shifts in business performance.

**Formula:**
```
MoM Growth % = ((Current Month Revenue - Previous Month Revenue) / Previous Month Revenue) * 100
```

**Interpretation:**
- Positive MoM: Business is growing compared to last month
- Negative MoM: Business declined compared to last month
- Volatile MoM: Indicates seasonality or irregular patterns

**Use Cases:**
- Monthly executive dashboards
- Early warning system for performance issues
- Tracking immediate impact of marketing campaigns

#### 2. Year-over-Year (YoY) Growth

Year-over-year growth compares performance to the same period in the previous year, eliminating seasonal effects that might distort MoM comparisons.

**Formula:**
```
YoY Growth % = ((Current Month Revenue - Same Month Last Year Revenue) / Same Month Last Year Revenue) * 100
```

**Why YoY is Critical:**
- Accounts for seasonality (compares December to December, not December to November)
- Better indicator of long-term trends
- Industry standard for growth reporting

**Interpretation:**
- Positive YoY: Business is growing compared to last year's baseline
- Negative YoY: Business is contracting year-over-year
- Consistent YoY: Sustainable growth trajectory

#### 3. Moving Averages

Moving averages smooth out short-term fluctuations to reveal underlying trends. We implement a 3-month moving average.

**What it Does:**
- Reduces noise from day-to-day or month-to-month volatility
- Makes trends more visible
- Helps distinguish signal from noise

**Interpretation:**
- If actual revenue consistently above moving average: upward trend
- If actual revenue consistently below moving average: downward trend
- Actual revenue crossing moving average: potential trend reversal

**Use Cases:**
- Financial forecasting
- Identifying trend direction
- Smoothing seasonal variations

### SQL Implementation Notes

The revenue trends analysis uses the `LAG()` window function extensively:

```sql
-- Month-over-month comparison
LAG(revenue) OVER (ORDER BY year, month)

-- Year-over-year comparison (goes back 12 months)
LAG(revenue, 12) OVER (ORDER BY year, month)
```

The `LAG()` function accesses data from a previous row without requiring a self-join, making period-over-period comparisons efficient and readable.

---

## Cohort Analysis

### What is Cohort Analysis?

Cohort analysis groups customers by a shared characteristic (typically acquisition date) and tracks their behavior over time. This technique is fundamental in SaaS, subscription businesses, and e-commerce for understanding customer retention and lifetime value.

### Why Cohort Analysis Matters

Traditional retention metrics (like "overall retention rate") can be misleading because they mix customers acquired at different times. Cohort analysis provides a clearer picture by:

1. **Isolating acquisition effects:** Compare customers acquired in January vs February
2. **Tracking lifecycle patterns:** See how behavior evolves from month 0 to month 12
3. **Validating product changes:** Did retention improve for cohorts after a feature launch?
4. **Forecasting LTV:** Use retention curves to predict future purchase behavior

### Understanding Retention Curves

A retention curve shows what percentage of a cohort remains active over time. For e-commerce:

**Typical Pattern:**
- Month 0: 100% (by definition, everyone made first purchase)
- Month 1: 20-30% (repeat purchase rate)
- Month 3: 15-20%
- Month 6: 10-15%
- Month 12: 5-10%

**Interpretation:**

**Good Retention Curve:**
- High month-1 retention (>25% for e-commerce, >40% for SaaS)
- Gradual decline (not sharp drop-off)
- Plateau effect (retention stabilizes after initial drop)

**Poor Retention Curve:**
- Sharp drop after month 0 (weak onboarding or poor product-market fit)
- Continuously declining without plateau (no loyal customer base)
- Low absolute numbers (most customers never return)

### Cohort Comparison

Comparing cohorts reveals business health trends:

**Improving Retention (Good Sign):**
- Newer cohorts retain better than older cohorts
- Indicates product improvements, better onboarding, or smarter acquisition

**Degrading Retention (Warning Sign):**
- Newer cohorts retain worse than older cohorts
- May indicate declining product quality, increased competition, or poor acquisition targeting

### SQL Implementation Notes

Our cohort analysis uses a multi-step CTE approach:

1. **customer_cohorts:** Assign each customer to their acquisition month
2. **customer_orders:** Track all months when customers purchased
3. **cohort_data:** Calculate months since first order for each purchase
4. **cohort_sizes:** Determine initial cohort size (denominator for retention rate)
5. **Final calculation:** Retention % = (Retained Customers / Cohort Size) * 100

The use of `DATEDIFF(MONTH, cohort_month, order_month)` is critical for calculating "months since first order," which is the x-axis of retention curves.

---

## Customer Churn Analysis

### What is Churn?

Churn occurs when a customer stops doing business with you. In subscription businesses, churn is explicit (cancellation). In transactional businesses like e-commerce, churn is implicit (customer stops purchasing).

### Defining Churn Thresholds

Since e-commerce doesn't have explicit cancellations, we define churn based on inactivity periods:

- **Active:** 0-90 days since last purchase
- **At Risk:** 91-180 days (needs re-engagement)
- **Churning:** 181-365 days (likely to churn without intervention)
- **Churned:** 365+ days (assumed lost)

**Important:** These thresholds are business-specific. A grocery store might use 30/60/90 days, while a furniture store might use 180/365/730 days. Calibrate to your product's natural purchase cycle.

### Churn Rate Calculation

```
Churn Rate = (Number of Churned Customers / Total Customers) * 100
```

**Healthy Churn Rates:**
- E-commerce: 20-30% annual churn is typical
- SaaS (B2C): 5-7% monthly churn
- SaaS (B2B): 2-3% monthly churn

### Business Actions by Lifecycle Stage

**Active Customers (0-90 days):**
- Focus: Retention and expansion
- Actions: Loyalty programs, upselling, referral requests
- Priority: Medium (they're engaged, but need nurturing)

**At Risk Customers (91-180 days):**
- Focus: Re-engagement
- Actions: Email campaigns, personalized offers, surveys
- Priority: High (most cost-effective segment to save)

**Churning Customers (181-365 days):**
- Focus: Win-back
- Actions: Aggressive discounts, urgent outreach
- Priority: Medium (expensive to recover, low success rate)

**Churned Customers (365+ days):**
- Focus: Minimal investment
- Actions: Occasional win-back attempts, analyze exit reasons
- Priority: Low (very expensive to recover, focus on prevention)

### SQL Implementation Notes

The churn analysis uses `CASE` statements to bucket customers into lifecycle stages based on `days_since_last_order`. The key calculation:

```sql
DATEDIFF(DAY, MAX(d.date), CURRENT_DATE()) AS days_since_last_order
```

This gives us recency, which is the foundation of churn detection in transactional businesses.

---

## Seasonality and Peak Period Analysis

### What is Seasonality?

Seasonality refers to predictable patterns that repeat at regular intervals (monthly, weekly, daily). Understanding seasonality is critical for:

- Inventory planning
- Marketing budget allocation
- Cash flow forecasting
- Staffing decisions

### Monthly Seasonality

Retail businesses typically see:
- **Peak months:** November, December (holiday shopping)
- **Low months:** January, February (post-holiday slump)

Our analysis reveals what percentage of annual revenue each month contributes, allowing for better year-round planning.

### Day of Week Patterns

Different businesses have different weekly patterns:
- **B2C e-commerce:** Often peaks Thursday-Sunday
- **B2B:** Typically peaks Tuesday-Thursday
- **Grocery:** Weekend spikes

Understanding your business's pattern helps optimize email campaigns (send on high-traffic days) and promotional timing.

### Pareto Analysis (80/20 Rule)

The Pareto principle suggests that ~80% of revenue comes from ~20% of days. Our peak shopping times analysis tests this hypothesis by calculating:

1. Daily revenue across all days
2. Cumulative revenue as we go down the ranked list
3. At what percentage of days do we hit 80% of total revenue?

**High Concentration (e.g., 15% of days = 80% of revenue):**
- Indicates reliance on promotional events or Black Friday-style spikes
- High revenue volatility
- Need strategies to drive baseline traffic

**Low Concentration (e.g., 60% of days = 80% of revenue):**
- More stable, predictable revenue
- Less promotional dependency
- Indicates strong baseline demand

### SQL Implementation Notes

The Pareto analysis uses window functions to calculate cumulative totals:

```sql
SUM(revenue) OVER (ORDER BY revenue DESC) AS cumulative_revenue
```

This creates a running total, allowing us to see how revenue accumulates as we add more days.

---

## Business Applications

### 1. Financial Forecasting

**Using Time-Series Trends:**
- Extrapolate MoM growth rates to project future revenue
- Apply moving averages to smooth out noise in forecasts
- Use YoY growth to set annual targets

**Using Seasonality:**
- Adjust forecasts for known seasonal peaks and troughs
- Compare current month to same month historically (YoY)

### 2. Product-Market Fit Validation

**Using Cohort Retention:**
- High retention (>20% month 3) indicates strong product-market fit
- Improving retention across cohorts suggests product improvements are working
- Plateau in retention curve indicates loyal customer base exists

### 3. Customer Success Prioritization

**Using Churn Analysis:**
- Focus resources on "At Risk" segment (highest ROI for retention efforts)
- Automate outreach to churning customers
- Analyze churned customers to identify early warning signs

### 4. Marketing Optimization

**Using Cohort Analysis:**
- Compare retention by acquisition channel (Google Ads cohort vs Facebook cohort)
- Identify which channels bring highest-quality customers
- Calculate LTV by cohort to inform customer acquisition cost (CAC) limits

**Using Seasonality:**
- Time email campaigns for high-traffic days
- Plan promotional calendar around historical peaks
- Allocate ad spend proportionally to seasonal demand

### 5. Inventory and Supply Chain

**Using Seasonality:**
- Stock up before expected peak months (October for November/December)
- Reduce inventory during low seasons to minimize holding costs

**Using Peak Day Analysis:**
- Prepare warehouse capacity for known high-volume days
- Staff customer support for expected spikes

---

## SQL Techniques Used

### 1. Window Functions

**LAG() - Access Previous Row:**
```sql
LAG(revenue) OVER (ORDER BY year, month)  -- Previous month
LAG(revenue, 12) OVER (ORDER BY year, month)  -- Same month last year
```

**Moving Averages:**
```sql
AVG(revenue) OVER (
  ORDER BY year, month
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
)  -- 3-month moving average
```

**Cumulative Totals:**
```sql
SUM(revenue) OVER (ORDER BY revenue DESC)  -- Running total
```

### 2. Date Functions

**DATE_TRUNC() - Round to Period:**
```sql
DATE_TRUNC('MONTH', date)  -- Round to first day of month
```

**DATEDIFF() - Calculate Time Difference:**
```sql
DATEDIFF(MONTH, cohort_month, order_month)  -- Months between dates
DATEDIFF(DAY, last_order_date, CURRENT_DATE())  -- Days since last order
```

### 3. Common Table Expressions (CTEs)

CTEs make complex queries readable by breaking them into logical steps:

```sql
WITH step1 AS (...),
     step2 AS (...),
     step3 AS (...)
SELECT * FROM step3;
```

### 4. CASE Statements for Bucketing

Categorizing continuous variables into discrete segments:

```sql
CASE
  WHEN days_since_last_order <= 90 THEN 'Active'
  WHEN days_since_last_order <= 180 THEN 'At Risk'
  ELSE 'Churned'
END AS customer_status
```

---

## Conclusion

Time-series and cohort analytics transform raw transactional data into actionable business insights. By tracking trends, retention, and customer lifecycle patterns, businesses can:

- Make data-driven forecasts
- Validate product-market fit
- Prioritize customer success efforts
- Optimize marketing spend
- Plan inventory and resources

The SQL scripts in this project demonstrate production-grade analytical patterns used by data teams at high-growth companies. These techniques form the foundation of business intelligence and are essential skills for data analysts, analytics engineers, and data scientists working in commercial contexts.

---

## Further Reading

- **Cohort Analysis:** "Lean Analytics" by Alistair Croll and Benjamin Yoskovitz
- **Time-Series:** "Forecasting: Principles and Practice" by Rob Hyndman
- **Churn Prediction:** "Fighting Churn with Data" by Carl Gold
- **SQL Window Functions:** PostgreSQL/Snowflake official documentation

## Related Documentation

- [Customer and Product Analytics Guide](./customer-product-analytics.md) (Step 11)
- [Dimensional Modeling Guide](../architecture/dimensional-model.md) (Step 8)
- [Data Quality and Validation](../data-loading.md) (Step 6)
