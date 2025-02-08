# Customer and Product Analytics Documentation

## Overview

This directory contains advanced SQL analytics queries that demonstrate business intelligence capabilities for the e-commerce data warehouse. These queries leverage dimensional modeling, window functions, statistical analysis, and complex joins to extract actionable insights about customer behavior and product performance.

The analytics layer sits on top of the production dimensional model (snowflake schema) and answers strategic business questions that inform marketing, merchandising, and customer service strategies.

---

## Analytics Queries

### 1. Customer Lifetime Value (CLV) Analysis

**File**: `sql/analytics/01_customer_lifetime_value.sql`

#### Business Question
Who are our most valuable customers, and what are their purchasing patterns over their entire relationship with the company?

#### What It Does
Calculates comprehensive customer lifetime value metrics including:
- Total revenue generated per customer
- Order frequency and average order value
- Customer lifespan (days between first and last purchase)
- Daily revenue contribution rate
- Revenue-based customer ranking

#### SQL Techniques Used
- **Common Table Expressions (CTEs)**: Organizes query into logical steps for readability
- **Window Functions**: `RANK()` to rank customers by lifetime revenue
- **Date Calculations**: `DATEDIFF()` to calculate customer lifespan in days
- **Aggregate Functions**: `COUNT()`, `SUM()`, `AVG()` for customer metrics
- **Safe Division**: `NULLIF()` to prevent division by zero for single-purchase customers
- **Multi-table Joins**: Combines fact and dimension tables for complete customer view

#### How to Interpret Results
- **High `lifetime_revenue` with high `total_orders`**: Loyal, valuable customers worthy of VIP treatment
- **High `revenue_per_day`**: Customers with concentrated purchasing patterns (short lifespan, high spend)
- **Long `customer_lifespan_days`**: Customers with sustained engagement over time
- **Top `revenue_rank`**: Your most important customers for retention efforts

#### Business Actions
- **Top 10%**: Assign dedicated account managers, offer personalized service, VIP programs
- **High revenue, declining frequency**: Win-back campaigns to re-engage
- **High AOV, low frequency**: Upsell complementary products to increase order frequency
- **Long lifespan**: Study these customers to understand what drives loyalty

---

### 2. RFM Analysis (Recency, Frequency, Monetary)

**File**: `sql/analytics/02_rfm_analysis.sql`

#### Business Question
How should we segment our customers based on their purchasing behavior to optimize marketing spend and customer engagement?

#### What It Does
Implements the classic RFM marketing framework:
- **Recency**: Days since last purchase (how recently active)
- **Frequency**: Number of orders (how often they buy)
- **Monetary**: Total lifetime spend (how much they spend)

Each customer receives a score of 1-5 for each dimension, then classified into segments:
- **Champions**: Best customers (recent, frequent, high spend)
- **Loyal Customers**: Regular buyers with good engagement
- **Potential Loyalists**: Recent customers with growth potential
- **At Risk**: Previously engaged but declining activity
- **Hibernating**: Inactive customers needing reactivation
- **Need Attention**: Mixed signals requiring personalized approach

#### SQL Techniques Used
- **Multi-level CTEs**: Three-step analysis (calculate metrics → score → segment)
- **Window Functions**: `NTILE(5)` to divide customers into quintiles
- **Scoring Logic**: Inverted scoring for recency (lower days = better = higher score)
- **CASE Expressions**: Complex segmentation logic based on score combinations
- **Date Calculations**: `CURRENT_DATE()` and `DATEDIFF()` for recency metrics

#### How to Interpret Results
- **High `rfm_total_score` (12-15)**: Your best customers - focus on retention
- **Low `recency_score` (<3)**: Haven't purchased recently - re-engagement needed
- **High `monetary_score`, low `frequency_score`**: Infrequent big spenders - increase touchpoints
- **Segment = "At Risk"**: Previously good customers declining - urgent intervention

#### Business Actions by Segment

| Segment | Characteristics | Marketing Strategy |
|---------|----------------|-------------------|
| **Champions** | R=5, F=5, M=5 | VIP programs, early access, referral incentives, personalized service |
| **Loyal Customers** | R=4-5, F=3-5, M=3-5 | Cross-sell campaigns, bundle offers, loyalty rewards |
| **Potential Loyalists** | R=4-5, F=2-3, M=2-3 | Onboarding programs, product education, loyalty incentives |
| **At Risk** | R=1-2, F=1-2, M=3-5 | Win-back campaigns, special discounts, feedback surveys |
| **Hibernating** | R=1-2, F=1-3, M=1-3 | Reactivation emails, aggressive promotions, "We miss you" campaigns |
| **Need Attention** | Mixed scores | A/B testing, personalized engagement, preference surveys |

---

### 3. Product Affinity Analysis (Market Basket)

**File**: `sql/analytics/03_product_affinity.sql`

#### Business Question
Which products are frequently purchased together, and how can we leverage these relationships for cross-selling and merchandising?

#### What It Does
Identifies product pairs that appear together in the same order:
- Counts how many times each pair was co-purchased
- Calculates what percentage of all orders include the pair
- Filters for statistically significant relationships (10+ co-purchases)
- Returns top 50 product combinations

#### SQL Techniques Used
- **Self-Join**: Joins the same table to itself to find product pairs within orders
- **Inequality Join**: `p1.product_key < p2.product_key` prevents duplicates and self-pairs
- **CTEs**: Organizes distinct products per order
- **Subquery in SELECT**: Calculates total order count for percentage
- **HAVING Clause**: Filters for meaningful relationships (COUNT >= 10)
- **Aggregate Functions**: `COUNT()` to measure co-purchase frequency

#### How to Interpret Results
- **High `times_purchased_together`**: Strong product relationship (complementary goods)
- **High `co_purchase_percentage`**: Common pairing across customer base
- **Similar product categories**: Natural bundling opportunity (e.g., matching decor items)
- **Different categories**: Cross-category appeal (e.g., gift sets)

#### Business Actions
- **Bundling Strategy**: Create product bundles with discount for top pairs
- **Website Recommendations**: "Customers who bought X also bought Y" recommendations
- **Physical Store Layout**: Place complementary products near each other
- **Inventory Management**: Order correlated products together to maintain stock balance
- **Promotional Campaigns**: Design themed promotions around popular combinations
- **Personalized Marketing**: Email customers who bought A suggesting they try B

#### Example Interpretation
If "White Hanging Heart T-Light Holder" and "Cream Cupid Hearts Coat Hanger" appear together in 50 orders (1.2% of orders), this indicates a strong home decor theme. Action: Create a "Romantic Home" bundle with 10% discount.

---

### 4. Product Performance Analysis

**File**: `sql/analytics/04_product_performance.sql`

#### Business Question
How are our top products performing over time, and what are the underlying trends beyond monthly volatility?

#### What It Does
Analyzes monthly sales performance for the top 5 products by revenue:
- Units sold and revenue per month
- Product ranking within each month (top seller = rank 1)
- Cumulative revenue (running total over time)
- 3-month moving average to smooth volatility

#### SQL Techniques Used
- **Window Functions with PARTITION BY**:
  - `RANK()` partitioned by month to show monthly rankings
  - `SUM()` partitioned by product for running totals
  - `AVG()` partitioned by product for moving averages
- **Frame Clauses**:
  - `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` for cumulative sum
  - `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` for 3-month average
- **Subquery in WHERE**: Dynamic filtering for top 5 products
- **Multi-table Joins**: Combines fact table with date and product dimensions

#### How to Interpret Results

**Monthly Rank**:
- Consistent `monthly_rank = 1`: Stable top performer
- Volatile ranking: Seasonal or promotional sensitivity
- Declining rank over time: Product losing competitive position

**Cumulative Revenue**:
- Steep slope: Accelerating growth (product gaining traction)
- Linear slope: Steady, predictable performance
- Flattening slope: Product reaching maturity or declining

**Moving Average vs. Actual Revenue**:
- Actual > Moving Avg: Strong current performance (above trend)
- Actual < Moving Avg: Weak current performance (below trend)
- Stable moving average: Predictable demand (good for inventory planning)

#### Business Actions
- **Accelerating Growth**: Increase inventory, expand marketing budget, consider product line extensions
- **Declining Trend**: Investigate causes (competition, quality issues, market saturation), consider refresh or discontinuation
- **Seasonal Patterns**: Plan inventory levels based on moving average predictions, schedule promotions during slow periods
- **Consistent Top Rankers**: Protect these products (quality control, prevent stockouts, maintain pricing)

---

### 5. Customer Segmentation Analysis

**File**: `sql/analytics/05_customer_segmentation.sql`

#### Business Question
How should we divide our customer base into value tiers, and what are the characteristics of each segment?

#### What It Does
Uses statistical methods (percentiles) to segment customers into tiers:
- **High Value**: Top 10% of customers by lifetime spend (90th percentile+)
- **Medium Value**: 60th-90th percentile (upper-middle tier)
- **Low Value**: Bottom 60% (majority of customer base)

Then aggregates segment characteristics by country:
- Customer count per segment
- Average lifetime spend
- Average order frequency
- Average order value
- Average product diversity

#### SQL Techniques Used
- **PERCENTILE_CONT**: Calculates continuous percentiles for dynamic thresholds
- **WITHIN GROUP**: Required clause for ordered-set aggregate functions
- **Subqueries in CASE**: Dynamically determines segment thresholds
- **Multi-level Aggregation**: Customer-level metrics aggregated to segment level
- **Custom ORDER BY**: Uses CASE expression to order segments logically (High → Medium → Low)

#### How to Interpret Results

**Segment Distribution** (typical pattern):
- High Value: ~10% of customers, 40-50% of revenue (Pareto principle)
- Medium Value: ~30% of customers, 30-35% of revenue
- Low Value: ~60% of customers, 15-30% of revenue

**Key Metrics**:
- High `avg_total_spend` with high `avg_orders_per_customer`: Loyal, frequent buyers
- High `avg_order_value`: Large basket size per transaction
- High `avg_unique_products`: Product explorers with diverse interests
- Geographic concentration: Which countries have highest-value customers

#### Business Actions by Segment

**High Value Customers (Top 10%)**:
- Dedicated account managers
- Priority customer service (dedicated phone line, faster response)
- Exclusive previews and early access to new products
- Personalized communication (not mass marketing)
- VIP events and experiences
- Premium loyalty benefits

**Medium Value Customers (30%)**:
- Upselling campaigns (increase avg_order_value)
- Cross-selling different product categories (increase diversity)
- Loyalty program incentives to move to High tier
- Bundle offers and volume discounts
- Regular engagement campaigns

**Low Value Customers (60%)**:
- Automated marketing (email drip campaigns)
- Self-service support (chatbots, knowledge base)
- First-purchase discounts (acquire new customers cheaply)
- Promotions to increase order frequency
- Cost-efficient acquisition channels

#### Example Interpretation
If UK has 150 High Value customers with `avg_total_spend = $18,000` and `avg_orders_per_customer = 52`, but Germany has only 20 High Value customers with `avg_total_spend = $12,000`, this suggests:
1. UK is the core market - protect these customers
2. Germany has growth potential - invest in customer development

---

## Glossary of Terms

### Analytics Concepts

**CLV (Customer Lifetime Value)**: The total revenue a customer generates over their entire relationship with the company. Critical metric for determining customer acquisition costs and marketing ROI.

**RFM Analysis**: Marketing framework that segments customers based on:
- **Recency**: How recently they purchased (recent = more engaged)
- **Frequency**: How often they purchase (frequent = more loyal)
- **Monetary**: How much they spend (high spend = more valuable)

**Market Basket Analysis**: Technique to identify products frequently purchased together (product affinity). Also called association rule mining or affinity analysis.

**Churn Risk**: Likelihood that a customer will stop purchasing. Indicated by low recency scores and declining frequency.

**Product Affinity**: Strength of relationship between two products based on co-purchase frequency. Used for recommendations and bundling.

**Customer Segmentation**: Dividing customers into groups with similar characteristics for targeted marketing strategies.

**Moving Average**: Average calculated over a sliding window of time periods to smooth out short-term fluctuations and identify trends.

**Percentile**: Statistical measure indicating the value below which a percentage of observations fall. 90th percentile = top 10%.

### SQL Concepts

**CTE (Common Table Expression)**: Temporary named result set defined using WITH clause. Improves query readability and organization.

**Window Function**: Function that performs calculations across rows related to current row (within a "window"). Examples: RANK(), SUM(), AVG() with OVER clause.

**PARTITION BY**: Divides result set into partitions (groups) for window function calculations. Like GROUP BY but doesn't collapse rows.

**Frame Clause**: Defines which rows are included in window function calculation (e.g., ROWS BETWEEN 2 PRECEDING AND CURRENT ROW).

**NTILE(n)**: Window function that divides rows into n approximately equal groups (buckets). NTILE(5) creates quintiles.

**RANK()**: Window function that assigns rank to rows within partition based on ORDER BY. Tied values get same rank.

**Self-Join**: Joining a table to itself to compare rows within the same table. Used for finding relationships (like product pairs).

**PERCENTILE_CONT**: Calculates continuous percentile using linear interpolation. Returns exact value at specified percentile.

**Subquery**: Query nested inside another query. Can be in SELECT, FROM, WHERE clauses.

---

## How to Run the Queries

### Prerequisites
1. Complete Steps 1-10 of the implementation plan
2. Ensure production dimensional model is loaded with data
3. Have Snowflake account with appropriate permissions

### Execution Steps

#### Option 1: Snowflake Web UI (Recommended for Portfolio Demo)
1. Log in to Snowflake web interface
2. Navigate to Worksheets
3. Open one of the analytics SQL files
4. Copy entire script into worksheet
5. Ensure correct context:
   ```sql
   USE ROLE ACCOUNTADMIN;
   USE WAREHOUSE COMPUTE_WH;
   USE DATABASE ECOMMERCE_DW;
   USE SCHEMA PRODUCTION;
   ```
6. Click "Run" button (or Ctrl/Cmd + Enter)
7. View results in Results pane
8. Export results to CSV/Excel if needed

#### Option 2: SnowSQL Command Line
```bash
# Connect to Snowflake
snowsql -a <account> -u <username> -d ECOMMERCE_DW -s PRODUCTION

# Run query
!source sql/analytics/01_customer_lifetime_value.sql

# Export results
!set output_file=/path/to/output.csv
!set output_format=csv
!source sql/analytics/01_customer_lifetime_value.sql
```

#### Option 3: Python with Snowflake Connector
```python
import snowflake.connector

# Connect
conn = snowflake.connector.connect(
    user='<username>',
    password='<password>',
    account='<account>',
    warehouse='COMPUTE_WH',
    database='ECOMMERCE_DW',
    schema='PRODUCTION'
)

# Read SQL file
with open('sql/analytics/01_customer_lifetime_value.sql', 'r') as f:
    query = f.read()

# Execute
cursor = conn.cursor()
cursor.execute(query)

# Fetch results
results = cursor.fetchall()
column_names = [desc[0] for desc in cursor.description]

# Convert to DataFrame
import pandas as pd
df = pd.DataFrame(results, columns=column_names)
print(df)

# Close
cursor.close()
conn.close()
```

### Performance Considerations
- All queries include `LIMIT` clauses to prevent runaway costs
- Running all 5 queries should complete in under 5 minutes
- Snowflake warehouse size: XS or S is sufficient for this dataset
- Consider creating materialized views for frequently-run queries in production

### Customization Tips
- **Change LIMIT values**: Increase from 50/100 to see more results
- **Adjust time periods**: Modify date filters for specific analysis windows
- **Change percentile thresholds**: Use different cutoffs for segmentation (e.g., 0.95 for top 5%)
- **Add filters**: Filter by specific countries, product categories, or date ranges
- **Modify window frame sizes**: Change 3-month moving average to 6-month

---

## Business Value Summary

These analytics queries demonstrate:

1. **Strategic Customer Management**: Identify and prioritize high-value customers for retention
2. **Targeted Marketing**: Segment customers for personalized campaigns based on behavior
3. **Revenue Optimization**: Discover cross-selling and bundling opportunities
4. **Demand Forecasting**: Use trends and moving averages for inventory planning
5. **Data-Driven Decision Making**: Replace gut instinct with statistical evidence

**Portfolio Showcase**: This analytics layer demonstrates proficiency in:
- Advanced SQL (CTEs, window functions, self-joins, statistical functions)
- Business intelligence and marketing analytics concepts (CLV, RFM, market basket)
- Dimensional modeling applied to real-world analytics questions
- Clear documentation and communication of technical concepts to business stakeholders

---

## Next Steps

### For Further Analysis
1. **Cohort Analysis**: Track customer behavior by acquisition month/quarter
2. **Time Series Forecasting**: Predict future sales using historical trends
3. **Churn Prediction**: Build predictive model for customer attrition
4. **Price Elasticity**: Analyze impact of price changes on demand
5. **Geographic Analysis**: Heat maps of revenue by country/region
6. **Product Recommendation Engine**: Collaborative filtering based on purchase history

### For Production Deployment
1. **Materialized Views**: Create for frequently-run queries to improve performance
2. **Scheduled Refresh**: Use Snowflake tasks to refresh analytics daily/weekly
3. **BI Tool Integration**: Connect Tableau, Power BI, or Looker for visualization
4. **Alerting**: Set up notifications for key metric thresholds (e.g., churn spike)
5. **Data Quality Checks**: Add validation queries to ensure accurate analytics

---

## References

- **RFM Analysis**: Bult, J. R., & Wansbeek, T. (1995). Optimal selection for direct mail. *Marketing Science*, 14(4), 378-394.
- **Market Basket Analysis**: Agrawal, R., & Srikant, R. (1994). Fast algorithms for mining association rules. *Proc. VLDB*, 487-499.
- **Customer Lifetime Value**: Gupta, S., et al. (2006). Modeling customer lifetime value. *Journal of Service Research*, 9(2), 139-155.
- **Snowflake Window Functions**: https://docs.snowflake.com/en/sql-reference/functions-analytic.html
- **Statistical Functions**: https://docs.snowflake.com/en/sql-reference/functions-aggregation.html

---

*Last updated: 2025-11-11*
*Project: Cloud Data Warehouse - AWS S3 + Snowflake*
*Portfolio: Advanced SQL Analytics for E-commerce*
