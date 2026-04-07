-- ============================================================
-- Global Partners - Athena Table Setup
-- Run each statement in AWS Athena Query Editor
-- ============================================================

-- 1. Create Athena Database
CREATE DATABASE IF NOT EXISTS global_partners_gold;

-- ============================================================
-- 2. fact_daily_clv
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS global_partners_gold.fact_daily_clv (
    user_id          STRING,
    total_clv        DOUBLE,
    total_orders     BIGINT,
    first_order_date DATE,
    last_order_date  DATE,
    avg_order_value  DOUBLE,
    is_loyalty       BOOLEAN,
    clv_tier         STRING,
    snapshot_date    STRING
)
STORED AS PARQUET
LOCATION 's3://global-partners-datalake/gold/fact_daily_clv/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- ============================================================
-- 3. fact_rfm_segments
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS global_partners_gold.fact_rfm_segments (
    user_id       STRING,
    recency_days  BIGINT,
    frequency     BIGINT,
    monetary      DOUBLE,
    r_score       INT,
    f_score       INT,
    m_score       INT,
    rfm_score     INT,
    segment       STRING,
    snapshot_date STRING
)
STORED AS PARQUET
LOCATION 's3://global-partners-datalake/gold/fact_rfm_segments/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- ============================================================
-- 4. fact_churn_indicators
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS global_partners_gold.fact_churn_indicators (
    user_id               STRING,
    days_since_last_order BIGINT,
    total_orders          BIGINT,
    first_order_date      DATE,
    last_order_date       DATE,
    total_spend           DOUBLE,
    tenure_days           INT,
    avg_order_gap_days    DOUBLE,
    spend_last_30d        DOUBLE,
    spend_prior_30d       DOUBLE,
    spend_pct_change      DOUBLE,
    churn_risk_tag        STRING,
    snapshot_date         STRING
)
STORED AS PARQUET
LOCATION 's3://global-partners-datalake/gold/fact_churn_indicators/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- ============================================================
-- 5. fact_sales_trends
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS global_partners_gold.fact_sales_trends (
    order_date        DATE,
    restaurant_id     STRING,
    item_category     STRING,
    daily_revenue     DOUBLE,
    daily_orders      BIGINT,
    unique_customers  BIGINT,
    avg_order_value   DOUBLE,
    day_of_week       STRING,
    week              INT,
    month             STRING,
    year              INT,
    is_weekend        BOOLEAN,
    is_holiday        BOOLEAN,
    holiday_name      STRING
)
STORED AS PARQUET
LOCATION 's3://global-partners-datalake/gold/fact_sales_trends/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- ============================================================
-- 6. fact_loyalty_comparison
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS global_partners_gold.fact_loyalty_comparison (
    is_loyalty      BOOLEAN,
    customer_count  BIGINT,
    avg_clv         DOUBLE,
    avg_orders      DOUBLE,
    avg_order_value DOUBLE,
    total_revenue   DOUBLE,
    snapshot_date   STRING
)
STORED AS PARQUET
LOCATION 's3://global-partners-datalake/gold/fact_loyalty_comparison/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- ============================================================
-- 7. fact_location_performance
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS global_partners_gold.fact_location_performance (
    restaurant_id    STRING,
    total_revenue    DOUBLE,
    avg_order_value  DOUBLE,
    total_orders     BIGINT,
    unique_customers BIGINT,
    active_days      BIGINT,
    orders_per_day   DOUBLE,
    revenue_rank     INT,
    performance_tier STRING,
    snapshot_date    STRING
)
STORED AS PARQUET
LOCATION 's3://global-partners-datalake/gold/fact_location_performance/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- ============================================================
-- 8. fact_discount_effectiveness
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS global_partners_gold.fact_discount_effectiveness (
    has_discount          BOOLEAN,
    order_date            DATE,
    order_count           BIGINT,
    gross_revenue         DOUBLE,
    total_discount_amount DOUBLE,
    net_revenue           DOUBLE,
    avg_order_value       DOUBLE,
    unique_customers      BIGINT,
    discount_rate_pct     DOUBLE,
    snapshot_date         STRING
)
STORED AS PARQUET
LOCATION 's3://global-partners-datalake/gold/fact_discount_effectiveness/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- ============================================================
-- 9. dim_customer
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS global_partners_gold.dim_customer (
    user_id               STRING,
    total_clv             DOUBLE,
    total_orders          BIGINT,
    first_order_date      DATE,
    last_order_date       DATE,
    avg_order_value       DOUBLE,
    is_loyalty            BOOLEAN,
    clv_tier              STRING,
    snapshot_date         STRING,
    recency_days          BIGINT,
    frequency             BIGINT,
    monetary              DOUBLE,
    rfm_score             INT,
    segment               STRING,
    days_since_last_order BIGINT,
    churn_risk_tag        STRING
)
STORED AS PARQUET
LOCATION 's3://global-partners-datalake/gold/dim_customer/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- ============================================================
-- 10. Verify all tables created
-- ============================================================
SHOW TABLES IN global_partners_gold;
