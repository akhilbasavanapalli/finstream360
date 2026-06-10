-- =============================================================================
-- FinStream360 · Snowflake DDL
-- =============================================================================
-- Creates the full schema hierarchy consumed by Power BI and ad-hoc analytics.
-- Data flows: Databricks Gold (Delta) → Snowflake External Stage → COPY INTO
--
-- Author : Akhil Basavanapalli
-- Tech   : Snowflake, SQL, T-SQL
-- =============================================================================

USE ROLE     SYSADMIN;
USE WAREHOUSE FINSTREAM360_WH;

-- ─────────────────────────────────────────────────────────────────────────────
-- 0 · Warehouse & Database
-- ─────────────────────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS FINSTREAM360_WH
    WAREHOUSE_SIZE  = 'MEDIUM'
    AUTO_SUSPEND    = 120
    AUTO_RESUME     = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'FinStream360 analytics workload warehouse';

CREATE DATABASE IF NOT EXISTS FINSTREAM360;

CREATE SCHEMA IF NOT EXISTS FINSTREAM360.BRONZE;
CREATE SCHEMA IF NOT EXISTS FINSTREAM360.SILVER;
CREATE SCHEMA IF NOT EXISTS FINSTREAM360.GOLD;
CREATE SCHEMA IF NOT EXISTS FINSTREAM360.STAGING;
CREATE SCHEMA IF NOT EXISTS FINSTREAM360.AUDIT;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1 · Staging tables (landing from ADLS via Snowpipe / COPY INTO)
-- ─────────────────────────────────────────────────────────────────────────────
USE SCHEMA FINSTREAM360.STAGING;

CREATE OR REPLACE TABLE STG_TRANSACTIONS (
    transaction_id      VARCHAR(36)     NOT NULL,
    customer_id         VARCHAR(36)     NOT NULL,
    card_type           VARCHAR(20),
    card_last4          VARCHAR(4),
    merchant_name       VARCHAR(255),
    merchant_category   VARCHAR(50),
    merchant_state      VARCHAR(2),
    amount_usd          NUMBER(15, 2),
    currency            VARCHAR(3)      DEFAULT 'USD',
    transaction_ts      TIMESTAMP_NTZ,
    is_fraud            BOOLEAN,
    fraud_reason        VARCHAR(100),
    card_present        BOOLEAN,
    response_code       VARCHAR(5),
    event_created_at    TIMESTAMP_NTZ,
    -- Audit cols
    _loaded_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    _row_num            NUMBER
) COMMENT = 'Raw transaction records loaded from ADLS Gold layer';

CREATE OR REPLACE TABLE STG_CUSTOMERS (
    customer_id         VARCHAR(36)     NOT NULL,
    full_name           VARCHAR(200),
    email               VARCHAR(200),
    phone               VARCHAR(50),
    home_state          VARCHAR(2),
    credit_score        NUMBER(5),
    card_type           VARCHAR(20),
    card_last4          VARCHAR(4),
    credit_limit_usd    NUMBER(12, 2),
    account_open_date   DATE,
    is_active           BOOLEAN,
    account_age_days    NUMBER(6),
    _loaded_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)
) COMMENT = 'Customer reference data';

-- ─────────────────────────────────────────────────────────────────────────────
-- 2 · Gold Reporting Tables
-- ─────────────────────────────────────────────────────────────────────────────
USE SCHEMA FINSTREAM360.GOLD;

CREATE OR REPLACE TABLE DAILY_TXN_SUMMARY (
    summary_id          NUMBER AUTOINCREMENT PRIMARY KEY,
    txn_date            DATE            NOT NULL,
    txn_year            NUMBER(4),
    txn_month           NUMBER(2),
    merchant_category   VARCHAR(50),
    card_type           VARCHAR(20),
    total_transactions  NUMBER,
    total_amount_usd    NUMBER(18, 2),
    avg_amount_usd      NUMBER(12, 4),
    max_amount_usd      NUMBER(12, 2),
    min_amount_usd      NUMBER(12, 2),
    unique_customers    NUMBER,
    fraud_count         NUMBER,
    fraud_amount_usd    NUMBER(18, 2),
    fraud_rate_pct      NUMBER(6, 4),
    weekend_txn_count   NUMBER,
    late_night_txn_count NUMBER,
    gold_created_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (txn_date, merchant_category)
COMMENT = 'Daily transaction summary by merchant category and card type';

CREATE OR REPLACE TABLE CUSTOMER_360 (
    customer_id             VARCHAR(36)     NOT NULL PRIMARY KEY,
    full_name               VARCHAR(200),
    home_state              VARCHAR(2),
    credit_score            NUMBER(5),
    credit_limit_usd        NUMBER(12, 2),
    card_type               VARCHAR(20),
    account_open_date       DATE,
    account_age_days        NUMBER(6),
    total_transactions      NUMBER,
    lifetime_spend_usd      NUMBER(18, 2),
    avg_txn_usd             NUMBER(12, 4),
    max_single_txn_usd      NUMBER(12, 2),
    unique_categories       NUMBER,
    fraud_flag_count        NUMBER,
    last_txn_ts             TIMESTAMP_NTZ,
    first_txn_ts            TIMESTAMP_NTZ,
    days_active             NUMBER,
    avg_daily_spend         NUMBER(12, 4),
    cross_state_txn_count   NUMBER,
    avg_amount_to_limit_ratio NUMBER(8, 6),
    risk_tier               VARCHAR(10),
    gold_created_at         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = '360-degree customer view with lifetime value and risk profile';

CREATE OR REPLACE TABLE FRAUD_ALERTS_HOURLY (
    alert_id            NUMBER AUTOINCREMENT PRIMARY KEY,
    alert_ts            TIMESTAMP_NTZ   NOT NULL,
    txn_year            NUMBER(4),
    txn_month           NUMBER(2),
    txn_day             NUMBER(2),
    txn_hour            NUMBER(2),
    merchant_category   VARCHAR(50),
    merchant_state      VARCHAR(2),
    fraud_count         NUMBER,
    fraud_amount_usd    NUMBER(18, 2),
    affected_customers  NUMBER,
    avg_fraud_amount    NUMBER(12, 4),
    severity            VARCHAR(10),
    gold_created_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (alert_ts, severity)
COMMENT = 'Hourly fraud alert aggregations for the risk operations team';

CREATE OR REPLACE TABLE STATE_PERFORMANCE (
    perf_id             NUMBER AUTOINCREMENT PRIMARY KEY,
    txn_year            NUMBER(4),
    txn_month           NUMBER(2),
    txn_month_label     VARCHAR(7),
    merchant_state      VARCHAR(2),
    total_transactions  NUMBER,
    total_volume_usd    NUMBER(18, 2),
    avg_txn_usd         NUMBER(12, 4),
    unique_customers    NUMBER,
    fraud_count         NUMBER,
    fraud_rate_pct      NUMBER(6, 4),
    gold_created_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Monthly state-level performance for geographic analysis';

CREATE OR REPLACE TABLE FRAUD_PREDICTIONS (
    transaction_id      VARCHAR(36)     NOT NULL PRIMARY KEY,
    label               NUMBER(1),
    prediction          NUMBER(1),
    fraud_probability   NUMBER(8, 6),
    fraud_score_band    VARCHAR(10),
    scored_at           TIMESTAMP_NTZ
)
COMMENT = 'MLlib GBT fraud detection model scores';

-- ─────────────────────────────────────────────────────────────────────────────
-- 3 · Audit / Data Quality table
-- ─────────────────────────────────────────────────────────────────────────────
USE SCHEMA FINSTREAM360.AUDIT;

CREATE OR REPLACE TABLE PIPELINE_RUN_LOG (
    run_id              NUMBER AUTOINCREMENT PRIMARY KEY,
    pipeline_name       VARCHAR(100),
    layer               VARCHAR(20),
    run_start_ts        TIMESTAMP_NTZ,
    run_end_ts          TIMESTAMP_NTZ,
    rows_processed      NUMBER,
    rows_failed         NUMBER,
    dq_pass_rate_pct    NUMBER(6, 4),
    status              VARCHAR(20),
    error_message       VARCHAR(2000),
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);
