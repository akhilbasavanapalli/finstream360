-- =============================================================================
-- FinStream360 · Snowflake Stored Procedures
-- =============================================================================
-- Encapsulates ETL merge logic and data quality checks as callable procedures.
-- Invoked by Snowflake Tasks on a scheduled basis.
--
-- Author : Akhil Basavanapalli
-- =============================================================================

USE DATABASE FINSTREAM360;
USE WAREHOUSE FINSTREAM360_WH;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1 · LOAD_DAILY_TXN_SUMMARY
--     Merges today's aggregated summary from staging into Gold.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE GOLD.LOAD_DAILY_TXN_SUMMARY(run_date DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_merged  NUMBER;
    run_start    TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
BEGIN
    MERGE INTO GOLD.DAILY_TXN_SUMMARY AS tgt
    USING (
        SELECT
            t.transaction_ts::DATE          AS txn_date,
            YEAR(t.transaction_ts)          AS txn_year,
            MONTH(t.transaction_ts)         AS txn_month,
            t.merchant_category,
            t.card_type,
            COUNT(*)                         AS total_transactions,
            SUM(t.amount_usd)               AS total_amount_usd,
            AVG(t.amount_usd)               AS avg_amount_usd,
            MAX(t.amount_usd)               AS max_amount_usd,
            MIN(t.amount_usd)               AS min_amount_usd,
            COUNT(DISTINCT t.customer_id)   AS unique_customers,
            SUM(IFF(t.is_fraud, 1, 0))      AS fraud_count,
            SUM(IFF(t.is_fraud, t.amount_usd, 0)) AS fraud_amount_usd,
            ROUND(SUM(IFF(t.is_fraud, 1, 0)) / COUNT(*) * 100, 4) AS fraud_rate_pct,
            SUM(IFF(DAYOFWEEK(t.transaction_ts) IN (1,7), 1, 0)) AS weekend_txn_count,
            SUM(IFF(HOUR(t.transaction_ts) BETWEEN 0 AND 5, 1, 0)) AS late_night_txn_count
        FROM STAGING.STG_TRANSACTIONS t
        WHERE t.transaction_ts::DATE = :run_date
          AND t.amount_usd > 0
        GROUP BY 1, 2, 3, 4, 5
    ) AS src
    ON  tgt.txn_date         = src.txn_date
    AND tgt.merchant_category = src.merchant_category
    AND tgt.card_type         = src.card_type
    WHEN MATCHED THEN UPDATE SET
        total_transactions   = src.total_transactions,
        total_amount_usd     = src.total_amount_usd,
        avg_amount_usd       = src.avg_amount_usd,
        max_amount_usd       = src.max_amount_usd,
        min_amount_usd       = src.min_amount_usd,
        unique_customers     = src.unique_customers,
        fraud_count          = src.fraud_count,
        fraud_amount_usd     = src.fraud_amount_usd,
        fraud_rate_pct       = src.fraud_rate_pct,
        weekend_txn_count    = src.weekend_txn_count,
        late_night_txn_count = src.late_night_txn_count,
        gold_created_at      = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        txn_date, txn_year, txn_month, merchant_category, card_type,
        total_transactions, total_amount_usd, avg_amount_usd, max_amount_usd,
        min_amount_usd, unique_customers, fraud_count, fraud_amount_usd,
        fraud_rate_pct, weekend_txn_count, late_night_txn_count
    ) VALUES (
        src.txn_date, src.txn_year, src.txn_month, src.merchant_category, src.card_type,
        src.total_transactions, src.total_amount_usd, src.avg_amount_usd, src.max_amount_usd,
        src.min_amount_usd, src.unique_customers, src.fraud_count, src.fraud_amount_usd,
        src.fraud_rate_pct, src.weekend_txn_count, src.late_night_txn_count
    );

    rows_merged := SQLROWCOUNT;

    INSERT INTO AUDIT.PIPELINE_RUN_LOG
        (pipeline_name, layer, run_start_ts, run_end_ts, rows_processed, rows_failed, status)
    VALUES
        ('LOAD_DAILY_TXN_SUMMARY', 'GOLD', :run_start, CURRENT_TIMESTAMP(), :rows_merged, 0, 'SUCCESS');

    RETURN 'Loaded ' || :rows_merged || ' rows for ' || :run_date;
END;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2 · LOAD_CUSTOMER_360
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE GOLD.LOAD_CUSTOMER_360()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_merged NUMBER;
    run_start   TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
BEGIN
    MERGE INTO GOLD.CUSTOMER_360 AS tgt
    USING (
        SELECT
            c.customer_id,
            c.full_name,
            c.home_state,
            c.credit_score,
            c.credit_limit_usd,
            c.card_type,
            c.account_open_date,
            DATEDIFF('day', c.account_open_date, CURRENT_DATE()) AS account_age_days,
            COUNT(t.transaction_id)                               AS total_transactions,
            SUM(t.amount_usd)                                     AS lifetime_spend_usd,
            AVG(t.amount_usd)                                     AS avg_txn_usd,
            MAX(t.amount_usd)                                     AS max_single_txn_usd,
            COUNT(DISTINCT t.merchant_category)                   AS unique_categories,
            SUM(IFF(t.is_fraud, 1, 0))                           AS fraud_flag_count,
            MAX(t.transaction_ts)                                 AS last_txn_ts,
            MIN(t.transaction_ts)                                 AS first_txn_ts,
            DATEDIFF('day', MIN(t.transaction_ts), MAX(t.transaction_ts)) AS days_active,
            SUM(IFF(t.merchant_state <> c.home_state, 1, 0))     AS cross_state_txn_count,
            AVG(COALESCE(t.amount_usd / NULLIF(c.credit_limit_usd, 0), 0)) AS avg_amount_to_limit_ratio,
            CASE
                WHEN SUM(IFF(t.is_fraud, 1, 0)) >= 3 THEN 'HIGH'
                WHEN SUM(IFF(t.is_fraud, 1, 0)) >= 1 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS risk_tier
        FROM STAGING.STG_CUSTOMERS c
        LEFT JOIN STAGING.STG_TRANSACTIONS t USING (customer_id)
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    ) AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN UPDATE SET
        full_name                 = src.full_name,
        home_state                = src.home_state,
        credit_score              = src.credit_score,
        credit_limit_usd          = src.credit_limit_usd,
        card_type                 = src.card_type,
        account_open_date         = src.account_open_date,
        account_age_days          = src.account_age_days,
        total_transactions        = src.total_transactions,
        lifetime_spend_usd        = src.lifetime_spend_usd,
        avg_txn_usd               = src.avg_txn_usd,
        max_single_txn_usd        = src.max_single_txn_usd,
        unique_categories         = src.unique_categories,
        fraud_flag_count          = src.fraud_flag_count,
        last_txn_ts               = src.last_txn_ts,
        first_txn_ts              = src.first_txn_ts,
        days_active               = src.days_active,
        cross_state_txn_count     = src.cross_state_txn_count,
        avg_amount_to_limit_ratio = src.avg_amount_to_limit_ratio,
        risk_tier                 = src.risk_tier,
        gold_created_at           = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT VALUES (
        src.customer_id, src.full_name, src.home_state, src.credit_score,
        src.credit_limit_usd, src.card_type, src.account_open_date, src.account_age_days,
        src.total_transactions, src.lifetime_spend_usd, src.avg_txn_usd,
        src.max_single_txn_usd, src.unique_categories, src.fraud_flag_count,
        src.last_txn_ts, src.first_txn_ts, src.days_active,
        ROUND(src.lifetime_spend_usd / NULLIF(src.days_active, 0), 4),
        src.cross_state_txn_count, src.avg_amount_to_limit_ratio, src.risk_tier,
        CURRENT_TIMESTAMP()
    );

    rows_merged := SQLROWCOUNT;
    RETURN 'Customer 360 loaded: ' || :rows_merged || ' rows';
END;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3 · DATA QUALITY CHECK PROCEDURE
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE AUDIT.RUN_DQ_CHECKS(check_date DATE)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    var results = {};
    var checks = [
        {
            name: "null_transaction_ids",
            query: `SELECT COUNT(*) AS cnt FROM STAGING.STG_TRANSACTIONS
                    WHERE transaction_id IS NULL`,
            threshold: 0
        },
        {
            name: "negative_amounts",
            query: `SELECT COUNT(*) AS cnt FROM STAGING.STG_TRANSACTIONS
                    WHERE amount_usd <= 0`,
            threshold: 0
        },
        {
            name: "future_transactions",
            query: `SELECT COUNT(*) AS cnt FROM STAGING.STG_TRANSACTIONS
                    WHERE transaction_ts > CURRENT_TIMESTAMP()`,
            threshold: 0
        },
        {
            name: "duplicate_transaction_ids",
            query: `SELECT COUNT(*) - COUNT(DISTINCT transaction_id) AS cnt
                    FROM STAGING.STG_TRANSACTIONS`,
            threshold: 0
        },
        {
            name: "fraud_rate_sanity",
            query: `SELECT ROUND(SUM(IFF(is_fraud,1,0))/COUNT(*)*100, 2) AS cnt
                    FROM STAGING.STG_TRANSACTIONS`,
            threshold: 10   // alert if fraud > 10%
        }
    ];

    var all_passed = true;
    checks.forEach(function(check) {
        var stmt   = snowflake.createStatement({sqlText: check.query});
        var result = stmt.execute();
        result.next();
        var val    = result.getColumnValue(1);
        var passed = (check.name === "fraud_rate_sanity") ? val <= check.threshold : val <= check.threshold;
        results[check.name] = { value: val, passed: passed };
        if (!passed) all_passed = false;
    });

    results["all_passed"] = all_passed;
    return results;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4 · Scheduled Tasks (replace Airflow for Snowflake-native scheduling)
-- ─────────────────────────────────────────────────────────────────────────────
-- Daily task: load yesterday's summary at 3 AM UTC
CREATE OR REPLACE TASK GOLD.TASK_DAILY_TXN_SUMMARY
    WAREHOUSE = FINSTREAM360_WH
    SCHEDULE  = 'USING CRON 0 3 * * * UTC'
AS
    CALL GOLD.LOAD_DAILY_TXN_SUMMARY(DATEADD('day', -1, CURRENT_DATE()));

-- Weekly task: refresh Customer 360 every Sunday
CREATE OR REPLACE TASK GOLD.TASK_CUSTOMER_360_WEEKLY
    WAREHOUSE = FINSTREAM360_WH
    SCHEDULE  = 'USING CRON 0 4 * * 0 UTC'
AS
    CALL GOLD.LOAD_CUSTOMER_360();

-- Enable tasks
ALTER TASK GOLD.TASK_DAILY_TXN_SUMMARY     RESUME;
ALTER TASK GOLD.TASK_CUSTOMER_360_WEEKLY   RESUME;
