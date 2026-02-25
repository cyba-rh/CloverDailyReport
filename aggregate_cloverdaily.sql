-- Aggregate CLOVERTRANS data into CLOVERDAILY table
USE Landing_Operational;
GO

-- Clear existing data (remove this line for incremental updates)
TRUNCATE TABLE dbo.CLOVERDAILY;
GO

WITH MerchantFirstDates AS (
    -- Step 1: Calculate first transaction dates per MID (across all time)
    SELECT 
        mid,
        MIN(CASE WHEN status = 'succeeded' AND amount > 0 THEN transaction_date END) AS first_transaction_date,
        MIN(CASE WHEN status = 'succeeded' AND amount > 1000 THEN transaction_date END) AS first_business_transaction_date
    FROM dbo.CLOVERTRANS
    WHERE transaction_date IS NOT NULL
        AND mid IS NOT NULL
    GROUP BY mid
),
DailyAggregates AS (
    -- Step 2: Aggregate daily totals by MID and merchant
    SELECT 
        CAST(transaction_date AS DATE) AS day,
        mid,
        merchant_name,
        SUM(CASE WHEN status = 'succeeded' THEN amount/100.0 ELSE 0 END) AS amount_sum,
        COUNT(CASE WHEN status = 'succeeded' AND amount > 0 THEN 1 END) AS amount_count,
        SUM(CASE WHEN status = 'succeeded' THEN refunded_amount/100.0 ELSE 0 END) AS refund_sum,
        COUNT(CASE WHEN status = 'succeeded' AND refunded_amount > 0 THEN 1 END) AS refund_count
    FROM dbo.CLOVERTRANS
    WHERE transaction_date IS NOT NULL
        AND mid IS NOT NULL 
        AND merchant_name IS NOT NULL
    GROUP BY 
        CAST(transaction_date AS DATE),
        mid,
        merchant_name
),
DailyTotals AS (
    -- Step 3: Calculate net total and join with first dates
    SELECT 
        da.day,
        da.mid,
        da.merchant_name,
        da.amount_sum,
        da.amount_count,
        da.refund_sum,
        da.refund_count,
        (da.amount_sum - da.refund_sum) AS total,
        mfd.first_transaction_date,
        mfd.first_business_transaction_date
    FROM DailyAggregates da
    LEFT JOIN MerchantFirstDates mfd ON da.mid = mfd.mid
),
RunningTotals AS (
    -- Step 4: Calculate MTD and YTD running totals
    SELECT 
        day,
        mid,
        merchant_name,
        amount_sum,
        amount_count,
        refund_sum,
        refund_count,
        total,
        first_transaction_date,
        first_business_transaction_date,
        -- MTD: Sum all totals for the same MID from start of current month to current day
        SUM(total) OVER (
            PARTITION BY mid, YEAR(day), MONTH(day) 
            ORDER BY day 
            ROWS UNBOUNDED PRECEDING
        ) AS mtd_total,
        -- YTD: Sum all totals for the same MID from start of current year to current day
        SUM(total) OVER (
            PARTITION BY mid, YEAR(day) 
            ORDER BY day 
            ROWS UNBOUNDED PRECEDING
        ) AS ytd_total
    FROM DailyTotals
)
-- Step 5: Insert aggregated data into CLOVERDAILY table
INSERT INTO dbo.CLOVERDAILY (
    day, 
    mid, 
    merchant_name, 
    amount_sum,
    amount_count,
    refund_sum,
    refund_count,
    total,
    first_transaction_date,
    first_business_transaction_date,
    mtd_total, 
    ytd_total
)
SELECT 
    day,
    mid,
    merchant_name,
    ROUND(amount_sum, 2) AS amount_sum,
    amount_count,
    ROUND(refund_sum, 2) AS refund_sum,
    refund_count,
    ROUND(total, 2) AS total,
    first_transaction_date,
    first_business_transaction_date,
    ROUND(mtd_total, 2) AS mtd_total,
    ROUND(ytd_total, 2) AS ytd_total
FROM RunningTotals
ORDER BY day, mid;

-- Display summary of what was processed
SELECT 
    MIN(day) AS earliest_date,
    MAX(day) AS latest_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT mid) AS unique_merchants,
    SUM(amount_sum) AS total_amount,
    SUM(amount_count) AS total_amount_transactions,
    SUM(refund_sum) AS total_refunds,
    SUM(refund_count) AS total_refund_transactions,
    SUM(total) AS net_total,
    MIN(first_transaction_date) AS earliest_transaction,
    MIN(first_business_transaction_date) AS earliest_business_transaction
FROM dbo.CLOVERDAILY;

PRINT 'CLOVERDAILY aggregation completed successfully';