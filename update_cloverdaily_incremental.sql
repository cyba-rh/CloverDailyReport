-- Incremental update script for CLOVERDAILY table
-- Run this daily to update only new or changed data
USE Landing_Operational;
GO

-- Get the last processed date (if any)
DECLARE @LastProcessedDate DATE;
SELECT @LastProcessedDate = ISNULL(MAX(day), '1900-01-01') FROM dbo.CLOVERDAILY;

PRINT 'Processing data from: ' + CAST(@LastProcessedDate AS VARCHAR(20));


-- Identify (day, mid) pairs to be updated
WITH ToUpdate AS (
    SELECT DISTINCT CAST(transaction_date AS DATE) AS day, mid
    FROM dbo.CLOVERTRANS
    WHERE transaction_date IS NOT NULL
        AND mid IS NOT NULL
        AND merchant_name IS NOT NULL
        AND CAST(transaction_date AS DATE) >= @LastProcessedDate
)
DELETE d
FROM dbo.CLOVERDAILY d
INNER JOIN ToUpdate t
    ON d.day = t.day AND d.mid = t.mid;

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
    -- Step 2: Aggregate daily totals by MID and merchant (only new/updated data)
    SELECT 
        CAST(transaction_date AS DATE) AS day,
        mid,
        merchant_name,
        MAX(SourceAccountCreated) AS SourceAccountCreated,
        SUM(CASE WHEN status = 'succeeded' AND transaction_type = 'charge' THEN amount/100.0 ELSE 0 END) AS amount_sum,
        COUNT(CASE WHEN status = 'succeeded' AND transaction_type = 'charge' AND amount > 0 THEN 1 END) AS amount_count,
        SUM(CASE WHEN status = 'succeeded' AND transaction_type = 'refund' THEN refunded_amount/100.0 ELSE 0 END) AS refund_sum,
        COUNT(CASE WHEN status = 'succeeded' AND transaction_type = 'refund' AND refunded_amount > 0 THEN 1 END) AS refund_count,
        SUM(CASE WHEN status = 'succeeded' AND transaction_type = 'void' THEN amount/100.0 ELSE 0 END) AS void_sum,
        COUNT(CASE WHEN status = 'succeeded' AND transaction_type = 'void' AND amount > 0 THEN 1 END) AS voidcount
    FROM dbo.CLOVERTRANS
    WHERE transaction_date IS NOT NULL
        AND mid IS NOT NULL 
        AND merchant_name IS NOT NULL
        AND CAST(transaction_date AS DATE) >= @LastProcessedDate
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
        da.SourceAccountCreated,
        da.amount_sum,
        da.amount_count,
        da.refund_sum,
        da.refund_count,
        da.void_sum,
        da.voidcount,
        (da.amount_sum - da.refund_sum) AS total,
        (da.amount_count + da.refund_count) AS totalcount,
        mfd.first_transaction_date,
        mfd.first_business_transaction_date
    FROM DailyAggregates da
    LEFT JOIN MerchantFirstDates mfd ON da.mid = mfd.mid
),
AllHistoricalData AS (
    -- Step 4: Combine new data with existing historical data for running totals
    SELECT day, mid, merchant_name, SourceAccountCreated, amount_sum, amount_count, refund_sum, refund_count, void_sum, voidcount, total, totalcount, first_transaction_date, first_business_transaction_date
    FROM DailyTotals
    
    UNION ALL
    
    SELECT day, mid, merchant_name, SourceAccountCreated, amount_sum, amount_count, refund_sum, refund_count, void_sum, voidcount, total, totalcount, first_transaction_date, first_business_transaction_date
    FROM dbo.CLOVERDAILY
    WHERE day < @LastProcessedDate
),
RunningTotals AS (
    -- Step 5: Calculate MTD and YTD running totals including historical data
    SELECT 
        day,
        mid,
        merchant_name,
        SourceAccountCreated,
        amount_sum,
        amount_count,
        refund_sum,
        refund_count,
        void_sum,
        voidcount,
        total,
        totalcount,
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
    FROM AllHistoricalData
)
-- Step 6: Insert only the new/updated data
INSERT INTO dbo.CLOVERDAILY (
    day, 
    mid, 
    merchant_name, 
    SourceAccountCreated,
    amount_sum,
    amount_count,
    refund_sum,
    refund_count,
    void_sum,
    voidcount,
    total,
    totalcount,
    first_transaction_date,
    first_business_transaction_date,
    mtd_total, 
    ytd_total
)
SELECT 
    day,
    mid,
    merchant_name,
    SourceAccountCreated,
    ROUND(amount_sum, 2) AS amount_sum,
    amount_count,
    ROUND(refund_sum, 2) AS refund_sum,
    refund_count,
    ROUND(void_sum, 2) AS void_sum,
    voidcount,
    ROUND(total, 2) AS total,
    totalcount,
    first_transaction_date,
    first_business_transaction_date,
    ROUND(mtd_total, 2) AS mtd_total,
    ROUND(ytd_total, 2) AS ytd_total
FROM RunningTotals
WHERE day >= @LastProcessedDate
ORDER BY day, mid;

-- Display summary of what was processed
SELECT 
    MIN(day) AS earliest_date_processed,
    MAX(day) AS latest_date_processed,
    COUNT(*) AS new_records_added,
    SUM(amount_count) AS new_amount_transactions,
    SUM(refund_count) AS new_refund_transactions
FROM dbo.CLOVERDAILY
WHERE day >= @LastProcessedDate;

PRINT 'Incremental CLOVERDAILY update completed successfully';