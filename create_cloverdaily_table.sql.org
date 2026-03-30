-- Create CLOVERDAILY table
USE Landing_Operational;


-- Drop table if it exists
IF OBJECT_ID('Landing_Operational.dbo.CLOVERDAILY', 'U') IS NOT NULL
    DROP TABLE Landing_Operational.dbo.CLOVERDAILY;


-- Create the CLOVERDAILY table
CREATE TABLE Landing_Operational.dbo.CLOVERDAILY (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    day DATE NOT NULL,
    mid VARCHAR(50) NOT NULL,
    merchant_name VARCHAR(255) NOT NULL,
    amount_sum DECIMAL(18,2) NOT NULL DEFAULT 0,
    amount_count INT NOT NULL DEFAULT 0,
    refund_sum DECIMAL(18,2) NOT NULL DEFAULT 0,
    refund_count INT NOT NULL DEFAULT 0,
    total DECIMAL(18,2) NOT NULL DEFAULT 0,
    totalcount INT NOT NULL DEFAULT 0,
    void_sum DECIMAL(18,2) NOT NULL DEFAULT 0,
    voidcount INT NOT NULL DEFAULT 0,
    first_transaction_date DATETIME2 NULL,
    first_business_transaction_date DATETIME2 NULL,
    mtd_total DECIMAL(18,2) NOT NULL DEFAULT 0,
    ytd_total DECIMAL(18,2) NOT NULL DEFAULT 0,
    sourceAccountCreated DATETIME2 NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    updated_at DATETIME2 NOT NULL DEFAULT GETDATE()
);


-- Create indexes for better performance
CREATE INDEX IX_CLOVERDAILY_day_mid ON Landing_Operational.dbo.CLOVERDAILY (day, mid);
CREATE INDEX IX_CLOVERDAILY_day ON Landing_Operational.dbo.CLOVERDAILY (day);
CREATE INDEX IX_CLOVERDAILY_mid ON Landing_Operational.dbo.CLOVERDAILY (mid);

PRINT 'CLOVERDAILY table created successfully';